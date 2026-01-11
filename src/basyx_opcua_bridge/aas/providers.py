from __future__ import annotations

import asyncio
import base64
import json
import ssl
import hashlib
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Tuple, cast

import structlog
from basyx.aas import model as aas_model
from basyx.aas.adapter.json import json_serialization

from basyx_opcua_bridge.aas.events import EventHints, RecentWriteCache, parse_basyx_topic
from basyx_opcua_bridge.config.models import AasProviderConfig, AasEventsConfig, SyncDirection
from basyx_opcua_bridge.mapping.engine import MappingEngine, ResolvedMapping, XSD_TO_AAS_DATATYPE
from basyx_opcua_bridge.sync.control import WriteRequest

logger = structlog.get_logger(__name__)

_ID_SHORT_MAX_LEN = 64


def _derive_id_short(identifier: str, used: set[str]) -> str:
    candidate = re.split(r"[#/:]", identifier)[-1] or identifier
    candidate = re.sub(r"[^A-Za-z0-9_]", "_", candidate)
    if not candidate:
        candidate = "submodel"
    if not candidate[0].isalpha():
        candidate = f"sm_{candidate}"
    candidate = candidate[:_ID_SHORT_MAX_LEN]
    if candidate in used:
        suffix = hashlib.sha1(identifier.encode("utf-8")).hexdigest()[:6]
        candidate = f"{candidate[:_ID_SHORT_MAX_LEN - 7]}_{suffix}"
    used.add(candidate)
    return candidate


class AasProvider(Protocol):
    @property
    def enable_events(self) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def register_mappings(self, mappings: List[ResolvedMapping]) -> None: ...

    async def update_property(self, mapping: ResolvedMapping, value: Any) -> None: ...

    def write_requests(self, shutdown_event: asyncio.Event) -> AsyncIterator[WriteRequest]: ...

    async def provision(self, mappings: List[ResolvedMapping]) -> None: ...


@dataclass(frozen=True)
class MappingKey:
    submodel_id: str
    aas_id_short: str


class MemoryAasProvider:
    def __init__(self, config: AasProviderConfig, mapping_engine: MappingEngine) -> None:
        self._config = config
        self._engine = mapping_engine
        self._submodels: Dict[str, aas_model.Submodel] = {}
        self._mappings_by_key: Dict[MappingKey, ResolvedMapping] = {}
        self._mappings_by_id_short: Dict[str, List[ResolvedMapping]] = {}
        self._write_queue: asyncio.Queue[WriteRequest] = asyncio.Queue(maxsize=1000)
        self._started = False

    @property
    def enable_events(self) -> bool:
        return self._config.enable_events

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def register_mappings(self, mappings: List[ResolvedMapping]) -> None:
        submodel_ids = {m.rule.submodel_id for m in mappings}
        used_id_shorts = {sm.id_short for sm in self._submodels.values() if getattr(sm, "id_short", None)}
        for submodel_id in submodel_ids:
            if submodel_id in self._submodels:
                continue
            id_short = _derive_id_short(submodel_id, used_id_shorts)
            submodel = aas_model.Submodel(id_=submodel_id, id_short=id_short)
            self._submodels[submodel_id] = submodel
            self._engine.register_submodel(submodel, "ns=0")

        self._mappings_by_key.clear()
        self._mappings_by_id_short.clear()
        for mapping in mappings:
            key = MappingKey(mapping.rule.submodel_id, mapping.rule.aas_id_short)
            self._mappings_by_key[key] = mapping
            self._mappings_by_id_short.setdefault(mapping.rule.aas_id_short, []).append(mapping)

    async def update_property(self, mapping: ResolvedMapping, value: Any) -> None:
        if not mapping.element:
            logger.warning("mapping_element_missing", node_id=mapping.rule.opcua_node_id)
            return
        if hasattr(mapping.element, "value"):
            mapping.element.value = value
            return
        logger.warning("mapping_element_not_writable", node_id=mapping.rule.opcua_node_id)

    async def write_requests(self, shutdown_event: asyncio.Event) -> AsyncIterator[WriteRequest]:
        if not self._config.enable_events:
            return
        while not shutdown_event.is_set():
            try:
                request = await asyncio.wait_for(self._write_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            yield request

    async def provision(self, mappings: List[ResolvedMapping]) -> None:
        await self.register_mappings(mappings)

    def get_property_value(self, aas_id_short: str, submodel_id: Optional[str] = None) -> Any:
        mapping = self._resolve_mapping(aas_id_short, submodel_id)
        if mapping and mapping.element and hasattr(mapping.element, "value"):
            return mapping.element.value
        return None

    async def submit_write(
        self,
        aas_id_short: str,
        value: Any,
        submodel_id: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> bool:
        mapping = self._resolve_mapping(aas_id_short, submodel_id)
        if not mapping:
            logger.warning("write_mapping_not_found", aas_id_short=aas_id_short, submodel_id=submodel_id)
            return False
        request = WriteRequest(node_id=mapping.rule.opcua_node_id, value=value, user_id=user_id)
        try:
            self._write_queue.put_nowait(request)
            return True
        except asyncio.QueueFull:
            logger.warning("write_queue_full", node_id=request.node_id)
            return False

    def _resolve_mapping(self, aas_id_short: str, submodel_id: Optional[str]) -> Optional[ResolvedMapping]:
        if submodel_id:
            return self._mappings_by_key.get(MappingKey(submodel_id, aas_id_short))
        matches = self._mappings_by_id_short.get(aas_id_short, [])
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.warning("ambiguous_aas_id_short", aas_id_short=aas_id_short)
        return None


class HttpAasProvider:
    def __init__(self, config: AasProviderConfig, mapping_engine: MappingEngine) -> None:
        if not config.url:
            raise ValueError("AAS provider URL is required for HTTP providers")
        self._config = config
        self._engine = mapping_engine
        self._base_url = self._normalize_base_url(str(config.url))
        self._poll_interval = config.poll_interval_seconds
        self._timeout = config.request_timeout_seconds
        self._encode_ids = config.encode_identifiers
        self._auto_create_submodels = config.auto_create_submodels
        self._auto_create_elements = config.auto_create_elements
        self._events = config.events
        self._recent_writes = (
            RecentWriteCache(
                config.events.dedup_ttl_seconds,
                config.events.dedup_max_entries,
            )
            if config.events.dedup_enabled
            else None
        )
        self._submodels: Dict[str, aas_model.Submodel] = {}
        self._mappings_by_key: Dict[MappingKey, ResolvedMapping] = {}
        self._mappings_by_id_short: Dict[str, List[ResolvedMapping]] = {}
        self._control_mappings: List[ResolvedMapping] = []
        self._last_values: Dict[str, Any] = {}
        self._started = False

    def _normalize_base_url(self, url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        path = parsed.path.rstrip("/")
        if path.endswith("/submodels"):
            base_path = path[: -len("/submodels")]
            parsed = parsed._replace(path=base_path or "/")
            return parsed.geturl().rstrip("/")
        if path.endswith("/aas"):
            return parsed.geturl().rstrip("/")
        parsed = parsed._replace(path=f"{path}/aas" if path else "/aas")
        return parsed.geturl().rstrip("/")

    @property
    def enable_events(self) -> bool:
        return self._config.enable_events

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def register_mappings(self, mappings: List[ResolvedMapping]) -> None:
        submodel_ids = {m.rule.submodel_id for m in mappings}
        used_id_shorts = {sm.id_short for sm in self._submodels.values() if getattr(sm, "id_short", None)}
        for submodel_id in submodel_ids:
            if submodel_id in self._submodels:
                continue
            id_short = _derive_id_short(submodel_id, used_id_shorts)
            submodel = aas_model.Submodel(id_=submodel_id, id_short=id_short)
            self._submodels[submodel_id] = submodel
            self._engine.register_submodel(submodel, "ns=0")

        self._mappings_by_key.clear()
        self._mappings_by_id_short.clear()
        for mapping in mappings:
            key = MappingKey(mapping.rule.submodel_id, mapping.rule.aas_id_short)
            self._mappings_by_key[key] = mapping
            self._mappings_by_id_short.setdefault(mapping.rule.aas_id_short, []).append(mapping)

        self._control_mappings = [
            mapping for mapping in mappings
            if mapping.rule.direction in (SyncDirection.AAS_TO_OPCUA, SyncDirection.BIDIRECTIONAL)
        ]

        if self._auto_create_submodels:
            await self._ensure_submodels()

    async def update_property(self, mapping: ResolvedMapping, value: Any) -> None:
        if not mapping:
            return
        ok = await self._write_value(mapping, value)
        if ok:
            self._remember_write(mapping, value)
            return
        if not ok and self._auto_create_elements:
            await self._ensure_element(mapping)
            ok = await self._write_value(mapping, value)
            if ok:
                self._remember_write(mapping, value)

    async def write_requests(self, shutdown_event: asyncio.Event) -> AsyncIterator[WriteRequest]:
        if not self._config.enable_events:
            return
        if self._events.enabled and self._events.mqtt_url and self._events.mqtt_topic:
            async for request in self._mqtt_write_requests(shutdown_event):
                yield request
            return

        while not shutdown_event.is_set():
            for mapping in list(self._control_mappings):
                value = await self._read_value(mapping)
                if value is None:
                    continue
                if self._is_recent_write(mapping, value):
                    continue
                cache_key = mapping.rule.opcua_node_id
                last_value = self._last_values.get(cache_key)
                if last_value != value:
                    self._last_values[cache_key] = value
                    yield WriteRequest(node_id=mapping.rule.opcua_node_id, value=value)
            await asyncio.sleep(self._poll_interval)

    async def provision(self, mappings: List[ResolvedMapping]) -> None:
        await self.register_mappings(mappings)
        if self._auto_create_elements:
            for mapping in mappings:
                await self._ensure_element(mapping)

    def _resolve_mapping(self, aas_id_short: str, submodel_id: Optional[str]) -> Optional[ResolvedMapping]:
        if submodel_id:
            return self._mappings_by_key.get(MappingKey(submodel_id, aas_id_short))
        matches = self._mappings_by_id_short.get(aas_id_short, [])
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.warning("ambiguous_aas_id_short", aas_id_short=aas_id_short)
        return None

    async def _ensure_submodels(self) -> None:
        for submodel_id, submodel in self._submodels.items():
            status, _ = await self._request_json("GET", self._submodel_url(submodel_id))
            if status == 200:
                continue
            if status == 404:
                minimal = aas_model.Submodel(id_=submodel.id, id_short=submodel.id_short)
                payload = json.loads(json.dumps(minimal, cls=json_serialization.AASToJsonEncoder))
                created, _ = await self._request_json("POST", self._submodels_url(), payload)
                if created in (200, 201, 204, 409):
                    continue
            logger.warning("submodel_create_failed", submodel_id=submodel_id, status=status)

    async def _ensure_element(self, mapping: ResolvedMapping) -> None:
        if not mapping.element:
            return
        payload = json.loads(json.dumps(mapping.element, cls=json_serialization.AASToJsonEncoder))
        status, _ = await self._request_json(
            "POST",
            f"{self._submodel_url(mapping.rule.submodel_id)}/submodel-elements",
            payload,
        )
        if status not in (200, 201, 204, 409):
            logger.warning("submodel_element_create_failed", node_id=mapping.rule.opcua_node_id, status=status)

    async def _read_value(self, mapping: ResolvedMapping) -> Any:
        for suffix in ("$value", "value"):
            status, body = await self._request_json(
                "GET",
                f"{self._element_url(mapping.rule.submodel_id, mapping.rule.aas_id_short)}/{suffix}",
            )
            if status == 200 and body is not None:
                value = self._extract_value(body)
                return self._coerce_value(value, mapping.rule.value_type)
        return None

    async def _write_value(self, mapping: ResolvedMapping, value: Any) -> bool:
        payloads = [
            value,
            {"value": value, "valueType": mapping.rule.value_type},
        ]
        for suffix in ("$value", "value"):
            url = f"{self._element_url(mapping.rule.submodel_id, mapping.rule.aas_id_short)}/{suffix}"
            for method in ("PATCH", "PUT"):
                for payload in payloads:
                    status, _ = await self._request_json(method, url, payload)
                    if status in (200, 204):
                        return True
        payload = self._build_element_payload(mapping, value)
        status, _ = await self._request_json(
            "PUT",
            self._element_url(mapping.rule.submodel_id, mapping.rule.aas_id_short),
            payload,
        )
        return status in (200, 201, 204)
        return False

    def _submodels_url(self) -> str:
        return f"{self._base_url}/submodels"

    def _submodel_url(self, submodel_id: str) -> str:
        return f"{self._base_url}/submodels/{self._encode_identifier(submodel_id)}"

    def _element_url(self, submodel_id: str, id_short: str) -> str:
        encoded_id_short = urllib.parse.quote(id_short, safe="")
        return f"{self._submodel_url(submodel_id)}/submodel-elements/{encoded_id_short}"

    def _encode_identifier(self, identifier: str) -> str:
        if not self._encode_ids:
            return urllib.parse.quote(identifier, safe="")
        encoded = base64.urlsafe_b64encode(identifier.encode("utf-8")).decode("ascii")
        return encoded.rstrip("=")

    def _extract_value(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "value" in payload:
            return payload.get("value")
        return payload

    def _coerce_value(self, value: Any, xsd_type: str) -> Any:
        if value is None:
            return None
        if isinstance(value, (int, float, bool)):
            return value
        if xsd_type == "xs:boolean":
            if isinstance(value, str):
                return value.strip().lower() in {"true", "1", "yes"}
            return bool(value)
        if xsd_type in {
            "xs:byte",
            "xs:unsignedByte",
            "xs:short",
            "xs:unsignedShort",
            "xs:int",
            "xs:unsignedInt",
            "xs:long",
            "xs:unsignedLong",
        }:
            try:
                return int(value)
            except (TypeError, ValueError):
                return value
        if xsd_type in {"xs:float", "xs:double"}:
            try:
                return float(value)
            except (TypeError, ValueError):
                return value
        return value

    def _build_element_payload(self, mapping: ResolvedMapping, value: Any) -> dict[str, Any]:
        element = mapping.element
        if element is None:
            value_type = XSD_TO_AAS_DATATYPE.get(mapping.rule.value_type, aas_model.datatypes.String)
            element = aas_model.Property(
                id_short=mapping.rule.aas_id_short,
                value_type=value_type,
                value=value,
            )
        else:
            if hasattr(element, "value"):
                element.value = value
        payload = json.loads(json.dumps(element, cls=json_serialization.AASToJsonEncoder))
        return cast(dict[str, Any], payload)

    async def _request_json(
        self, method: str, url: str, payload: Any | None = None
    ) -> Tuple[int, Any | None]:
        body = None
        headers = {
            "Accept": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return await asyncio.to_thread(self._sync_request, method, url, body, headers)

    def _sync_request(
        self, method: str, url: str, body: bytes | None, headers: Dict[str, str]
    ) -> Tuple[int, Any | None]:
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read()
                if not raw:
                    return response.status, None
                try:
                    return response.status, json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    return response.status, raw.decode("utf-8")
        except urllib.error.HTTPError as e:
            try:
                raw = e.read()
                if raw:
                    try:
                        return e.code, json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        return e.code, raw.decode("utf-8")
            except Exception:
                pass
            return e.code, None
        except Exception as e:
            logger.warning("aas_request_failed", url=url, error=str(e))
            return 0, None

    async def _mqtt_write_requests(self, shutdown_event: asyncio.Event) -> AsyncIterator[WriteRequest]:
        events = self._events
        if not events.mqtt_url or not events.mqtt_topic:
            return
        try:
            from asyncio_mqtt import Client as MqttClient
        except Exception as exc:
            logger.warning("mqtt_dependency_missing", error=str(exc))
            return

        parsed = urllib.parse.urlparse(str(events.mqtt_url))
        host = parsed.hostname or ""
        port = parsed.port or (8883 if parsed.scheme == "mqtts" else 1883)
        tls_context = ssl.create_default_context() if parsed.scheme == "mqtts" else None

        async with MqttClient(
            host,
            port=port,
            username=events.mqtt_username,
            password=events.mqtt_password,
            tls_context=tls_context,
        ) as client:
            async with client.filtered_messages(events.mqtt_topic) as messages:
                await client.subscribe(events.mqtt_topic, qos=events.mqtt_qos)
                async for message in messages:
                    if shutdown_event.is_set():
                        return
                    requests = self._parse_event_message(message.payload, events, str(message.topic))
                    if requests:
                        for request in requests:
                            yield request

    def _parse_event_message(
        self,
        payload: bytes,
        events: AasEventsConfig,
        topic: Optional[str] = None,
    ) -> Optional[List[WriteRequest]]:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except Exception:
            logger.warning("mqtt_payload_invalid", payload=payload[:256])
            return None

        topic_hints = parse_basyx_topic(topic) if topic else EventHints()

        if isinstance(decoded, dict):
            patches = decoded.get("patches") or decoded.get("operations") or decoded.get("patch")
            if isinstance(patches, list):
                requests: List[WriteRequest] = []
                for patch in patches:
                    if not isinstance(patch, dict):
                        continue
                    request = self._build_request(patch, events, topic_hints)
                    if request:
                        requests.append(request)
                return requests or None

            request = self._build_request(decoded, events, topic_hints)
            if request:
                return [request]
            return None

        if topic_hints.id_short:
            mapping = self._resolve_mapping(topic_hints.id_short, topic_hints.submodel_id)
            if not mapping:
                logger.warning(
                    "mqtt_payload_mapping_missing",
                    id_short=topic_hints.id_short,
                    submodel_id=topic_hints.submodel_id,
                )
                return None
            if self._is_recent_write(mapping, decoded):
                logger.debug("mqtt_dedup_skipped", id_short=topic_hints.id_short)
                return None
            return [WriteRequest(node_id=mapping.rule.opcua_node_id, value=decoded)]

        return None

    def _build_request(
        self,
        decoded: dict[str, Any],
        events: AasEventsConfig,
        topic_hints: EventHints,
    ) -> Optional[WriteRequest]:
        id_short, submodel_id, value = self._extract_event_fields(decoded, events, topic_hints)

        if not id_short:
            logger.warning("mqtt_payload_missing_id_short")
            return None

        mapping = self._resolve_mapping(str(id_short), str(submodel_id) if submodel_id else None)
        if not mapping and submodel_id:
            mapping = self._resolve_mapping(str(id_short), None)
        if not mapping:
            logger.warning("mqtt_payload_mapping_missing", id_short=id_short, submodel_id=submodel_id)
            return None
        if self._is_recent_write(mapping, value):
            logger.debug("mqtt_dedup_skipped", id_short=id_short, submodel_id=submodel_id)
            return None

        return WriteRequest(node_id=mapping.rule.opcua_node_id, value=value)

    def _extract_event_fields(
        self,
        decoded: dict[str, Any],
        events: AasEventsConfig,
        topic_hints: EventHints,
    ) -> tuple[Optional[str], Optional[str], Any]:
        candidates = [
            decoded,
            decoded.get("data"),
            decoded.get("payload"),
            decoded.get("event"),
            decoded.get("submodelElement"),
            decoded.get("submodel_element"),
        ]

        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            id_short = candidate.get(events.payload_id_short_key) or candidate.get("idShort") or candidate.get("id_short")
            if not id_short and "idShortPath" in candidate:
                id_short_path = str(candidate.get("idShortPath"))
                id_short = id_short_path.split("/")[-1].split(".")[-1]
            if not id_short and "path" in candidate:
                id_short = self._id_short_from_path(str(candidate.get("path")))
            submodel_id = candidate.get(events.payload_submodel_id_key) or candidate.get("submodelId") or candidate.get("submodelIdentifier")
            value = candidate.get(events.payload_value_key, candidate.get("value"))
            if not id_short and topic_hints.id_short:
                id_short = topic_hints.id_short
            if not submodel_id and topic_hints.submodel_id:
                submodel_id = topic_hints.submodel_id
            if id_short:
                return str(id_short), str(submodel_id) if submodel_id else None, value

        return None, None, None

    def _id_short_from_path(self, path: str) -> Optional[str]:
        if not path:
            return None
        normalized = path.strip("/")
        if "submodelElements" in normalized:
            tail = normalized.split("submodelElements", 1)[1].strip("/")
            for suffix in ("/value", "/$value"):
                if tail.endswith(suffix):
                    tail = tail[: -len(suffix)]
            if tail:
                return tail.split("/")[-1]
        return None

    def _dedup_key(self, mapping: ResolvedMapping) -> str:
        return f"{mapping.rule.submodel_id}:{mapping.rule.aas_id_short}"

    def _remember_write(self, mapping: ResolvedMapping, value: Any) -> None:
        if self._recent_writes:
            self._recent_writes.remember(self._dedup_key(mapping), value)

    def _is_recent_write(self, mapping: ResolvedMapping, value: Any) -> bool:
        if not self._recent_writes:
            return False
        return self._recent_writes.matches(self._dedup_key(mapping), value)


def build_aas_provider(config: AasProviderConfig, mapping_engine: MappingEngine) -> AasProvider:
    if config.type == "memory":
        return MemoryAasProvider(config, mapping_engine)

    if config.type in {"basyx", "aasx-server"}:
        return HttpAasProvider(config, mapping_engine)

    raise NotImplementedError(f"AAS provider '{config.type}' is not implemented")
