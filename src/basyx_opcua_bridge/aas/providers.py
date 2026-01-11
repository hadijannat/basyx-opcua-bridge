from __future__ import annotations

import asyncio
import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol, Tuple

import structlog
from basyx.aas import model as aas_model
from basyx.aas.adapter.json import json_serialization

from basyx_opcua_bridge.config.models import AasProviderConfig, SyncDirection
from basyx_opcua_bridge.mapping.engine import MappingEngine, ResolvedMapping
from basyx_opcua_bridge.sync.control import WriteRequest

logger = structlog.get_logger(__name__)


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
        for submodel_id in submodel_ids:
            if submodel_id in self._submodels:
                continue
            submodel = aas_model.Submodel(id_=submodel_id)
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
        self._base_url = str(config.url).rstrip("/")
        self._poll_interval = config.poll_interval_seconds
        self._timeout = config.request_timeout_seconds
        self._encode_ids = config.encode_identifiers
        self._auto_create_submodels = config.auto_create_submodels
        self._auto_create_elements = config.auto_create_elements
        self._submodels: Dict[str, aas_model.Submodel] = {}
        self._mappings_by_key: Dict[MappingKey, ResolvedMapping] = {}
        self._mappings_by_id_short: Dict[str, List[ResolvedMapping]] = {}
        self._control_mappings: List[ResolvedMapping] = []
        self._last_values: Dict[str, Any] = {}
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
        for submodel_id in submodel_ids:
            if submodel_id in self._submodels:
                continue
            submodel = aas_model.Submodel(id_=submodel_id)
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
        if not ok and self._auto_create_elements:
            await self._ensure_element(mapping)
            await self._write_value(mapping, value)

    async def write_requests(self, shutdown_event: asyncio.Event) -> AsyncIterator[WriteRequest]:
        if not self._config.enable_events:
            return
        while not shutdown_event.is_set():
            for mapping in list(self._control_mappings):
                value = await self._read_value(mapping)
                if value is None:
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

    async def _ensure_submodels(self) -> None:
        for submodel_id, submodel in self._submodels.items():
            status, _ = await self._request_json("GET", self._submodel_url(submodel_id))
            if status == 200:
                continue
            if status == 404:
                payload = json.loads(json.dumps(submodel, cls=json_serialization.AASToJsonEncoder))
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


def build_aas_provider(config: AasProviderConfig, mapping_engine: MappingEngine) -> AasProvider:
    if config.type == "memory":
        return MemoryAasProvider(config, mapping_engine)

    if config.type in {"basyx", "aasx-server"}:
        return HttpAasProvider(config, mapping_engine)

    raise NotImplementedError(f"AAS provider '{config.type}' is not implemented")
