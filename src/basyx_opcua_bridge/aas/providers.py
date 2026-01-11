from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional, Protocol

import structlog
from basyx.aas import model as aas_model

from basyx_opcua_bridge.config.models import AasProviderConfig
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


def build_aas_provider(config: AasProviderConfig, mapping_engine: MappingEngine) -> AasProvider:
    if config.type == "memory":
        return MemoryAasProvider(config, mapping_engine)

    raise NotImplementedError(f"AAS provider '{config.type}' is not implemented")
