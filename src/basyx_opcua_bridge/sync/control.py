from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

import structlog
from asyncua import ua

if TYPE_CHECKING:
    from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
    from basyx_opcua_bridge.mapping.engine import MappingEngine
    from basyx_opcua_bridge.observability.metrics import MetricsCollector
    from basyx_opcua_bridge.security.audit import AuditLogger

logger = structlog.get_logger(__name__)

@dataclass
class WriteRequest:
    node_id: str
    value: Any
    user_id: str | None = None

class ControlManager:
    def __init__(self, connection_pool: OpcUaConnectionPool, mapping_engine: MappingEngine, metrics: MetricsCollector, audit: AuditLogger | None = None):
        self._pool = connection_pool
        self._engine = mapping_engine
        self._metrics = metrics
        self._audit = audit
        self._sem = asyncio.Semaphore(10)
        self._queue: asyncio.Queue[WriteRequest] = asyncio.Queue(maxsize=1000)
        self._running = False

    async def write(self, request: WriteRequest) -> bool:
        async with self._sem:
            try:
                opc_val, variant_type = self._engine.transform_to_opcua(request.node_id, request.value)
                mapping = self._engine.get_mapping_for_node(request.node_id)
                if not mapping:
                    raise ValueError(f"No mapping for node {request.node_id}")

                endpoint_url = self._pool.resolve_endpoint_url(mapping.rule.endpoint)
                previous_value = None
                async with self._pool.get_connection(endpoint_url) as conn:
                    node = conn.client.get_node(request.node_id)
                    if self._audit:
                        try:
                            previous_value = await node.read_value()
                        except Exception:
                            previous_value = None

                    variant = ua.Variant(opc_val, ua.VariantType(variant_type))
                    await node.write_value(variant)

                if self._audit:
                    await self._audit.log_write(request.node_id, request.user_id, previous_value, request.value)
                
                self._metrics.record_sync_event("aas_to_opcua", True)
                return True
            except Exception as e:
                logger.error("write_failed", error=str(e))
                self._metrics.record_sync_event("aas_to_opcua", False)
                return False

    async def enqueue_write(self, request: WriteRequest) -> bool:
        try:
            self._queue.put_nowait(request)
            return True
        except asyncio.QueueFull:
            logger.warning("control_queue_full_drop_newest", node_id=request.node_id)
            return False

    async def run(self, shutdown_event: asyncio.Event) -> None:
        if self._running:
            return
        self._running = True
        try:
            while not shutdown_event.is_set():
                try:
                    request = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                await self.write(request)
        except asyncio.CancelledError:
            return
        finally:
            self._running = False
