from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

import structlog

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

    async def write(self, request: WriteRequest) -> bool:
        async with self._sem:
            try:
                opc_val, _ = self._engine.transform_to_opcua(request.node_id, request.value)
                
                # Write logic would go here
                # conn.write(...)
                
                if self._audit:
                    await self._audit.log_write(request.node_id, request.user_id, None, request.value)
                
                self._metrics.record_sync_event("aas_to_opcua", True)
                return True
            except Exception as e:
                logger.error("write_failed", error=str(e))
                self._metrics.record_sync_event("aas_to_opcua", False)
                return False
