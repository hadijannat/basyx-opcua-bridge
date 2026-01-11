from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
    from basyx_opcua_bridge.mapping.engine import MappingEngine
    from basyx_opcua_bridge.config.models import AasProviderConfig
    from basyx_opcua_bridge.observability.metrics import MetricsCollector
    from basyx_opcua_bridge.security.audit import AuditLogger
    from basyx_opcua_bridge.sync.monitor import MonitoringManager
    from basyx_opcua_bridge.sync.control import ControlManager

from .monitor import MonitoringManager
from .control import ControlManager
from basyx_opcua_bridge.config.models import SyncDirection

logger = structlog.get_logger(__name__)

class SyncManager:
    def __init__(
        self,
        connection_pool: OpcUaConnectionPool,
        mapping_engine: MappingEngine,
        aas_provider: AasProviderConfig,
        metrics: MetricsCollector,
        subscription_interval_ms: int = 500,
        monitor_queue_maxsize: int = 10000,
        audit: AuditLogger | None = None,
    ) -> None:
        self._engine = mapping_engine
        self.monitor = MonitoringManager(
            connection_pool,
            mapping_engine,
            metrics,
            subscription_interval_ms=subscription_interval_ms,
            queue_maxsize=monitor_queue_maxsize,
        )
        self.control = ControlManager(connection_pool, mapping_engine, metrics, audit=audit)

    async def start(self) -> None:
        # Pass resolved mappings
        mappings = self._engine.resolved_mappings()
        monitor_mappings = [
            m for m in mappings
            if m.rule.direction in (SyncDirection.OPCUA_TO_AAS, SyncDirection.BIDIRECTIONAL)
        ]
        await self.monitor.start(monitor_mappings)

    async def stop(self) -> None:
        await self.monitor.stop()

    async def run(self, shutdown_event: asyncio.Event) -> None:
        mappings = self._engine.resolved_mappings()
        monitor_mappings = [
            m for m in mappings
            if m.rule.direction in (SyncDirection.OPCUA_TO_AAS, SyncDirection.BIDIRECTIONAL)
        ]

        async with asyncio.TaskGroup() as tg:
            if monitor_mappings:
                tg.create_task(self.monitor.run(monitor_mappings, shutdown_event))
            else:
                logger.warning("no_monitor_mappings_configured")
            tg.create_task(self.control.run(shutdown_event))
