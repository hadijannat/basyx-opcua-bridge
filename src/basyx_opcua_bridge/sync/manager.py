from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
    from basyx_opcua_bridge.mapping.engine import MappingEngine
    from basyx_opcua_bridge.config.models import AasProviderConfig
    from basyx_opcua_bridge.observability.metrics import MetricsCollector
    from basyx_opcua_bridge.sync.monitor import MonitoringManager
    from basyx_opcua_bridge.sync.control import ControlManager

from .monitor import MonitoringManager
from .control import ControlManager

class SyncManager:
    def __init__(
        self,
        connection_pool: OpcUaConnectionPool,
        mapping_engine: MappingEngine,
        aas_provider: AasProviderConfig,
        metrics: MetricsCollector,
    ) -> None:
        self.monitor = MonitoringManager(connection_pool, mapping_engine, metrics)
        self.control = ControlManager(connection_pool, mapping_engine, metrics)

    async def start(self) -> None:
        # Pass resolved mappings
        mappings = list(self.monitor._engine._resolved.values())
        await self.monitor.start(mappings)

    async def stop(self) -> None:
        await self.monitor.stop()
