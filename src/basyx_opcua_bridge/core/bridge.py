from __future__ import annotations

import asyncio
from typing import Optional

import structlog
from basyx_opcua_bridge.config.models import BridgeConfig
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.mapping.engine import MappingEngine
from basyx_opcua_bridge.sync.manager import SyncManager
from basyx_opcua_bridge.security.x509 import CertificateManager
from basyx_opcua_bridge.observability.metrics import MetricsCollector

logger = structlog.get_logger(__name__)

class Bridge:
    def __init__(self, config: BridgeConfig) -> None:
        self._config = config
        self._connection_pool: Optional[OpcUaConnectionPool] = None
        self._mapping_engine: Optional[MappingEngine] = None
        self._sync_manager: Optional[SyncManager] = None
        self._cert_manager: Optional[CertificateManager] = None
        self._metrics = MetricsCollector(port=config.observability.metrics_port)
        self._shutdown_event = asyncio.Event()
        self._is_running = False

    async def start(self) -> None:
        if self._is_running:
            return
        logger.info("bridge_starting")

        if self._config.observability.metrics_enabled:
            self._metrics.start_server()

        self._cert_manager = CertificateManager(self._config.security)
        await self._cert_manager.load_certificates()

        self._connection_pool = OpcUaConnectionPool(
            endpoints=self._config.opcua.endpoints,
            cert_manager=self._cert_manager,
            pool_size=self._config.opcua.connection_pool_size
        )
        await self._connection_pool.connect()

        self._mapping_engine = MappingEngine(self._config.mappings, self._config.semantic)

        self._sync_manager = SyncManager(
            connection_pool=self._connection_pool,
            mapping_engine=self._mapping_engine,
            aas_provider=self._config.aas,
            metrics=self._metrics
        )
        await self._sync_manager.start()

        self._is_running = True
        logger.info("bridge_started")

    async def stop(self) -> None:
        if not self._is_running:
            return
        logger.info("bridge_stopping")
        self._shutdown_event.set()

        if self._sync_manager:
            await self._sync_manager.stop()
        
        if self._connection_pool:
            await self._connection_pool.disconnect()

        self._is_running = False
        logger.info("bridge_stopped")

    async def wait_until_stopped(self) -> None:
        await self._shutdown_event.wait()
