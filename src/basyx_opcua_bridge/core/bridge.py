from __future__ import annotations

import asyncio
from typing import Optional

import structlog
from basyx_opcua_bridge.config.models import BridgeConfig
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.mapping.engine import MappingEngine
from basyx_opcua_bridge.sync.manager import SyncManager
from basyx_opcua_bridge.aas.providers import AasProvider, build_aas_provider
from basyx_opcua_bridge.security.audit import AuditLogger
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
        self._audit_logger: Optional[AuditLogger] = None
        self._aas_provider: Optional[AasProvider] = None
        self._metrics = MetricsCollector(port=config.observability.metrics_port)
        self._shutdown_event = asyncio.Event()
        self._is_running = False
        self._run_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._run_task and not self._run_task.done():
            return
        self._run_task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._shutdown_event.set()
        if self._run_task:
            await self._run_task

    async def wait_until_stopped(self) -> None:
        if self._run_task:
            await self._run_task
        else:
            await self._shutdown_event.wait()

    async def run(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._shutdown_event.clear()
        logger.info("bridge_starting")

        if self._config.observability.metrics_enabled:
            self._metrics.start_server()

        if self._config.security.audit_log_enabled:
            self._audit_logger = AuditLogger()

        self._cert_manager = CertificateManager(self._config.security)
        await self._cert_manager.load_certificates()

        self._connection_pool = OpcUaConnectionPool(
            endpoints=self._config.opcua.endpoints,
            cert_manager=self._cert_manager,
            pool_size=self._config.opcua.connection_pool_size
        )
        await self._connection_pool.connect()

        self._mapping_engine = MappingEngine(self._config.mappings, self._config.semantic)

        self._aas_provider = build_aas_provider(self._config.aas, self._mapping_engine)
        await self._aas_provider.start()
        await self._aas_provider.register_mappings(self._mapping_engine.resolved_mappings())

        self._sync_manager = SyncManager(
            connection_pool=self._connection_pool,
            mapping_engine=self._mapping_engine,
            aas_provider=self._aas_provider,
            metrics=self._metrics,
            subscription_interval_ms=self._config.opcua.subscription_interval_ms,
            monitor_queue_maxsize=self._config.opcua.monitor_queue_maxsize,
            audit=self._audit_logger,
        )

        logger.info("bridge_started")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._connection_pool.maintain_connections(self._shutdown_event))
                tg.create_task(self._sync_manager.run(self._shutdown_event))
                await self._shutdown_event.wait()
        except* Exception as e:
            logger.critical("bridge_crashed", error=str(e))
            raise
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        logger.info("bridge_stopping")
        if self._sync_manager:
            await self._sync_manager.stop()

        if self._connection_pool:
            await self._connection_pool.disconnect()

        if self._aas_provider:
            await self._aas_provider.stop()

        self._is_running = False
        self._shutdown_event.clear()
        logger.info("bridge_stopped")
