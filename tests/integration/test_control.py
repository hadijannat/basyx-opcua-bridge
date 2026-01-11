import asyncio
import pytest

from basyx_opcua_bridge.aas.providers import MemoryAasProvider
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.mapping.engine import MappingEngine
from basyx_opcua_bridge.observability.metrics import MetricsCollector
from prometheus_client.registry import CollectorRegistry
from basyx_opcua_bridge.security.x509 import CertificateManager
from basyx_opcua_bridge.sync.manager import SyncManager


@pytest.mark.asyncio
async def test_aas_to_opcua_write(opcua_simulator, sample_config):
    cert_manager = CertificateManager(sample_config.security)
    pool = OpcUaConnectionPool(sample_config.opcua.endpoints, cert_manager)
    await pool.connect()

    engine = MappingEngine(sample_config.mappings)
    provider = MemoryAasProvider(sample_config.aas, engine)
    await provider.start()
    await provider.register_mappings(engine.resolved_mappings())

    metrics = MetricsCollector(9092, registry=CollectorRegistry())
    sync = SyncManager(pool, engine, provider, metrics, subscription_interval_ms=100)

    shutdown_event = asyncio.Event()
    task = asyncio.create_task(sync.run(shutdown_event))

    try:
        ok = await provider.submit_write("Speed", 55.0, submodel_id="urn:test", user_id="tester")
        assert ok is True

        node = opcua_simulator.get_node("ns=2;s=Speed")
        value = None
        for _ in range(20):
            await asyncio.sleep(0.1)
            value = await node.read_value()
            if value == 55.0:
                break

        assert value == 55.0
    finally:
        shutdown_event.set()
        await task
        await pool.disconnect()
        await provider.stop()
