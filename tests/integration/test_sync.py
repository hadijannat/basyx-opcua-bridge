import pytest
import asyncio
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.mapping.engine import MappingEngine
from basyx_opcua_bridge.sync.monitor import MonitoringManager
from basyx_opcua_bridge.observability.metrics import MetricsCollector
from prometheus_client.registry import CollectorRegistry
from basyx_opcua_bridge.config.models import EndpointConfig
from basyx_opcua_bridge.security.x509 import CertificateManager
from basyx_opcua_bridge.aas.providers import MemoryAasProvider

@pytest.mark.asyncio
async def test_opcua_to_aas_sync(opcua_simulator, sample_config):
    # 1. Setup Connection Pool
    cert_manager = CertificateManager(sample_config.security)
    endpoint = EndpointConfig(
        url="opc.tcp://localhost:4840", 
        name="sim",
        security_policy="None",
        security_mode="None"
    )
    pool = OpcUaConnectionPool([endpoint], cert_manager)
    await pool.connect()
    
    # 2. Setup Mapping Engine
    engine = MappingEngine(sample_config.mappings)
    
    # 3. Setup AAS Provider (memory)
    provider = MemoryAasProvider(sample_config.aas, engine)
    await provider.start()
    await provider.register_mappings(engine.resolved_mappings())
    
    # 4. Setup Monitor
    metrics = MetricsCollector(9091, registry=CollectorRegistry()) # Use different port
    monitor = MonitoringManager(pool, engine, metrics, provider)
    
    # 5. Start Monitor
    mappings = list(engine._resolved.values())
    await monitor.start(mappings)
    
    # 6. Trigger Change in OPC UA Simulator
    # We need to write to the node in the simulator
    # Since opcua_simulator fixture returns the Server object, we can write directly
    
    # Node "Temperature" is ns=2;s=Temperature (from sample_config + conftest)
    # Wait, in conftest I created variables in a new namespace index.
    # checking conftest: `idx = await server.register_namespace(uri)`
    # The index will be 2 usually (0=UA, 1=Local, 2=Our).
    # so ns=2;s=Temperature is likely correct.
    
    var_node = opcua_simulator.get_node("ns=2;s=Temperature")
    # Write a new value
    await var_node.write_value(42.0)
    
    # 7. Wait for sync and verify update
    try:
        for _ in range(20):
            await asyncio.sleep(0.1)
            if provider.get_property_value("Temperature", "urn:test") == 42.0:
                break
    finally:
        await monitor.stop()
        await pool.disconnect()
        await provider.stop()

    assert provider.get_property_value("Temperature", "urn:test") == 42.0
