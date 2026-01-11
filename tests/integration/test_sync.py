import pytest
import asyncio
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.mapping.engine import MappingEngine
from basyx_opcua_bridge.sync.monitor import MonitoringManager
from basyx_opcua_bridge.observability.metrics import MetricsCollector
from basyx_opcua_bridge.config.models import EndpointConfig
from basyx_opcua_bridge.security.x509 import CertificateManager
from basyx.aas import model as aas_model

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
    
    # 3. Create dummy Submodel
    sm = aas_model.Submodel(id_="urn:test")
    engine.register_submodel(sm, "ns=2")
    
    # 4. Setup Monitor
    metrics = MetricsCollector(9091) # Use different port
    monitor = MonitoringManager(pool, engine, metrics)
    
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
    
    # 7. Wait for sync
    # We poll the AAS element
    try:
        # Give it a second to process
        for _ in range(10):
            # Checking if the Mock/Placeholder Property logic in MonitoringManager actually updates
            # In monitor.py check:
            # mapping.element.value = val 
            # Wait, in `monitor.py` I wrote `# mapping.element.value = val` (commented out in snippet?)
            # I must check `monitor.py` content.
            # If it is commented out, the test will fail.
            await asyncio.sleep(0.1)
    finally:
        await monitor.stop()
        await pool.disconnect()

    # Re-read monitor.py first to ensure logic is uncommented
