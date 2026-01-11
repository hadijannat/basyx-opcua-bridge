import asyncio
from typing import AsyncGenerator
import pytest
import pytest_asyncio
from asyncua import Server, ua
from basyx_opcua_bridge.config.models import BridgeConfig, OpcUaConfig, EndpointConfig, MappingRule

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture
async def opcua_simulator() -> AsyncGenerator[Server, None]:
    """
    Spin up a simulated OPC UA server for integration tests.
    """
    server = Server()
    await server.init()
    
    server.set_endpoint("opc.tcp://localhost:4840")
    server.set_server_name("Test Server")
    
    # Setup our own namespace
    uri = "http://test.example.org"
    idx = await server.register_namespace(uri)
    
    # Create a dummy object
    objects = server.nodes.objects
    obj = await objects.add_object(idx, "TestObject")
    
    # Add variables to monitor
    # Force String NodeIds to match sample_config
    
    # Temp: Double, ns=idx;s=Temperature
    nodeid_temp = ua.NodeId("Temperature", idx, ua.NodeIdType.String)
    temp = await obj.add_variable(nodeid_temp, "Temperature", 25.0)
    await temp.set_writable()
    
    # Speed: Double, writable, ns=idx;s=Speed
    nodeid_speed = ua.NodeId("Speed", idx, ua.NodeIdType.String)
    speed = await obj.add_variable(nodeid_speed, "Speed", 0.0)
    await speed.set_writable()
    
    await server.start()
    
    yield server
    
    await server.stop()

@pytest.fixture
def sample_config() -> BridgeConfig:
    return BridgeConfig(
        opcua=OpcUaConfig(
            endpoints=[EndpointConfig(url="opc.tcp://localhost:4840", name="test")]
        ),
        mappings=[
            MappingRule(
                opcua_node_id="ns=2;s=Temperature", # Matches simulator
                aas_id_short="Temperature",
                submodel_id="urn:test",
                value_type="xs:double"
            ),
            MappingRule(
                opcua_node_id="ns=2;s=Speed",
                aas_id_short="Speed",
                submodel_id="urn:test",
                value_type="xs:double",
                range_constraint={"min_value": 0, "max_value": 100}
            )
        ]
    )
