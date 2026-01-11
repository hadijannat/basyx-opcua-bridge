import pytest
from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
from basyx_opcua_bridge.config.models import EndpointConfig
from basyx_opcua_bridge.security.x509 import CertificateManager

@pytest.mark.asyncio
async def test_connection_pool_connect(opcua_simulator, sample_config):
    # Setup
    cert_manager = CertificateManager(sample_config.security)
    # mock logic to skip loading real certs if config is empty or handle None
    # actually CertificateManager handles None gracefully if config paths are None
    
    # Update endpoint to match simulator
    endpoint = EndpointConfig(
        url="opc.tcp://localhost:4840", 
        name="sim",
        security_policy="None",
        security_mode="None"
    )
    
    pool = OpcUaConnectionPool(
        endpoints=[endpoint],
        cert_manager=cert_manager,
        pool_size=1
    )
    
    # Test Connect
    await pool.connect()
    
    async with pool.get_connection(endpoint.url) as conn:
        assert conn.is_connected
        assert conn.endpoint.url == "opc.tcp://localhost:4840"
        
        # Test Subscription Creation
        sub = await conn.create_subscription()
        assert sub is not None
        assert len(conn.subscriptions) == 1
    
    # Test Disconnect
    await pool.disconnect()
    
    # Verify disconnected
    # (Checking private state for test purposes)
    assert len(pool._connections) == 0
