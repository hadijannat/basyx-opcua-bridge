import pytest

from basyx_opcua_bridge.discovery import DiscoveryOptions, discover_opcua
from basyx_opcua_bridge.config.models import SyncDirection


@pytest.mark.asyncio
async def test_discovery_deterministic(opcua_simulator):
    options = DiscoveryOptions(
        endpoint_url="opc.tcp://localhost:4840",
        endpoint_name="sim",
        namespaces=[2],
        include_pattern="Temperature|Speed",
        max_depth=4,
        group_strategy="namespace",
        aas_type="memory",
    )

    result_a = await discover_opcua(options)
    result_b = await discover_opcua(options)

    mappings_a = [m.model_dump() for m in result_a.mappings]
    mappings_b = [m.model_dump() for m in result_b.mappings]

    assert mappings_a == mappings_b
    assert len(mappings_a) == 2

    ids = {m["aas_id_short"] for m in mappings_a}
    assert ids == {"Temperature", "Speed"}

    for mapping in result_a.mappings:
        assert mapping.direction == SyncDirection.BIDIRECTIONAL
