import base64
import json

import pytest

from basyx_opcua_bridge.aas.events import RecentWriteCache, parse_basyx_topic
from basyx_opcua_bridge.aas.providers import HttpAasProvider
from basyx_opcua_bridge.config.models import AasEventsConfig, AasProviderConfig, MappingRule
from basyx_opcua_bridge.mapping.engine import MappingEngine


def test_parse_basyx_topic():
    submodel_id = "urn:factory:submodel:sensors"
    encoded = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
    topic = (
        "sm-repository/default/submodels/"
        f"{encoded}/submodelElements/Sensors/Temperature/updated"
    )

    hints = parse_basyx_topic(topic)
    assert hints.submodel_id == submodel_id
    assert hints.id_short_path == "Sensors/Temperature"
    assert hints.id_short == "Temperature"


def test_recent_write_cache_ttl():
    now = 100.0

    def clock() -> float:
        return now

    cache = RecentWriteCache(ttl_seconds=1.0, max_entries=10, clock=clock)
    cache.remember("key", {"value": 1})
    assert cache.matches("key", {"value": 1})

    now += 2.0
    assert not cache.matches("key", {"value": 1})


@pytest.mark.asyncio
async def test_mqtt_topic_value_only_payload():
    rule = MappingRule(
        opcua_node_id="ns=1;s=Temp",
        aas_id_short="Temp",
        submodel_id="urn:factory:submodel:sensors",
        value_type="xs:double",
    )
    engine = MappingEngine([rule])
    config = AasProviderConfig(
        type="basyx",
        url="http://localhost:8080/api/v3.0",
        enable_events=True,
        auto_create_submodels=False,
        auto_create_elements=False,
        events=AasEventsConfig(enabled=True, dedup_enabled=False),
    )
    provider = HttpAasProvider(config, engine)
    await provider.register_mappings(engine.resolved_mappings())

    encoded = base64.urlsafe_b64encode(rule.submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
    topic = f"sm-repository/default/submodels/{encoded}/submodelElements/Temp/updated"
    payload = json.dumps(55.0).encode("utf-8")

    requests = provider._parse_event_message(payload, config.events, topic)
    assert requests is not None
    assert requests[0].node_id == rule.opcua_node_id
