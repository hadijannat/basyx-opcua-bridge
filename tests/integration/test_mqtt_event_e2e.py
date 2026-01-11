import asyncio
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest
from asyncua import Client
from basyx.aas import model as aas_model
from basyx.aas.adapter.json import json_serialization


RUN_E2E = os.getenv("RUN_MQTT_E2E") == "1"
SM_REPO_BASE_URL = os.getenv("E2E_SM_REPO_URL", "http://localhost:8081")
OPCUA_URL = os.getenv("E2E_OPCUA_URL", "opc.tcp://localhost:4840")
E2E_TIMEOUT = float(os.getenv("E2E_TIMEOUT", "20"))


def _encode_identifier(identifier: str) -> str:
    encoded = base64.urlsafe_b64encode(identifier.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _request_json(method: str, url: str, payload: object | None = None) -> int:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception:
        return 0


async def _wait_for_http(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = await asyncio.to_thread(_request_json, "GET", url)
        if status == 200:
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for HTTP 200 from {url}")


async def _wait_for_opcua_value(node_id: str, expected: float, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    async with Client(OPCUA_URL) as client:
        node = client.get_node(node_id)
        while time.monotonic() < deadline:
            value = await node.read_value()
            if value == expected:
                return
            await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for OPC UA value {expected}")


@pytest.mark.asyncio
async def test_mqtt_event_triggers_opcua_write():
    if not RUN_E2E:
        pytest.skip("RUN_MQTT_E2E not set")

    await _wait_for_http(f"{SM_REPO_BASE_URL}/submodels", timeout=E2E_TIMEOUT)

    submodel_id = "urn:example:submodel:1"
    submodel = aas_model.Submodel(id_=submodel_id, id_short="Sensors")
    submodel_payload = json.loads(json.dumps(submodel, cls=json_serialization.AASToJsonEncoder))

    status = await asyncio.to_thread(
        _request_json,
        "POST",
        f"{SM_REPO_BASE_URL}/submodels",
        submodel_payload,
    )
    assert status in (200, 201, 204, 409)

    prop = aas_model.Property(
        id_short="Temperature",
        value_type=aas_model.datatypes.Double,
        value=20.0,
    )
    prop_payload = json.loads(json.dumps(prop, cls=json_serialization.AASToJsonEncoder))
    encoded = _encode_identifier(submodel_id)
    status = await asyncio.to_thread(
        _request_json,
        "POST",
        f"{SM_REPO_BASE_URL}/submodels/{encoded}/submodel-elements",
        prop_payload,
    )
    assert status in (200, 201, 204, 409)

    await asyncio.sleep(2.0)
    target_value = 55.0
    status = await asyncio.to_thread(
        _request_json,
        "PATCH",
        f"{SM_REPO_BASE_URL}/submodels/{encoded}/submodel-elements/Temperature/$value",
        {"value": target_value},
    )
    assert status in (200, 204)

    await _wait_for_opcua_value("ns=2;s=Temperature", target_value, timeout=E2E_TIMEOUT)
