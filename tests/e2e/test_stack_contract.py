import asyncio
import base64
import json
import os
import time
import urllib.error
import urllib.request

import pytest
from asyncua import Client


RUN_STACK_E2E = os.getenv("RUN_STACK_E2E") == "1"
SM_REPO_BASE_URL = os.getenv("E2E_SM_REPO_URL", "http://localhost:8081")
OPCUA_URL = os.getenv("E2E_OPCUA_URL", "opc.tcp://localhost:4840")
METRICS_URL = os.getenv("E2E_METRICS_URL", "http://localhost:9090/metrics")
E2E_TIMEOUT = float(os.getenv("E2E_TIMEOUT", "60"))


def _encode_identifier(identifier: str) -> str:
    encoded = base64.urlsafe_b64encode(identifier.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _request_json(url: str) -> tuple[int, object | None]:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
            if not raw:
                return response.status, None
            try:
                return response.status, json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                return response.status, raw.decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception:
        return 0, None


async def _resolve_sm_repo_base(base_url: str, timeout: float) -> str:
    deadline = time.monotonic() + timeout
    base = base_url.rstrip("/")
    candidates = [
        base,
        f"{base}/submodel-repository",
        f"{base}/api/v3.0",
        f"{base}/aas",
    ]
    probe_paths = ("/submodels", "/v3/api-docs")
    while time.monotonic() < deadline:
        for candidate in candidates:
            for path in probe_paths:
                status, _ = await asyncio.to_thread(_request_json, f"{candidate}{path}")
                if status == 200:
                    return candidate
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for HTTP 200 from {base}")


def _extract_value(payload: object) -> object | None:
    if isinstance(payload, dict):
        if "value" in payload:
            return payload.get("value")
        if "data" in payload and isinstance(payload["data"], dict):
            return payload["data"].get("value")
    return None


async def _wait_for_aas_value(
    base_url: str,
    submodel_id: str,
    id_short: str,
    expected: float,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    encoded = _encode_identifier(submodel_id)
    element_url = f"{base_url}/submodels/{encoded}/submodel-elements/{id_short}"
    while time.monotonic() < deadline:
        status, payload = await asyncio.to_thread(_request_json, element_url)
        if status == 200 and payload is not None:
            value = _extract_value(payload)
            if value == expected:
                return
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for AAS value {expected}")


async def _wait_for_metrics(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, payload = await asyncio.to_thread(_request_json, METRICS_URL)
        if status == 200 and isinstance(payload, str):
            if "bridge_sync_events_total" in payload and "bridge_active_subscriptions" in payload:
                return
        await asyncio.sleep(0.5)
    raise AssertionError("Timed out waiting for metrics to be available")


@pytest.mark.asyncio
async def test_stack_contract_opcua_to_aas_and_metrics():
    if not RUN_STACK_E2E:
        pytest.skip("RUN_STACK_E2E not set")

    base_url = await _resolve_sm_repo_base(SM_REPO_BASE_URL, timeout=E2E_TIMEOUT)

    target_value = 42.0
    async with Client(OPCUA_URL) as client:
        node = client.get_node("ns=2;s=Temperature")
        await node.write_value(target_value)

    await _wait_for_aas_value(
        base_url,
        submodel_id="urn:example:submodel:1",
        id_short="Temperature",
        expected=target_value,
        timeout=E2E_TIMEOUT,
    )

    await _wait_for_metrics(timeout=E2E_TIMEOUT)
