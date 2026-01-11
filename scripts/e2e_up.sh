#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker/compose.e2e.yml}"
timeout="${E2E_TIMEOUT:-60}"

echo "Starting E2E stack..."
docker compose -f "$compose_file" up -d --build

python - <<'PY'
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

timeout = float(os.getenv("E2E_TIMEOUT", "60"))
deadline = time.time() + timeout

base_url = os.getenv("E2E_SM_REPO_URL", "http://localhost:8081").rstrip("/")
submodels_url = f"{base_url}/submodels"
openapi_url = f"{base_url}/v3/api-docs"

def wait_for_http(urls) -> None:
    while time.time() < deadline:
        for url in urls:
            try:
                with urllib.request.urlopen(url, timeout=2) as resp:
                    if resp.status == 200:
                        print(f"Ready: {url}")
                        return
            except urllib.error.URLError:
                continue
        time.sleep(1)
    raise SystemExit(f"Timed out waiting for {', '.join(urls)}")

def wait_for_tcp(host: str, port: int) -> None:
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Ready: tcp://{host}:{port}")
                return
        except OSError:
            time.sleep(1)
    raise SystemExit(f"Timed out waiting for tcp://{host}:{port}")

wait_for_http((submodels_url, openapi_url))

opcua_url = os.getenv("E2E_OPCUA_URL", "opc.tcp://localhost:4840")
parsed = urllib.parse.urlparse(opcua_url)
host = parsed.hostname or "localhost"
port = parsed.port or 4840
wait_for_tcp(host, port)
PY

echo "E2E stack is ready."
