#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker/compose.e2e.yml}"
timeout="${E2E_TIMEOUT:-60}"

if [ "${E2E_UP_SKIP_START:-0}" != "1" ]; then
  echo "Starting E2E stack..."
  docker compose -f "$compose_file" up -d --build
fi

python - <<'PY'
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import subprocess

timeout = float(os.getenv("E2E_TIMEOUT", "60"))
deadline = time.time() + timeout

base_url = os.getenv("E2E_SM_REPO_URL", "http://localhost:8081").rstrip("/")
candidates = [
    base_url,
    f"{base_url}/submodel-repository",
    f"{base_url}/api/v3.0",
    f"{base_url}/aas",
]
probe_paths = ("/submodels", "/v3/api-docs")

def wait_for_http(base_candidates, paths) -> None:
    while time.time() < deadline:
        for base in base_candidates:
            for path in paths:
                url = f"{base}{path}"
                try:
                    with urllib.request.urlopen(url, timeout=2) as resp:
                        if resp.status == 200:
                            print(f"Ready: {url}")
                            return
                except (urllib.error.URLError, ConnectionResetError):
                    continue
        time.sleep(1)
    compose_file = os.getenv("COMPOSE_FILE", "docker/compose.e2e.yml")
    try:
        ps = subprocess.run(
            ["docker", "compose", "-f", compose_file, "ps"],
            check=False,
            capture_output=True,
            text=True,
        )
        print("docker compose ps:", ps.stdout.strip() or ps.stderr.strip())
        logs = subprocess.run(
            ["docker", "compose", "-f", compose_file, "logs", "sm-repo"],
            check=False,
            capture_output=True,
            text=True,
        )
        print("sm-repo logs:", logs.stdout.strip() or logs.stderr.strip())
    except Exception as exc:
        print(f"Failed to fetch docker logs: {exc}")
    raise SystemExit(
        f"Timed out waiting for {', '.join([b + p for b in base_candidates for p in paths])}"
    )

def wait_for_tcp(host: str, port: int) -> None:
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Ready: tcp://{host}:{port}")
                return
        except OSError:
            time.sleep(1)
    raise SystemExit(f"Timed out waiting for tcp://{host}:{port}")

wait_for_http(candidates, probe_paths)

opcua_url = os.getenv("E2E_OPCUA_URL", "opc.tcp://localhost:4840")
parsed = urllib.parse.urlparse(opcua_url)
host = parsed.hostname or "localhost"
port = parsed.port or 4840
wait_for_tcp(host, port)
PY

echo "E2E stack is ready."
