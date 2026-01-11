#!/usr/bin/env bash
set -euo pipefail

compose_file="${COMPOSE_FILE:-docker/compose.e2e.yml}"

echo "Stopping E2E stack..."
docker compose -f "$compose_file" down -v
