# Golden Path Demo (Docker Compose)

This demo launches a full, working environment:

- OPC UA simulator
- BaSyx Submodel Repository (AAS backend)
- BaSyx OPC UA Bridge
- Optional Prometheus scrape of bridge metrics

## Quick Start

```bash
docker compose -f examples/docker-compose/compose.yml up --build
```

If port `1883` is already in use on your host, override it:

```bash
MQTT_HOST_PORT=1884 docker compose -f examples/docker-compose/compose.yml up --build
```

## What You Should See

- The bridge starts and connects to the OPC UA simulator.
- The Submodel Repository is reachable at `http://localhost:8081`.
- Prometheus metrics are exposed at `http://localhost:9090/metrics`.

### Verify OPC UA → AAS

List Submodels (the bridge will auto‑create the submodel on first sync):

```bash
curl http://localhost:8081/submodels
```

### Verify AAS → OPC UA

Update the property value via the AAS API (event‑driven through MQTT):

```bash
python - <<'PY'
import base64
submodel_id = "urn:example:submodel:1"
encoded = base64.urlsafe_b64encode(submodel_id.encode()).decode().rstrip("=")
print(encoded)
PY
```

```bash
curl -X PATCH \
  -H "Content-Type: application/json" \
  -d '{"value": 55.0}' \
  http://localhost:8081/submodels/<encoded>/submodel-elements/Temperature/$value
```

Check the bridge logs to confirm the OPC UA write:

```bash
docker compose -f examples/docker-compose/compose.yml logs -f bridge
```

## Metrics (Optional)

Start the stack with Prometheus:

```bash
docker compose -f examples/docker-compose/compose.yml --profile metrics up --build
```

Prometheus will be available at `http://localhost:9091` and scrapes `bridge:9090/metrics`.

## Tear Down

```bash
docker compose -f examples/docker-compose/compose.yml down -v
```
