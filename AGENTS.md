# Repository Guidelines

## Project Structure & Module Organization
The core Python package lives in `src/basyx_opcua_bridge/` and is organized by responsibility:
`core/` (bridge orchestration), `config/` (Pydantic models), `mapping/` (OPC UA ↔ AAS conversions),
`sync/` (monitor/controller), `security/` (X.509 + audit), `observability/` (metrics), and `cli/`
(Typer entrypoint). Tests are under `tests/` with `unit/` and `integration/` suites. Runtime configuration
is stored in `config/` (e.g., `config/bridge.yaml`). Supporting materials live in `docs/`, container assets
in `docker/`, and helper scripts in `scripts/`.

## Build, Test, and Development Commands
- `scripts/setup_dev.sh` bootstraps a dev environment with `uv`, installs extras, and generates dev certs.
- `uv venv && uv pip install -e ".[dev]"` creates a local venv with dev tooling.
- `python -m basyx_opcua_bridge.cli.main --config config/bridge.yaml` runs the bridge locally.
- `basyx-bridge --config config/bridge.yaml` runs the installed CLI entrypoint.
- `pytest tests/` runs all tests; `pytest tests/ --cov=src --cov-report=html` adds coverage.
- `ruff check src/` (lint) and `mypy src/` (type-check) validate code quality.
- `docker build -t basyx-opcua-bridge -f docker/Dockerfile .` builds the container image.

## Coding Style & Naming Conventions
Python 3.11+ is required. Use 4-space indentation and keep lines ≤ 88 chars (Ruff). Prefer explicit type
annotations; `mypy` runs in strict mode. Follow Python naming conventions: `snake_case` for modules,
functions, and variables; `CamelCase` for classes. Keep async code idiomatic (`async`/`await`) in `sync/`
and network-facing modules.

## Testing Guidelines
Testing uses `pytest` with `pytest-asyncio` (`asyncio_mode = auto`). Name tests `test_*.py` and group by
scope: `tests/unit/` for pure logic and `tests/integration/` for system interactions. Mark async tests
with `@pytest.mark.asyncio` when needed. Run integration tests with `pytest tests/integration/`.

## Commit & Pull Request Guidelines
The existing history uses Conventional Commit prefixes (e.g., `feat:`). Follow the same pattern for new
commits (`feat:`, `fix:`, `docs:`, `chore:`). For PRs, include a concise summary, list tests run, and note
any config changes (like updates to `config/bridge.yaml`) so reviewers can validate behavior quickly.

## Security & Configuration Tips
Certificates are managed in `certs/` and can be generated via `scripts/setup_dev.sh`. Treat real keys
and credentials as sensitive; avoid committing production secrets or private certs to the repo.
