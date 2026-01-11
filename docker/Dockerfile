# Stage 1: Build
FROM python:3.11-slim-bookworm as builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml .
# Install into a virtual environment
RUN uv venv && uv pip install -r pyproject.toml

# Stage 2: Runtime
FROM python:3.11-slim-bookworm
WORKDIR /app
# Copy virtual env
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY src /app/src
COPY config /app/config

# Security: Run as non-root
RUN useradd -m bridgeuser
USER bridgeuser

ENTRYPOINT ["python", "-m", "basyx_opcua_bridge.cli.main"]
CMD ["--config", "/app/config/bridge.yaml"]
