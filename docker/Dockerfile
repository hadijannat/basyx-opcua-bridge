# Stage 1: Build
FROM python:3.11-slim-bookworm as builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml .
COPY README.md .
COPY src /app/src
# Install into a virtual environment
RUN uv venv && uv pip install -r pyproject.toml && uv pip install .

# Stage 2: Runtime
FROM python:3.11-slim-bookworm
WORKDIR /app
# Copy virtual env
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY config /app/config

# Security: Run as non-root
RUN useradd -m bridgeuser
USER bridgeuser

ENTRYPOINT ["basyx-bridge", "main"]
CMD ["--config", "/app/config/bridge.yaml"]
