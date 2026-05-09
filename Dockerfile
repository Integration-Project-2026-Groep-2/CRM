FROM python:3.13-slim

WORKDIR /app

# curl gives the Docker healthcheck a real probe of the MCP /health endpoint
# (was: `python -c "sys.exit(0)"` no-op that flagged hung processes as healthy).
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Dependencies als aparte laag -- betere build cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# crm-mcp package -- aparte laag zodat een tool-only wijziging niet de
# requirements.txt-laag invalidatet. Pull mcp/pydantic deps mee via pyproject.
COPY crm-mcp/pyproject.toml ./crm-mcp/
COPY crm-mcp/crm_mcp/ ./crm-mcp/crm_mcp/
RUN pip install --no-cache-dir ./crm-mcp

COPY src/ ./src/
COPY scripts/ ./scripts/

# Non-root user: standaard security vereiste
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# MCP server draait als daemon-thread in dit proces (zie src/mcp_thread.py).
# Alleen op het docker-network bereikbaar -- geen ports: in compose, expose:.
EXPOSE 7001

# Docker healthcheck -- probes the MCP /health endpoint which validates SF
# connectivity + AMQP publisher bound state. 503 → unhealthy, 200 → healthy.
HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=30s \
  CMD curl -fsS http://localhost:7001/health || exit 1

CMD ["python", "-m", "src.main"]
