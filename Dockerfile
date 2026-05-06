FROM python:3.13-slim

WORKDIR /app

# Dependencies als aparte laag -- betere build cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# crm-mcp package -- aparte laag zodat een tool-only wijziging niet de
# requirements.txt-laag invalidatet. Pull mcp/pydantic deps mee via pyproject.
COPY crm-mcp/pyproject.toml crm-mcp/README.md ./crm-mcp/
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

# Docker healthcheck -- onafhankelijk van de RabbitMQ heartbeat
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

CMD ["python", "-m", "src.main"]
