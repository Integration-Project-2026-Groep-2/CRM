FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY crm_mcp/ ./crm_mcp/

RUN pip install --no-cache-dir .

RUN adduser --disabled-password --gecos "" appuser
USER appuser

EXPOSE 7001

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

CMD ["crm-mcp"]
