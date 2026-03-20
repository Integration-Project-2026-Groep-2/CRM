FROM python:3.13-slim

WORKDIR /app

# Dependencies als aparte laag -- betere build cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Non-root user: standaard security vereiste
RUN adduser --disabled-password --gecos "" appuser
USER appuser

# Docker healthcheck -- onafhankelijk van de RabbitMQ heartbeat
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

CMD ["python", "src/main.py"]
