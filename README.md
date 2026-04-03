# CRM — Integration Project 2025/2026

Salesforce-integratie voor het Desideriushogeschool eventmanagementplatform. Communiceert met andere teams via RabbitMQ (XML-formaat).

## Quick start

```bash
# 1. Clone
git clone https://github.com/Integration-Project-2026-Groep-2/CRM.git
cd CRM

# 2. Environment
cp .env.example .env
# Vul Salesforce credentials in

# 3. Start (Docker)
docker compose up --build

# RabbitMQ management UI: http://localhost:15672 (guest / guest)
```

## Development

```bash
# Virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Dependencies
pip install -r requirements-dev.txt

# Tests
pytest -v

# E2E tegen bestaande lokale stack
pytest tests/e2e -v

# E2E met expliciete lokale docker autostart
E2E_AUTO_START_LOCAL_STACK=1 pytest tests/e2e -v
```

## Architectuur

Eén Docker container → één Python process → 3 asyncio tasks + sender utility:

| Module | Verantwoordelijkheid |
|---|---|
| `heartbeat.py` | XML heartbeat elke seconde → `heartbeat.direct` exchange (Contract 7) |
| `status.py` | CPU/mem/disk → `crm.status.checked` (Contract 8) |
| `receiver.py` | Luistert op 11 queues van andere teams |
| `sender.py` | Utility module — receiver handlers roepen publish functies aan |

## Contracten

26 XML-contracten (AsyncAPI v1.6.0). Formele spec:

- **Bron**: [`docs/crm-asyncapi-v1.yaml`](docs/crm-asyncapi-v1.yaml)
- **Online bekijken**: [AsyncAPI Studio](https://studio.asyncapi.com/?url=https://raw.githubusercontent.com/Integration-Project-2026-Groep-2/CRM/main/docs/crm-asyncapi-v1.yaml)

## Team

| Rol | Naam |
|---|---|
| Team Lead | Lars Cowé |
| Developer / Tester | Brend Van Den Eynde |
| Developer / Tester | Waïl Zemouri |
| Developer / Tester | Jelle Schroeven |
| Project Manager | Iliès Mazouz |
| Project Manager | Andrei Mikhaylov |
