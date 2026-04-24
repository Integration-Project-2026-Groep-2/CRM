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
| `receiver.py` | Thin runner — declareert inbound queues en dispatcht naar handlers in `src/handlers/` via `QUEUE_REGISTRY` |
| `polling.py` | Polt Salesforce periodiek op out-of-band Contact/Account wijzigingen (admin in SF UI); publiceert contracts 13/14/18/19/22/23 |
| `sender.py` | Utility module — handlers én polling roepen `publish_*` functies aan |

### Package-structuur (`src/`)

- `handlers/` — 19 event-handler files + gedeelde helpers (`_transport.py`, `_helpers.py`, team-specifieke helpers) + `_registry.py` (queue → handler routing table)
- `salesforce/` — per-SObject modules: `client.py` (auth + describe-cache), `contacts/` (sub-package met `client`, `mapping`, `matching`, `updates`), `accounts.py`, `sessions.py`, `payments.py`
- `salesforce_client.py` — facade-shim die alle `salesforce/` exports herpubliceert voor backward-compat
- `receiver.py`, `sender.py`, `polling.py`, `heartbeat.py`, `main.py`, `config.py`, `connection.py`, `xml_validator.py`, `country_code.py` — top-level modules

## Contracten

32 XML-contracten (AsyncAPI v1.8.0). Formele spec:

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
