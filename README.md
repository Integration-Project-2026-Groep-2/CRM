# CRM — Integration Project 2025/2026

Salesforce integration for the Desideriushogeschool event-management platform
(Groep 2). CRM is the master-data owner for **Contacts** (participants) and
**Accounts** (companies). It exchanges data with peer teams exclusively over
RabbitMQ in XSD-validated XML — there is no inter-team HTTP API, since Salesforce
itself is cloud SaaS.

## Architecture

One Docker container → one Python process. The main thread runs
`asyncio.run(main)` and supervises five long-running tasks, plus the bundled CRM
MCP server on a daemon thread (FastMCP runs its own event loop).

| Task / thread  | Kind            | Responsibility |
|----------------|-----------------|----------------|
| `heartbeat`    | asyncio task    | Heartbeat XML → `heartbeat.direct` / `routing.heartbeat` (default 1 s) |
| `status_check` | asyncio task    | StatusCheck XML (uptime, cpu/mem/disk) → `statuscheck.direct` / `routing.statuscheck` (default 120 s) |
| `receiver`     | asyncio task †  | Consume 24 inbound queues, XSD-validate, write Salesforce |
| `polling`      | asyncio task †  | Detect out-of-band Salesforce UI edits, publish `crm.*` events (default 60 s) |
| `log_publisher`| asyncio task    | Drain the in-process log queue → LogEvent XML → `logs.direct` / `routing.log` |
| `crm-mcp`      | daemon thread   | MCP server for the AI team's master agent (see [crm-mcp](#crm-mcp)) |

† Restartable with exponential backoff (1 s → 60 s, max 10 restarts); the other
tasks log-and-exit on failure.

`sender.py` is a utility module, not a task — the single outbound publish path
(`contact.topic`, the fanout `crm.user.conflict`, and `logs.direct`).

**Failure routing.** Each inbound work-queue has a sibling `<queue>.retry`
(`x-message-ttl=30000`, dead-lettered back to the producer exchange) for
transient errors. Terminal failures go to `contact.dlq` → `crm.dlq.queue`. Retry
budgets: `MissingDependencyError` 15 attempts (Salesforce write races), generic
errors 5; Salesforce rate-limits skip retry and dead-letter after a 60 s back-off.
`controlroom.warning.issued` is no-retry (log + ack).

### Source layout (`src/`)

- `main.py` — entrypoint, task supervision, signal-driven shutdown
- `receiver.py` / `sender.py` / `polling.py` — inbound dispatch / outbound publish / Salesforce change-polling
- `heartbeat.py` / `status.py` / `logging_rabbitmq.py` — periodic and streaming emitters
- `mcp_thread.py` — boots the bundled `crm-mcp` server in-process
- `connection.py` / `config.py` / `xml_validator.py` / `country_code.py` — AMQP connection, env config, hardened XSD validation (anti-XXE), country normalization
- `handlers/` — 24 event handlers, `_registry.py` (queue → handler table), per-team helpers, and `_transport.py` (retry/DLQ)
- `salesforce/` — the Salesforce access layer (below); `salesforce_client.py` re-exports it for backward compatibility

## Salesforce layer

`src/salesforce/` wraps `simple-salesforce`. Every call runs in a thread pool
(`asyncio.to_thread`) with per-request timeouts, connection retries, and
automatic session reauth. Three SObjects are managed:

| Module | SObject | Role |
|--------|---------|------|
| `contacts/` | `Contact` | create/upsert (by email), matching (none/unique/ambiguous), per-team updates, soft-delete, record → XML mapping |
| `accounts.py` | `Account` | create, upsert (by VAT number), matching, Facturatie sync, soft-deactivate |
| `sessions.py` | `Session_Registration__c` | junction lifecycle — deprecated (session ownership moved to Planning) |
| `payments.py` | `Contact` | stamps the payment timestamp (C16) and serves unpaid lookups (C17) |

`CRM_ID__c` (a UUID) is the cross-system master ID, preserved across updates.
Deduplication uses native Salesforce external IDs — Contact by email, Account by
VAT number, registration by `Registration_ID__c`. Deletes are **soft only** (the
active flag is set to false), never physical (GDPR).

## Messaging & contracts

All inter-team traffic is XML on the shared RabbitMQ broker, validated against
`src/schema/crm-schema-v1.xsd` on the way in and out. Inbound queues are
`crm.`-prefixed to avoid collisions on shared topic exchanges; outbound canonical
events publish as `crm.*` on `contact.topic`.

| Peer team   | Direction | What |
|-------------|-----------|------|
| Frontend    | in / out  | Registrations and companies in; `crm.user.*` / `crm.company.*` out |
| Facturatie  | in / out  | User and company sync in; company response and invoice request out |
| Mailing     | in / out  | User sync in; mail requests and user events out |
| Planning    | in / out  | User sync and session updates in; user events out |
| Kassa       | in / out  | Person-lookup, unpaid and payment-confirmed in; lookup/unpaid responses out |
| Controlroom | in / out  | Warnings in; heartbeat, status, and conflicts (fanout) out |
| IoT         | in        | Badge-link (planned, not yet consumed) |

Formal spec: [`docs/crm-asyncapi-v1.yaml`](docs/crm-asyncapi-v1.yaml) — AsyncAPI
3.1.0, document version 1.11.1, 35 contracts / 38 message schemas. View it in
[AsyncAPI Studio](https://studio.asyncapi.com/?url=https://raw.githubusercontent.com/Integration-Project-2026-Groep-2/CRM/main/docs/crm-asyncapi-v1.yaml).
Broker exchange topology: [`docs/rabbitmq-exchanges.md`](docs/rabbitmq-exchanges.md).

## crm-mcp

`crm-mcp/` is a self-contained sub-package: an MCP server exposing the CRM's
Salesforce data to the AI team's master agent (the Rust `mcp-master` client).
Built on the official MCP Python SDK (FastMCP), it serves streamable-http on
`:7001` plus a `/health` route. It reaches Salesforce through the same
`simple-salesforce` stack and reuses the project's `SALESFORCE_*` credentials;
all SOQL literals are escaped.

It ships two ways from one codebase:

- **Embedded (production):** the CRM container boots it on a daemon thread
  (`src/mcp_thread.py`), toggled by `CRM_MCP_ENABLED` (default on).
- **Standalone:** `pip install ./crm-mcp`, then run the `crm-mcp` console script.
  Write tools are disabled in this mode (no broker bound).

Tools: 17 read-only (contact/company/registration lookups, counts, summaries) and
6 write (`create` / `update` / `delete` for contact and company). Write tools are
approval-gated (`requires_approval`), soft-delete only, and each broadcasts an
XSD-validated `crm.*` XML event so consumer teams stay in sync.

## Quick start (Docker)

```bash
git clone https://github.com/Integration-Project-2026-Groep-2/CRM.git
cd CRM
cp .env.example .env          # fill in Salesforce credentials
docker compose up --build     # starts crm + rabbitmq:3.13-management
```

RabbitMQ management UI: http://localhost:15672 (guest / guest). The MCP server is
internal only (container port 7001, not published). `.env.example` lists the
configuration; `RABBITMQ_URL` and the `SALESFORCE_USERNAME` / `SALESFORCE_PASSWORD` /
`SALESFORCE_SECURITY_TOKEN` secrets are required (`SALESFORCE_DOMAIN` defaults to `login`).

## Development

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
ruff check src/ tests/         # matches CI
```

Stack: Python 3.13, `aio-pika` (AMQP), `lxml` (XML + XSD), `simple-salesforce`,
`aiohttp`, `psutil`, `pycountry`, `python-dotenv`.

Local demo dashboard — monitoring + interactive CRUD over RabbitMQ:
`python scripts/dashboard_server.py` (http://localhost:8050). The rest of `scripts/`
are manual dev/Salesforce probes.

## Tests

Unit tests need no broker. Integration and e2e tests need a RabbitMQ broker on
port **5675** (they auto-skip when it is unreachable):

```bash
docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine

pytest tests/unit/ -v          # no broker needed
pytest tests/integration/ -v   # needs the broker above (CRM_TEST_RABBITMQ_URL)
pytest tests/e2e -v            # needs broker + Salesforce creds (+ POLLING_INTEGRATION_USER_ID)
pip install "./crm-mcp[dev]" && pytest crm-mcp/tests/ -v   # MCP sub-package suite
```

For e2e against a fresh stack: `E2E_AUTO_START_LOCAL_STACK=1 pytest tests/e2e -v`.
Skip Salesforce-dependent e2e with `--skip-sf`.

## Deploy

- **CI** (push and pull requests): `ruff` + `pip-audit`, unit tests, integration
  tests (RabbitMQ service container). e2e does not run in CI.
- **CD** (CI green on `main`): builds and pushes the image to
  `ghcr.io/integration-project-2026-groep-2/crm` (`sha-<short>`, `latest-dev`),
  then dispatches `update-dev-image` to the `k8s-manifests` repo (ArgoCD GitOps).
- **Production:** publish a GitHub Release (or run *Promote to Prod*) → the image
  is retagged by digest to `:<version>` + `:latest` and `update-prod-image` is
  dispatched.

## Team

| Role | Name |
|------|------|
| Team Lead | Lars Cowé |
| Developer / Tester | Brend Van Den Eynde |
| Developer / Tester | Waïl Zemouri |

Project managers Iliès Mazouz and Andrei Mikhaylov coordinate at group level
across all teams.
