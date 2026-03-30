"""CRM Demo Dashboard — Monitoring & Interactive CRUD.

Standalone web server that displays:
- Heartbeat status (parsed from CRM container logs)
- System status: CPU/memory/disk (via psutil)
- Salesforce Contacts table (polled via simple_salesforce)
- CRUD event log (in-memory buffer)
- Interactive CRUD buttons: CREATE, UPDATE, DELETE via RabbitMQ

RabbitMQ note: The CRUD endpoints publish to inbound queues and consume from
response queues. This is a DEMO TOOL — do not run while other consumers are
connected, as it will dequeue their messages.

Usage:
  pip install -r requirements-dev.txt
  python scripts/dashboard_server.py
  Open http://localhost:8050
"""

import asyncio
import logging
import os
import random
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aio_pika
from aio_pika import DeliveryMode
from aiohttp import web
from dotenv import load_dotenv
from lxml import etree
from simple_salesforce import Salesforce

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
CONTAINER_NAME = os.getenv("DASHBOARD_CONTAINER", "crm")
SF_CACHE_TTL = 5  # seconds
POLL_INTERVAL = 0.5  # seconds between response polls
POLL_TIMEOUT = 15  # max seconds to wait for CRM response

# In-memory state
_events: deque[dict] = deque(maxlen=50)
_sf_cache: dict = {"data": [], "expires": 0}
_last_contact: dict = {}  # {email, reg_id, r} — set by CREATE, used by UPDATE/DELETE


# ---------------------------------------------------------------------------
# Salesforce
# ---------------------------------------------------------------------------

def _get_sf_client() -> Salesforce | None:
    try:
        return Salesforce(
            username=os.getenv("SALESFORCE_USERNAME", ""),
            password=os.getenv("SALESFORCE_PASSWORD", ""),
            security_token=os.getenv("SALESFORCE_SECURITY_TOKEN", ""),
            domain=os.getenv("SALESFORCE_DOMAIN", "login"),
        )
    except Exception as exc:
        logger.warning("Salesforce connection failed: %s", exc)
        return None


def _query_contacts(sf: Salesforce) -> list[dict]:
    result = sf.query(
        "SELECT CRM_ID__c, FirstName, LastName, Email, Role__c "
        "FROM Contact ORDER BY CreatedDate DESC LIMIT 20"
    )
    return [
        {
            "id": r.get("CRM_ID__c", ""),
            "firstName": r.get("FirstName", ""),
            "lastName": r.get("LastName", ""),
            "email": r.get("Email", ""),
            "role": r.get("Role__c", ""),
        }
        for r in result.get("records", [])
    ]


# ---------------------------------------------------------------------------
# Docker log parsing
# ---------------------------------------------------------------------------

def _docker_inspect() -> dict:
    """Get container health and uptime via docker inspect."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format",
             "{{.State.Status}}|{{.State.Running}}|{{.State.StartedAt}}|{{.State.Health.Status}}",
             CONTAINER_NAME],
            capture_output=True, text=True, timeout=5,
        )
        parts = result.stdout.strip().split("|")
        if len(parts) >= 3:
            return {
                "status": parts[0],
                "running": parts[1] == "true",
                "started_at": parts[2],
                "health": parts[3] if len(parts) > 3 else "unknown",
            }
    except Exception:
        pass
    return {"status": "unknown", "running": False, "started_at": "", "health": "unknown"}


def _get_container_health() -> dict:
    """Determine heartbeat status from container state.

    Heartbeat publishes at DEBUG level (not visible in INFO logs),
    so we use container running state + health check as proxy.
    """
    info = _docker_inspect()
    running = info["running"]
    healthy = info["health"] == "healthy"
    return {
        "heartbeat_alive": running and healthy,
        "container_running": running,
        "container_status": info["status"],
        "container_health": info["health"],
        "started_at": info["started_at"],
    }


def _get_system_metrics() -> dict:
    """Get real system metrics from the host via psutil (same lib CRM uses)."""
    try:
        import psutil
        return {
            "cpu": psutil.cpu_percent(interval=0.1) / 100.0,
            "memory": psutil.virtual_memory().percent / 100.0,
            "disk": psutil.disk_usage("C:\\" if os.name == "nt" else "/").percent / 100.0,
        }
    except Exception:
        return {"cpu": 0.0, "memory": 0.0, "disk": 0.0}


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

async def handle_index(request: web.Request) -> web.Response:
    html_path = Path(__file__).parent / "dashboard.html"
    return web.Response(text=html_path.read_text(encoding="utf-8"), content_type="text/html")


async def handle_health(request: web.Request) -> web.Response:
    health = await asyncio.to_thread(_get_container_health)
    return web.json_response(health)


async def handle_status(request: web.Request) -> web.Response:
    metrics = await asyncio.to_thread(_get_system_metrics)
    return web.json_response(metrics)


async def handle_contacts(request: web.Request) -> web.Response:
    now = time.time()
    if now < _sf_cache["expires"]:
        return web.json_response(_sf_cache["data"])

    sf = await asyncio.to_thread(_get_sf_client)
    if sf is None:
        return web.json_response(
            {"error": "Salesforce connection failed"}, status=503
        )

    contacts = await asyncio.to_thread(_query_contacts, sf)
    _sf_cache["data"] = contacts
    _sf_cache["expires"] = now + SF_CACHE_TTL
    return web.json_response(contacts)


async def handle_get_events(request: web.Request) -> web.Response:
    return web.json_response(list(_events))


async def handle_post_event(request: web.Request) -> web.Response:
    body = await request.json()
    if body.get("_clear"):
        _events.clear()
        return web.json_response({"ok": True, "cleared": True})
    _events.appendleft(body)
    return web.json_response({"ok": True})


# ---------------------------------------------------------------------------
# RabbitMQ lifecycle + CRUD endpoints
# ---------------------------------------------------------------------------

async def _rmq_startup(app: web.Application) -> None:
    """Open persistent RabbitMQ connection and drain stale messages."""
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    try:
        app["rmq_conn"] = await aio_pika.connect_robust(rmq_url)
        app["rmq_channel"] = await app["rmq_conn"].channel()
        logger.info("RabbitMQ connected: %s", rmq_url.split("@")[-1])

        # Drain stale messages from response queues (once at startup)
        ch = app["rmq_channel"]
        for q_name in ("crm.user.confirmed", "crm.user.updated", "crm.user.deactivated"):
            q = await ch.declare_queue(q_name, durable=True)
            drained = 0
            while True:
                msg = await q.get(fail=False)
                if not msg:
                    break
                await msg.ack()
                drained += 1
            if drained:
                logger.info("Drained %d stale message(s) from %s", drained, q_name)
    except Exception as exc:
        logger.warning("RabbitMQ connection failed: %s — CRUD buttons will be disabled", exc)
        app["rmq_conn"] = None
        app["rmq_channel"] = None


async def _rmq_cleanup(app: web.Application) -> None:
    conn = app.get("rmq_conn")
    if conn and not conn.is_closed:
        await conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _poll_response(queue: aio_pika.abc.AbstractQueue) -> bytes | None:
    """Poll queue every 500ms for up to POLL_TIMEOUT seconds."""
    elapsed = 0.0
    while elapsed < POLL_TIMEOUT:
        msg = await queue.get(fail=False)
        if msg:
            await msg.ack()
            return msg.body
        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    return None


async def handle_crud_create(request: web.Request) -> web.Response:
    ch = request.app.get("rmq_channel")
    if ch is None:
        return web.json_response({"ok": False, "error": "RabbitMQ not connected"}, status=503)

    r = random.randint(10000, 99999)
    email = f"demo.user.{r}@shiftfestival.be"
    reg_id = f"REG-DEMO-{r}"

    xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>{reg_id}</registrationId>
    <firstName>Shift{r}</firstName>
    <lastName>Deelnemer{r}</lastName>
    <email>{email}</email>
    <sessionId>SESS-SHIFT-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+3247{r}</phone>
</Registration>""".encode("utf-8")

    response_q = await ch.declare_queue("crm.user.confirmed", durable=True)
    await ch.default_exchange.publish(
        aio_pika.Message(body=xml, delivery_mode=DeliveryMode.PERSISTENT),
        routing_key="frontend.registration.created",
    )

    body = await _poll_response(response_q)
    if body:
        _last_contact.update({"email": email, "reg_id": reg_id, "r": r})
        _sf_cache["expires"] = 0  # force refresh
        _events.appendleft({"type": "CREATE", "contract": "C13", "email": email, "timestamp": _now()})
        return web.json_response({"ok": True, "email": email, "xml": body.decode(errors="replace")})

    return web.json_response({"ok": False, "error": "Timeout waiting for crm.user.confirmed"}, status=504)


async def handle_crud_update(request: web.Request) -> web.Response:
    ch = request.app.get("rmq_channel")
    if ch is None:
        return web.json_response({"ok": False, "error": "RabbitMQ not connected"}, status=503)
    if not _last_contact:
        return web.json_response({"ok": False, "error": "No contact created yet"}, status=400)

    email = _last_contact["email"]
    reg_id = _last_contact["reg_id"]
    r = _last_contact["r"]

    xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>{reg_id}</registrationId>
    <email>{email}</email>
    <sessionId>SESS-SHIFT-001</sessionId>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Updated{r}</firstName>
        <lastName>Gewijzigd{r}</lastName>
        <phone>+3249{r}</phone>
    </updatedFields>
</RegistrationChange>""".encode("utf-8")

    response_q = await ch.declare_queue("crm.user.updated", durable=True)
    await ch.default_exchange.publish(
        aio_pika.Message(body=xml, delivery_mode=DeliveryMode.PERSISTENT),
        routing_key="frontend.registration.updated",
    )

    body = await _poll_response(response_q)
    if body:
        _sf_cache["expires"] = 0
        _events.appendleft({"type": "UPDATE", "contract": "C18", "email": email, "timestamp": _now()})
        return web.json_response({"ok": True, "email": email, "xml": body.decode(errors="replace")})

    return web.json_response({"ok": False, "error": "Timeout waiting for crm.user.updated"}, status=504)


async def handle_crud_delete(request: web.Request) -> web.Response:
    ch = request.app.get("rmq_channel")
    if ch is None:
        return web.json_response({"ok": False, "error": "RabbitMQ not connected"}, status=503)
    if not _last_contact:
        return web.json_response({"ok": False, "error": "No contact created yet"}, status=400)

    email = _last_contact["email"]
    reg_id = _last_contact["reg_id"]

    xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>{reg_id}</registrationId>
    <email>{email}</email>
    <sessionId>SESS-SHIFT-001</sessionId>
    <changeType>cancelled</changeType>
</RegistrationChange>""".encode("utf-8")

    response_q = await ch.declare_queue("crm.user.deactivated", durable=True)
    await ch.default_exchange.publish(
        aio_pika.Message(body=xml, delivery_mode=DeliveryMode.PERSISTENT),
        routing_key="frontend.registration.updated",
    )

    body = await _poll_response(response_q)
    if body:
        _last_contact.clear()
        _sf_cache["expires"] = 0
        _events.appendleft({"type": "DELETE", "contract": "C22", "email": email, "timestamp": _now()})
        return web.json_response({"ok": True, "email": email, "xml": body.decode(errors="replace")})

    return web.json_response({"ok": False, "error": "Timeout waiting for crm.user.deactivated"}, status=504)


# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(_rmq_startup)
    app.on_cleanup.append(_rmq_cleanup)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/health", handle_health)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/contacts", handle_contacts)
    app.router.add_get("/api/events", handle_get_events)
    app.router.add_post("/api/events", handle_post_event)
    app.router.add_post("/api/crud/create", handle_crud_create)
    app.router.add_post("/api/crud/update", handle_crud_update)
    app.router.add_post("/api/crud/delete", handle_crud_delete)
    return app


if __name__ == "__main__":
    logger.info("CRM Dashboard starting on http://localhost:%d", PORT)
    web.run_app(create_app(), port=PORT)
