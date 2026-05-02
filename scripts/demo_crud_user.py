"""Demo script — Full User CRUD Flow via RabbitMQ + Salesforce.

Demonstrates CREATE -> UPDATE -> DELETE with XML messages on RabbitMQ,
processed by the CRM container, with Salesforce as backend.

Prerequisites:
  - CRM container running (docker compose up OR deployed on VM)
  - RabbitMQ reachable (RABBITMQ_URL in .env)
  - Salesforce credentials configured

Usage:
  python scripts/demo_crud_user.py
"""

import asyncio
import os
import random
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen

import aio_pika
from aio_pika import DeliveryMode, ExchangeType
from dotenv import load_dotenv
from lxml import etree

load_dotenv()

POLL_INTERVAL = 0.5
POLL_TIMEOUT = 15.0
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8050")


def _push_event(event_type: str, contract: str, email: str) -> None:
    """Push event to the dashboard (best-effort, no failure on error)."""
    import json

    payload = json.dumps({
        "type": event_type,
        "contract": contract,
        "email": email,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }).encode()
    try:
        req = Request(
            f"{DASHBOARD_URL}/api/events",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urlopen(req, timeout=2)
    except Exception:
        pass  # dashboard may not be running
BANNER = """
============================================================
  CRM DEMO  --  Volledige User CRUD Flow
  Desideriushogeschool  --  ShiftFestival
  {}
============================================================
"""


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _pretty_xml(body: bytes, indent: int = 6) -> str:
    """Parse XML bytes and return indented, human-readable string."""
    try:
        root = etree.fromstring(body)
        etree.indent(root, space="  ")
        lines = etree.tostring(root, encoding="unicode", pretty_print=True).strip().splitlines()
        pad = " " * indent
        return "\n".join(f"{pad}{line}" for line in lines)
    except Exception:
        return " " * indent + body.decode(errors="replace")


async def _wait_for_message(queue: aio_pika.abc.AbstractQueue, label: str) -> bytes | None:
    """Poll queue every 500ms for up to POLL_TIMEOUT seconds."""
    print("      Waiting for CRM response", end="", flush=True)
    elapsed = 0.0
    while elapsed < POLL_TIMEOUT:
        msg = await queue.get(fail=False)
        if msg:
            await msg.ack()
            print()
            return msg.body
        print(".", end="", flush=True)
        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    print()
    print(f"      X  No {label} received after {POLL_TIMEOUT:.0f}s. Check: docker logs crm")
    return None


async def main() -> None:
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    print(BANNER.format(_now()))
    print(f"  RabbitMQ: {rmq_url.split('@')[-1] if '@' in rmq_url else rmq_url}")
    print()

    connection = await aio_pika.connect_robust(rmq_url)
    async with connection:
        channel = await connection.channel()
        user_exchange = await channel.declare_exchange("user.topic", ExchangeType.TOPIC, durable=True)
        contact_exchange = await channel.declare_exchange(
            "contact.topic", ExchangeType.TOPIC, durable=True,
        )

        # Exclusive+auto_delete observation queues bound to contact.topic.
        # Prevents collision with other teams' real consumer queues that may
        # share the crm.user.* routing keys on contact.topic.
        confirmed_q = await channel.declare_queue(
            "crm.debug.demo.user.confirmed", exclusive=True, auto_delete=True,
        )
        await confirmed_q.bind(contact_exchange, routing_key="crm.user.confirmed")
        updated_q = await channel.declare_queue(
            "crm.debug.demo.user.updated", exclusive=True, auto_delete=True,
        )
        await updated_q.bind(contact_exchange, routing_key="crm.user.updated")
        deactivated_q = await channel.declare_queue(
            "crm.debug.demo.user.deactivated", exclusive=True, auto_delete=True,
        )
        await deactivated_q.bind(contact_exchange, routing_key="crm.user.deactivated")

        r = random.randint(10000, 99999)
        email = f"demo.user.{r}@shiftfestival.be"
        reg_id = f"REG-DEMO-{r}"
        first_name = f"Shift{r}"
        last_name = f"Deelnemer{r}"
        results: list[bool] = []

        # ── STEP 1: CREATE ─────────────────────────────────────────
        print("[1/5] CREATE -- Nieuwe inschrijving (C1 -> C13)")
        print(f"      Email: {email}")
        print("      Publishing to: frontend.registration.created")

        create_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>{reg_id}</registrationId>
    <firstName>{first_name}</firstName>
    <lastName>{last_name}</lastName>
    <email>{email}</email>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+3247{r}</phone>
</Registration>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=create_xml, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.created",
        )

        body = await _wait_for_message(confirmed_q, "crm.user.confirmed")
        if body:
            print("      OK  crm.user.confirmed ontvangen (C13)")
            print(_pretty_xml(body))
            _push_event("CREATE", "C13", email)
            results.append(True)
        else:
            results.append(False)

        print()

        # ── STEP 2: UPDATE ─────────────────────────────────────────
        print("[2/5] UPDATE -- Wijzig deelnemerdata (C2 -> C18)")
        print("      Publishing to: frontend.registration.updated (changeType=updated)")

        update_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>{reg_id}</registrationId>
    <email>{email}</email>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Updated{r}</firstName>
        <lastName>Gewijzigd{r}</lastName>
        <phone>+3249{r}</phone>
    </updatedFields>
</RegistrationChange>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=update_xml, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.updated",
        )

        body = await _wait_for_message(updated_q, "crm.user.updated")
        if body:
            print("      OK  crm.user.updated ontvangen (C18)")
            _push_event("UPDATE", "C18", email)
            results.append(True)
        else:
            results.append(False)

        print()

        # ── STEP 3: VERIFY UPDATE ──────────────────────────────────
        print("[3/5] VERIFY -- Gewijzigde velden in C18 response")
        if body:
            root = etree.fromstring(body)
            print(f"      id:        {root.findtext('id', 'N/A')}")
            print(f"      email:     {root.findtext('email', 'N/A')}")
            print(f"      firstName: {root.findtext('firstName', 'N/A')}  (was: {first_name})")
            print(f"      lastName:  {root.findtext('lastName', 'N/A')}  (was: {last_name})")
            print(f"      isActive:  {root.findtext('isActive', 'N/A')}")
            print(f"      updatedAt: {root.findtext('updatedAt', 'N/A')}")
        else:
            print("      (overgeslagen -- geen C18 response ontvangen)")

        print()

        # ── STEP 4: DELETE ─────────────────────────────────────────
        print("[4/5] DELETE -- Annuleer inschrijving (C2 -> C22)")
        print("      Publishing to: frontend.registration.updated (changeType=cancelled)")

        cancel_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>{reg_id}</registrationId>
    <email>{email}</email>
    <changeType>cancelled</changeType>
</RegistrationChange>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=cancel_xml, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.updated",
        )

        body = await _wait_for_message(deactivated_q, "crm.user.deactivated")
        if body:
            print("      OK  crm.user.deactivated ontvangen (C22)")
            print(_pretty_xml(body))
            _push_event("DELETE", "C22", email)
            results.append(True)
        else:
            results.append(False)

        print()

        # ── STEP 5: SUMMARY ───────────────────────────────────────
        labels = ["CREATE", "UPDATE", "DELETE"]
        contracts = ["C1 -> C13", "C2 -> C18", "C2 -> C22"]
        print("[5/5] SAMENVATTING")
        print()
        for label, contract, ok in zip(labels, contracts, results):
            status = "OK" if ok else "FAIL"
            print(f"      {label:8s}  {contract:12s}  {status}")

        passed = sum(results)
        total = len(results)
        print()
        print(f"      Resultaat: {passed}/{total} CRUD operaties succesvol")
        print()
        print("=" * 60)

        if passed < total:
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
