"""Test CREATE — Stuurt C1 registration, wacht op C13 user.confirmed.

Usage:
  python scripts/test_create.py
  docker exec crm python scripts/test_create.py

Prints het email adres dat je nodig hebt voor test_update.py en test_delete.py.
"""

import asyncio
import os
import random

import aio_pika
from aio_pika import DeliveryMode, ExchangeType
from dotenv import load_dotenv
from lxml import etree

load_dotenv()

POLL_INTERVAL = 0.5
POLL_TIMEOUT = 15.0


def _pretty_xml(body: bytes) -> str:
    try:
        root = etree.fromstring(body)
        etree.indent(root, space="  ")
        return etree.tostring(root, encoding="unicode", pretty_print=True).strip()
    except Exception:
        return body.decode(errors="replace")


def _message_matches_email(body: bytes, expected_email: str) -> bool:
    try:
        root = etree.fromstring(body)
    except Exception:
        return False
    return root.findtext("email") == expected_email


async def _wait_for_message(
    queue: aio_pika.abc.AbstractQueue,
    label: str,
    expected_email: str,
) -> bytes | None:
    print(f"  Waiting for {label}", end="", flush=True)
    elapsed = 0.0
    while elapsed < POLL_TIMEOUT:
        msg = await queue.get(fail=False)
        if msg:
            body = msg.body
            await msg.ack()
            if _message_matches_email(body, expected_email):
                print()
                return body
        print(".", end="", flush=True)
        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    print()
    return None


async def main() -> None:
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    r = random.randint(10000, 99999)
    email = f"demo.user.{r}@shiftfestival.be"
    reg_id = f"REG-DEMO-{r}"

    print("=== TEST CREATE (C1 -> C13) ===")
    print(f"  Email:  {email}")
    print(f"  RegID:  {reg_id}")
    print()

    connection = await aio_pika.connect_robust(rmq_url)
    async with connection:
        channel = await connection.channel()
        user_exchange = await channel.declare_exchange("user.topic", ExchangeType.TOPIC, durable=True)
        outbound_exchange = await channel.declare_exchange("contact.topic", ExchangeType.TOPIC, durable=True)
        confirmed_q = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await confirmed_q.bind(outbound_exchange, routing_key="crm.user.confirmed")

        xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>{reg_id}</registrationId>
    <firstName>Shift{r}</firstName>
    <lastName>Deelnemer{r}</lastName>
    <email>{email}</email>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+3247{r}</phone>
</Registration>""".encode("utf-8")

        print("  Publishing to: frontend.registration.created")
        print("  Listening on:  contact.topic -> crm.user.confirmed (temporary test queue)")
        await user_exchange.publish(
            aio_pika.Message(body=xml, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.created",
        )

        body = await _wait_for_message(confirmed_q, "crm.user.confirmed", email)
        if body:
            print("  OK  crm.user.confirmed ontvangen (C13)")
            print()
            print(_pretty_xml(body))
            print()
            print("  === GEBRUIK VOOR UPDATE/DELETE ===")
            print(f"  docker exec crm python scripts/test_update.py {email} {reg_id}")
            print(f"  docker exec crm python scripts/test_delete.py {email} {reg_id}")
        else:
            print(f"  FAIL  Geen response na {POLL_TIMEOUT:.0f}s")
            print("  Verwacht outbound via: contact.topic -> crm.user.confirmed")
            print("  Check: docker logs crm --tail 20")
            raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
