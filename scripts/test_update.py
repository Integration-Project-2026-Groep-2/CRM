"""Test UPDATE — Stuurt C2 (changeType=updated), wacht op C18 user.updated.

Usage:
  python scripts/test_update.py <email> <reg_id>
  docker exec crm python scripts/test_update.py <email> <reg_id>

Example:
  docker exec crm python scripts/test_update.py demo.user.12345@shiftfestival.be REG-DEMO-12345
"""

import asyncio
import os
import sys

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
    if len(sys.argv) < 3:
        print("Usage: python scripts/test_update.py <email> <reg_id>")
        print("  Run test_create.py first to get these values.")
        raise SystemExit(1)

    email = sys.argv[1]
    reg_id = sys.argv[2]
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    print("=== TEST UPDATE (C2 -> C18) ===")
    print(f"  Email:  {email}")
    print(f"  RegID:  {reg_id}")
    print()

    connection = await aio_pika.connect_robust(rmq_url)
    async with connection:
        channel = await connection.channel()
        user_exchange = await channel.declare_exchange("user.topic", ExchangeType.TOPIC, durable=True)
        outbound_exchange = await channel.declare_exchange("contact.topic", ExchangeType.TOPIC, durable=True)
        updated_q = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await updated_q.bind(outbound_exchange, routing_key="crm.user.updated")

        xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>{reg_id}</registrationId>
    <email>{email}</email>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Updated</firstName>
        <lastName>Gewijzigd</lastName>
        <phone>+32499999999</phone>
    </updatedFields>
</RegistrationChange>""".encode("utf-8")

        print("  Publishing to: frontend.registration.updated (changeType=updated)")
        print("  Listening on:  contact.topic -> crm.user.updated (temporary test queue)")
        await user_exchange.publish(
            aio_pika.Message(body=xml, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.updated",
        )

        body = await _wait_for_message(updated_q, "crm.user.updated", email)
        if body:
            print("  OK  crm.user.updated ontvangen (C18)")
            print()
            root = etree.fromstring(body)
            print(f"  id:        {root.findtext('id', 'N/A')}")
            print(f"  email:     {root.findtext('email', 'N/A')}")
            print(f"  firstName: {root.findtext('firstName', 'N/A')}")
            print(f"  lastName:  {root.findtext('lastName', 'N/A')}")
            print(f"  isActive:  {root.findtext('isActive', 'N/A')}")
            print(f"  updatedAt: {root.findtext('updatedAt', 'N/A')}")
            print()
            print("  === VOOR DELETE ===")
            print(f"  docker exec crm python scripts/test_delete.py {email} {reg_id}")
        else:
            print(f"  FAIL  Geen response na {POLL_TIMEOUT:.0f}s")
            print("  Verwacht outbound via: contact.topic -> crm.user.updated")
            print("  Check: docker logs crm --tail 20")
            raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
