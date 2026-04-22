"""Docker integration test — Contract 2: frontend.registration.updated.

Prerequisites:
  - docker compose up (CRM + RabbitMQ running)
  - A test contact must already exist in Salesforce (created via docker_test_reg.py)

Usage:
  python scripts/docker_test_reg_update.py
"""

import asyncio
import os
import random

import aio_pika
from aio_pika import ExchangeType
from dotenv import load_dotenv

load_dotenv()


async def main():
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    connection = await aio_pika.connect_robust(rmq_url)
    async with connection:
        channel = await connection.channel()
        user_exchange = await channel.declare_exchange("user.topic", ExchangeType.TOPIC, durable=True)
        contact_exchange = await channel.declare_exchange(
            "contact.topic", ExchangeType.TOPIC, durable=True,
        )

        # Exclusive observation queues — auto-deleted on disconnect, isolated
        # from real consumer queues that other teams may maintain.
        updated_q = await channel.declare_queue(
            "crm.debug.docker-test.user.updated", exclusive=True, auto_delete=True,
        )
        await updated_q.bind(contact_exchange, routing_key="crm.user.updated")
        deactivated_q = await channel.declare_queue(
            "crm.debug.docker-test.user.deactivated", exclusive=True, auto_delete=True,
        )
        await deactivated_q.bind(contact_exchange, routing_key="crm.user.deactivated")

        r = random.randint(1000, 9999)
        email = f"docker.test.user.{r}@example.com"

        # --- Step 1: Create a registration first (so the contact exists) ---
        reg_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-UPD-TEST-{r}</registrationId>
    <firstName>Docker</firstName>
    <lastName>TestUser</lastName>
    <email>{email}</email>
    <sessionId>SESS-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+32412345678</phone>
</Registration>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=reg_xml, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.created",
        )
        print(f"[1/4] Published registration for {email}")
        print("      Waiting 8 seconds for CRM to create contact in Salesforce...")
        await asyncio.sleep(8)

        # Observe crm.user.confirmed so we know CRM received our registration.
        # Exclusive queue — auto-deleted on disconnect to avoid collision with
        # other teams' real consumer queues on contact.topic.
        confirmed_q = await channel.declare_queue(
            "crm.debug.docker-test.user.confirmed", exclusive=True, auto_delete=True,
        )
        await confirmed_q.bind(contact_exchange, routing_key="crm.user.confirmed")
        confirmed_msg = await confirmed_q.get(fail=False)
        if confirmed_msg:
            await confirmed_msg.ack()
            print("      ✓ Contact created (crm.user.confirmed received)")
        else:
            print("      ⚠ No crm.user.confirmed received — update/cancel may fail")

        # --- Step 2: Send update ---
        update_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>REG-UPD-TEST-{r}</registrationId>
    <email>{email}</email>
    <sessionId>SESS-001</sessionId>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>UpdatedDocker</firstName>
        <lastName>UpdatedTestUser</lastName>
        <phone>+32499999999</phone>
    </updatedFields>
</RegistrationChange>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=update_xml, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.updated",
        )
        print(f"\n[2/4] Published update for {email}")
        print("      Waiting 6 seconds for CRM to process...")
        await asyncio.sleep(6)

        msg = await updated_q.get(fail=False)
        if msg:
            print("      ✓ Received crm.user.updated:")
            print(f"        {msg.body.decode()[:200]}")
            await msg.ack()
        else:
            print("      ✗ No crm.user.updated received. Check docker logs crm.")

        # --- Step 3: Send cancellation ---
        cancel_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>{email}</email>
    <sessionId>SESS-001</sessionId>
    <changeType>cancelled</changeType>
</RegistrationChange>""".encode("utf-8")

        await user_exchange.publish(
            aio_pika.Message(body=cancel_xml, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.updated",
        )
        print(f"\n[3/4] Published cancellation for {email}")
        print("      Waiting 6 seconds for CRM to process...")
        await asyncio.sleep(6)

        msg = await deactivated_q.get(fail=False)
        if msg:
            print("      ✓ Received crm.user.deactivated:")
            print(f"        {msg.body.decode()}")
            await msg.ack()
        else:
            print("      ✗ No crm.user.deactivated received. Check docker logs crm.")

        print("\n[4/4] Integration test complete.")


if __name__ == "__main__":
    asyncio.run(main())
