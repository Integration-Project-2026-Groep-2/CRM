import asyncio
import os
import random

import aio_pika
from dotenv import load_dotenv

load_dotenv()


async def main():
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    connection = await aio_pika.connect_robust(rmq_url)
    async with connection:
        channel = await connection.channel()

        confirmed_q = await channel.declare_queue("crm.user.confirmed", durable=True, exclusive=True)

        # Generate a unique email every run
        r = random.randint(1000, 9999)
        email = f"docker.test.user.{r}@example.com"

        xml_payload = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-TEST-DOCKER-{r}</registrationId>
    <firstName>Docker</firstName>
    <lastName>TestUser</lastName>
    <email>{email}</email>
    <sessionId>SESS-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+32412345678</phone>
</Registration>""".encode("utf-8")

        await channel.default_exchange.publish(
            aio_pika.Message(body=xml_payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.created",
        )
        print(f"Published registration to frontend.registration.created with email {email}")

        print("Waiting 6 seconds for CRM to process and Salesforce to respond...")
        await asyncio.sleep(6)

        msg = await confirmed_q.get(fail=False)
        if msg:
            print("Received confirmation on crm.user.confirmed:")
            print(msg.body.decode())
            await msg.ack()
        else:
            print("Did not receive a confirmation message. Check docker logs crm.")


if __name__ == "__main__":
    asyncio.run(main())
