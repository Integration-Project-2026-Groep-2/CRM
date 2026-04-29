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

        r = random.randint(100000, 999999)
        email = f"docker.test.user.{r}@example.com"
        first_name = f"Dock{r}"
        last_name = f"Test{r}"
        phone = f"+32{random.randint(400000000, 499999999)}"

        xml_payload = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-TEST-DOCKER-{r}</registrationId>
    <firstName>{first_name}</firstName>
    <lastName>{last_name}</lastName>
    <email>{email}</email>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>{phone}</phone>
</Registration>""".encode("utf-8")

        exchange = await channel.declare_exchange(
            "user.topic", type=ExchangeType.TOPIC, durable=True,
        )
        await exchange.publish(
            aio_pika.Message(body=xml_payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="frontend.registration.created",
        )
        # Poll queues removed
        await asyncio.sleep(5)

        # NO CONSUMING SO IT STAYS IN THE QUEUE
        print("\n\nTest run finished. The message crm.mail.requested is left in RabbitMQ for you to inspect!")


if __name__ == "__main__":
    asyncio.run(main())
