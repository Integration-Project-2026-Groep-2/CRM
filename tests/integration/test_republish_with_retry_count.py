"""Integration test against a real RabbitMQ broker.

Verifies that `_republish_with_retry_count` actually works end-to-end with a
real aio-pika connection. This is the regression test for the production bug
seen on 2026-04-20 where the function threw
`AttributeError: 'Channel' object has no attribute 'declare_exchange'`
because the mocked unit tests did not match aio-pika's real API surface
(IncomingMessage.channel returns raw aiormq.Channel, not the wrapper).

Requirements:
- A RabbitMQ broker reachable via the `CRM_TEST_RABBITMQ_URL` env var
  (default: `amqp://guest:guest@localhost:5675/`). Spin one up with:
      docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13
  or reuse any dev broker by setting the env var.

The test is skipped automatically when the broker is unreachable so unit-test
only CI runs are not broken, but it is REQUIRED before shipping any change
that touches message republishing.
"""

import asyncio
import os
import uuid

import aio_pika
import pytest
from aio_pika import ExchangeType

RABBITMQ_URL = os.getenv("CRM_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5675/")


async def _broker_reachable() -> bool:
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=2)
    except Exception:  # noqa: BLE001
        return False
    await connection.close()
    return True


@pytest.fixture
def _skip_if_no_broker():
    if not asyncio.run(_broker_reachable()):
        pytest.skip(
            f"RabbitMQ not reachable at {RABBITMQ_URL}. Start a local broker and retry.",
        )


@pytest.mark.asyncio
async def test_republish_through_real_aio_pika_channel(_skip_if_no_broker):
    """End-to-end: publish → consume → republish → consume again, with real broker."""
    from src.receiver import _republish_with_retry_count

    exchange_name = f"test-republish-{uuid.uuid4().hex[:8]}"
    queue_name = f"test-republish-q-{uuid.uuid4().hex[:8]}"
    routing_key = "test.republish"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            exchange_name, type=ExchangeType.TOPIC, durable=False, auto_delete=True,
        )
        queue = await channel.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key=routing_key)

        # Publish an initial message (no x-retry-count header yet).
        await exchange.publish(
            aio_pika.Message(
                body=b"<Test/>",
                headers={"custom": "keep-me"},
                content_type="application/xml",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )

        # First delivery: consume, then call _republish_with_retry_count.
        first = await queue.get(timeout=5)
        assert first is not None
        assert first.body == b"<Test/>"
        assert (first.headers or {}).get("x-retry-count") is None

        await _republish_with_retry_count(first, new_count=1)

        # Second delivery: fresh copy with x-retry-count=1 should now be in queue.
        second = await queue.get(timeout=5, fail=False)
        assert second is not None, "republished message did not come back to queue"
        assert second.body == b"<Test/>"
        assert second.headers["x-retry-count"] == 1
        assert second.headers["custom"] == "keep-me"
        await second.ack()

        # Third republish: counter bumps to 5 (simulating later attempts).
        await exchange.publish(
            aio_pika.Message(
                body=b"<Bump/>",
                headers={"x-retry-count": 4},
                content_type="application/xml",
            ),
            routing_key=routing_key,
        )
        bumped = await queue.get(timeout=5)
        assert bumped is not None
        await _republish_with_retry_count(bumped, new_count=5)

        after_bump = await queue.get(timeout=5, fail=False)
        assert after_bump is not None
        assert after_bump.headers["x-retry-count"] == 5
        await after_bump.ack()

    finally:
        await connection.close()
