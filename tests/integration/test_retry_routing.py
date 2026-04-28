"""End-to-end integration test for the TTL-DLX retry topology.

Validates Lucas's pattern (TL Facturatie/Mailing): a `<work>.retry` queue with
`x-message-ttl` and a DLX back to the producer-exchange + producer-rk re-delivers
the message to the work-queue once the TTL expires.

Requires a RabbitMQ broker reachable via `CRM_TEST_RABBITMQ_URL`.
"""

import asyncio
import os
import uuid

import aio_pika
import pytest
from aio_pika import ExchangeType

from src.handlers._transport import _RETRY_EXCHANGE, _publish_to_retry_exchange

RABBITMQ_URL = os.getenv("CRM_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5675/")
RETRY_TTL_MS = 500  # short TTL so the test re-delivers in under a second


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
async def test_retry_queue_redelivers_to_work_queue_after_ttl(_skip_if_no_broker):
    """Publish to <work>.retry → wait TTL → message reappears on work-queue with x-death."""
    suffix = uuid.uuid4().hex[:8]
    producer_exchange_name = f"test-retry-src-{suffix}"
    work_queue_name = f"test-retry-work-{suffix}"
    retry_queue_name = f"{work_queue_name}.retry"
    producer_rk = "test.retry.routing"
    correlation_marker = uuid.uuid4().hex

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()

        producer_exchange = await channel.declare_exchange(
            producer_exchange_name, ExchangeType.TOPIC, durable=False, auto_delete=True,
        )
        work_queue = await channel.declare_queue(
            work_queue_name, durable=False, auto_delete=True,
        )
        await work_queue.bind(producer_exchange, routing_key=producer_rk)

        retry_queue = await channel.declare_queue(
            retry_queue_name,
            durable=False,
            auto_delete=True,
            arguments={
                "x-message-ttl": RETRY_TTL_MS,
                "x-dead-letter-exchange": producer_exchange_name,
                "x-dead-letter-routing-key": producer_rk,
            },
        )
        retry_exchange = await channel.declare_exchange(
            _RETRY_EXCHANGE, ExchangeType.DIRECT, durable=True,
        )
        await retry_queue.bind(retry_exchange, routing_key=f"{work_queue_name}.retry")

        # Publish a probe directly to the work-queue, then ack-and-republish through
        # _publish_to_retry_exchange so the headers match what the production code emits.
        await producer_exchange.publish(
            aio_pika.Message(
                body=b"<Probe/>",
                headers={"x-correlation-marker": correlation_marker},
                content_type="application/xml",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=producer_rk,
        )

        first = await work_queue.get(timeout=5)
        assert first is not None
        assert first.body == b"<Probe/>"
        assert first.headers["x-correlation-marker"] == correlation_marker

        await _publish_to_retry_exchange(
            first,
            work_queue=work_queue_name,
            next_retry_count=1,
            error_tag="missing-CRM_ID__c",
            extra_headers={"x-missing-CRM_ID__c": "uuid-probe"},
        )

        # Poll the work-queue for re-delivery once the TTL expires.
        redelivered = None
        for _ in range(40):
            candidate = await work_queue.get(timeout=1, fail=False)
            if candidate is None:
                await asyncio.sleep(0.1)
                continue
            if (candidate.headers or {}).get("x-correlation-marker") == correlation_marker:
                redelivered = candidate
                break
            await candidate.reject(requeue=True)

        assert redelivered is not None, "retry-queue did not redeliver to work-queue"
        assert redelivered.body == b"<Probe/>"
        assert redelivered.headers["x-correlation-marker"] == correlation_marker
        assert redelivered.headers["x-retry-count"] == 1
        assert redelivered.headers["x-error"] == "missing-CRM_ID__c"
        assert redelivered.headers["x-missing-CRM_ID__c"] == "uuid-probe"
        assert redelivered.headers["x-original-routing-key"] == producer_rk
        assert redelivered.headers["x-retry-queue"] == work_queue_name

        x_death = redelivered.headers.get("x-death")
        assert x_death, "RabbitMQ did not annotate x-death on TTL-expired message"
        assert x_death[0]["queue"] == retry_queue_name
        assert x_death[0]["reason"] == "expired"
        await redelivered.ack()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_publish_to_retry_exchange_acks_original(_skip_if_no_broker):
    """Sanity: _publish_to_retry_exchange ack's the source message."""
    suffix = uuid.uuid4().hex[:8]
    work_queue_name = f"test-retry-ack-{suffix}"
    retry_queue_name = f"{work_queue_name}.retry"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()

        # Publish-and-fetch a probe so we have a real IncomingMessage to ack.
        ephemeral_queue = await channel.declare_queue(
            work_queue_name, durable=False, auto_delete=True,
        )
        ephemeral_exchange = await channel.declare_exchange(
            f"test-retry-ack-ex-{suffix}",
            ExchangeType.DIRECT,
            durable=False,
            auto_delete=True,
        )
        await ephemeral_queue.bind(ephemeral_exchange, routing_key=work_queue_name)

        retry_queue = await channel.declare_queue(
            retry_queue_name,
            durable=False,
            auto_delete=True,
            arguments={
                "x-message-ttl": 60_000,  # long enough that we observe before TTL fires
                "x-dead-letter-exchange": ephemeral_exchange.name,
                "x-dead-letter-routing-key": work_queue_name,
            },
        )
        retry_exchange = await channel.declare_exchange(
            _RETRY_EXCHANGE, ExchangeType.DIRECT, durable=True,
        )
        await retry_queue.bind(retry_exchange, routing_key=f"{work_queue_name}.retry")

        await ephemeral_exchange.publish(
            aio_pika.Message(body=b"<Sanity/>"),
            routing_key=work_queue_name,
        )
        msg = await ephemeral_queue.get(timeout=5)
        assert msg is not None

        await _publish_to_retry_exchange(
            msg, work_queue=work_queue_name, next_retry_count=1,
            error_tag="processing-error",
        )

        # Original is acked (no second delivery on the work-queue) and a copy lives
        # in the retry-queue.
        assert await ephemeral_queue.get(timeout=1, fail=False) is None
        retry_msg = await retry_queue.get(timeout=2)
        assert retry_msg is not None
        assert retry_msg.body == b"<Sanity/>"
        assert retry_msg.headers["x-retry-count"] == 1
        await retry_msg.reject(requeue=False)
    finally:
        await connection.close()
