"""Integration tests for the TTL-DLX failure topology against a real broker.

Asserts that:
1. `_ensure_dlq_topology` declares `crm.retry`, `crm.dlq` and `crm.dlq.queue`
   idempotently and binds the ops-queue with routing-key `#`.
2. A queue declared with `x-dead-letter-exchange="crm.dlq"` plus a
   `reject(requeue=False)` actually routes the message to `crm.dlq.queue`
   with all custom headers preserved and an `x-death` entry appended by
   RabbitMQ.

Requires a RabbitMQ broker reachable via `CRM_TEST_RABBITMQ_URL` (default
`amqp://guest:guest@localhost:5675/`). Skips silently when unreachable so
unit-only CI does not break, but is REQUIRED before shipping any change to
the failure topology.
"""

import asyncio
import os
import uuid

import aio_pika
import pytest
from aio_pika import ExchangeType

from src.receiver import (
    _DLQ_EXCHANGE,
    _DLQ_OPS_QUEUE,
    _RETRY_EXCHANGE,
    _ensure_dlq_topology,
)

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
async def test_ensure_dlq_topology_is_idempotent(_skip_if_no_broker):
    """Calling `_ensure_dlq_topology` twice on the same channel must not raise."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await _ensure_dlq_topology(channel)
        await _ensure_dlq_topology(channel)

        retry_ex = await channel.declare_exchange(
            _RETRY_EXCHANGE, ExchangeType.TOPIC, durable=True, passive=True,
        )
        dlq_ex = await channel.declare_exchange(
            _DLQ_EXCHANGE, ExchangeType.TOPIC, durable=True, passive=True,
        )
        dlq_q = await channel.declare_queue(_DLQ_OPS_QUEUE, durable=True, passive=True)
        assert retry_ex.name == _RETRY_EXCHANGE
        assert dlq_ex.name == _DLQ_EXCHANGE
        assert dlq_q.name == _DLQ_OPS_QUEUE
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_rejected_message_routes_to_dlq_with_headers_preserved(_skip_if_no_broker):
    """Source queue rejects → message lands in `crm.dlq.queue` with x-death + custom headers."""
    work_exchange_name = f"test-dlx-src-{uuid.uuid4().hex[:8]}"
    work_queue_name = f"test-dlx-work-{uuid.uuid4().hex[:8]}"
    routing_key = "test.dlx.routing"
    correlation_marker = uuid.uuid4().hex

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await _ensure_dlq_topology(channel)

        work_exchange = await channel.declare_exchange(
            work_exchange_name, ExchangeType.TOPIC, durable=False, auto_delete=True,
        )
        work_queue = await channel.declare_queue(
            work_queue_name,
            durable=False,
            auto_delete=True,
            arguments={"x-dead-letter-exchange": _DLQ_EXCHANGE},
        )
        await work_queue.bind(work_exchange, routing_key=routing_key)

        # Snapshot the ops-queue depth before publishing so other parallel tests
        # leaking into crm.dlq.queue do not poison the assertion. We poll for
        # *our* correlation_marker rather than just length += 1.
        ops_queue = await channel.declare_queue(_DLQ_OPS_QUEUE, durable=True, passive=True)

        await work_exchange.publish(
            aio_pika.Message(
                body=b"<Probe/>",
                headers={
                    "x-correlation-marker": correlation_marker,
                    "x-error": "processing-error",
                    "x-error-class": "ValueError",
                    "x-original-routing-key": routing_key,
                },
                content_type="application/xml",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )

        delivered = await work_queue.get(timeout=5)
        assert delivered is not None
        assert delivered.body == b"<Probe/>"
        await delivered.reject(requeue=False)

        dead = None
        for _ in range(20):
            candidate = await ops_queue.get(timeout=1, fail=False)
            if candidate is None:
                await asyncio.sleep(0.1)
                continue
            if (candidate.headers or {}).get("x-correlation-marker") == correlation_marker:
                dead = candidate
                break
            await candidate.reject(requeue=True)

        assert dead is not None, "rejected message did not reach crm.dlq.queue"
        assert dead.body == b"<Probe/>"
        assert dead.headers["x-correlation-marker"] == correlation_marker
        assert dead.headers["x-error"] == "processing-error"
        assert dead.headers["x-error-class"] == "ValueError"
        assert dead.headers["x-original-routing-key"] == routing_key

        x_death = dead.headers.get("x-death")
        assert x_death, "RabbitMQ did not annotate x-death on dead-lettered message"
        assert x_death[0]["queue"] == work_queue_name
        assert x_death[0]["reason"] == "rejected"
        await dead.ack()
    finally:
        await connection.close()
