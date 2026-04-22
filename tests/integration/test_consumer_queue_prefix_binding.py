"""Integration test: consumer-prefixed queue binding correctness.

Proves that CRM's renamed inbound queues (with `crm.` prefix) correctly
receive messages published by producers using the ORIGINAL routing key.
This is the critical binding invariant for the queue-rename rollout:

    queue name        = crm.<producer>.<event>   (CRM-local)
    routing key (bind) = <producer>.<event>      (unchanged; producers publish this)

If any rename leaves `routing_key` pointing at the new name instead of the
old one, producers' messages stop arriving. This test catches that class
of bug before deploy.

The test uses `_declare_and_bind` from `src.receiver` directly (same code
path production runs) so any future change to that helper is validated.

Requirements:
- RabbitMQ broker reachable via `CRM_TEST_RABBITMQ_URL`
  (default: `amqp://guest:guest@localhost:5675/`).
  Spin one up with:
      docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine

Skipped automatically when the broker is unreachable.
"""

from __future__ import annotations

import asyncio
import os

import aio_pika
import pytest
from aio_pika import DeliveryMode, ExchangeType

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
            f"RabbitMQ not reachable at {RABBITMQ_URL}. "
            "Start a local broker: docker run -d --name crm-test-rabbitmq "
            "-p 5675:5672 rabbitmq:3.13-alpine",
        )


# (queue_name, producer_routing_key, exchange_name)
# Each tuple represents one of the renamed CRM consumer queues. The routing
# key is what the producer team publishes with — it must match the binding
# declared by the receiver for messages to arrive.
_RENAMED_QUEUES: list[tuple[str, str, str]] = [
    ("crm.frontend.registration.created", "frontend.registration.created", "user.topic"),
    ("crm.frontend.registration.updated", "frontend.registration.updated", "user.topic"),
    ("crm.facturatie.user.created", "facturatie.user.created", "user.topic"),
    ("crm.facturatie.user.updated", "facturatie.user.updated", "user.topic"),
    ("crm.facturatie.user.deactivated", "facturatie.user.deactivated", "user.topic"),
    ("crm.facturatie.company.created", "facturatie.company.created", "company.topic"),
    ("crm.facturatie.company.updated", "facturatie.company.updated", "company.topic"),
    ("crm.facturatie.company.deactivated", "facturatie.company.deactivated", "company.topic"),
    ("crm.mailing.user.created", "mailing.user.created", "user.topic"),
    ("crm.mailing.user.updated", "mailing.user.updated", "user.topic"),
    ("crm.mailing.user.deactivated", "mailing.user.deactivated", "user.topic"),
    ("crm.planning.user.created", "planning.user.created", "user.topic"),
    ("crm.planning.user.updated", "planning.user.updated", "user.topic"),
    ("crm.planning.user.deactivated", "planning.user.deactivated", "user.topic"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("queue_name", "routing_key", "exchange_name"), _RENAMED_QUEUES)
async def test_old_routing_key_lands_in_prefixed_queue(
    _skip_if_no_broker, queue_name: str, routing_key: str, exchange_name: str,
) -> None:
    """Producer publishes with old routing key → message arrives in new prefixed queue.

    Uses `_declare_and_bind` from src.receiver (production code path) to declare
    the consumer queue, then publishes from a separate channel using the old
    routing key. If the binding was set up wrong (e.g., routing_key left as
    queue_name), the publish would silently land nowhere.
    """
    from src.receiver import _declare_and_bind

    # Use a unique queue-name per test to avoid cross-test pollution in shared broker.
    # We override durable=False + add a test suffix so the real production queue
    # is not created in the broker during tests.
    test_queue_name = f"{queue_name}.it-{os.urandom(4).hex()}"

    consumer_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    producer_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        consumer_channel = await consumer_conn.channel()

        # Patch the global map so _declare_and_bind finds our test queue name.
        from src.receiver import _INBOUND_EXCHANGE
        _INBOUND_EXCHANGE[test_queue_name] = exchange_name
        try:
            queue = await _declare_and_bind(
                consumer_channel, test_queue_name, durable=False, routing_key=routing_key,
            )
        finally:
            _INBOUND_EXCHANGE.pop(test_queue_name, None)

        producer_channel = await producer_conn.channel()
        producer_exchange = await producer_channel.declare_exchange(
            exchange_name, type=ExchangeType.TOPIC, durable=True,
        )
        payload = f"<Probe><routingKey>{routing_key}</routingKey></Probe>".encode()
        await producer_exchange.publish(
            aio_pika.Message(body=payload, delivery_mode=DeliveryMode.NOT_PERSISTENT),
            routing_key=routing_key,
        )

        received = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            assert received.body == payload
            assert received.routing_key == routing_key
        finally:
            await received.ack()
    finally:
        await consumer_conn.close()
        await producer_conn.close()


@pytest.mark.asyncio
async def test_inbound_exchange_map_only_contains_expected_new_keys() -> None:
    """Sanity: _INBOUND_EXCHANGE has no stale old-style keys for renamed contracts.

    Guards against partial renames — if a new PR accidentally adds back
    `planning.user.created` without the `crm.` prefix, this fails fast.
    """
    from src.receiver import _INBOUND_EXCHANGE

    stale_keys = {
        "frontend.registration.created",
        "frontend.registration.updated",
        "facturatie.user.created",
        "facturatie.user.updated",
        "facturatie.user.deactivated",
        "facturatie.company.created",
        "facturatie.company.updated",
        "facturatie.company.deactivated",
        "mailing.user.created",
        "planning.user.created",
        "planning.user.updated",
        "planning.user.deactivated",
    }
    leaked = stale_keys & set(_INBOUND_EXCHANGE)
    assert not leaked, f"Stale pre-rename keys leaked back into _INBOUND_EXCHANGE: {leaked}"
