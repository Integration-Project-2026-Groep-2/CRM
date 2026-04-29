"""Integration test: C20 inbound — Mailing → CRM: bounce reported.

Contract 20
  Queue: mailing.bounce.reported | Exchange: mail.topic | Root: BounceReported
  Status: handler not yet implemented (PENDING_EXCHANGES)

Tests:
1. Broker-wiring: publish BounceReported on mail.topic, consume, validate XSD.
2. Fast sanity: _INBOUND_EXCHANGE maps mailing.bounce.reported → mail.topic.

Skipped automatically when broker is unreachable.
"""

from __future__ import annotations

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
            f"RabbitMQ not reachable at {RABBITMQ_URL}. "
            "Start a local broker: docker run -d --name crm-test-rabbitmq "
            "-p 5675:5672 rabbitmq:3.13-alpine",
        )


async def _publish_and_consume(
    *,
    exchange_name: str,
    queue_name: str,
    routing_key: str,
    xml_body: bytes,
) -> bytes:
    """Bind a fresh throwaway queue to the given exchange, publish XML, return body."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            exchange_name, type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key=routing_key)

        await exchange.publish(
            aio_pika.Message(
                body=xml_body,
                content_type="application/xml",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )

        incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            return incoming.body
        finally:
            await incoming.ack()
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# Contract 20 — inbound broker wiring + XSD validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c20_bounce_reported_hard_flows_through_mail_topic(_skip_if_no_broker):
    """C20 — publish BounceReported (hard) on mail.topic, consume it, validate XSD.

    # TODO: handler not yet implemented — add handler invocation test once
    #       src/handlers/mailing_bounce_reported.py exists.
    """
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<BounceReported>
    <email>bounce@example.com</email>
    <bounceType>hard</bounceType>
    <bouncedAt>2026-04-22T09:30:00Z</bouncedAt>
</BounceReported>"""

    received = await _publish_and_consume(
        exchange_name="mail.topic",
        queue_name=f"test-c20-{uuid.uuid4().hex[:8]}",
        routing_key="mailing.bounce.reported",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "BounceReported"
    assert doc.findtext("email") == "bounce@example.com"
    assert doc.findtext("bounceType") == "hard"


@pytest.mark.asyncio
async def test_c20_bounce_reported_soft_flows_through_mail_topic(_skip_if_no_broker):
    """C20 — bounceType=soft is accepted by the schema."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<BounceReported>
    <email>softbounce@example.com</email>
    <bounceType>soft</bounceType>
    <bouncedAt>2026-04-22T10:00:00Z</bouncedAt>
</BounceReported>"""

    received = await _publish_and_consume(
        exchange_name="mail.topic",
        queue_name=f"test-c20-soft-{uuid.uuid4().hex[:8]}",
        routing_key="mailing.bounce.reported",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "BounceReported"
    assert doc.findtext("bounceType") == "soft"


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c20_mail_topic():
    """C20 — _INBOUND_EXCHANGE maps mailing.bounce.reported → mail.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["mailing.bounce.reported"] == "mail.topic"
