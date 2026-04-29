"""Integration test: C12 inbound — IoT → CRM: badge linked.

Contract 12
  Queue: iot.badge.linked | Exchange: planning.topic | Root: BadgeLink
  Status: handler not yet implemented (PENDING_EXCHANGES)

Tests:
1. Broker-wiring: publish BadgeLink on planning.topic, consume, validate XSD.
2. Fast sanity: _INBOUND_EXCHANGE maps iot.badge.linked → planning.topic.

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
# Contract 12 — inbound broker wiring + XSD validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c12_badge_link_flows_through_planning_topic(_skip_if_no_broker):
    """C12 — publish BadgeLink on planning.topic, consume it, validate XSD.

    # TODO: handler not yet implemented — add handler invocation test once
    #       src/handlers/iot_badge_linked.py exists.
    """
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<BadgeLink>
    <badgeId>BADGE-C12-001</badgeId>
    <contactEmail>visitor@example.com</contactEmail>
    <linkedAt>2026-04-22T09:30:00Z</linkedAt>
</BadgeLink>"""

    received = await _publish_and_consume(
        exchange_name="planning.topic",
        queue_name=f"test-c12-{uuid.uuid4().hex[:8]}",
        routing_key="iot.badge.linked",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "BadgeLink"
    assert doc.findtext("badgeId") == "BADGE-C12-001"
    assert doc.findtext("contactEmail") == "visitor@example.com"


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c12_planning_topic():
    """C12 — _INBOUND_EXCHANGE maps iot.badge.linked → planning.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["iot.badge.linked"] == "planning.topic"
