"""Integration test: C11 inbound — Planning → CRM: session update.

Contract 11
  Queue: planning.session.updated | Exchange: planning.topic | Root: SessionUpdate

Tests:
1. Broker-wiring: publish SessionUpdate on planning.topic, consume, validate XSD.
2. Fast sanity: _INBOUND_EXCHANGE maps planning.session.updated → planning.topic.

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
# Contract 11 — inbound broker wiring + XSD validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c11_session_update_flows_through_planning_topic(_skip_if_no_broker):
    """C11 — publish SessionUpdate on planning.topic, consume it, validate XSD."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<SessionUpdate>
    <sessionId>SESS-C11-001</sessionId>
    <sessionName>Tech Talk 2026</sessionName>
    <changeType>rescheduled</changeType>
</SessionUpdate>"""

    received = await _publish_and_consume(
        exchange_name="planning.topic",
        queue_name=f"test-c11-{uuid.uuid4().hex[:8]}",
        routing_key="planning.session.updated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "SessionUpdate"
    assert doc.findtext("sessionId") == "SESS-C11-001"
    assert doc.findtext("changeType") == "rescheduled"


@pytest.mark.asyncio
async def test_c11_session_update_cancelled(_skip_if_no_broker):
    """C11 — changeType=cancelled is accepted by the schema."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<SessionUpdate>
    <sessionId>SESS-C11-002</sessionId>
    <sessionName>Workshop: Cloud</sessionName>
    <changeType>cancelled</changeType>
</SessionUpdate>"""

    received = await _publish_and_consume(
        exchange_name="planning.topic",
        queue_name=f"test-c11-cancel-{uuid.uuid4().hex[:8]}",
        routing_key="planning.session.updated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "SessionUpdate"
    assert doc.findtext("changeType") == "cancelled"


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c11_planning_topic():
    """C11 — _INBOUND_EXCHANGE maps planning.session.updated → planning.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["planning.session.updated"] == "planning.topic"
