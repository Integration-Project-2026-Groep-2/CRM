"""Integration test: C27/C28/C29 inbound — Mailing → CRM user sync.

Contract 27 — Mailing → CRM: user created
  Queue: crm.mailing.user.created | Routing key: mailing.user.created
  Exchange: user.topic | Root: MailingUserCreated
  Required fields: id (UUID), email, isActive

Contract 28 — Mailing → CRM: user updated
  Queue: crm.mailing.user.updated | Routing key: mailing.user.updated
  Exchange: user.topic | Root: MailingUserUpdated (same MailingUserPayloadType)

Contract 29 — Mailing → CRM: user deactivated
  Queue: crm.mailing.user.deactivated | Routing key: mailing.user.deactivated
  Exchange: user.topic | Root: MailingUserDeactivated

Tests:
1. Broker-wiring + XSD validation for each contract.
2. Fast sanity: _INBOUND_EXCHANGE maps all three consumer-prefixed queues →
   user.topic.

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

_MAILING_UUID = "b2c3d4e5-f6a7-4890-9abc-def012345678"


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
# Contract 27 — MailingUserCreated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c27_mailing_user_created_flows_through_user_topic(_skip_if_no_broker):
    """C27 — publish MailingUserCreated on user.topic (rk: mailing.user.created), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserCreated>
    <id>{_MAILING_UUID}</id>
    <email>mailing.c27@example.com</email>
    <isActive>true</isActive>
</MailingUserCreated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c27-{uuid.uuid4().hex[:8]}",
        routing_key="mailing.user.created",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "MailingUserCreated"
    assert doc.findtext("id") == _MAILING_UUID
    assert doc.findtext("email") == "mailing.c27@example.com"


# ---------------------------------------------------------------------------
# Contract 28 — MailingUserUpdated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c28_mailing_user_updated_flows_through_user_topic(_skip_if_no_broker):
    """C28 — publish MailingUserUpdated on user.topic (rk: mailing.user.updated), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserUpdated>
    <id>{_MAILING_UUID}</id>
    <email>mailing.c28@example.com</email>
    <isActive>true</isActive>
</MailingUserUpdated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c28-{uuid.uuid4().hex[:8]}",
        routing_key="mailing.user.updated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "MailingUserUpdated"
    assert doc.findtext("id") == _MAILING_UUID
    assert doc.findtext("email") == "mailing.c28@example.com"


# ---------------------------------------------------------------------------
# Contract 29 — MailingUserDeactivated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c29_mailing_user_deactivated_flows_through_user_topic(_skip_if_no_broker):
    """C29 — publish MailingUserDeactivated on user.topic (rk: mailing.user.deactivated), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserDeactivated>
    <id>{_MAILING_UUID}</id>
    <email>mailing.c29@example.com</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</MailingUserDeactivated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c29-{uuid.uuid4().hex[:8]}",
        routing_key="mailing.user.deactivated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "MailingUserDeactivated"
    assert doc.findtext("id") == _MAILING_UUID
    assert doc.findtext("email") == "mailing.c29@example.com"


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c27_c28_c29_user_topic():
    """C27/28/29 — _INBOUND_EXCHANGE maps all three queues → user.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["crm.mailing.user.created"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.mailing.user.updated"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.mailing.user.deactivated"] == "user.topic"
