"""Integration test: C30/C31/C32 inbound — Planning → CRM user sync.

Contract 30 — Planning → CRM: user created
  Queue: crm.planning.user.created | Routing key: planning.user.created
  Exchange: user.topic | Root: PlanningUserCreated
  Required fields: id (UUID), email, firstName, lastName, role (SPEAKER|VISITOR), gdprConsent

Contract 31 — Planning → CRM: user updated
  Queue: crm.planning.user.updated | Routing key: planning.user.updated
  Exchange: user.topic | Root: PlanningUserUpdated (same PlanningUserPayloadType)

Contract 32 — Planning → CRM: user deactivated
  Queue: crm.planning.user.deactivated | Routing key: planning.user.deactivated
  Exchange: user.topic | Root: PlanningUserDeactivated

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

_PLANNING_UUID = "c3d4e5f6-a7b8-4901-abcd-ef0123456789"


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
# Contract 30 — PlanningUserCreated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c30_planning_user_created_flows_through_user_topic(_skip_if_no_broker):
    """C30 — publish PlanningUserCreated on user.topic (rk: planning.user.created), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserCreated>
    <id>{_PLANNING_UUID}</id>
    <email>planning.c30@example.com</email>
    <firstName>Jane</firstName>
    <lastName>Janssen</lastName>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</PlanningUserCreated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c30-{uuid.uuid4().hex[:8]}",
        routing_key="planning.user.created",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "PlanningUserCreated"
    assert doc.findtext("id") == _PLANNING_UUID
    assert doc.findtext("email") == "planning.c30@example.com"
    assert doc.findtext("gdprConsent") == "true"


# ---------------------------------------------------------------------------
# Contract 31 — PlanningUserUpdated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c31_planning_user_updated_flows_through_user_topic(_skip_if_no_broker):
    """C31 — publish PlanningUserUpdated on user.topic (rk: planning.user.updated), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserUpdated>
    <id>{_PLANNING_UUID}</id>
    <email>planning.c31@example.com</email>
    <firstName>Jane</firstName>
    <lastName>Janssen</lastName>
    <role>SPEAKER</role>
    <gdprConsent>true</gdprConsent>
</PlanningUserUpdated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c31-{uuid.uuid4().hex[:8]}",
        routing_key="planning.user.updated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "PlanningUserUpdated"
    assert doc.findtext("id") == _PLANNING_UUID
    assert doc.findtext("role") == "SPEAKER"


# ---------------------------------------------------------------------------
# Contract 32 — PlanningUserDeactivated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c32_planning_user_deactivated_flows_through_user_topic(_skip_if_no_broker):
    """C32 — publish PlanningUserDeactivated on user.topic (rk: planning.user.deactivated), validate XSD."""
    from src.xml_validator import validate

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserDeactivated>
    <id>{_PLANNING_UUID}</id>
    <email>planning.c32@example.com</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</PlanningUserDeactivated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c32-{uuid.uuid4().hex[:8]}",
        routing_key="planning.user.deactivated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "PlanningUserDeactivated"
    assert doc.findtext("id") == _PLANNING_UUID
    assert doc.findtext("email") == "planning.c32@example.com"


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c30_c31_c32_user_topic():
    """C30/31/32 — _INBOUND_EXCHANGE maps all three queues → user.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["crm.planning.user.created"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.planning.user.updated"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.planning.user.deactivated"] == "user.topic"
