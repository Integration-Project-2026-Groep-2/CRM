"""Integration test: C10a inbound + C10b outbound (requestId echo).

Contract 10a — Kassa → CRM: person lookup request
  Queue: kassa.person.lookup.requested | Exchange: payment.topic | Root: PersonLookupRequest

Contract 10b — CRM → Kassa: person lookup response
  Queue: crm.person.lookup.responded | Exchange: contact.topic | Root: PersonLookupResponse

Tests:
1. Broker-wiring: publish PersonLookupRequest on payment.topic, consume, validate XSD.
2. Outbound: invoke handler with mocked SF (contact not found), assert
   PersonLookupResponse on contact.topic echoes the requestId.
3. Fast sanity: _INBOUND_EXCHANGE maps kassa.person.lookup.requested → payment.topic.

Skipped automatically when broker is unreachable.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import pytest
from aio_pika import ExchangeType

from src import sender, xml_validator

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


def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


# ---------------------------------------------------------------------------
# Contract 10a — inbound broker wiring + XSD validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c10a_person_lookup_request_flows_through_payment_topic(_skip_if_no_broker):
    """C10a — publish PersonLookupRequest on payment.topic, consume it, validate XSD."""
    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<PersonLookupRequest>
    <requestId>REQ-C10A-001</requestId>
    <email>lookup@example.com</email>
</PersonLookupRequest>"""

    received = await _publish_and_consume(
        exchange_name="payment.topic",
        queue_name=f"test-c10a-{uuid.uuid4().hex[:8]}",
        routing_key="kassa.person.lookup.requested",
        xml_body=xml_body,
    )

    doc = xml_validator.validate(received)
    assert doc.tag == "PersonLookupRequest"
    assert doc.findtext("requestId") == "REQ-C10A-001"
    assert doc.findtext("email") == "lookup@example.com"


# ---------------------------------------------------------------------------
# Contract 10b — outbound: handler + requestId echo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c10b_person_lookup_responded_echoes_request_id(_skip_if_no_broker):
    """C10b — handler reads requestId from inbound XML and echoes it in PersonLookupResponse."""
    from src.handlers import kassa_person_lookup_requested

    request_id = f"REQ-ECHO-{uuid.uuid4().hex[:8]}"
    queue_name = f"test-c10b-{uuid.uuid4().hex[:8]}"

    xml_body = (
        f"<?xml version='1.0' encoding='utf-8'?>"
        f"<PersonLookupRequest>"
        f"<requestId>{request_id}</requestId>"
        f"<email>lookup@example.com</email>"
        f"</PersonLookupRequest>"
    ).encode()

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel_consumer = await connection.channel()
        exchange = await channel_consumer.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel_consumer.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key="crm.person.lookup.responded")

        channel_sender = await connection.channel()
        await sender.init(channel_sender)

        msg = _make_message(xml_body)
        sf_mock = MagicMock()

        with patch.object(
            kassa_person_lookup_requested,
            "get_contact_for_person_lookup",
            new=AsyncMock(return_value=None),
        ):
            await kassa_person_lookup_requested.handle(msg, sf_mock)

        msg.ack.assert_called_once()

        received = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            doc = xml_validator.validate(received.body)
            assert doc.tag == "PersonLookupResponse"
            assert doc.findtext("requestId") == request_id
            assert doc.findtext("found") == "false"
            assert doc.findtext("linkedToCompany") == "false"
        finally:
            await received.ack()
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c10a_payment_topic():
    """C10a — _INBOUND_EXCHANGE maps kassa.person.lookup.requested → payment.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["kassa.person.lookup.requested"] == "payment.topic"
