"""Integration test: C5a inbound + C5b outbound (requestId echo).

Contract 5a — Facturatie → CRM: request company data
  Queue: facturatie.company.requested | Exchange: invoice.topic | Root: CompanyRequest
  Status: handler not yet implemented (PENDING_EXCHANGES)

Contract 5b — CRM → Facturatie: company data response
  Queue: crm.company.responded | Exchange: contact.topic | Root: CompanyResponse

Tests:
1. Broker-wiring: publish CompanyRequest on invoice.topic, consume, validate XSD.
2. Outbound: call sender.publish_company_responded, assert CompanyResponse on
   contact.topic carries correct requestId (echo).
3. Fast sanity: _INBOUND_EXCHANGE maps facturatie.company.requested → invoice.topic.

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
# Contract 5a — inbound broker wiring + XSD validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c5a_company_request_flows_through_invoice_topic(_skip_if_no_broker):
    """C5a — publish CompanyRequest on invoice.topic, consume it, validate XSD."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyRequest>
    <requestId>REQ-C5A-001</requestId>
    <vatNumber>BE0123456789</vatNumber>
</CompanyRequest>"""

    received = await _publish_and_consume(
        exchange_name="invoice.topic",
        queue_name=f"test-c5a-{uuid.uuid4().hex[:8]}",
        routing_key="facturatie.company.requested",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "CompanyRequest"
    assert doc.findtext("requestId") == "REQ-C5A-001"
    assert doc.findtext("vatNumber") == "BE0123456789"


# ---------------------------------------------------------------------------
# Contract 5b — outbound: sender.publish_company_responded + requestId echo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c5b_company_responded_carries_request_id(_skip_if_no_broker):
    """C5b — publish_company_responded echoes the requestId in the response XML.

    No handler for C5a yet (PENDING_EXCHANGES), so the sender function is
    invoked directly.
    # TODO: replace direct sender call with handler invocation once C5a handler
    #       is implemented in src/handlers/.
    """
    from src import sender, xml_validator

    request_id = f"REQ-ECHO-{uuid.uuid4().hex[:8]}"
    queue_name = f"test-c5b-{uuid.uuid4().hex[:8]}"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel_consumer = await connection.channel()
        exchange = await channel_consumer.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel_consumer.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key="crm.company.responded")

        channel_sender = await connection.channel()
        await sender.init(channel_sender)

        await sender.publish_company_responded(request_id, {"found": False})

        msg = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            doc = xml_validator.validate(msg.body)
            assert doc.tag == "CompanyResponse"
            assert doc.findtext("requestId") == request_id
            assert doc.findtext("found") == "false"
        finally:
            await msg.ack()
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c5a_invoice_topic():
    """C5a — _INBOUND_EXCHANGE maps facturatie.company.requested → invoice.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["facturatie.company.requested"] == "invoice.topic"
