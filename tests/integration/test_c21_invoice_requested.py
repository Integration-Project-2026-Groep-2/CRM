"""Integration test: C21 outbound — CRM → Facturatie: invoice requested.

Contract 21
  Queue: crm.invoice.requested | Exchange: contact.topic | Root: InvoiceRequested

Tests:
1. Broker-wiring: publish a valid InvoiceRequested XML on contact.topic,
   consume it, validate XSD.

# TODO: sender.publish_invoice_requested not yet implemented in src/sender.py.
#       This test verifies broker wiring and XSD validity by publishing raw XML
#       directly. Replace with a sender function call once implemented.

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
# Contract 21 — broker wiring + XSD validation
# (sender function not yet implemented — raw publish)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c21_invoice_requested_xsd_valid_and_flows_through_contact_topic(
    _skip_if_no_broker,
):
    """C21 — publish InvoiceRequested on contact.topic, consume it, validate XSD.

    # TODO: replace raw publish with sender.publish_invoice_requested() once
    #       that function is added to src/sender.py.
    """
    from src.xml_validator import validate

    user_id = "a1b2c3d4-e5f6-4789-89ab-cdef01234567"
    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<InvoiceRequested>
    <userId>{user_id}</userId>
    <email>invoice@example.com</email>
    <firstName>Jan</firstName>
    <lastName>Janssen</lastName>
    <street>Teststraat</street>
    <houseNumber>1</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussel</city>
    <country>BE</country>
    <requestedAt>2026-04-22T09:30:00Z</requestedAt>
</InvoiceRequested>""".encode()

    received = await _publish_and_consume(
        exchange_name="contact.topic",
        queue_name=f"test-c21-{uuid.uuid4().hex[:8]}",
        routing_key="crm.invoice.requested",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "InvoiceRequested"
    assert doc.findtext("userId") == user_id
    assert doc.findtext("email") == "invoice@example.com"
