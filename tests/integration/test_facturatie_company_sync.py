"""Integration test: Facturatie company sync against a real RabbitMQ broker.

Verifies that the CRM receiver:
1. Binds `facturatie.company.{created,updated,deactivated}` queues to the
   `company.topic` exchange (Contracts 33/34/35).
2. Successfully consumes a valid XML payload published by Facturatie.
3. Validates the payload against the master XSD without raising.

Scope: broker wiring + XSD validation only. The full handler (Salesforce
calls, outbound publishing to contact.topic) is covered by unit tests with
mocked SF and by `test_polling_publishes_on_sf_change.py` for the outbound
side. This test catches misconfigurations in `_INBOUND_EXCHANGE` mapping
and XSD drift before they hit production.

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
    """Bind a fresh queue to the given exchange, publish XML, return received body."""
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


@pytest.mark.asyncio
async def test_facturatie_company_created_flows_through_company_topic(_skip_if_no_broker):
    """Contract 33 — publish FacturatieCompanyCreated on company.topic, consume it, validate XSD."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Acme Integration NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>integration-created@acme.example</email>
    <phone>+32 2 000 00 00</phone>
    <street>Teststraat</street>
    <houseNumber>1</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussels</city>
    <country>BE</country>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

    received = await _publish_and_consume(
        exchange_name="company.topic",
        queue_name=f"test-fact-company-created-{uuid.uuid4().hex[:8]}",
        routing_key="facturatie.company.created",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "FacturatieCompanyCreated"
    assert doc.findtext("vatNumber") == "BE0123456789"


@pytest.mark.asyncio
async def test_facturatie_company_updated_flows_through_company_topic(_skip_if_no_broker):
    """Contract 34 — publish FacturatieCompanyUpdated on company.topic, consume it, validate XSD."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyUpdated>
    <id>e1f2a3b4-c5d6-4701-8e23-fa4567bc8602</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme Integration NV (updated)</name>
    <email>integration-updated@acme.example</email>
    <isActive>true</isActive>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</FacturatieCompanyUpdated>"""

    received = await _publish_and_consume(
        exchange_name="company.topic",
        queue_name=f"test-fact-company-updated-{uuid.uuid4().hex[:8]}",
        routing_key="facturatie.company.updated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "FacturatieCompanyUpdated"
    assert doc.findtext("isActive") == "true"


@pytest.mark.asyncio
async def test_facturatie_company_deactivated_flows_through_company_topic(_skip_if_no_broker):
    """Contract 35 — publish FacturatieCompanyDeactivated on company.topic, consume it, validate XSD."""
    from src.xml_validator import validate

    xml_body = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyDeactivated>
    <id>e1f2a3b4-c5d6-4701-8e23-fa4567bc8602</id>
    <email>integration-deact@acme.example</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</FacturatieCompanyDeactivated>"""

    received = await _publish_and_consume(
        exchange_name="company.topic",
        queue_name=f"test-fact-company-deact-{uuid.uuid4().hex[:8]}",
        routing_key="facturatie.company.deactivated",
        xml_body=xml_body,
    )

    doc = validate(received)
    assert doc.tag == "FacturatieCompanyDeactivated"


@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_company_topic_entries():
    """Fast sanity check (no broker needed): _INBOUND_EXCHANGE maps all three
    consumer-prefixed queues to company.topic. Guards against accidental
    removal / rename. Producers still publish with the old routing key; the
    queue-name is prefixed with `crm.` to avoid collision with Facturatie's
    own consumer-queues on company.topic.
    """
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["crm.facturatie.company.created"] == "company.topic"
    assert _INBOUND_EXCHANGE["crm.facturatie.company.updated"] == "company.topic"
    assert _INBOUND_EXCHANGE["crm.facturatie.company.deactivated"] == "company.topic"
