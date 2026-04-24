"""Integration regression: C33 handler emits full address on C14 wire.

Publishes a Facturatie C33 message, mocks the Salesforce upsert so the
handler doesn't need a live SF org, and asserts the resulting
`crm.company.confirmed` body on `contact.topic` carries every required
address field.

Broker requirement identical to the other integration tests — start a local
broker with:
    docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine
"""
from __future__ import annotations

import asyncio
import os
import uuid
from unittest.mock import AsyncMock, patch

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
            f"RabbitMQ not reachable at {RABBITMQ_URL}. Start a local broker and retry.",
        )


_C33_XML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<FacturatieCompanyCreated>
  <name>Acme NV</name>
  <vatNumber>{vat}</vatNumber>
  <email>billing@acme.be</email>
  <phone>+32 2 555 01 02</phone>
  <street>Kerkstraat</street>
  <houseNumber>12</houseNumber>
  <postalCode>1000</postalCode>
  <city>Brussel</city>
  <country>BE</country>
  <createdAt>2026-04-21T10:00:00Z</createdAt>
</FacturatieCompanyCreated>"""


def _make_upserted_account(crm_id: str, vat: str) -> dict:
    return {
        "Id": "001FAKE000000001",
        "CRM_ID__c": crm_id,
        "Name": "Acme NV",
        "VAT_Number__c": vat,
        "Email__c": "billing@acme.be",
        "Phone": "+32 2 555 01 02",
        "BillingStreet": "Kerkstraat",
        "House_Number__c": "12",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussel",
        "BillingCountryCode": "BE",
        "BillingCountry": "Belgium",
        "IsActive__c": True,
    }


@pytest.mark.asyncio
async def test_c33_handler_emits_full_address_on_c14_wire(
    _skip_if_no_broker,
):
    from src import sender
    from src.receiver import handle_facturatie_company_created

    crm_id = str(uuid.uuid4())
    vat = "BE0123456789"
    queue_name = f"test-c14-after-c33-{uuid.uuid4().hex[:8]}"

    account_return = _make_upserted_account(crm_id, vat)

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel_consumer = await connection.channel()
        exchange = await channel_consumer.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel_consumer.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key="crm.company.confirmed")

        channel_sender = await connection.channel()
        await sender.init(channel_sender)

        incoming = AsyncMock(spec=aio_pika.IncomingMessage)
        incoming.body = _C33_XML_TEMPLATE.format(vat=vat).encode("utf-8")

        sf_mock = AsyncMock()

        with (
            patch(
                "src.receiver.upsert_account_by_vat",
                new=AsyncMock(return_value=account_return),
            ),
            patch(
                "src.receiver._build_facturatie_account_data",
                new=AsyncMock(return_value={}),
            ),
        ):
            await handle_facturatie_company_created(incoming, sf_mock)

        msg = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            body = msg.body
            assert b"<CompanyConfirmed>" in body
            assert b"<street>Kerkstraat</street>" in body
            assert b"<houseNumber>12</houseNumber>" in body
            assert b"<postalCode>1000</postalCode>" in body
            assert b"<city>Brussel</city>" in body
            assert b"<country>BE</country>" in body
            assert b"<phone>+32 2 555 01 02</phone>" in body
        finally:
            await msg.ack()
    finally:
        await connection.close()
