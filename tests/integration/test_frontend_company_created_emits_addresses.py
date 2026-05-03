"""Integration regression: C3 handler emits full address on C14 wire.

Publishes a Frontend C3 message, mocks the Salesforce upsert so the handler
doesn't need a live SF org, and asserts the resulting `crm.company.confirmed`
body on `contact.topic` carries every required address field.

Also asserts the deferred-publish path: a C3-XSD-valid payload with no
address must NOT publish C14 — the polling task is responsible for emission
once an admin completes the SF Account.

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


_C3_FULL_XML = """<?xml version="1.0" encoding="UTF-8"?>
<CompanyCreated>
  <name>Acme NV</name>
  <vatNumber>{vat}</vatNumber>
  <email>info@acme.be</email>
  <phone>+32 2 555 01 02</phone>
  <street>Kerkstraat</street>
  <houseNumber>12</houseNumber>
  <postalCode>1000</postalCode>
  <city>Brussel</city>
  <country>BE</country>
</CompanyCreated>"""

_C3_MINIMAL_XML = """<?xml version="1.0" encoding="UTF-8"?>
<CompanyCreated>
  <name>Minimal NV</name>
  <vatNumber>{vat}</vatNumber>
</CompanyCreated>"""


def _make_full_account(crm_id: str, vat: str) -> dict:
    return {
        "Id": "001FAKE000000001",
        "CRM_ID__c": crm_id,
        "Name": "Acme NV",
        "VAT_Number__c": vat,
        "Email__c": "info@acme.be",
        "Phone": "+32 2 555 01 02",
        "BillingStreet": "Kerkstraat",
        "House_Number__c": "12",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussel",
        "BillingCountryCode": "BE",
        "BillingCountry": "Belgium",
        "IsActive__c": True,
    }


def _make_addressless_account(crm_id: str, vat: str) -> dict:
    return {
        "Id": "001FAKE000000002",
        "CRM_ID__c": crm_id,
        "Name": "Minimal NV",
        "VAT_Number__c": vat,
        "IsActive__c": True,
    }


@pytest.mark.asyncio
async def test_c3_handler_emits_full_address_on_c14_wire(_skip_if_no_broker):
    from src import sender
    from src.receiver import handle_company_created

    crm_id = str(uuid.uuid4())
    vat = "BE0123456789"
    queue_name = f"test-c14-after-c3-full-{uuid.uuid4().hex[:8]}"

    account_return = _make_full_account(crm_id, vat)

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
        incoming.body = _C3_FULL_XML.format(vat=vat).encode("utf-8")

        sf_mock = AsyncMock()

        with (
            patch(
                "src.handlers.frontend_company_created.upsert_account_by_vat",
                new=AsyncMock(return_value=account_return),
            ),
            patch(
                "src.handlers.frontend_company_created._build_frontend_account_data",
                new=AsyncMock(return_value={}),
            ),
        ):
            await handle_company_created(incoming, sf_mock)

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


@pytest.mark.asyncio
async def test_c3_handler_defers_c14_for_addressless_payload(_skip_if_no_broker):
    """Minimal C3 payload (no address) must ack cleanly without publishing C14.

    Polling will emit C14 later once an admin completes the Salesforce Account.
    """
    from src import sender
    from src.receiver import handle_company_created

    crm_id = str(uuid.uuid4())
    vat = "BE0987654321"
    queue_name = f"test-c14-after-c3-defer-{uuid.uuid4().hex[:8]}"

    account_return = _make_addressless_account(crm_id, vat)

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
        incoming.body = _C3_MINIMAL_XML.format(vat=vat).encode("utf-8")

        sf_mock = AsyncMock()

        with (
            patch(
                "src.handlers.frontend_company_created.upsert_account_by_vat",
                new=AsyncMock(return_value=account_return),
            ),
            patch(
                "src.handlers.frontend_company_created._build_frontend_account_data",
                new=AsyncMock(return_value={}),
            ),
        ):
            await handle_company_created(incoming, sf_mock)

        incoming.ack.assert_awaited_once()
        incoming.reject.assert_not_awaited()

        # No message must reach the C14 wire — defer to polling.
        msg = await queue.get(timeout=1, fail=False)
        assert msg is None, "C14 must NOT be published for addressless payload"
    finally:
        await connection.close()
