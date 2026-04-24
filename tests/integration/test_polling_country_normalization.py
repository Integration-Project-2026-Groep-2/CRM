"""Integration regression: polling emits ISO-2 country on company.confirmed.

Covers both paths:
 1. BillingCountryCode="BE" (Picklists enabled) → <country>BE</country>.
 2. BillingCountry="Belgium" with no code → <country>BE</country> via pycountry.

Runs the full polling → sender → RabbitMQ wire path so XSD validation in the
sender is exercised against real XML bytes. The broker requirement + skip
behavior mirrors tests/integration/test_polling_publishes_on_sf_change.py.

Start the broker locally with:
    docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine
"""
from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def _make_account_record(crm_id: str, vat: str, **country_fields) -> dict:
    return {
        "Id": "001FAKE000000001",
        "CRM_ID__c": crm_id,
        "Name": "Acme NV",
        "VAT_Number__c": vat,
        "Email__c": "info@acme.be",
        "BillingStreet": "Kerkstraat",
        "House_Number__c": "12",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussel",
        "IsActive__c": True,
        "CreatedDate": "2026-04-21T09:00:00.000+0000",
        "SystemModstamp": "2026-04-21T10:00:00.000+0000",
        "LastModifiedById": "005ADMIN0000001",
        **country_fields,
    }


def _make_sf_mock(account_records: list[dict]) -> MagicMock:
    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.Account = MagicMock()
    sf.Contact.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "FirstName", "LastName", "Email",
            "CreatedDate", "SystemModstamp", "LastModifiedById", "IsActive__c",
        ]],
    }
    # Both picklist fields exposed via describe so _build_account_select_fields
    # will include BillingCountryCode in the SOQL SELECT list.
    sf.Account.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "Name", "VAT_Number__c",
            "CreatedDate", "SystemModstamp", "LastModifiedById",
            "IsActive__c", "Email__c",
            "BillingStreet", "House_Number__c", "BillingPostalCode",
            "BillingCity", "BillingCountry", "BillingCountryCode",
        ]],
    }
    sf.Contact.update = MagicMock()
    sf.Account.update = MagicMock()
    sf.restful = MagicMock(return_value={"id": "005REAL_INT_USER"})

    def _query(soql: str):
        low = soql.lower()
        if "from user" in low:
            return {"records": [{"Id": "005INTEGRATION01"}]}
        if "order by systemmodstamp desc" in low:
            return {"records": [{"SystemModstamp": "2026-04-01T00:00:00.000+0000"}]}
        return {"records": []}

    def _query_all(soql: str):
        low = soql.lower()
        if "from account" in low:
            return {"records": account_records}
        return {"records": []}

    sf.query.side_effect = _query
    sf.query_all.side_effect = _query_all
    return sf


async def _run_polling_once_and_capture(
    records: list[dict], routing_key: str, tmp_path: Path,
) -> bytes:
    from src import polling, sender
    from src.config import Config

    sf = _make_sf_mock(records)
    config = Config(
        rabbitmq_url=RABBITMQ_URL,
        salesforce_username="integration@example.com",
        salesforce_password="pw",
        salesforce_security_token="tok",
        salesforce_domain="login",
        heartbeat_interval_seconds=0,
        system_name="CRM",
        polling_interval_seconds=0,
        polling_state_path=str(tmp_path / f"checkpoint-{uuid.uuid4().hex[:6]}.json"),
        polling_integration_user_id=None,
        log_level="INFO",
    )

    queue_name = f"test-crm-company-{uuid.uuid4().hex[:8]}"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel_consumer = await connection.channel()
        exchange = await channel_consumer.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel_consumer.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key=routing_key)

        channel_sender = await connection.channel()
        await sender.init(channel_sender)

        async def _fake_login(*_args, **_kwargs):
            return sf

        patcher = patch("src.salesforce_client.get_salesforce_client", _fake_login)
        patcher.start()
        task = asyncio.create_task(polling.run_polling(config))
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            patcher.stop()

        incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            return incoming.body
        finally:
            await incoming.ack()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_polling_emits_iso2_country_when_billing_country_code_present(
    _skip_if_no_broker, tmp_path: Path,
):
    """Picklists enabled: BillingCountryCode='BE' + BillingCountry='Belgium' →
    outbound XML contains <country>BE</country>, not the derived label."""
    from src.country_code import to_iso_alpha2

    to_iso_alpha2.cache_clear()
    record = _make_account_record(
        crm_id=str(uuid.uuid4()),
        vat="BE0123456789",
        BillingCountryCode="BE",
        BillingCountry="Belgium",
    )

    body = await _run_polling_once_and_capture(
        [record], routing_key="crm.company.confirmed", tmp_path=tmp_path,
    )

    assert b"<country>BE</country>" in body
    assert b"<country>Belgium</country>" not in body
    # Full address must flow through for the newly-required C14 fields.
    assert b"<street>Kerkstraat</street>" in body
    assert b"<houseNumber>12</houseNumber>" in body
    assert b"<postalCode>1000</postalCode>" in body
    assert b"<city>Brussel</city>" in body


@pytest.mark.asyncio
async def test_polling_normalizes_full_country_name_via_pycountry(
    _skip_if_no_broker, tmp_path: Path,
):
    """Picklists off: only BillingCountry populated — pycountry resolves
    'Belgium' to 'BE' before the sender validates the XSD."""
    from src.country_code import to_iso_alpha2

    to_iso_alpha2.cache_clear()
    record = _make_account_record(
        crm_id=str(uuid.uuid4()),
        vat="BE0987654321",
        BillingCountryCode=None,
        BillingCountry="Belgium",
    )

    body = await _run_polling_once_and_capture(
        [record], routing_key="crm.company.confirmed", tmp_path=tmp_path,
    )

    assert b"<country>BE</country>" in body
    assert b"<street>Kerkstraat</street>" in body
    assert b"<houseNumber>12</houseNumber>" in body
    assert b"<postalCode>1000</postalCode>" in body
    assert b"<city>Brussel</city>" in body
