"""Integration test: polling task publishes contracts via a real RabbitMQ broker.

Mocks the Salesforce client (no live SF org required) but uses a real
local RabbitMQ so we verify that the sender's XML truly lands on the
`contact.topic` exchange with the expected routing keys.

Requirements:
- RabbitMQ broker reachable via CRM_TEST_RABBITMQ_URL (default amqp://guest:guest@localhost:5675/).
  Spin up locally with:
      docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-management

The test is skipped automatically when the broker is unreachable.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from unittest.mock import MagicMock

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


def _make_contact_record(crm_id: str, email: str) -> dict:
    return {
        "Id": "003FAKE000000001",
        "CRM_ID__c": crm_id,
        "FirstName": "Admin",
        "LastName": "Testuser",
        "Email": email,
        "Phone": "+3212345",
        "Role__c": "VISITOR",
        "GDPR_Consent__c": True,
        "IsActive__c": True,
        "CreatedDate": "2026-04-21T09:00:00.000+0000",
        "SystemModstamp": "2026-04-21T10:00:00.000+0000",
        "LastModifiedById": "005ADMIN0000001",
    }


def _make_sf_mock(contact_records: list[dict]) -> MagicMock:
    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.Account = MagicMock()
    # Polling builds the SELECT list dynamically from describe(). All REQUIRED
    # fields must appear here or polling raises before we reach the publish path.
    sf.Contact.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "FirstName", "LastName", "Email",
            "CreatedDate", "SystemModstamp", "LastModifiedById",
            "Phone", "Role__c", "GDPR_Consent__c", "IsActive__c",
        ]],
    }
    sf.Account.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "Name", "VAT_Number__c",
            "CreatedDate", "SystemModstamp", "LastModifiedById",
            "IsActive__c",
        ]],
    }
    sf.Contact.update = MagicMock()
    sf.Account.update = MagicMock()
    # Polling also probes sf.restful for the admin-collision warning.
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
        if "from contact" in low:
            return {"records": contact_records}
        return {"records": []}

    sf.query.side_effect = _query
    sf.query_all.side_effect = _query_all
    return sf


@pytest.mark.asyncio
async def test_polling_publishes_crm_user_confirmed_to_contact_topic(
    _skip_if_no_broker, tmp_path: Path,
):
    """An admin-created Contact in Salesforce appears as crm.user.confirmed on RabbitMQ."""
    from src import polling, sender
    from src.config import Config

    crm_id = str(uuid.uuid4())
    email = f"polling-test-{uuid.uuid4().hex[:6]}@example.com"
    sf = _make_sf_mock(contact_records=[_make_contact_record(crm_id, email)])

    config = Config(
        rabbitmq_url=RABBITMQ_URL,
        salesforce_username="integration@example.com",
        salesforce_password="pw",
        salesforce_security_token="tok",
        salesforce_domain="login",
        heartbeat_interval_seconds=0,
        system_name="CRM",
        polling_interval_seconds=0,
        polling_state_path=str(tmp_path / "checkpoint.json"),
        polling_integration_user_id=None,
        log_level="INFO",
    )

    queue_name = f"test-crm-user-confirmed-{uuid.uuid4().hex[:8]}"

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        # Consumer-side: declare the same topic exchange the sender uses and
        # bind a fresh throwaway queue to the routing key we expect.
        channel_consumer = await connection.channel()
        exchange = await channel_consumer.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        queue = await channel_consumer.declare_queue(
            queue_name, durable=False, auto_delete=True,
        )
        await queue.bind(exchange, routing_key="crm.user.confirmed")

        # Producer-side: wire the sender module exactly as main.py does.
        channel_sender = await connection.channel()
        await sender.init(channel_sender)

        # run_polling now creates its own SF client via get_salesforce_client;
        # patch that module-level function so we inject our mock sf.
        from unittest.mock import patch

        async def _fake_login(*_args, **_kwargs):
            return sf

        # run_polling loops forever; cancel after enough time for one cycle.
        patcher = patch("src.salesforce_client.get_salesforce_client", _fake_login)
        patcher.start()
        task = asyncio.create_task(polling.run_polling(config))
        await asyncio.sleep(0.5)  # allow cycle to run and publish
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            patcher.stop()

        # Consume the published message.
        incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            body = incoming.body
            assert b"<UserConfirmed>" in body or b"<UserConfirmed " in body
            assert email.encode("utf-8") in body
            assert crm_id.encode("utf-8") in body
        finally:
            await incoming.ack()
    finally:
        await connection.close()
