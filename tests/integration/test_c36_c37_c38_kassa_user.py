"""Integration test: C36/C37/C38 inbound — Kassa → CRM user sync.

Contract 36 — Kassa → CRM: user created
  Queue: crm.kassa.user.created | Routing key: kassa.user.created
  Exchange: user.topic | Root: KassaUserCreated
  Validator: validate_kassa()

Contract 37 — Kassa → CRM: user updated
  Queue: crm.kassa.user.updated | Routing key: kassa.user.updated
  Exchange: user.topic | Root: KassaUserUpdated
  Validator: validate_kassa()

Contract 38 — Kassa → CRM: user deactivated
  Queue: crm.kassa.user.deactivated | Routing key: kassa.user.deactivated
  Exchange: user.topic | Root: UserDeactivated (shared root from kassa-user.xsd)
  Validator: validate_kassa()

Note: C36-C38 use the standalone kassa-user.xsd schema (validate_kassa()), NOT
the master schema (validate()). KassaUserCreated/KassaUserUpdated roots are
Kassa-specific to avoid naming collision with Facturatie's UserCreated/UserUpdated.

Tests:
1. Broker-wiring + XSD validation (validate_kassa) for each contract.
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

_KASSA_UUID = "d4e5f6a7-b8c9-4012-bcde-f01234567890"


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
# Contract 36 — KassaUserCreated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c36_kassa_user_created_flows_through_user_topic(_skip_if_no_broker):
    """C36 — publish KassaUserCreated on user.topic (rk: kassa.user.created), validate_kassa."""
    from src.xml_validator import validate_kassa

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserCreated>
    <userId>{_KASSA_UUID}</userId>
    <firstName>Koen</firstName>
    <lastName>Janssen</lastName>
    <email>kassa.c36@example.com</email>
    <badgeCode>BADGE-C36-001</badgeCode>
    <role>VISITOR</role>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</KassaUserCreated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c36-{uuid.uuid4().hex[:8]}",
        routing_key="kassa.user.created",
        xml_body=xml_body,
    )

    doc = validate_kassa(received)
    assert doc.tag == "KassaUserCreated"
    assert doc.findtext("userId") == _KASSA_UUID
    assert doc.findtext("email") == "kassa.c36@example.com"
    assert doc.findtext("badgeCode") == "BADGE-C36-001"


# ---------------------------------------------------------------------------
# Contract 37 — KassaUserUpdated inbound
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c37_kassa_user_updated_flows_through_user_topic(_skip_if_no_broker):
    """C37 — publish KassaUserUpdated on user.topic (rk: kassa.user.updated), validate_kassa."""
    from src.xml_validator import validate_kassa

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserUpdated>
    <userId>{_KASSA_UUID}</userId>
    <firstName>Koen</firstName>
    <lastName>Janssen</lastName>
    <email>kassa.c37@example.com</email>
    <badgeCode>BADGE-C37-001</badgeCode>
    <role>CASHIER</role>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</KassaUserUpdated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c37-{uuid.uuid4().hex[:8]}",
        routing_key="kassa.user.updated",
        xml_body=xml_body,
    )

    doc = validate_kassa(received)
    assert doc.tag == "KassaUserUpdated"
    assert doc.findtext("userId") == _KASSA_UUID
    assert doc.findtext("role") == "CASHIER"


# ---------------------------------------------------------------------------
# Contract 38 — UserDeactivated inbound (kassa-user.xsd)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c38_kassa_user_deactivated_flows_through_user_topic(_skip_if_no_broker):
    """C38 — publish UserDeactivated on user.topic (rk: kassa.user.deactivated), validate_kassa.

    Note: C38 reuses the <UserDeactivated> root from kassa-user.xsd (same
    structure as C22/C26 in the master schema, but validated against the
    standalone kassa-user.xsd to avoid duplicate element compilation errors).
    """
    from src.xml_validator import validate_kassa

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>{_KASSA_UUID}</id>
    <email>kassa.c38@example.com</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</UserDeactivated>""".encode()

    received = await _publish_and_consume(
        exchange_name="user.topic",
        queue_name=f"test-c38-{uuid.uuid4().hex[:8]}",
        routing_key="kassa.user.deactivated",
        xml_body=xml_body,
    )

    doc = validate_kassa(received)
    assert doc.tag == "UserDeactivated"
    assert doc.findtext("id") == _KASSA_UUID
    assert doc.findtext("email") == "kassa.c38@example.com"


# ---------------------------------------------------------------------------
# Contract 37 — bug-scenario regression: email-fallback bootstrap link
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_c37_kassa_user_updated_bootstraps_link_via_email_when_kassa_id_missing(_skip_if_no_broker):
    """Regression: C37 arrives for a Contact that exists by email but has no Kassa_ID__c.

    Pre-fix this would have raised MissingDependencyError 15× and DLQ'd. After the
    email-fallback fix, the handler bootstraps the link (stamp Kassa_ID__c via
    ensure_contact_identifiers, fill blank fields via backfill_kassa_contact_fields)
    and publishes crm.user.confirmed (C13) instead of crm.user.updated (C18) because
    consumers haven't seen this Contact's CRM_ID__c stamped to Kassa yet.

    Mocks Salesforce only — sender, validator, aio-pika channel, and broker run for real.
    """
    from unittest.mock import AsyncMock, MagicMock, patch

    from src import sender, xml_validator
    from src.handlers import kassa_user_updated

    bootstrap_kassa_id = "f1e2d3c4-b5a6-4978-8eba-cdef01234567"
    bootstrap_email = "kassa.bootstrap.c37@example.com"
    existing_crm_id = "11111111-2222-4333-8444-555555555555"

    xml_body = f"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserUpdated>
    <userId>{bootstrap_kassa_id}</userId>
    <firstName>Bootstrap</firstName>
    <lastName>Test</lastName>
    <email>{bootstrap_email}</email>
    <badgeCode>BADGE-BOOT-001</badgeCode>
    <role>VISITOR</role>
    <updatedAt>2026-04-29T18:51:29Z</updatedAt>
</KassaUserUpdated>""".encode()

    existing_by_email = {
        "Id": "003test000c37boot",
        "CRM_ID__c": existing_crm_id,
        "Kassa_ID__c": None,
        "Email": bootstrap_email,
        "FirstName": "Bootstrap",
        "LastName": "Test",
        "Role__c": "VISITOR",
        "Badge_Code__c": None,
        "Company_ID__c": None,
        "IsActive__c": True,
        "GDPR_Consent__c": True,
    }
    after_ensure = dict(existing_by_email, Kassa_ID__c=bootstrap_kassa_id)
    after_backfill = dict(after_ensure, Badge_Code__c="BADGE-BOOT-001")

    consumer_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    sender_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        consumer_channel = await consumer_conn.channel()
        consumer_exchange = await consumer_channel.declare_exchange(
            "contact.topic", type=ExchangeType.TOPIC, durable=True,
        )
        probe_queue = await consumer_channel.declare_queue(
            f"crm.user.confirmed.it-{uuid.uuid4().hex[:8]}",
            durable=False,
            exclusive=True,
            auto_delete=True,
        )
        await probe_queue.bind(consumer_exchange, routing_key="crm.user.confirmed")
        # Bind a second probe to assert NO crm.user.updated is emitted.
        updated_probe = await consumer_channel.declare_queue(
            f"crm.user.updated.it-{uuid.uuid4().hex[:8]}",
            durable=False,
            exclusive=True,
            auto_delete=True,
        )
        await updated_probe.bind(consumer_exchange, routing_key="crm.user.updated")

        sender_channel = await sender_conn.channel()
        await sender.init(sender_channel)

        msg = MagicMock()
        msg.body = xml_body
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()
        sf_mock = MagicMock()

        with (
            patch.object(kassa_user_updated, "has_contact_kassa_id_field", new=AsyncMock(return_value=True)),
            patch.object(kassa_user_updated, "get_contact_match_by_kassa_id", new=AsyncMock(return_value=("none", None))),
            patch.object(kassa_user_updated, "get_contact_match_by_email", new=AsyncMock(return_value=("unique", existing_by_email))),
            patch.object(kassa_user_updated, "ensure_contact_identifiers", new=AsyncMock(return_value=after_ensure)) as mock_ensure,
            patch.object(kassa_user_updated, "backfill_kassa_contact_fields", new=AsyncMock(return_value=after_backfill)) as mock_backfill,
            patch.object(kassa_user_updated, "update_kassa_contact", new=AsyncMock()) as mock_update,
        ):
            await kassa_user_updated.handle(msg, sf_mock)

            # Bootstrap-link side effects.
            mock_ensure.assert_awaited_once()
            assert mock_ensure.await_args.kwargs["kassa_id"] == bootstrap_kassa_id
            mock_backfill.assert_awaited_once()
            mock_update.assert_not_called()
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

        # C13 must land on contact.topic with the EXISTING CRM_ID__c (not a new one).
        received = await asyncio.wait_for(probe_queue.get(timeout=5), timeout=5.0)
        try:
            confirmed_xml = received.body
            parsed = xml_validator.validate(confirmed_xml)
            assert parsed.tag == "UserConfirmed"
            assert parsed.findtext("id") == existing_crm_id
            assert parsed.findtext("email") == bootstrap_email
        finally:
            await received.ack()

        # C18 must NOT have been published.
        from aio_pika.exceptions import QueueEmpty

        with pytest.raises(QueueEmpty):
            await updated_probe.get(timeout=1, fail=True)
    finally:
        await consumer_conn.close()
        await sender_conn.close()


# ---------------------------------------------------------------------------
# Fast sanity — no broker needed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receiver_inbound_exchange_map_includes_c36_c37_c38_user_topic():
    """C36/37/38 — _INBOUND_EXCHANGE maps all three queues → user.topic."""
    from src.receiver import _INBOUND_EXCHANGE

    assert _INBOUND_EXCHANGE["crm.kassa.user.created"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.kassa.user.updated"] == "user.topic"
    assert _INBOUND_EXCHANGE["crm.kassa.user.deactivated"] == "user.topic"
