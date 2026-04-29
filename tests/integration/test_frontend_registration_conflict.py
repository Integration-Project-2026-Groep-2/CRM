"""Integration test: Contract 1 → Contract 15 (frontend registration → user conflict).

Drives `handle_registration` against a real RabbitMQ broker for the
duplicate-email-with-incompatible-fields branch and asserts that:

1. A `UserConflict` XML lands on the `crm.user.conflict` fanout exchange.
2. The XML validates against the production XSD chain.
3. `detectedAt` is a server-minted UTC ISO-8601 timestamp (the Frontend
   Registration XSD has no `detectedAt`, so the handler must mint one — a
   mock-only test misses this because the validator is stubbed out).

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
import re
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import pytest
from aio_pika import ExchangeType

from src import sender, xml_validator

RABBITMQ_URL = os.getenv("CRM_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5675/")

VALID_REG_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-IT-CONFLICT</registrationId>
    <firstName>John</firstName>
    <lastName>Doe</lastName>
    <email>conflict.it@example.com</email>
    <sessionId>SESS-IT</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <company>Acme Corp</company>
</Registration>"""


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


def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@pytest.mark.asyncio
async def test_duplicate_registration_publishes_validated_user_conflict(_skip_if_no_broker) -> None:
    """End-to-end: handler builds a UserConflict that round-trips through the XSD + broker.

    Mocks Salesforce client only — every other layer (sender, validator,
    aio-pika channel, broker) runs for real.
    """
    from src.handlers import frontend_registration_created

    consumer_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    sender_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        # Bind a transient consumer queue to the fanout exchange BEFORE the
        # handler publishes; otherwise the message is dropped (no queue).
        consumer_channel = await consumer_conn.channel()
        consumer_exchange = await consumer_channel.declare_exchange(
            "crm.user.conflict", type=ExchangeType.FANOUT, durable=True,
        )
        probe_queue = await consumer_channel.declare_queue(
            f"crm.user.conflict.it-{os.urandom(4).hex()}",
            durable=False,
            exclusive=True,
            auto_delete=True,
        )
        await probe_queue.bind(consumer_exchange)

        # Initialize sender against the broker so publish_user_conflict
        # has a live channel + declared fanout exchange.
        sender_channel = await sender_conn.channel()
        await sender.init(sender_channel)

        existing_contact = {
            "Id": "003xxx",
            "Registration_ID__c": "OTHER",
            "FirstName": "Other",
            "LastName": "Person",
            "Role__c": "VISITOR",
            "Company_ID__c": "Old Co",
        }

        with (
            patch.object(
                frontend_registration_created,
                "get_contact_by_email",
                new=AsyncMock(return_value=existing_contact),
            ),
            patch.object(
                frontend_registration_created,
                "create_contact",
                new=AsyncMock(),
            ) as mock_create,
        ):
            msg = _make_message(VALID_REG_XML)
            sf_mock = MagicMock()
            await frontend_registration_created.handle(msg, sf_mock)

            mock_create.assert_not_called()
            msg.ack.assert_called_once()

        received = await asyncio.wait_for(probe_queue.get(timeout=5), timeout=5.0)
        try:
            xml_bytes = received.body

            # The XML the handler produced must validate against the production XSD.
            parsed = xml_validator.validate(xml_bytes)

            assert parsed.tag == "UserConflict"
            assert parsed.findtext("email") == "conflict.it@example.com"
            existing_el = parsed.find("existingValue")
            incoming_el = parsed.find("incomingValue")
            assert existing_el is not None and incoming_el is not None
            assert existing_el.findtext("firstName") == "Other"
            assert existing_el.findtext("lastName") == "Person"
            assert existing_el.findtext("company") == "Old Co"
            assert incoming_el.findtext("firstName") == "John"
            assert incoming_el.findtext("lastName") == "Doe"
            assert incoming_el.findtext("company") == "Acme Corp"

            detected_at = parsed.findtext("detectedAt") or ""
            assert re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", detected_at,
            ), f"detectedAt={detected_at!r} is not a UTC ISO-8601 timestamp"
        finally:
            await received.ack()
    finally:
        await consumer_conn.close()
        await sender_conn.close()
