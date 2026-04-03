"""E2E tests — verify full contract flows via RabbitMQ.

Publishes inbound XML (simulating other teams) and verifies the CRM container
produces the expected outbound XML or Salesforce side effects.

Requires:
  - A running CRM container connected to RabbitMQ and Salesforce
  - RabbitMQ accessible on RABBITMQ_URL

Usage:
    # Against local Docker
    docker compose up --build -d
    python -m pytest tests/e2e/ -v

    # Skip Salesforce-dependent tests
    python -m pytest tests/e2e/ -v -m "not salesforce"
"""

import asyncio
import os
import random
import string
from datetime import datetime, timezone

import aio_pika
import pytest
from aio_pika import ExchangeType
from dotenv import load_dotenv
from lxml import etree
from simple_salesforce import Salesforce

load_dotenv()

# .env may contain "rabbitmq" hostname (for Docker networking).
# Tests run on the host, so replace with localhost if needed.
_raw_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
RABBITMQ_URL = _raw_url.replace("@rabbitmq:", "@localhost:") if "@rabbitmq:" in _raw_url else _raw_url
TIMEOUT = 15  # seconds — CRM needs time for Salesforce round-trip


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def connection():
    """RabbitMQ connection per test."""
    conn = await aio_pika.connect_robust(RABBITMQ_URL)
    yield conn
    await conn.close()


@pytest.fixture
async def channel(connection):
    ch = await connection.channel()
    yield ch


@pytest.fixture
async def inbound_exchanges(channel):
    """Declare inbound topic exchanges (simulating other teams)."""
    exchanges = {}
    for name in ["user.topic", "planning.topic", "payment.topic", "invoice.topic", "mail.topic"]:
        exchanges[name] = await channel.declare_exchange(
            name, type=ExchangeType.TOPIC, durable=True,
        )
    return exchanges


@pytest.fixture
async def outbound_exchange(channel):
    """Declare contact.topic to consume CRM's outbound messages."""
    return await channel.declare_exchange(
        "contact.topic", type=ExchangeType.TOPIC, durable=True,
    )


@pytest.fixture
async def sf_client():
    """Create a real Salesforce client for e2e verification."""
    username = os.getenv("SALESFORCE_USERNAME")
    password = os.getenv("SALESFORCE_PASSWORD")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN")
    domain = os.getenv("SALESFORCE_DOMAIN", "login")

    if not username or not password or not security_token:
        pytest.skip("Salesforce credentials missing in environment for e2e test")

    return await asyncio.to_thread(
        Salesforce,
        username=username,
        password=password,
        security_token=security_token,
        domain=domain,
    )


def _unique_email() -> str:
    """Generate a unique email to avoid Salesforce dedup conflicts."""
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"e2e.test.{suffix}@example.com"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _create_temp_queue(channel, exchange, routing_key: str):
    """Create a temporary auto-delete queue bound to an exchange."""
    queue = await channel.declare_queue("", exclusive=True, auto_delete=True)
    await queue.bind(exchange, routing_key=routing_key)
    return queue


async def _consume_one(queue, timeout: float = TIMEOUT) -> etree._Element | None:
    """Poll for one message until timeout. Returns parsed XML or None."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            msg = await queue.get(no_ack=False, fail=False)
            if msg is not None:
                await msg.ack()
                return etree.fromstring(msg.body)
        except aio_pika.exceptions.QueueEmpty:
            pass
        await asyncio.sleep(0.5)
    return None


async def _drain_queue(queue) -> None:
    """Remove leftover messages from a queue."""
    while True:
        try:
            msg = await asyncio.wait_for(queue.get(no_ack=False, fail=False), timeout=0.5)
            if msg is None:
                break
            await msg.ack()
        except (asyncio.TimeoutError, aio_pika.exceptions.QueueEmpty):
            break


async def _publish(exchange, routing_key: str, xml: str) -> None:
    """Publish XML payload to an exchange."""
    await exchange.publish(
        aio_pika.Message(
            body=xml.encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


async def _queue_message_count(channel, queue_name: str) -> int:
    """Check how many messages are in a named queue (passive declare)."""
    try:
        queue = await channel.declare_queue(queue_name, passive=True)
        return queue.declaration_result.message_count
    except Exception:
        return -1


def _escape_soql(value: str) -> str:
    """Escape single quotes for SOQL queries in e2e verification helpers."""
    return value.replace("'", "''")


def _normalize_utc_datetime(value: str) -> datetime:
    """Normalize ISO-8601-like timestamps to UTC without microseconds."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    elif len(normalized) >= 5 and normalized[-5] in "+-" and normalized[-3] != ":":
        normalized = normalized[:-5] + normalized[-5:-2] + ":" + normalized[-2:]

    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


async def _get_unique_contact_by_email(sf_client, email: str) -> dict | None:
    """Fetch exactly one Salesforce Contact by email for e2e verification."""
    query = (
        "SELECT Id, Email, CRM_ID__c, Registration_ID__c, Paid_At__c "
        f"FROM Contact WHERE Email = '{_escape_soql(email)}'"
    )
    result = await asyncio.to_thread(sf_client.query, query)

    if result["totalSize"] == 0:
        return None
    if result["totalSize"] > 1:
        raise AssertionError(f"Expected unique Contact for {email}, found {result['totalSize']}")
    return result["records"][0]


async def _wait_for_contact_paid_at(
    sf_client, email: str, expected_paid_at: str, timeout: float = TIMEOUT,
) -> dict:
    """Poll Salesforce until the Contact has the expected payment timestamp."""
    expected_dt = _normalize_utc_datetime(expected_paid_at)
    deadline = asyncio.get_event_loop().time() + timeout
    last_seen_paid_at: str | None = None

    while asyncio.get_event_loop().time() < deadline:
        contact = await _get_unique_contact_by_email(sf_client, email)
        if contact is not None:
            last_seen_paid_at = contact.get("Paid_At__c")
            if last_seen_paid_at is not None:
                seen_dt = _normalize_utc_datetime(last_seen_paid_at)
                if seen_dt == expected_dt:
                    return contact
        await asyncio.sleep(0.5)

    raise AssertionError(
        f"Contact {email} did not reach Paid_At__c={expected_paid_at!r} "
        f"within {timeout}s (last seen: {last_seen_paid_at!r})"
    )


# ---------------------------------------------------------------------------
# Contract 9 — Controlroom → CRM: system warning (no Salesforce needed)
# ---------------------------------------------------------------------------


class TestContract9Warning:
    """C9: controlroom.warning.issued → CRM logs as error, no outbound."""

    @pytest.mark.asyncio
    async def test_valid_warning_is_consumed(self, channel, inbound_exchanges):
        """A valid warning XML should be consumed from the queue (not stuck)."""
        queue_name = "controlroom.warning.issued"
        queue = await channel.declare_queue(queue_name, durable=False)
        await queue.bind(inbound_exchanges["planning.topic"], routing_key=queue_name)
        await _drain_queue(queue)

        xml = """<?xml version='1.0' encoding='utf-8'?>
<Warning>
    <serviceId>CRM</serviceId>
    <message>E2E test waarschuwing</message>
    <type>user</type>
</Warning>"""

        await _publish(inbound_exchanges["planning.topic"], queue_name, xml)

        # Wait for CRM to consume it
        await asyncio.sleep(3)

        # Queue should be empty — CRM consumed and acked
        remaining = await _queue_message_count(channel, queue_name)
        assert remaining == 0, f"Warning message still in queue (count={remaining})"

    @pytest.mark.asyncio
    async def test_invalid_xml_is_rejected(self, channel, inbound_exchanges):
        """Invalid XML should be rejected (not requeued), queue stays empty."""
        queue_name = "controlroom.warning.issued"
        queue = await channel.declare_queue(queue_name, durable=False)
        await queue.bind(inbound_exchanges["planning.topic"], routing_key=queue_name)
        await _drain_queue(queue)

        await _publish(inbound_exchanges["planning.topic"], queue_name, "dit is geen xml <<<")

        await asyncio.sleep(3)

        remaining = await _queue_message_count(channel, queue_name)
        assert remaining == 0, f"Invalid message still in queue (count={remaining})"


# ---------------------------------------------------------------------------
# Contract 1 → Contracts 13 + 6 — Registration → User Confirmed + Mail
# ---------------------------------------------------------------------------


@pytest.mark.salesforce
class TestContract1Registration:
    """C1: frontend.registration.created → C13 crm.user.confirmed + C6 crm.mail.requested."""

    @pytest.mark.asyncio
    async def test_new_registration_produces_user_confirmed(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """A new registration should produce a UserConfirmed on contact.topic."""
        email = _unique_email()

        # Listen for outbound
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-E2E-{random.randint(100000, 999999)}</registrationId>
    <firstName>E2E</firstName>
    <lastName>Test</lastName>
    <email>{email}</email>
    <sessionId>SESS-E2E-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", xml)

        # Wait for CRM to process via Salesforce and publish confirmation
        result = await _consume_one(q_confirmed)

        assert result is not None, "No UserConfirmed message received within timeout"
        assert result.tag == "UserConfirmed"
        assert result.findtext("email") == email
        assert result.findtext("firstName") == "E2E"
        assert result.findtext("lastName") == "Test"
        assert result.findtext("role") == "VISITOR"
        assert result.findtext("isActive") == "true"
        assert result.findtext("gdprConsent") == "true"
        assert result.findtext("id") is not None, "Missing UUID in UserConfirmed"
        assert result.findtext("confirmedAt") is not None

    @pytest.mark.asyncio
    async def test_new_registration_produces_mail_requested(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """A new registration should also produce a MailRequest on contact.topic."""
        email = _unique_email()

        q_mail = await _create_temp_queue(channel, outbound_exchange, "crm.mail.requested")
        await _drain_queue(q_mail)

        xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-E2E-{random.randint(100000, 999999)}</registrationId>
    <firstName>Mail</firstName>
    <lastName>Test</lastName>
    <email>{email}</email>
    <sessionId>SESS-E2E-002</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", xml)

        result = await _consume_one(q_mail)

        assert result is not None, "No MailRequest message received within timeout"
        assert result.tag == "MailRequest"
        assert result.findtext("header/source") == "CRM"
        assert result.findtext("mailType") == "registration_confirmation"
        assert result.findtext("recipient/email") == email
        assert result.findtext("dynamic_data/guest_name") is not None

    @pytest.mark.asyncio
    async def test_gdpr_false_produces_no_outbound(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """Registration with gdprConsent=false should be rejected, no outbound."""
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-E2E-GDPR-{random.randint(100000, 999999)}</registrationId>
    <firstName>NoGdpr</firstName>
    <lastName>Test</lastName>
    <email>{_unique_email()}</email>
    <sessionId>SESS-E2E-003</sessionId>
    <role>VISITOR</role>
    <gdprConsent>false</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", xml)

        result = await _consume_one(q_confirmed, timeout=5)
        assert result is None, "UserConfirmed should NOT be produced for gdprConsent=false"

    @pytest.mark.asyncio
    async def test_invalid_xml_produces_no_outbound(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """Invalid XML should be rejected, no outbound."""
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", "broken xml <<<")

        result = await _consume_one(q_confirmed, timeout=5)
        assert result is None, "UserConfirmed should NOT be produced for invalid XML"


# ---------------------------------------------------------------------------
# Contract 2 (updated) → Contract 18 — Registration Update → User Updated
# ---------------------------------------------------------------------------


@pytest.mark.salesforce
class TestContract2Updated:
    """C2 changeType=updated → C18 crm.user.updated."""

    @pytest.mark.asyncio
    async def test_update_produces_user_updated(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """Updating an existing contact should produce a UserUpdated message."""
        email = _unique_email()

        # First create the contact via C1
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        reg_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-E2E-UPD-{random.randint(100000, 999999)}</registrationId>
    <firstName>Before</firstName>
    <lastName>Update</lastName>
    <email>{email}</email>
    <sessionId>SESS-E2E-004</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", reg_xml)
        confirmed = await _consume_one(q_confirmed)
        assert confirmed is not None, "Prerequisite: C1 must produce UserConfirmed first"

        # Now send the update
        q_updated = await _create_temp_queue(channel, outbound_exchange, "crm.user.updated")
        await _drain_queue(q_updated)

        update_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>{email}</email>
    <sessionId>SESS-E2E-004</sessionId>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>After</firstName>
    </updatedFields>
</RegistrationChange>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.updated", update_xml)

        result = await _consume_one(q_updated)

        assert result is not None, "No UserUpdated message received within timeout"
        assert result.tag == "UserUpdated"
        assert result.findtext("email") == email
        assert result.findtext("firstName") == "After"
        assert result.findtext("updatedAt") is not None
        assert result.findtext("id") is not None


# ---------------------------------------------------------------------------
# Contract 2 (cancelled) → Contract 22 — Registration Cancel → User Deactivated
# ---------------------------------------------------------------------------


@pytest.mark.salesforce
class TestContract2Cancelled:
    """C2 changeType=cancelled → C22 crm.user.deactivated."""

    @pytest.mark.asyncio
    async def test_cancel_produces_user_deactivated(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """Cancelling an existing contact should produce a UserDeactivated message."""
        email = _unique_email()

        # First create the contact via C1
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        reg_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-E2E-CAN-{random.randint(100000, 999999)}</registrationId>
    <firstName>ToCancel</firstName>
    <lastName>User</lastName>
    <email>{email}</email>
    <sessionId>SESS-E2E-005</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", reg_xml)
        confirmed = await _consume_one(q_confirmed)
        assert confirmed is not None, "Prerequisite: C1 must produce UserConfirmed first"

        # Now cancel
        q_deactivated = await _create_temp_queue(channel, outbound_exchange, "crm.user.deactivated")
        await _drain_queue(q_deactivated)

        cancel_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>{email}</email>
    <sessionId>SESS-E2E-005</sessionId>
    <changeType>cancelled</changeType>
</RegistrationChange>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.updated", cancel_xml)

        result = await _consume_one(q_deactivated)

        assert result is not None, "No UserDeactivated message received within timeout"
        assert result.tag == "UserDeactivated"
        assert result.findtext("email") == email
        assert result.findtext("deactivatedAt") is not None
        assert result.findtext("id") is not None

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_produces_no_outbound(
        self, channel, inbound_exchanges, outbound_exchange,
    ):
        """Cancelling a non-existent contact should produce no outbound."""
        q_deactivated = await _create_temp_queue(channel, outbound_exchange, "crm.user.deactivated")
        await _drain_queue(q_deactivated)

        cancel_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>nonexistent.e2e.{random.randint(100000,999999)}@example.com</email>
    <sessionId>SESS-E2E-NONE</sessionId>
    <changeType>cancelled</changeType>
</RegistrationChange>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.updated", cancel_xml)

        result = await _consume_one(q_deactivated, timeout=5)
        assert result is None, "UserDeactivated should NOT be produced for non-existent contact"


# ---------------------------------------------------------------------------
# Contract 16 — Payment Confirmed → Salesforce Paid_At__c update
# ---------------------------------------------------------------------------


# TODO: Add negative C16 e2e coverage for unknown contacts and registrationId mismatches.
@pytest.mark.salesforce
class TestContract16PaymentConfirmed:
    """C16: kassa.payment.confirmed updates Contact.Paid_At__c in Salesforce."""

    @pytest.mark.asyncio
    async def test_payment_confirmed_updates_contact_paid_at(
        self, channel, inbound_exchanges, outbound_exchange, sf_client,
    ):
        """Happy path: C16 is consumed and Paid_At__c is updated in Salesforce."""
        email = _unique_email()
        registration_id = f"REG-E2E-PAY-{random.randint(100000, 999999)}"

        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        registration_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>{registration_id}</registrationId>
    <firstName>Payment</firstName>
    <lastName>Test</lastName>
    <email>{email}</email>
    <sessionId>SESS-E2E-006</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

        await _publish(inbound_exchanges["user.topic"], "frontend.registration.created", registration_xml)

        confirmed = await _consume_one(q_confirmed)
        assert confirmed is not None, "Prerequisite: C1 must produce UserConfirmed first"

        user_id = confirmed.findtext("id")
        assert user_id is not None, "UserConfirmed did not contain an id for Contract 16 lookup"

        queue_name = "kassa.payment.confirmed"
        payment_queue = await channel.declare_queue(queue_name, durable=True)
        await payment_queue.bind(inbound_exchanges["payment.topic"], routing_key=queue_name)
        await _drain_queue(payment_queue)

        paid_at = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
        payment_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<PaymentConfirmed>
    <userId>{user_id}</userId>
    <email>{email}</email>
    <registrationId>{registration_id}</registrationId>
    <amount>49.95</amount>
    <currency>EUR</currency>
    <paidAt>{paid_at}</paidAt>
</PaymentConfirmed>"""

        await _publish(inbound_exchanges["payment.topic"], queue_name, payment_xml)

        contact = await _wait_for_contact_paid_at(sf_client, email, paid_at)
        remaining = await _queue_message_count(channel, queue_name)

        assert remaining == 0, f"PaymentConfirmed message still in queue (count={remaining})"
        assert contact["Email"] == email
        assert _normalize_utc_datetime(contact["Paid_At__c"]) == _normalize_utc_datetime(paid_at)
