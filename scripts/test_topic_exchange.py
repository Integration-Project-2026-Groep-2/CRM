"""Test script — verify topic exchange routing for ALL CRM contracts.

Tests both inbound (other teams → CRM) and outbound (CRM → other teams)
exchange routing. Publishes via the correct topic exchange and verifies
the message arrives at the expected queue.

Does NOT require the CRM container to be running — creates temporary
test queues and cleans up after itself.

Usage:
    # Against local RabbitMQ (docker compose up rabbitmq)
    python scripts/test_topic_exchange.py

    # Against Azure VM (via SSH tunnel)
    RABBITMQ_URL=amqp://lapin:<password>@localhost/ python scripts/test_topic_exchange.py

    # Verbose mode (show XML payloads)
    python scripts/test_topic_exchange.py --verbose
"""

import asyncio
import os
import sys
from dataclasses import dataclass

import aio_pika
from aio_pika import ExchangeType
from dotenv import load_dotenv

load_dotenv()

TIMEOUT_SECONDS = 3


@dataclass
class TestCase:
    name: str
    exchange: str
    routing_key: str
    xml: str
    durable: bool
    direction: str  # "inbound" or "outbound"


# ---------------------------------------------------------------------------
# Inbound: other teams → CRM (receiver binds to these exchanges)
# ---------------------------------------------------------------------------

INBOUND_TESTS: list[TestCase] = [
    # user.topic — Frontend
    TestCase(
        name="C1 — Frontend: nieuwe registratie",
        exchange="user.topic",
        routing_key="frontend.registration.created",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-EXTEST-001</registrationId>
    <firstName>Exchange</firstName>
    <lastName>Test</lastName>
    <email>exchange.test@example.com</email>
    <sessionId>SESS-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>""",
    ),
    TestCase(
        name="C2 — Frontend: registratie update",
        exchange="user.topic",
        routing_key="frontend.registration.updated",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>exchange.test@example.com</email>
    <sessionId>SESS-001</sessionId>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Updated</firstName>
    </updatedFields>
</RegistrationChange>""",
    ),
    TestCase(
        name="C3 — Frontend: nieuw bedrijf",
        exchange="user.topic",
        routing_key="frontend.company.created",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>Exchange Test BV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@exchangetest.be</email>
</CompanyCreated>""",
    ),

    # invoice.topic — Facturatie
    TestCase(
        name="C5a — Facturatie: bedrijfsdata request",
        exchange="invoice.topic",
        routing_key="facturatie.company.requested",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<CompanyRequest>
    <requestId>REQ-EXTEST-001</requestId>
    <vatNumber>BE0123456789</vatNumber>
</CompanyRequest>""",
    ),

    # planning.topic — Controlroom
    TestCase(
        name="C9 — Controlroom: systeemwaarschuwing",
        exchange="planning.topic",
        routing_key="controlroom.warning.issued",
        durable=False,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<Warning>
    <serviceId>CRM</serviceId>
    <message>Exchange routing test waarschuwing</message>
    <type>user</type>
</Warning>""",
    ),

    # payment.topic — Kassa
    TestCase(
        name="C10a — Kassa: persoonscheck request",
        exchange="payment.topic",
        routing_key="kassa.person.lookup.requested",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<PersonLookupRequest>
    <requestId>REQ-EXTEST-002</requestId>
    <email>exchange.test@example.com</email>
</PersonLookupRequest>""",
    ),
    TestCase(
        name="C16 — Kassa: betaling bevestigd",
        exchange="payment.topic",
        routing_key="kassa.payment.confirmed",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<PaymentConfirmed>
    <email>exchange.test@example.com</email>
    <amount>25.00</amount>
    <currency>EUR</currency>
    <paidAt>2026-04-01T14:00:00Z</paidAt>
</PaymentConfirmed>""",
    ),
    TestCase(
        name="C17a — Kassa: niet-betaalden request",
        exchange="payment.topic",
        routing_key="kassa.unpaid.requested",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UnpaidRequest>
    <requestId>REQ-EXTEST-003</requestId>
</UnpaidRequest>""",
    ),

    # planning.topic — Planning
    TestCase(
        name="C11 — Planning: sessiewijziging",
        exchange="planning.topic",
        routing_key="planning.session.updated",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<SessionUpdate>
    <sessionId>SESS-001</sessionId>
    <sessionName>Keynote ShiftFestival</sessionName>
    <newTime>2026-06-15T10:00:00Z</newTime>
    <changeType>rescheduled</changeType>
</SessionUpdate>""",
    ),

    # planning.topic — IoT
    TestCase(
        name="C12 — IoT: badge koppelen",
        exchange="planning.topic",
        routing_key="iot.badge.linked",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<BadgeLink>
    <badgeId>BADGE-EXTEST-001</badgeId>
    <contactEmail>exchange.test@example.com</contactEmail>
    <linkedAt>2026-04-01T15:00:00Z</linkedAt>
</BadgeLink>""",
    ),

    # mail.topic — Mailing
    TestCase(
        name="C20 — Mailing: bounce report",
        exchange="mail.topic",
        routing_key="mailing.bounce.reported",
        durable=True,
        direction="inbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<BounceReported>
    <email>exchange.test@example.com</email>
    <bounceType>hard</bounceType>
    <reason>Mailbox does not exist</reason>
    <bouncedAt>2026-04-01T12:00:00Z</bouncedAt>
</BounceReported>""",
    ),
]

# ---------------------------------------------------------------------------
# Outbound: CRM → other teams (sender publishes to contact.topic)
# ---------------------------------------------------------------------------

OUTBOUND_TESTS: list[TestCase] = [
    TestCase(
        name="C13 — CRM: user confirmed",
        exchange="contact.topic",
        routing_key="crm.user.confirmed",
        durable=True,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UserConfirmed>
    <id>550e8400-e29b-41d4-a716-446655440000</id>
    <email>exchange.test@example.com</email>
    <firstName>Exchange</firstName>
    <lastName>Test</lastName>
    <role>VISITOR</role>
    <isActive>true</isActive>
    <gdprConsent>true</gdprConsent>
    <confirmedAt>2026-04-01T10:00:00Z</confirmedAt>
</UserConfirmed>""",
    ),
    TestCase(
        name="C14 — CRM: company confirmed",
        exchange="contact.topic",
        routing_key="crm.company.confirmed",
        durable=True,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<CompanyConfirmed>
    <id>660e8400-e29b-41d4-a716-446655440001</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Exchange Test BV</name>
    <email>info@exchangetest.be</email>
    <isActive>true</isActive>
    <confirmedAt>2026-04-01T10:00:00Z</confirmedAt>
</CompanyConfirmed>""",
    ),
    TestCase(
        name="C5b — CRM: company response",
        exchange="contact.topic",
        routing_key="crm.company.responded",
        durable=False,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<CompanyResponse>
    <requestId>REQ-EXTEST-001</requestId>
    <found>true</found>
    <id>660e8400-e29b-41d4-a716-446655440001</id>
    <name>Exchange Test BV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@exchangetest.be</email>
</CompanyResponse>""",
    ),
    TestCase(
        name="C6 — CRM: mail request",
        exchange="contact.topic",
        routing_key="crm.mail.requested",
        durable=True,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<MailRequest>
    <header>
        <source>CRM</source>
        <timestamp>2026-04-01T10:00:00Z</timestamp>
    </header>
    <mailType>registration_confirmation</mailType>
    <recipient>
        <email>exchange.test@example.com</email>
        <name>Exchange Test</name>
    </recipient>
    <dynamic_data>
        <guest_name>Exchange Test</guest_name>
    </dynamic_data>
</MailRequest>""",
    ),
    TestCase(
        name="C8 — CRM: status check",
        exchange="contact.topic",
        routing_key="crm.status.checked",
        durable=False,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<StatusCheck>
    <serviceId>CRM</serviceId>
    <timestamp>2026-04-01T10:00:00Z</timestamp>
    <status>healthy</status>
    <uptime>3600</uptime>
    <systemLoad>
        <cpu>0.23</cpu>
        <memory>0.41</memory>
        <disk>0.15</disk>
    </systemLoad>
</StatusCheck>""",
    ),
    TestCase(
        name="C10b — CRM: person lookup response",
        exchange="contact.topic",
        routing_key="crm.person.lookup.responded",
        durable=False,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<PersonLookupResponse>
    <requestId>REQ-EXTEST-002</requestId>
    <found>true</found>
    <linkedToCompany>false</linkedToCompany>
    <id>550e8400-e29b-41d4-a716-446655440000</id>
</PersonLookupResponse>""",
    ),
    TestCase(
        name="C17b — CRM: unpaid response",
        exchange="contact.topic",
        routing_key="crm.unpaid.responded",
        durable=False,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UnpaidResponse>
    <requestId>REQ-EXTEST-003</requestId>
    <persons>
        <person>
            <id>550e8400-e29b-41d4-a716-446655440000</id>
            <firstName>Exchange</firstName>
            <lastName>Test</lastName>
            <email>exchange.test@example.com</email>
            <linkedToCompany>false</linkedToCompany>
        </person>
    </persons>
</UnpaidResponse>""",
    ),
    TestCase(
        name="C18 — CRM: user updated",
        exchange="contact.topic",
        routing_key="crm.user.updated",
        durable=True,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>550e8400-e29b-41d4-a716-446655440000</id>
    <email>exchange.test@example.com</email>
    <firstName>ExchangeUpdated</firstName>
    <lastName>Test</lastName>
    <role>VISITOR</role>
    <isActive>true</isActive>
    <gdprConsent>true</gdprConsent>
    <updatedAt>2026-04-01T11:00:00Z</updatedAt>
</UserUpdated>""",
    ),
    TestCase(
        name="C22 — CRM: user deactivated",
        exchange="contact.topic",
        routing_key="crm.user.deactivated",
        durable=True,
        direction="outbound",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>550e8400-e29b-41d4-a716-446655440000</id>
    <email>exchange.test@example.com</email>
    <deactivatedAt>2026-04-01T16:00:00Z</deactivatedAt>
</UserDeactivated>""",
    ),
]

# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

async def _consume_one(queue: aio_pika.abc.AbstractQueue, timeout: float) -> bytes | None:
    """Try to consume one message within timeout. Returns body or None."""
    try:
        msg = await asyncio.wait_for(queue.get(no_ack=False, fail=False), timeout=timeout)
        if msg is None:
            return None
        await msg.ack()
        return msg.body
    except (asyncio.TimeoutError, aio_pika.exceptions.QueueEmpty):
        return None


async def _drain_queue(queue: aio_pika.abc.AbstractQueue) -> None:
    """Remove any leftover messages from a queue."""
    while True:
        try:
            msg = await asyncio.wait_for(queue.get(no_ack=False, fail=False), timeout=0.5)
            if msg is None:
                break
            await msg.ack()
        except (asyncio.TimeoutError, aio_pika.exceptions.QueueEmpty):
            break


@dataclass
class NegativeTestCase:
    name: str
    correct_exchange: str
    wrong_exchange: str
    routing_key: str
    xml: str


NEGATIVE_CASES: list[NegativeTestCase] = [
    NegativeTestCase(
        name="NEGATIVE — C1: queue op user.topic, publish naar payment.topic",
        correct_exchange="user.topic",
        wrong_exchange="payment.topic",
        routing_key="frontend.registration.created",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-NEGATIVE-001</registrationId>
    <firstName>Should</firstName>
    <lastName>NotArrive</lastName>
    <email>negative@example.com</email>
    <sessionId>SESS-001</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>""",
    ),
    NegativeTestCase(
        name="NEGATIVE — C9: queue op planning.topic, publish naar user.topic",
        correct_exchange="planning.topic",
        wrong_exchange="user.topic",
        routing_key="controlroom.warning.issued",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<Warning>
    <serviceId>CRM</serviceId>
    <message>Should not arrive</message>
    <type>user</type>
</Warning>""",
    ),
    NegativeTestCase(
        name="NEGATIVE — C13: queue op contact.topic, publish naar user.topic",
        correct_exchange="contact.topic",
        wrong_exchange="user.topic",
        routing_key="crm.user.confirmed",
        xml="""<?xml version='1.0' encoding='utf-8'?>
<UserConfirmed>
    <id>550e8400-e29b-41d4-a716-446655440099</id>
    <email>negative@example.com</email>
    <firstName>Should</firstName>
    <lastName>NotArrive</lastName>
    <role>VISITOR</role>
    <isActive>true</isActive>
    <gdprConsent>true</gdprConsent>
    <confirmedAt>2026-04-01T10:00:00Z</confirmedAt>
</UserConfirmed>""",
    ),
]


async def run_test(
    channel: aio_pika.abc.AbstractChannel,
    test: TestCase,
    verbose: bool,
) -> tuple[str, bool, str]:
    """Run a single positive test case. Returns (name, passed, detail)."""
    exchange = await channel.declare_exchange(
        test.exchange, type=ExchangeType.TOPIC, durable=True,
    )

    test_queue_name = f"_test_.{test.routing_key}"
    queue = await channel.declare_queue(test_queue_name, durable=False, auto_delete=True)
    await queue.bind(exchange, routing_key=test.routing_key)
    await _drain_queue(queue)

    await exchange.publish(
        aio_pika.Message(body=test.xml.encode("utf-8")),
        routing_key=test.routing_key,
    )

    if verbose:
        print(f"    Published to {test.exchange} -> {test.routing_key}")

    body = await _consume_one(queue, timeout=TIMEOUT_SECONDS)

    await queue.unbind(exchange, routing_key=test.routing_key)
    await queue.delete()

    if body is not None:
        return test.name, True, f"delivered ({len(body)} bytes)"
    return test.name, False, "message NOT delivered (timeout)"


async def run_negative_test(
    channel: aio_pika.abc.AbstractChannel,
    test: NegativeTestCase,
    verbose: bool,
) -> tuple[str, bool, str]:
    """Run a negative test: bind queue to correct exchange, publish to wrong exchange."""
    correct_ex = await channel.declare_exchange(
        test.correct_exchange, type=ExchangeType.TOPIC, durable=True,
    )
    wrong_ex = await channel.declare_exchange(
        test.wrong_exchange, type=ExchangeType.TOPIC, durable=True,
    )

    test_queue_name = f"_test_neg_.{test.routing_key}"
    queue = await channel.declare_queue(test_queue_name, durable=False, auto_delete=True)
    # Bind to the CORRECT exchange only
    await queue.bind(correct_ex, routing_key=test.routing_key)
    await _drain_queue(queue)

    # Publish to the WRONG exchange
    await wrong_ex.publish(
        aio_pika.Message(body=test.xml.encode("utf-8")),
        routing_key=test.routing_key,
    )

    if verbose:
        print(f"    Queue bound to {test.correct_exchange}, published to {test.wrong_exchange}")

    body = await _consume_one(queue, timeout=TIMEOUT_SECONDS)

    await queue.unbind(correct_ex, routing_key=test.routing_key)
    await queue.delete()

    if body is None:
        return test.name, True, "correctly NOT delivered"
    return test.name, False, "message arrived but should NOT have"


async def main() -> None:
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    rmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    print(f"Connecting to {rmq_url}...")
    try:
        connection = await aio_pika.connect_robust(rmq_url)
    except Exception as e:
        print(f"\nFailed to connect: {e}")
        print("Is RabbitMQ running? Try: docker compose up rabbitmq -d")
        sys.exit(1)

    results: list[tuple[str, bool, str]] = []

    async with connection:
        channel = await connection.channel()

        # --- Inbound tests ---
        print(f"\n{'=' * 60}")
        print("INBOUND — other teams -> CRM (11 queues)")
        print(f"{'=' * 60}")
        for test in INBOUND_TESTS:
            name, passed, detail = await run_test(channel, test, verbose)
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {name}")
            if verbose or not passed:
                print(f"         {test.exchange} -> {test.routing_key}: {detail}")
            results.append((name, passed, detail))

        # --- Outbound tests ---
        print(f"\n{'=' * 60}")
        print("OUTBOUND — CRM -> other teams (contact.topic)")
        print(f"{'=' * 60}")
        for test in OUTBOUND_TESTS:
            name, passed, detail = await run_test(channel, test, verbose)
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {name}")
            if verbose or not passed:
                print(f"         {test.exchange} -> {test.routing_key}: {detail}")
            results.append((name, passed, detail))

        # --- Negative tests ---
        print(f"\n{'=' * 60}")
        print("NEGATIVE — wrong exchange should NOT deliver")
        print(f"{'=' * 60}")
        for test in NEGATIVE_CASES:
            name, passed, detail = await run_negative_test(channel, test, verbose)
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {name}")
            if verbose or not passed:
                print(f"         bound={test.correct_exchange}, published={test.wrong_exchange}: {detail}")
            results.append((name, passed, detail))

    # --- Summary ---
    total = len(results)
    passed = sum(1 for _, p, _ in results if p)
    failed = sum(1 for _, p, _ in results if not p)

    print(f"\n{'=' * 60}")
    print(f"RESULTS: {passed}/{total} passed, {failed} failed")
    print(f"{'=' * 60}")

    if failed:
        print("\nFailed tests:")
        for name, p, detail in results:
            if not p:
                print(f"  FAIL: {name} — {detail}")
        sys.exit(1)
    else:
        print("\nAll exchange routing tests passed!")


if __name__ == "__main__":
    asyncio.run(main())
