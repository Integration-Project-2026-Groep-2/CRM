"""RabbitMQ queue consumer — listens on the configured inbound queues."""

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING

import aio_pika
import aiormq
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from lxml import etree

from src import sender, xml_validator
from src.config import Config
from src.salesforce_client import (
    apply_is_active,
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    coerce_is_active,
    count_active_session_registrations,
    create_contact,
    deactivate_contact_record,
    deactivate_session_registration,
    ensure_contact_identifiers,
<<<<<<< feature/registration-receiver-v2
    get_contact_by_crm_id,
=======
    ensure_session_registration_active,
    get_active_session_participants,
>>>>>>> dev
    get_contact_by_email,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    get_contact_match_by_mailing_id,
    get_contact_match_by_planning_id,
    get_salesforce_client,
    get_session_registration_by_registration_id,
    get_unpaid_contacts,
    has_contact_mailing_id_field,
    has_contact_planning_id_field,
    has_session_registration_object,
    is_rate_limit_error,
    update_facturatie_contact,
    update_mailing_contact,
    update_payment_status,
    update_planning_contact,
    upsert_contact_by_email,
    upsert_session_registration,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)

# Queue → topic exchange mapping (Infra-beheerd, zie docs/rabbitmq-exchanges.md).
# Queue-namen volgen consumer-prefix conventie (`crm.<producer>.<event>`) wanneer
# de naam anders zou botsen met een consumer-queue van een ander team op een
# andere exchange. De routing keys blijven de producer-eventnamen — zie de
# run_receiver calls waar routing_key expliciet meegegeven wordt.
_INBOUND_EXCHANGE: dict[str, str] = {
    "frontend.registration.created": "user.topic",
    "frontend.registration.updated": "user.topic",
    "frontend.company.created": "user.topic",
    "facturatie.user.created": "user.topic",
    "facturatie.user.updated": "user.topic",
    "facturatie.user.deactivated": "user.topic",
    "mailing.user.created": "user.topic",
    "crm.mailing.user.updated": "user.topic",
    "crm.mailing.user.deactivated": "user.topic",
    "planning.user.created": "user.topic",
    "planning.user.updated": "user.topic",
    "planning.user.deactivated": "user.topic",
    "facturatie.company.requested": "invoice.topic",
    "kassa.person.lookup.requested": "payment.topic",
    "kassa.payment.confirmed": "payment.topic",
    "kassa.unpaid.requested": "payment.topic",
    "planning.session.updated": "planning.topic",
    "controlroom.warning.issued": "planning.topic",
    "iot.badge.linked": "planning.topic",
    "mailing.bounce.reported": "mail.topic",
}

_MAX_REQUEUE_ATTEMPTS = 5
_MAX_DEFERRAL_ATTEMPTS = 10
_RATE_LIMIT_SLEEP_SECONDS = 60
_BACKOFF_CAP_SECONDS = 30


def _delivery_attempt_count(message: aio_pika.IncomingMessage) -> int:
    """Return the retry count stored in our custom `x-retry-count` header.

    RabbitMQ does not track retry counts on classic queues when messages
    are requeued via basic.reject(requeue=True). We therefore maintain the
    counter ourselves: each time a handler requeues a message, it ack's
    the original and publishes a fresh copy with `x-retry-count` bumped
    by one (see `_republish_with_retry_count`).
    """
    headers = message.headers or {}
    try:
        return int(headers.get("x-retry-count", 0))
    except (TypeError, ValueError):
        return 0


def _exponential_backoff_seconds(attempt: int, cap: int = _BACKOFF_CAP_SECONDS) -> float:
    """Exponential backoff doubling on each attempt, capped.

    attempt 0 → 1s, 1 → 2s, 2 → 4s, 3 → 8s, 4 → 16s, 5+ → cap (30s by default).
    """
    if attempt < 0:
        attempt = 0
    return float(min(2 ** attempt, cap))


async def _republish_with_retry_count(
    message: aio_pika.IncomingMessage, new_count: int,
) -> None:
    """Ack the current message and publish a fresh copy with updated retry count.

    RabbitMQ's `basic.reject(requeue=True)` does not add tracking metadata
    to the requeued message, so we cannot count retries via the AMQP
    `x-death` header unless dead-lettering is configured. Instead, we ack
    the current delivery and publish a new copy of the payload to the same
    exchange+routing_key with an incremented `x-retry-count` header. The
    fresh message lands back in the same queue (RabbitMQ routes it via the
    queue's binding) and the next handler invocation reads the updated
    counter via `_delivery_attempt_count`.

    Implementation note: `aio_pika.IncomingMessage.channel` returns the raw
    `aiormq.Channel` (not the aio-pika wrapper), so we use the low-level
    `basic_publish` directly with an `aiormq.spec.Basic.Properties` struct.
    No exchange declare is needed — the broker already knows the exchange.

    Trade-off: message_id and timestamp are regenerated per retry, and the
    AMQP `redelivered` flag no longer reflects retries. We don't rely on
    either, so this is acceptable.
    """
    headers = dict(message.headers or {})
    headers["x-retry-count"] = new_count

    properties = aiormq.spec.Basic.Properties(
        content_type=message.content_type,
        content_encoding=message.content_encoding,
        headers=headers,
        delivery_mode=message.delivery_mode,
    )

    await message.channel.basic_publish(
        body=message.body,
        exchange=message.exchange or "",
        routing_key=message.routing_key or "",
        properties=properties,
    )
    await message.ack()


async def _handle_processing_error(
    contract: str, message: aio_pika.IncomingMessage, exc: Exception,
) -> None:
    """Centralised transient-error handling for receiver handlers.

    - Salesforce rate-limit → sleep then drop (no requeue, no self-DOS).
    - Max retries exceeded → drop without requeue.
    - Otherwise: exponential backoff, then republish with incremented retry
      count so the next attempt reads the correct counter.
    """
    if is_rate_limit_error(exc):
        logger.error(
            "%s — Salesforce rate limit hit; sleeping %ss then dropping: %s",
            contract, _RATE_LIMIT_SLEEP_SECONDS, exc,
        )
        await asyncio.sleep(_RATE_LIMIT_SLEEP_SECONDS)
        await message.reject(requeue=False)
        return
    attempts = _delivery_attempt_count(message)
    if attempts >= _MAX_REQUEUE_ATTEMPTS:
        logger.error(
            "%s — max retries (%d) exceeded; dropping: %s",
            contract, _MAX_REQUEUE_ATTEMPTS, exc,
        )
        await message.reject(requeue=False)
        return
    sleep_s = _exponential_backoff_seconds(attempts)
    logger.error(
        "%s — error (attempt %d/%d); sleeping %ss before requeue: %s",
        contract, attempts + 1, _MAX_REQUEUE_ATTEMPTS, sleep_s, exc,
    )
    await asyncio.sleep(sleep_s)
    await _republish_with_retry_count(message, attempts + 1)


async def _handle_out_of_order_deferral(
    contract: str,
    message: aio_pika.IncomingMessage,
    *,
    identifier_label: str,
    identifier_value: str,
) -> None:
    """Requeue with exponential backoff for create/update ordering races.

    When an update/deactivate arrives before the matching create is processed,
    we retry with increasing delays instead of tight-looping. This gives the
    create-handler breathing room and prevents burning Salesforce API quota
    on queries that will keep returning empty results.

    Drops the message after _MAX_DEFERRAL_ATTEMPTS retries — at that point the
    create almost certainly never arrived and further retries are pointless.

    Uses `_republish_with_retry_count` so the retry counter survives across
    deliveries (see that helper's docstring for rationale).
    """
    attempts = _delivery_attempt_count(message)
    if attempts >= _MAX_DEFERRAL_ATTEMPTS:
        logger.warning(
            "%s — deferred %d times for %s=%s; dropping (upstream create never arrived)",
            contract, attempts, identifier_label, identifier_value,
        )
        await message.reject(requeue=False)
        return
    sleep_s = _exponential_backoff_seconds(attempts)
    logger.warning(
        "%s deferred (attempt %d/%d) — no match for %s=%s; sleeping %ss before requeue",
        contract, attempts + 1, _MAX_DEFERRAL_ATTEMPTS,
        identifier_label, identifier_value, sleep_s,
    )
    await asyncio.sleep(sleep_s)
    await _republish_with_retry_count(message, attempts + 1)


async def _declare_and_bind(
    channel: AbstractChannel,
    queue_name: str,
    durable: bool,
    *,
    routing_key: str | None = None,
) -> aio_pika.abc.AbstractQueue:
    """Declare a queue and bind it to the mapped topic exchange.

    When `routing_key` is omitted, the queue-name is reused as routing key
    (point-to-point queues where the name matches the producer event). Passing
    an explicit routing_key is required when the queue-name is consumer-prefixed
    to avoid collisions while still binding to the producer's event.
    """
    queue = await channel.declare_queue(queue_name, durable=durable)
    exchange_name = _INBOUND_EXCHANGE.get(queue_name)
    if exchange_name:
        exchange = await channel.declare_exchange(
            exchange_name, type=ExchangeType.TOPIC, durable=True,
        )
        effective_routing_key = routing_key or queue_name
        await queue.bind(exchange, routing_key=effective_routing_key)
    return queue


async def run_receiver(
    connection: AbstractRobustConnection,
    config: Config,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Consume configured inbound messages, validate XML, process in Salesforce.

    Contract 9 is the first implemented handler and establishes the base structure
    for all future contract handlers.

    `shutdown_event` is forwarded to the Salesforce login retry loop so a
    graceful shutdown during a transient Salesforce outage does not hang the
    container.
    """
    channel = await connection.channel()
    sf_client = await get_salesforce_client(config, shutdown_event=shutdown_event)

    # Contract 9 — Controlroom → CRM: system warning
    # Queue: controlroom.warning.issued | Exchange: planning.topic | durable: false | US-26
    queue_warning = await _declare_and_bind(channel, "controlroom.warning.issued", durable=False)
    await queue_warning.consume(handle_warning)

    # Contract 1 — Frontend → CRM: new registration
    # Queue: frontend.registration.created | Exchange: user.topic | durable: true
    queue_registration = await _declare_and_bind(channel, "frontend.registration.created", durable=True)
    await queue_registration.consume(partial(handle_registration, sf=sf_client))

    # Contract 2 — Frontend → CRM: update/cancel registration
    # Queue: frontend.registration.updated | Exchange: user.topic | durable: true
    queue_reg_updated = await _declare_and_bind(channel, "frontend.registration.updated", durable=True)
    await queue_reg_updated.consume(partial(handle_registration_updated, sf=sf_client))

    # Contract 24 — Facturatie → CRM: manually created user
    # Queue: facturatie.user.created | Exchange: user.topic | durable: true
    queue_facturatie_user_created = await _declare_and_bind(channel, "facturatie.user.created", durable=True)
    await queue_facturatie_user_created.consume(partial(handle_facturatie_user_created, sf=sf_client))
<<<<<<< feature/registration-receiver-v2
    
    # Contract 25 — Facturatie → CRM: user updated
=======

    # Contract 25 — Facturatie → CRM: update existing CRM-linked user
>>>>>>> dev
    # Queue: facturatie.user.updated | Exchange: user.topic | durable: true
    queue_facturatie_user_updated = await _declare_and_bind(channel, "facturatie.user.updated", durable=True)
    await queue_facturatie_user_updated.consume(partial(handle_facturatie_user_updated, sf=sf_client))

<<<<<<< feature/registration-receiver-v2
    # Contract 26 — Facturatie → CRM: user deactivated (soft delete only)
    # Queue: facturatie.user.deactivated | Exchange: user.topic | durable: true
    queue_facturatie_user_deactivated = await _declare_and_bind(channel, "facturatie.user.deactivated", durable=True)
    await queue_facturatie_user_deactivated.consume(partial(handle_facturatie_user_deactivated, sf=sf_client))
    
=======
    # Contract 26 — Facturatie → CRM: deactivate existing CRM-linked user
    # Queue: facturatie.user.deactivated | Exchange: user.topic | durable: true
    queue_facturatie_user_deactivated = await _declare_and_bind(channel, "facturatie.user.deactivated", durable=True)
    await queue_facturatie_user_deactivated.consume(partial(handle_facturatie_user_deactivated, sf=sf_client))

>>>>>>> dev
    # Contract 27 — Mailing → CRM: new Mailing user sync
    # Queue: mailing.user.created | Exchange: user.topic | durable: true
    queue_mailing_user_created = await _declare_and_bind(channel, "mailing.user.created", durable=True)
    await queue_mailing_user_created.consume(partial(handle_mailing_user_created, sf=sf_client))

    # Contract 28 — Mailing → CRM: update existing Mailing user sync
    # Queue: crm.mailing.user.updated (routing key: mailing.user.updated)
    # Exchange: user.topic | durable: true. Consumer-prefixed queue voorkomt
    # collision met Mailing's eigen consumer-queue op contact.topic.
    queue_mailing_user_updated = await _declare_and_bind(
        channel,
        "crm.mailing.user.updated",
        durable=True,
        routing_key="mailing.user.updated",
    )
    await queue_mailing_user_updated.consume(partial(handle_mailing_user_updated, sf=sf_client))

    # Contract 29 — Mailing → CRM: deactivate existing Mailing user sync
    # Queue: crm.mailing.user.deactivated (routing key: mailing.user.deactivated)
    # Exchange: user.topic | durable: true.
    queue_mailing_user_deactivated = await _declare_and_bind(
        channel,
        "crm.mailing.user.deactivated",
        durable=True,
        routing_key="mailing.user.deactivated",
    )
    await queue_mailing_user_deactivated.consume(partial(handle_mailing_user_deactivated, sf=sf_client))

    # Contract 30 — Planning → CRM: new Planning user sync
    # Queue: planning.user.created | Exchange: user.topic | durable: true
    queue_planning_user_created = await _declare_and_bind(channel, "planning.user.created", durable=True)
    await queue_planning_user_created.consume(partial(handle_planning_user_created, sf=sf_client))

    # Contract 31 — Planning → CRM: update existing Planning user sync
    # Queue: planning.user.updated | Exchange: user.topic | durable: true
    queue_planning_user_updated = await _declare_and_bind(channel, "planning.user.updated", durable=True)
    await queue_planning_user_updated.consume(partial(handle_planning_user_updated, sf=sf_client))

    # Contract 32 — Planning → CRM: deactivate existing Planning user sync
    # Queue: planning.user.deactivated | Exchange: user.topic | durable: true
    queue_planning_user_deactivated = await _declare_and_bind(channel, "planning.user.deactivated", durable=True)
    await queue_planning_user_deactivated.consume(partial(handle_planning_user_deactivated, sf=sf_client))

    # Contract 3 — Frontend → CRM: create company
    # queue_company = await _declare_and_bind(channel, "frontend.company.created", durable=True)
    # await queue_company.consume(handle_company_created)

    # Contract 5a — Facturatie → CRM: request company data
    # queue_company_req = await _declare_and_bind(channel, "facturatie.company.requested", durable=True)
    # await queue_company_req.consume(handle_company_requested)

    # Contract 10a — Kassa → CRM: person lookup request
    # queue_person_lookup = await _declare_and_bind(channel, "kassa.person.lookup.requested", durable=True)
    # await queue_person_lookup.consume(handle_person_lookup)

    # Contract 16 — Kassa → CRM: payment confirmed
    queue_payment = await _declare_and_bind(channel, "kassa.payment.confirmed", durable=True)
    await queue_payment.consume(partial(handle_payment_confirmed, sf=sf_client))

    # Contract 17a — Kassa → CRM: unpaid persons request
    queue_unpaid = await _declare_and_bind(channel, "kassa.unpaid.requested", durable=True)
    await queue_unpaid.consume(partial(handle_unpaid_requested, sf=sf_client))

    # Contract 11 — Planning → CRM: session update
    # Queue: planning.session.updated | Exchange: planning.topic | durable: true
    queue_session = await _declare_and_bind(channel, "planning.session.updated", durable=True)
    await queue_session.consume(partial(handle_session_updated, sf=sf_client))

    # Contract 12 — IoT → CRM: badge linked (Release 2)
    # queue_badge = await _declare_and_bind(channel, "iot.badge.linked", durable=True)
    # await queue_badge.consume(handle_badge_linked)

    logger.info("Receiver started. Listening on all configured queues.")
    await asyncio.Future()  # run forever


async def handle_warning(message: aio_pika.IncomingMessage) -> None:
    """Contract 9 — Controlroom → CRM: system warning.

    Queue: controlroom.warning.issued | durable: false | US-26

    Behaviour:
    - Validates incoming XML against XSD
    - Logs as logger.error() — no crash
    - No Salesforce action required
    - Invalid XML: exception is caught, logged, and the message is rejected
      (not requeued — a structurally invalid message will never become valid)
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("Controlroom warning — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    logger.error(
        "Controlroom warning received: %s",
        etree.tostring(xml, encoding="unicode"),
    )
    await message.ack()


async def handle_payment_confirmed(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 16 — Kassa -> CRM: payment confirmed.

    Queue: kassa.payment.confirmed | durable: true | US-08, US-21

    Behaviour:
    - Validate XML against schema.
    - Update the canonical session registration payment state in Salesforce.
    - Sync Contact.Paid_At__c as a compatibility field.
    - Invalid XML: rejected without requeue.
    - Unknown/ambiguous Contact or registrationId mismatch: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PaymentConfirmed — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        user_id = xml.findtext("userId")
        email = xml.findtext("email") or ""
        registration_id = xml.findtext("registrationId")
        paid_at = xml.findtext("paidAt") or ""

        contact = await update_payment_status(
            sf,
            user_id=user_id,
            email=email,
            registration_id=registration_id,
            paid_at=paid_at,
        )
        if contact is None:
            await message.ack()
            return

        logger.info("Processed payment confirmation for %s", contact.get("Email", email))
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PaymentConfirmed", message, exc)


async def handle_unpaid_requested(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 17a — Kassa -> CRM: unpaid persons request.

    Queue: kassa.unpaid.requested | durable: true | US-07

    Behaviour:
    - Validate XML against schema.
    - Query Salesforce for unpaid Contacts.
    - Publish crm.unpaid.responded with the same requestId.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("UnpaidRequest — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        request_id = xml.findtext("requestId") or ""
        persons = await get_unpaid_contacts(sf)
        await sender.publish_unpaid_responded(request_id, persons)
        logger.info("Processed unpaid request %s with %d unpaid contacts", request_id, len(persons))
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("UnpaidRequest", message, exc)


async def handle_session_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 11 — Planning -> CRM: notify all active participants of a session change."""
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("SessionUpdate — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        if not await has_session_registration_object(sf):
            logger.error(
                "SessionUpdate rejected — Salesforce object Session_Registration__c is missing",
            )
            await message.reject(requeue=False)
            return

        session_id = xml.findtext("sessionId") or ""
        session_name = xml.findtext("sessionName") or ""
        change_type = xml.findtext("changeType") or ""
        new_time = _normalize_optional_text(xml.findtext("newTime"))
        new_location = _normalize_optional_text(xml.findtext("newLocation"))

        participants = await get_active_session_participants(sf, session_id)
        if not participants:
            logger.info(
                "SessionUpdate for sessionId=%s changeType=%s has no active participants",
                session_id,
                change_type,
            )
            await message.ack()
            return

        published_count = 0
        for participant in participants:
            email = _normalize_optional_text(participant.get("Email"))
            if email is None:
                logger.warning(
                    "Skipping session update notification for sessionId=%s because participant %s has no email",
                    session_id,
                    participant.get("Id"),
                )
                continue

            display_name = _build_mail_display_name(
                participant.get("FirstName"),
                participant.get("LastName"),
                email,
            )
            recipient = {"email": email, "name": display_name}
            dynamic_data = {
                "guest_name": display_name,
                "session_name": session_name,
            }
            if new_time is not None:
                dynamic_data["session_time"] = new_time
            if new_location is not None:
                dynamic_data["session_location"] = new_location

            await sender.publish_mail_requested("session_change", recipient, dynamic_data)
            published_count += 1

        logger.info(
            "Published %s crm.mail.requested messages for sessionId=%s changeType=%s",
            published_count,
            session_id,
            change_type,
        )
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("SessionUpdate", message, exc)


def _get_contact_is_active(contact: dict) -> bool:
    """Return the normalized active flag across supported Salesforce field names.

    Delegates to `coerce_is_active` so picklist values ("No"/"Yes") aren't
    misinterpreted by Python truthiness (bool('No') == True).
    """
    for active_field in ("IsActive__c", "Active__c", "Is_Active__c"):
        if active_field in contact:
            return coerce_is_active(contact[active_field])
    return True


async def _handle_out_of_order_deferral(
    contract_name: str,
    message: aio_pika.IncomingMessage,
    *,
    identifier_label: str,
    identifier_value: str,
) -> None:
    """Requeue out-of-order events so eventual create/update ordering can recover."""
    logger.warning(
        "%s deferred — no Contact found for %s %s; retrying for possible out-of-order delivery",
        contract_name,
        identifier_label,
        identifier_value,
    )
    await message.reject(requeue=True)


async def _handle_processing_error(
    contract_name: str,
    message: aio_pika.IncomingMessage,
    exc: Exception,
) -> None:
    """Handle processing errors consistently by logging and requeueing."""
    logger.error("%s — error processing message: %s", contract_name, exc)
    await message.reject(requeue=True)


def _build_user_data(contact: dict) -> dict:
    """Build user_data payload dict from a Salesforce contact record."""
    role = _normalize_optional_text(contact.get("Role__c")) or "VISITOR"
    gdpr_consent = contact.get("GDPR_Consent__c")
    if gdpr_consent is None:
        gdpr_consent = True

    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": role,
        "isActive": _get_contact_is_active(contact),
        "gdprConsent": gdpr_consent,
        "confirmedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if contact.get("Phone"):
        data["phone"] = contact["Phone"]
    if contact.get("Company_ID__c"):
        data["companyId"] = contact["Company_ID__c"]
    return data


def _build_user_deactivation_data(contact: dict, deactivated_at: str) -> dict[str, str]:
    """Build the outbound Contract 22 payload from a Salesforce Contact."""
    return {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "deactivatedAt": deactivated_at,
    }


def _build_full_name(first_name: str | None, last_name: str | None) -> str:
    """Build a display name from first/last name, skipping missing parts."""
    return f"{first_name or ''} {last_name or ''}".strip()


def _build_mail_display_name(
    first_name: str | None,
    last_name: str | None,
    email: str | None,
) -> str:
    """Build a non-empty display name for outbound mail requests."""
    full_name = _build_full_name(first_name, last_name)
    if full_name:
        return full_name
    return email or ""


def _contact_has_native_identity(contact: dict) -> bool:
    """Return whether the Contact is also owned by a native producer id."""
    return any(
        _normalize_optional_text(contact.get(field)) is not None
        for field in ("Planning_ID__c", "Mailing_ID__c")
    )


def _registration_fields_are_compatible(contact: dict, xml: etree._Element) -> bool:
    """Return whether a Contract 1 payload can safely reuse an existing Contact."""
    comparisons = (
        ("FirstName", xml.findtext("firstName")),
        ("LastName", xml.findtext("lastName")),
        ("Role__c", xml.findtext("role")),
    )
    for sf_field, incoming_value in comparisons:
        normalized_existing = _normalize_optional_text(contact.get(sf_field))
        normalized_incoming = _normalize_optional_text(incoming_value)
        if (
            normalized_existing is not None
            and normalized_incoming is not None
            and normalized_existing != normalized_incoming
        ):
            return False
    return True


def _normalize_optional_text(value: str | None) -> str | None:
    """Normalize optional text fields so blank values behave like absence."""
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def _normalize_email_for_compare(value: str | None) -> str | None:
    """Normalize email values for dedupe/conflict comparisons."""
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None

    return normalized.casefold()


def _derive_mailing_user_role(company_id: str | None) -> str:
    """Derive the CRM role for Mailing user sync payloads."""
    return "COMPANY_CONTACT" if company_id else "VISITOR"


def _get_mailing_last_name_for_contact(xml: etree._Element) -> str:
    """Resolve the Contact last name for Mailing payloads.

    Mailing's XSD allows lastName to be omitted, but Salesforce requires
    Contact.LastName. In that case CRM falls back to the validated email.
    """
    last_name = _normalize_optional_text(xml.findtext("lastName"))
    if last_name is not None:
        return last_name

    return _normalize_optional_text(xml.findtext("email")) or ""


def _get_effective_mailing_company_id(contact: dict, xml: etree._Element) -> str | None:
    """Return the effective company linkage for a Mailing create/reuse flow."""
    inbound_company_id = _normalize_optional_text(xml.findtext("companyId"))
    if inbound_company_id is not None:
        return inbound_company_id

    return _normalize_optional_text(contact.get("Company_ID__c"))


def _has_conflicting_optional_value(existing_value: str | None, incoming_value: str | None) -> bool:
    """Return True when a provided incoming value conflicts with a non-empty existing value."""
    normalized_incoming = _normalize_optional_text(incoming_value)
    if normalized_incoming is None:
        return False

    normalized_existing = _normalize_optional_text(existing_value)
    return normalized_existing is not None and normalized_existing != normalized_incoming


def _mailing_user_has_conflicting_data(contact: dict, xml: etree._Element) -> bool:
    """Detect create-path conflicts for Mailing user sync without mutating CRM data."""
    if _has_conflicting_optional_value(contact.get("FirstName"), xml.findtext("firstName")):
        return True
    if _has_conflicting_optional_value(contact.get("LastName"), xml.findtext("lastName")):
        return True

    incoming_company_id = _normalize_optional_text(xml.findtext("companyId"))
    effective_company_id = _get_effective_mailing_company_id(contact, xml)
    existing_role = _normalize_optional_text(contact.get("Role__c"))
    if effective_company_id is not None and existing_role not in (None, "VISITOR", "COMPANY_CONTACT"):
        return True

    return _has_conflicting_optional_value(contact.get("Company_ID__c"), incoming_company_id)


def _build_conflict_value(
    first_name: str | None,
    last_name: str | None,
    company: str | None = None,
) -> dict[str, str]:
    """Build one side of a Contract 15 payload with required name fields."""
    value = {
        "firstName": _normalize_optional_text(first_name) or "",
        "lastName": _normalize_optional_text(last_name) or "",
    }

    normalized_company = _normalize_optional_text(company)
    if normalized_company is not None:
        value["company"] = normalized_company
    return value


def _build_mailing_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Mailing payload."""
    return {
        "email": email,
        "existingValue": _build_conflict_value(
            contact.get("FirstName"),
            contact.get("LastName"),
            contact.get("Company_ID__c"),
        ),
        "incomingValue": _build_conflict_value(
            xml.findtext("firstName"),
            xml.findtext("lastName"),
            xml.findtext("companyId"),
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _build_facturatie_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Facturatie payload."""
    return {
        "email": email,
        "existingValue": _build_conflict_value(
            contact.get("FirstName"),
            contact.get("LastName"),
            contact.get("Company_ID__c"),
        ),
        "incomingValue": _build_conflict_value(
            xml.findtext("firstName"),
            xml.findtext("lastName"),
            xml.findtext("companyId"),
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _build_mailing_contact_data(xml: etree._Element) -> dict:
    """Map Contract 27 XML fields to Salesforce Contact fields."""
    company_id = _normalize_optional_text(xml.findtext("companyId"))
    contact_data = {
        "Mailing_ID__c": xml.findtext("id"),
        "Email": xml.findtext("email"),
        "GDPR_Consent__c": True,
        "LastName": _get_mailing_last_name_for_contact(xml),
        "Role__c": _derive_mailing_user_role(company_id),
    }

    first_name = _normalize_optional_text(xml.findtext("firstName"))
    if first_name is not None:
        contact_data["FirstName"] = first_name

    if company_id is not None:
        contact_data["Company_ID__c"] = company_id

    return contact_data


def _get_mailing_backfill_kwargs(contact: dict, xml: etree._Element) -> dict[str, object]:
    """Build the safe backfill fields for a compatible existing Mailing contact."""
    company_id = _get_effective_mailing_company_id(contact, xml)
    kwargs: dict[str, object] = {}

    first_name = _normalize_optional_text(xml.findtext("firstName"))
    if first_name is not None:
        kwargs["first_name"] = first_name

    kwargs["last_name"] = _get_mailing_last_name_for_contact(xml)

    if company_id is not None:
        kwargs["company_id"] = company_id

    kwargs["role"] = _derive_mailing_user_role(company_id)
    kwargs["gdpr_consent"] = True
    return kwargs


def _build_planning_contact_data(xml: etree._Element) -> dict:
    """Map Contract 30 XML fields to Salesforce Contact fields."""
    contact_data = {
        "Planning_ID__c": xml.findtext("id"),
        "Email": xml.findtext("email"),
        "FirstName": xml.findtext("firstName"),
        "LastName": xml.findtext("lastName"),
        "Role__c": xml.findtext("role"),
        "GDPR_Consent__c": True,
    }

    phone_number = _normalize_optional_text(xml.findtext("phoneNumber"))
    if phone_number is not None:
        contact_data["Phone"] = phone_number

    return contact_data


def _planning_user_has_conflicting_data(contact: dict, xml: etree._Element) -> bool:
    """Detect conflicting immutable profile data for Planning create-link logic."""
    if _has_conflicting_optional_value(contact.get("FirstName"), xml.findtext("firstName")):
        return True
    if _has_conflicting_optional_value(contact.get("LastName"), xml.findtext("lastName")):
        return True
    if _has_conflicting_optional_value(contact.get("Role__c"), xml.findtext("role")):
        return True
    return _has_conflicting_optional_value(contact.get("Phone"), xml.findtext("phoneNumber"))


def _build_planning_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Planning payload."""
    return {
        "email": email,
        "existingValue": _build_conflict_value(
            contact.get("FirstName"),
            contact.get("LastName"),
            contact.get("Company_ID__c"),
        ),
        "incomingValue": _build_conflict_value(
            xml.findtext("firstName"),
            xml.findtext("lastName"),
            xml.findtext("company"),
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


async def handle_planning_user_created(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 30 — Planning -> CRM: create or attach a Planning user identity.

    Queue: planning.user.created | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Resolve users primarily by Planning_ID__c (stable producer identifier).
    - Only use email as a secondary bootstrap key for safe first-time linking.
    - Persist Planning_ID__c on Contact for future idempotent matching.
    - Publish crm.user.confirmed or crm.user.conflict as needed.
    - Invalid XML: rejected without requeue.
    - Ambiguous Contacts: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        planning_id = xml.findtext("id") or ""
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "PlanningUserCreated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

        if not await has_contact_planning_id_field(sf):
            logger.error(
                "PlanningUserCreated rejected — Salesforce Contact field Planning_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        planning_match_status, existing_by_planning_id = await get_contact_match_by_planning_id(sf, planning_id)
        if planning_match_status == "ambiguous":
            logger.warning(
                "PlanningUserCreated ignored — ambiguous Planning_ID__c %s in Salesforce",
                planning_id,
            )
            await message.ack()
            return

        if planning_match_status == "unique":
            existing_contact = existing_by_planning_id

            existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
            incoming_email = _normalize_email_for_compare(email)
            if existing_email is not None and existing_email != incoming_email:
                logger.warning(
                    "PlanningUserCreated conflict — Planning ID %s already linked to email %s",
                    planning_id,
                    existing_contact.get("Email"),
                )
                await sender.publish_user_conflict(
                    _build_planning_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

            if existing_contact.get("GDPR_Consent__c") is False:
                logger.warning(
                    "PlanningUserCreated conflict — email %s already has explicit GDPR opt-out",
                    email,
                )
                await sender.publish_user_conflict(
                    _build_planning_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

            if _planning_user_has_conflicting_data(existing_contact, xml):
                logger.warning(
                    "PlanningUserCreated conflict — Planning ID %s exists with conflicting profile data",
                    planning_id,
                )
                await sender.publish_user_conflict(
                    _build_planning_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

            contact = await ensure_contact_identifiers(
                sf,
                existing_contact,
                planning_id=planning_id,
            )
            contact = await backfill_planning_contact_fields(
                sf,
                contact,
                first_name=xml.findtext("firstName") or "",
                last_name=xml.findtext("lastName") or "",
                role=xml.findtext("role") or "VISITOR",
                phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
                gdpr_consent=True,
            )
            await sender.publish_user_confirmed(_build_user_data(contact))
            logger.info("Published crm.user.confirmed for existing Planning user %s", email)
            await message.ack()
            return

        # No Planning_ID__c match yet: only a one-time safe bootstrap via unique email.
        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        if email_match_status == "ambiguous":
            logger.warning(
                "PlanningUserCreated ignored — ambiguous email %s in Salesforce",
                email,
            )
            await message.ack()
            return

        if email_match_status == "none":
            contact = await create_contact(sf, _build_planning_contact_data(xml))
            await sender.publish_user_confirmed(_build_user_data(contact))
            logger.info("Published crm.user.confirmed for new Planning user %s", email)
            await message.ack()
            return

        existing_contact = existing_by_email
        existing_planning_id = _normalize_optional_text(existing_contact.get("Planning_ID__c"))
        if existing_planning_id is not None and existing_planning_id != planning_id:
            logger.warning(
                "PlanningUserCreated conflict — email %s already linked to Planning ID %s",
                email,
                existing_planning_id,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if existing_contact.get("GDPR_Consent__c") is False:
            logger.warning(
                "PlanningUserCreated conflict — email %s already has explicit GDPR opt-out",
                email,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if _planning_user_has_conflicting_data(existing_contact, xml):
            logger.warning(
                "PlanningUserCreated conflict — email %s already exists with differing data",
                email,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            planning_id=planning_id,
        )
        contact = await backfill_planning_contact_fields(
            sf,
            contact,
            first_name=xml.findtext("firstName") or "",
            last_name=xml.findtext("lastName") or "",
            role=xml.findtext("role") or "VISITOR",
            phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
            gdpr_consent=True,
        )
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for linked Planning user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PlanningUserCreated", message, exc)


async def handle_planning_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 31 — Planning -> CRM: update an existing Planning-linked user.

    Queue: planning.user.updated | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Resolve the Contact strictly by Planning_ID__c.
    - Requeue unknown Planning identities so out-of-order create/update can recover.
    - Ack ambiguous Planning identities without retry.
    - Publish crm.user.conflict on email collisions.
    - Update Planning-owned fields authoritatively in Salesforce.
    - Publish crm.user.updated after a successful update.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        planning_id = xml.findtext("id") or ""
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "PlanningUserUpdated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

        if not await has_contact_planning_id_field(sf):
            logger.error(
                "PlanningUserUpdated rejected — Salesforce Contact field Planning_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        planning_match_status, existing_contact = await get_contact_match_by_planning_id(sf, planning_id)
        if planning_match_status == "none":
            await _handle_out_of_order_deferral(
                "PlanningUserUpdated",
                message,
                identifier_label="Planning_ID__c",
                identifier_value=planning_id,
            )
            return

        if planning_match_status == "ambiguous":
            logger.warning(
                "PlanningUserUpdated ignored — ambiguous Planning_ID__c %s in Salesforce",
                planning_id,
            )
            await message.ack()
            return

        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        if email_match_status == "ambiguous":
            logger.warning(
                "PlanningUserUpdated conflict — email %s is ambiguous in Salesforce",
                email,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
            logger.warning(
                "PlanningUserUpdated conflict — email %s already linked to another Contact",
                email,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_by_email, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            planning_id=planning_id,
        )
        contact = await update_planning_contact(
            sf,
            contact,
            email=email,
            first_name=xml.findtext("firstName") or "",
            last_name=xml.findtext("lastName") or "",
            role=xml.findtext("role") or "VISITOR",
            phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
        )
        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Planning user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PlanningUserUpdated", message, exc)


async def handle_planning_user_deactivated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 32 — Planning -> CRM: deactivate an existing Planning-linked user.

    Queue: planning.user.deactivated | durable: true

    Behaviour:
    - Validate XML against schema.
    - Require Salesforce support for Planning_ID__c.
    - Resolve the Contact strictly by Planning_ID__c.
    - Requeue unknown Planning identities so out-of-order create/deactivate can recover.
    - Ack ambiguous Planning identities without retry.
    - Trust Planning_ID__c over a stale payload email, but log the mismatch.
    - Perform a soft delete only and publish crm.user.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        planning_id = xml.findtext("id") or ""
        deactivated_at = xml.findtext("deactivatedAt") or ""

        if not await has_contact_planning_id_field(sf):
            logger.error(
                "PlanningUserDeactivated rejected — Salesforce Contact field Planning_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        planning_match_status, existing_contact = await get_contact_match_by_planning_id(sf, planning_id)
        if planning_match_status == "none":
            await _handle_out_of_order_deferral(
                "PlanningUserDeactivated",
                message,
                identifier_label="Planning_ID__c",
                identifier_value=planning_id,
            )
            return

        if planning_match_status == "ambiguous":
            logger.warning(
                "PlanningUserDeactivated ignored — ambiguous Planning_ID__c %s in Salesforce",
                planning_id,
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            planning_id=planning_id,
        )

        existing_email = _normalize_email_for_compare(contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
            logger.warning(
                "PlanningUserDeactivated email mismatch — Planning_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
                planning_id,
                contact.get("Email"),
                email,
            )

        contact = await deactivate_contact_record(
            sf,
            contact,
            log_value=f"Planning_ID__c {planning_id}",
        )
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at)
        )
        logger.info("Published crm.user.deactivated for Planning_ID__c %s", planning_id)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PlanningUserDeactivated", message, exc)


async def handle_mailing_user_created(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 27 — Mailing -> CRM: create or attach a Mailing user identity.

    Queue: mailing.user.created | durable: true

    Behaviour:
    - Validate XML against schema.
    - Mirror Mailing's `isActive` flag to the Contact active field.
    - Create a new Contact when the email and Mailing ID are new.
    - Reuse a unique existing Contact when the Mailing payload is idempotent.
    - When `isActive=false` reaches an existing Contact, deactivate it and
      publish crm.user.deactivated instead of crm.user.confirmed.
    - Publish crm.user.conflict when the email already exists with conflicting data.
    - Invalid XML: rejected without requeue.
    - Ambiguous Contacts: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        mailing_id = xml.findtext("id") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        if not await has_contact_mailing_id_field(sf):
            logger.error(
                "MailingUserCreated rejected — Salesforce Contact field Mailing_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        mailing_match_status, existing_by_mailing_id = await get_contact_match_by_mailing_id(sf, mailing_id)

        if mailing_match_status == "ambiguous":
            logger.warning(
                "MailingUserCreated ignored — ambiguous Mailing_ID__c %s in Salesforce",
                mailing_id,
            )
            await message.ack()
            return

        if mailing_match_status == "unique":
            existing_contact = existing_by_mailing_id
        else:
            if email_match_status == "ambiguous":
                logger.warning(
                    "MailingUserCreated ignored — ambiguous email %s in Salesforce",
                    email,
                )
                await message.ack()
                return

            if email_match_status == "none":
                contact_data = _build_mailing_contact_data(xml)
                if not is_active:
                    contact_data = await apply_is_active(sf, contact_data, False)
                contact = await create_contact(sf, contact_data)
                await sender.publish_user_confirmed(_build_user_data(contact))
                logger.info(
                    "Published crm.user.confirmed for new Mailing user %s (isActive=%s)",
                    email,
                    is_active,
                )
                await message.ack()
                return

            existing_contact = existing_by_email

        if email_match_status == "none":
            existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
            incoming_email = _normalize_email_for_compare(email)
            if existing_email != incoming_email:
                logger.warning(
                    "MailingUserCreated conflict — Mailing ID %s already linked to email %s",
                    mailing_id,
                    existing_contact.get("Email"),
                )
                await sender.publish_user_conflict(
                    _build_mailing_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

        if mailing_match_status == "unique":
            existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
            incoming_email = _normalize_email_for_compare(email)
            if existing_email is not None and existing_email != incoming_email:
                logger.warning(
                    "MailingUserCreated conflict — Mailing ID %s already linked to email %s",
                    mailing_id,
                    existing_contact.get("Email"),
                )
                await sender.publish_user_conflict(
                    _build_mailing_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

        if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
            logger.warning(
                "MailingUserCreated conflict — email %s and Mailing ID %s point to different Contacts",
                email,
                mailing_id,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        existing_mailing_id = _normalize_optional_text(existing_contact.get("Mailing_ID__c"))

        if existing_mailing_id is not None and existing_mailing_id != mailing_id:
            logger.warning(
                "MailingUserCreated conflict — email %s already linked to Mailing ID %s",
                email,
                existing_mailing_id,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if _mailing_user_has_conflicting_data(existing_contact, xml):
            logger.warning(
                "MailingUserCreated conflict — email %s already exists with differing data",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if existing_contact.get("GDPR_Consent__c") is False:
            logger.warning(
                "MailingUserCreated conflict — email %s already has explicit GDPR opt-out",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            mailing_id=mailing_id,
        )
        contact = await backfill_mailing_contact_fields(
            sf,
            contact,
            **_get_mailing_backfill_kwargs(contact, xml),
        )
        if not is_active:
            contact = await deactivate_contact_record(
                sf, contact, log_value=f"Mailing_ID__c {mailing_id}",
            )
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await sender.publish_user_deactivated(
                _build_user_deactivation_data(contact, deactivated_at),
            )
            logger.info(
                "Published crm.user.deactivated for existing Mailing user %s (isActive=false on create)",
                email,
            )
            await message.ack()
            return
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for existing Mailing user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("MailingUserCreated", message, exc)


async def handle_mailing_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 28 — Mailing -> CRM: update an existing Mailing-linked user.

    Queue: crm.mailing.user.updated (routing key: mailing.user.updated) | durable: true

    The `<id>` field carries the CRM master UUID (received by Mailing in
    crm.user.confirmed), not the original native Mailing ID. CRM resolves the
    Contact by `CRM_ID__c`; Mailing_ID__c remains on the Contact as a
    provenance record from the create flow.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by CRM_ID__c.
    - Requeue unknown CRM identities so out-of-order create/update can recover.
    - Ack ambiguous CRM identities without retry.
    - Publish crm.user.conflict on email collisions.
    - If `isActive=false`, soft-delete the Contact and publish crm.user.deactivated.
    - Otherwise update Mailing-owned fields authoritatively in Salesforce,
      reactivate when needed, and publish crm.user.updated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        crm_id = xml.findtext("id") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "MailingUserUpdated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "MailingUserUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        if email_match_status == "ambiguous":
            logger.warning(
                "MailingUserUpdated conflict — email %s is ambiguous in Salesforce",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
            logger.warning(
                "MailingUserUpdated conflict — email %s already linked to another Contact",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_by_email, xml)
            )
            await message.ack()
            return

        contact = existing_contact
        if not is_active:
            contact = await deactivate_contact_record(
                sf, contact, log_value=f"CRM_ID__c {crm_id}",
            )
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await sender.publish_user_deactivated(
                _build_user_deactivation_data(contact, deactivated_at),
            )
            logger.info(
                "Published crm.user.deactivated for Mailing user %s (isActive=false on update)",
                email,
            )
            await message.ack()
            return

        contact = await update_mailing_contact(
            sf,
            contact,
            email=email,
            first_name=_normalize_optional_text(xml.findtext("firstName")),
            last_name=_get_mailing_last_name_for_contact(xml),
            company_id=_normalize_optional_text(xml.findtext("companyId")),
        )
        if not _get_contact_is_active(contact):
            reactivation_update = await apply_is_active(sf, {}, True)
            if reactivation_update:
                contact_id = contact["Id"]
                await asyncio.to_thread(sf.Contact.update, contact_id, reactivation_update)
                contact = await asyncio.to_thread(sf.Contact.get, contact_id)
                logger.info(
                    "Reactivated Contact %s for Mailing user %s (isActive=true on update)",
                    contact_id,
                    email,
                )
        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Mailing user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("MailingUserUpdated", message, exc)


async def handle_mailing_user_deactivated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 29 — Mailing -> CRM: deactivate an existing Mailing-linked user.

    Queue: crm.mailing.user.deactivated (routing key: mailing.user.deactivated) | durable: true

    The `<id>` field carries the CRM master UUID (received by Mailing in
    crm.user.confirmed), not the original native Mailing ID. CRM resolves the
    Contact by `CRM_ID__c`.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by CRM_ID__c.
    - Requeue unknown CRM identities so out-of-order create/deactivate can recover.
    - Ack ambiguous CRM identities without retry.
    - Trust CRM_ID__c over a stale payload email, but log the mismatch.
    - Perform a soft delete only and publish crm.user.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        crm_id = xml.findtext("id") or ""
        deactivated_at = xml.findtext("deactivatedAt") or ""

        crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "MailingUserDeactivated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "MailingUserDeactivated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        contact = existing_contact

        existing_email = _normalize_email_for_compare(contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
            logger.warning(
                "MailingUserDeactivated email mismatch — CRM_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
                crm_id,
                contact.get("Email"),
                email,
            )

        contact = await deactivate_contact_record(
            sf,
            contact,
            log_value=f"CRM_ID__c {crm_id}",
        )
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at)
        )
        logger.info("Published crm.user.deactivated for CRM_ID__c %s", crm_id)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("MailingUserDeactivated", message, exc)

async def handle_registration(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 1 — Frontend -> CRM: new registration.

    Queue: frontend.registration.created | durable: true | US-02, US-04, US-05, US-19
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("Registration — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email")

        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning("Registration refused — gdprConsent=%s for email %s", gdpr_text, email)
            await message.reject(requeue=False)
            return

        role = xml.findtext("role")
        company = xml.findtext("company")
        if role == "COMPANY_CONTACT" and not company:
            logger.warning("COMPANY_CONTACT registration without company field for %s", email)

        existing_contact = await get_contact_by_email(sf, email)

        if existing_contact:
            reg_id_incoming = xml.findtext("registrationId")
            reg_id_existing = existing_contact.get("Registration_ID__c")

            if reg_id_incoming == reg_id_existing:
                logger.info("Retry for registrationId %s — republishing", reg_id_incoming)
                await sender.publish_user_confirmed(_build_user_data(existing_contact))
                full_name = _build_full_name(
                    existing_contact.get("FirstName"),
                    existing_contact.get("LastName"),
                )
                recipient = {"email": email, "name": full_name}
                dynamic_data = {"guest_name": full_name}
                await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)
                await message.ack()
                return

            logger.warning("Conflict: email %s exists with different registrationId", email)
            await message.ack()
            return

        contact_data = {
            "FirstName": xml.findtext("firstName"),
            "LastName": xml.findtext("lastName"),
            "Email": email,
            "Role__c": xml.findtext("role"),
            "GDPR_Consent__c": xml.findtext("gdprConsent") in ("true", "1"),
            "Registration_ID__c": xml.findtext("registrationId"),
        }

        phone = xml.findtext("phone")
        if phone:
            contact_data["Phone"] = phone

        logger.info("Creating new Salesforce Contact for %s", email)
        contact = await create_contact(sf, contact_data)

        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for %s", email)

        full_name = _build_full_name(
            contact_data.get("FirstName"),
            contact_data.get("LastName"),
        )
        recipient = {"email": email, "name": full_name}
        dynamic_data = {"guest_name": full_name}
        await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)
        logger.info("Published crm.mail.requested for %s", email)

        await message.ack()

    except Exception as exc:  # noqa: BLE001
        logger.error("Registration — error processing message: %s", exc)
        await message.reject(requeue=True)

async def handle_facturatie_user_created(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 24 — Facturatie -> CRM: manually created user.

    Queue: facturatie.user.created | durable: true

    Behaviour:
    - Validate XML against schema.
    - Mirror Facturatie's `isActive` flag to the Contact active field on create.
    - Reuse an existing unique Contact after ensuring canonical identifiers;
      deactivate it when `isActive=false` arrives for an existing Contact.
    - Create a new Contact when no Contact exists yet (inactive if
      `isActive=false`).
    - Do not publish crm.mail.requested for this flow.
    - Invalid XML: rejected without requeue.
    - Ambiguous Contacts: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        registration_id = xml.findtext("registrationId")
        match_status, existing_contact = await get_contact_match_by_email(sf, email)
        if match_status == "unique" and existing_contact is not None:
            contact = await ensure_contact_identifiers(
                sf,
                existing_contact,
                registration_id=registration_id,
            )
            if not is_active:
                contact = await deactivate_contact_record(
                    sf, contact, log_value=email,
                )
                deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                await sender.publish_user_deactivated(
                    _build_user_deactivation_data(contact, deactivated_at),
                )
                logger.info(
                    "Published crm.user.deactivated for existing Facturatie user %s (isActive=false)",
                    email,
                )
                await message.ack()
                return
            await sender.publish_user_confirmed(_build_user_data(contact))
            logger.info("Published crm.user.confirmed for existing Facturatie user %s", email)
            await message.ack()
            return

        if match_status == "ambiguous":
            logger.warning(
                "FacturatieUserCreated ignored — ambiguous email %s in Salesforce",
                email,
            )
            await message.ack()
            return

        contact_data = {
            "FirstName": xml.findtext("firstName"),
            "LastName": xml.findtext("lastName"),
            "Email": email,
            "Role__c": xml.findtext("role"),
            "GDPR_Consent__c": True,
        }
        if registration_id:
            contact_data["Registration_ID__c"] = registration_id

        phone = xml.findtext("phone")
        if phone:
            contact_data["Phone"] = phone

        company_id = xml.findtext("companyId")
        if company_id:
            contact_data["Company_ID__c"] = company_id

        if not is_active:
            contact_data = await apply_is_active(sf, contact_data, False)
        contact = await create_contact(sf, contact_data)
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info(
            "Published crm.user.confirmed for new Facturatie user %s (isActive=%s)",
            email,
            is_active,
        )
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieUserCreated", message, exc)


async def handle_facturatie_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 25 — Facturatie -> CRM: update an existing CRM-linked user.

    Queue: facturatie.user.updated | durable: true

    The `<id>` field carries the CRM master UUID (received by Facturatie in
    crm.user.confirmed, Option 2 UUID strategy). CRM resolves the Contact by
    `CRM_ID__c`.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by CRM_ID__c.
    - Requeue unknown CRM identities so out-of-order create/update can recover.
    - Ack ambiguous CRM identities without retry.
    - Publish crm.user.conflict on email collisions.
    - If `isActive=false`, soft-delete the Contact and publish crm.user.deactivated.
    - Otherwise update Facturatie-owned fields authoritatively in Salesforce,
      reactivate when needed, and publish crm.user.updated.
    - Specialized existing roles (ADMIN/SPEAKER/EVENT_MANAGER/CASHIER/BAR_STAFF)
      protect Role__c and Company_ID__c from Facturatie-side overwrites.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        crm_id = xml.findtext("id") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "FacturatieUserUpdated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "FacturatieUserUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        if email_match_status == "ambiguous":
            logger.warning(
                "FacturatieUserUpdated conflict — email %s is ambiguous in Salesforce",
                email,
            )
            await sender.publish_user_conflict(
                _build_facturatie_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
            logger.warning(
                "FacturatieUserUpdated conflict — email %s already linked to another Contact",
                email,
            )
            await sender.publish_user_conflict(
                _build_facturatie_user_conflict_data(email, existing_by_email, xml)
            )
            await message.ack()
            return

        contact = existing_contact
        if not is_active:
            contact = await deactivate_contact_record(
                sf, contact, log_value=f"CRM_ID__c {crm_id}",
            )
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await sender.publish_user_deactivated(
                _build_user_deactivation_data(contact, deactivated_at),
            )
            logger.info(
                "Published crm.user.deactivated for Facturatie user %s (isActive=false on update)",
                email,
            )
            await message.ack()
            return

        contact = await update_facturatie_contact(
            sf,
            contact,
            email=email,
            first_name=xml.findtext("firstName") or "",
            last_name=xml.findtext("lastName") or "",
            phone=_normalize_optional_text(xml.findtext("phone")),
            street=_normalize_optional_text(xml.findtext("street")),
            house_number=_normalize_optional_text(xml.findtext("houseNumber")),
            postal_code=_normalize_optional_text(xml.findtext("postalCode")),
            city=_normalize_optional_text(xml.findtext("city")),
            country=_normalize_optional_text(xml.findtext("country")),
            role=xml.findtext("role") or "",
            company_id=_normalize_optional_text(xml.findtext("companyId")),
        )
        if not _get_contact_is_active(contact):
            reactivation_update = await apply_is_active(sf, {}, True)
            if reactivation_update:
                contact_id = contact["Id"]
                await asyncio.to_thread(sf.Contact.update, contact_id, reactivation_update)
                contact = await asyncio.to_thread(sf.Contact.get, contact_id)
                logger.info(
                    "Reactivated Contact %s for Facturatie user %s (isActive=true on update)",
                    contact_id,
                    email,
                )
        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieUserUpdated", message, exc)


async def handle_facturatie_user_deactivated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 26 — Facturatie -> CRM: deactivate an existing CRM-linked user.

    Queue: facturatie.user.deactivated | durable: true

    The `<id>` field carries the CRM master UUID. CRM resolves the Contact by
    `CRM_ID__c` and performs a soft delete only (GDPR audit trail).

    Behaviour:
    - Validate XML against schema.
    - Resolve strictly by CRM_ID__c.
    - Requeue unknown identities (create may still be in flight).
    - Ack ambiguous identities without retry.
    - Trust CRM_ID__c over a stale payload email, but log the mismatch.
    - Soft delete and publish crm.user.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        crm_id = xml.findtext("id") or ""
        deactivated_at = xml.findtext("deactivatedAt") or ""

        crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "FacturatieUserDeactivated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "FacturatieUserDeactivated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        contact = existing_contact

        existing_email = _normalize_email_for_compare(contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
            logger.warning(
                "FacturatieUserDeactivated email mismatch — CRM_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
                crm_id,
                contact.get("Email"),
                email,
            )

        contact = await deactivate_contact_record(
            sf,
            contact,
            log_value=f"CRM_ID__c {crm_id}",
        )
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at)
        )
        logger.info("Published crm.user.deactivated for Facturatie CRM_ID__c %s", crm_id)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieUserDeactivated", message, exc)

async def handle_facturatie_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 25 — Facturatie -> CRM: existing CRM-linked user update.

    Queue: facturatie.user.updated | durable: true
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
<<<<<<< feature/registration-receiver-v2
        crm_id = xml.findtext("id") or ""
        email = xml.findtext("email") or ""
        first_name = xml.findtext("firstName")
        last_name = xml.findtext("lastName")
        phone = xml.findtext("phone")
        street = xml.findtext("street")
        house_number = xml.findtext("houseNumber")
        postal_code = xml.findtext("postalCode")
        city = xml.findtext("city")
        country = xml.findtext("country")
        role = xml.findtext("role")
        company_id = xml.findtext("companyId")
        badge_code = xml.findtext("badgeCode")
        is_active_text = xml.findtext("isActive")
=======
        email = xml.findtext("email")
        registration_id = xml.findtext("registrationId") or ""
        session_id = xml.findtext("sessionId") or ""

>>>>>>> dev
        gdpr_text = xml.findtext("gdprConsent")

        if gdpr_text not in ("true", "1"):
            logger.warning(
                "FacturatieUserUpdated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

<<<<<<< feature/registration-receiver-v2
        match_status, contact = await get_contact_match_by_crm_id(sf, crm_id)
        if match_status == "none":
            await _handle_out_of_order_deferral(
                "FacturatieUserUpdated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if match_status == "ambiguous":
            logger.warning(
                "FacturatieUserUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        requested_is_active = True
        if is_active_text is not None:
            requested_is_active = is_active_text.strip().lower() in ("true", "1")

        if not requested_is_active:
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            contact = await deactivate_contact_record(
                sf,
                contact,
                log_value=f"CRM_ID__c {crm_id}",
            )
            await sender.publish_user_deactivated(
                _build_user_deactivation_data(contact, deactivated_at)
            )
            logger.info("Published crm.user.deactivated for CRM_ID__c %s", crm_id)
=======
        if not await has_session_registration_object(sf):
            logger.error(
                "Registration rejected — Salesforce object Session_Registration__c is missing",
            )
            await message.reject(requeue=False)
            return

        # TODO: Switch to registrationId-based dedup as primary key (contract spec).
        #       Current R1 approach uses email as lookup; registrationId is secondary.

        role = xml.findtext("role")
        company = xml.findtext("company")
        if role == "COMPANY_CONTACT" and not company:
            logger.warning("COMPANY_CONTACT registration without company field for %s", email)

        existing_contact = await get_contact_by_email(sf, email)

        if existing_contact:
            existing_session_registration = await get_session_registration_by_registration_id(
                sf,
                registration_id,
            )

            if (
                existing_session_registration is not None
                and existing_session_registration.get("Is_Active__c") is not False
            ):
                # Retry na publish failure -> opnieuw publishen
                logger.info("Retry for registrationId %s — republishing", registration_id)

                await sender.publish_user_confirmed(_build_user_data(existing_contact))

                # C6: Publish mail request
                full_name = _build_mail_display_name(
                    existing_contact.get("FirstName"),
                    existing_contact.get("LastName"),
                    email,
                )

                recipient = {"email": email, "name": full_name}
                dynamic_data = {"guest_name": full_name}
                await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)

                await message.ack()
                return
            if existing_session_registration is not None:
                logger.info(
                    "Reactivating inactive registrationId %s via normal registration flow",
                    registration_id,
                )

            if not _registration_fields_are_compatible(existing_contact, xml):
                logger.warning(
                    "Conflict: email %s exists with incompatible person fields for registrationId %s",
                    email,
                    registration_id,
                )
                await message.ack()
                return

            contact = await ensure_contact_identifiers(
                sf,
                existing_contact,
                registration_id=registration_id,
            )
            await upsert_session_registration(
                sf,
                registration_id=registration_id,
                session_id=session_id,
                contact_id=contact["Id"],
            )

            logger.info(
                "Reusing existing Salesforce Contact for email=%s registrationId=%s",
                email,
                registration_id,
            )
            await sender.publish_user_confirmed(_build_user_data(contact))

            full_name = _build_mail_display_name(
                contact.get("FirstName"),
                contact.get("LastName"),
                email,
            )
            recipient = {"email": email, "name": full_name}
            dynamic_data = {"guest_name": full_name}
            await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)
>>>>>>> dev
            await message.ack()
            return

        update_payload: dict[str, str] = {}
        field_mapping = {
            "FirstName": first_name,
            "LastName": last_name,
            "Phone": phone,
            "Email": email,
<<<<<<< feature/registration-receiver-v2
            "MailingStreet": street,
            "House_Number__c": house_number,
            "MailingPostalCode": postal_code,
            "MailingCity": city,
            "MailingCountry": country,
            "Role__c": role,
            "Company_ID__c": company_id,
            "Badge_Code__c": badge_code,
=======
            "Role__c": xml.findtext("role"),
            "GDPR_Consent__c": xml.findtext("gdprConsent") in ("true", "1"),
            "Registration_ID__c": registration_id,
>>>>>>> dev
        }
        for sf_field, xml_value in field_mapping.items():
            if xml_value is not None:
                update_payload[sf_field] = xml_value

        await asyncio.to_thread(sf.Contact.update, contact["Id"], update_payload)
        contact = await asyncio.to_thread(sf.Contact.get, contact["Id"])

<<<<<<< feature/registration-receiver-v2
        if not _get_contact_is_active(contact):
            active_payload = await apply_is_active(sf, {}, True)
            await asyncio.to_thread(sf.Contact.update, contact["Id"], active_payload)
            contact = await asyncio.to_thread(sf.Contact.get, contact["Id"])

        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Facturatie user CRM_ID__c %s", crm_id)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieUserUpdated", message, exc)
=======
        # Company mapping deferred to Contract 3 (aparte taak)

        logger.info("Creating new Salesforce Contact for %s", email)
        contact = await create_contact(sf, contact_data)
        await upsert_session_registration(
            sf,
            registration_id=registration_id,
            session_id=session_id,
            contact_id=contact["Id"],
        )
>>>>>>> dev


<<<<<<< feature/registration-receiver-v2
async def handle_facturatie_user_deactivated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 26 — Facturatie -> CRM: CRM-linked user deactivation.
=======
        # Contract 6 (R1 scope) — publish registration_confirmation
        full_name = _build_mail_display_name(
            contact_data.get("FirstName"),
            contact_data.get("LastName"),
            email,
        )

        recipient = {"email": email, "name": full_name}
        dynamic_data = {"guest_name": full_name}
        await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)
        logger.info("Published crm.mail.requested for %s", email)
>>>>>>> dev

    Queue: facturatie.user.deactivated | durable: true
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        crm_id = xml.findtext("id") or ""
        email = xml.findtext("email") or ""

        contact = await get_contact_by_crm_id(sf, crm_id)
        if contact is None:
            logger.warning(
                "FacturatieUserDeactivated — no Contact found for CRM_ID %s", crm_id
            )
            await message.ack()
            return

        contact = await deactivate_contact_record(sf, contact, log_value=crm_id)
        deactivation_data = {
            "id": contact["CRM_ID__c"],
            "email": contact["Email"],
            "deactivatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        await sender.publish_user_deactivated(deactivation_data)
        logger.info("Published crm.user.deactivated for Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
<<<<<<< feature/registration-receiver-v2
        logger.error("FacturatieUserDeactivated — error processing message: %s", exc)
        await message.reject(requeue=True)
=======
        await _handle_processing_error("Registration", message, exc)
>>>>>>> dev

def _build_updated_user_data(contact: dict) -> dict:
    """Build user_data payload dict for crm.user.updated from a Salesforce record.

    Same structure as _build_user_data but with updatedAt instead of confirmedAt.
    Contract 18 requires the full user profile - consumers replace their local
    copy entirely, so all available fields must be included.
    """
    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": contact.get("Role__c", "VISITOR"),
        "isActive": _get_contact_is_active(contact),
        "gdprConsent": contact.get("GDPR_Consent__c", True),
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if contact.get("Phone"):
        data["phone"] = contact["Phone"]
    if contact.get("Company_ID__c"):
        data["companyId"] = contact["Company_ID__c"]
    if contact.get("Badge_Code__c"):
        data["badgeCode"] = contact["Badge_Code__c"]

    # Address fields - map all available SF fields for full profile
    address_mapping = {
        "MailingStreet": "street",
        "House_Number__c": "houseNumber",
        "MailingPostalCode": "postalCode",
        "MailingCity": "city",
        "MailingCountry": "country",
    }
    for sf_field, xml_field in address_mapping.items():
        value = contact.get(sf_field)
        if value:
            data[xml_field] = value

    return data


async def handle_registration_updated(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 2 — Frontend -> CRM: registration update or cancellation.

    Queue: frontend.registration.updated | durable: true | US-21 (R1), US-33 (R2)

    Behaviour:
    - Validate XML against schema (<RegistrationChange>).
    - Branch on changeType:
        updated   → Upsert Contact in Salesforce, keep the session registration active,
                    publish crm.user.updated (C18).
        cancelled → Soft-delete the session registration first; only soft-delete the
                    Contact and publish crm.user.deactivated (C22) when no active
                    registrations remain.
    - Contact removal remains soft delete only — never physically remove (GDPR).
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("RegistrationChange — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email")
        session_id = xml.findtext("sessionId")
        change_type = xml.findtext("changeType")

        logger.info(
            "Processing registration change: email=%s, sessionId=%s, changeType=%s",
            email, session_id, change_type,
        )

        if change_type == "updated":
            await _handle_update(xml, email, sf, message)
        elif change_type == "cancelled":
            await _handle_cancellation(xml, email, sf, message)
        else:
            # XSD validation should prevent this, but defence-in-depth
            logger.error("Unknown changeType '%s' for email %s — rejecting", change_type, email)
            await message.reject(requeue=False)

    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("RegistrationChange", message, exc)


async def _handle_update(
    xml: etree._Element, email: str, sf: "Salesforce", message: aio_pika.IncomingMessage
) -> None:
    """Process changeType=updated: upsert Contact and publish crm.user.updated."""
    if not await has_session_registration_object(sf):
        logger.error(
            "RegistrationChange updated rejected — Salesforce object Session_Registration__c is missing",
        )
        await message.reject(requeue=False)
        return

    update_data: dict = {}

    updated_fields = xml.find("updatedFields")
    if updated_fields is not None:
        field_mapping = {
            "firstName": "FirstName",
            "lastName": "LastName",
            "email": "Email",
            "phone": "Phone",
            "role": "Role__c",
            # company mapping deferred to Contract 3
        }
        for xml_field, sf_field in field_mapping.items():
            value = updated_fields.findtext(xml_field)
            if value is not None:
                update_data[sf_field] = value

    contact = await upsert_contact_by_email(sf, email, update_data)
    registration_id = _normalize_optional_text(xml.findtext("registrationId"))
    session_id = xml.findtext("sessionId") or ""
    await ensure_session_registration_active(
        sf,
        contact_id=contact["Id"],
        session_id=session_id,
        registration_id=registration_id,
    )

    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for %s", email)
    await message.ack()


async def _handle_cancellation(
    xml: etree._Element,
    email: str,
    sf: "Salesforce",
    message: aio_pika.IncomingMessage,
) -> None:
    """Process changeType=cancelled: deactivate registration, maybe Contact."""
    if not await has_session_registration_object(sf):
        logger.error(
            "RegistrationChange cancelled rejected — Salesforce object Session_Registration__c is missing",
        )
        await message.reject(requeue=False)
        return

    session_id = xml.findtext("sessionId") or ""
    registration_id = _normalize_optional_text(xml.findtext("registrationId"))
    contact = await get_contact_by_email(sf, email)

    if contact is None:
        # Contact doesn't exist — nothing to deactivate. Ack to prevent infinite requeue.
        logger.warning("Cancellation for unknown email %s — acking without action", email)
        await message.ack()
        return

    session_registration = await deactivate_session_registration(
        sf,
        registration_id=registration_id,
        contact_id=contact["Id"],
        session_id=session_id,
    )
    if session_registration is None:
        remaining_registrations = await count_active_session_registrations(sf, contact["Id"])
        native_identity = _contact_has_native_identity(contact)
        if remaining_registrations > 0 or native_identity:
            logger.warning(
                "Cancellation for %s session %s has no Session_Registration__c row — skipping legacy Contact fallback; remaining_active_registrations=%s native_identity=%s",
                email,
                session_id,
                remaining_registrations,
                native_identity,
            )
            await message.ack()
            return

        logger.warning(
            "Cancellation for %s session %s has no Session_Registration__c row — using legacy Contact fallback",
            email,
            session_id,
        )
    else:
        remaining_registrations = await count_active_session_registrations(sf, contact["Id"])
        native_identity = _contact_has_native_identity(contact)
        if remaining_registrations > 0 or native_identity:
            logger.info(
                "Cancelled registration for %s without deactivating Contact; remaining_active_registrations=%s native_identity=%s",
                email,
                remaining_registrations,
                native_identity,
            )
            await message.ack()
            return

    contact = await deactivate_contact_record(sf, contact, log_value=email)
    deactivation_data = _build_user_deactivation_data(
        contact,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    await sender.publish_user_deactivated(deactivation_data)
    logger.info("Published crm.user.deactivated for %s", email)
    await message.ack()
