"""RabbitMQ queue consumer — listens on the configured inbound queues."""

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from lxml import etree

from src import sender, xml_validator
from src.config import Config
from src.salesforce_client import (
    add_facturatie_customer_id_if_supported,
    backfill_mailing_contact_fields,
    create_contact,
    deactivate_contact,
    deactivate_contact_by_crm_id,
    ensure_contact_identifiers,
    get_contact_by_email,
    get_contact_match_by_email,
    get_contact_match_by_mailing_id,
    get_salesforce_client,
    get_unpaid_contacts,
    has_contact_mailing_id_field,
    update_contact_by_crm_id,
    update_mailing_contact,
    update_payment_status,
    upsert_contact_by_email,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)

# Queue → topic exchange mapping (Infra-beheerd, zie docs/rabbitmq-exchanges.md)
_INBOUND_EXCHANGE: dict[str, str] = {
    "frontend.registration.created": "user.topic",
    "frontend.registration.updated": "user.topic",
    "frontend.company.created": "user.topic",
    "facturatie.user.created": "user.topic",
    "facturatie.user.updated": "user.topic",
    "facturatie.user.deactivated": "user.topic",
    "mailing.user.created": "user.topic",
    "mailing.user.updated": "user.topic",
    "facturatie.company.requested": "invoice.topic",
    "kassa.person.lookup.requested": "payment.topic",
    "kassa.payment.confirmed": "payment.topic",
    "kassa.unpaid.requested": "payment.topic",
    "planning.session.updated": "planning.topic",
    "controlroom.warning.issued": "planning.topic",
    "iot.badge.linked": "planning.topic",
    "mailing.bounce.reported": "mail.topic",
}


async def _declare_and_bind(
    channel: AbstractChannel, queue_name: str, durable: bool,
) -> aio_pika.abc.AbstractQueue:
    """Declare a queue and bind it to the mapped topic exchange."""
    queue = await channel.declare_queue(queue_name, durable=durable)
    exchange_name = _INBOUND_EXCHANGE.get(queue_name)
    if exchange_name:
        exchange = await channel.declare_exchange(
            exchange_name, type=ExchangeType.TOPIC, durable=True,
        )
        await queue.bind(exchange, routing_key=queue_name)
    return queue


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume configured inbound messages, validate XML, process in Salesforce.

    Contract 9 is the first implemented handler and establishes the base structure
    for all future contract handlers.
    """
    channel = await connection.channel()
    sf_client = await get_salesforce_client(config)

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

    # Contract 25 — Facturatie → CRM: user updated
    # Queue: facturatie.user.updated | Exchange: user.topic | durable: true
    queue_facturatie_user_updated = await _declare_and_bind(channel, "facturatie.user.updated", durable=True)
    await queue_facturatie_user_updated.consume(partial(handle_facturatie_user_updated, sf=sf_client))

    # Contract 26 — Facturatie → CRM: user deactivated (soft delete only)
    # Queue: facturatie.user.deactivated | Exchange: user.topic | durable: true
    queue_facturatie_user_deactivated = await _declare_and_bind(channel, "facturatie.user.deactivated", durable=True)
    await queue_facturatie_user_deactivated.consume(partial(handle_facturatie_user_deactivated, sf=sf_client))

    # Contract 27 — Mailing → CRM: new Mailing user sync
    # Queue: mailing.user.created | Exchange: user.topic | durable: true
    queue_mailing_user_created = await _declare_and_bind(channel, "mailing.user.created", durable=True)
    await queue_mailing_user_created.consume(partial(handle_mailing_user_created, sf=sf_client))

    # Contract 28 — Mailing → CRM: update existing Mailing user sync
    # Queue: mailing.user.updated | Exchange: user.topic | durable: true
    queue_mailing_user_updated = await _declare_and_bind(channel, "mailing.user.updated", durable=True)
    await queue_mailing_user_updated.consume(partial(handle_mailing_user_updated, sf=sf_client))

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

    # Contract 11 — Planning → CRM: session update (Release 2)
    # queue_session = await _declare_and_bind(channel, "planning.session.updated", durable=True)
    # await queue_session.consume(handle_session_updated)

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
    - Update Contact.Paid_At__c in Salesforce.
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
        logger.error("PaymentConfirmed — error processing message: %s", exc)
        await message.reject(requeue=True)


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
        logger.error("UnpaidRequest — error processing message: %s", exc)
        await message.reject(requeue=True)


def _get_contact_is_active(contact: dict) -> bool:
    """Return the normalized active flag across supported Salesforce field names."""
    for active_field in ("IsActive__c", "Active__c", "Is_Active__c"):
        if active_field in contact:
            return bool(contact[active_field])
    return True


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


def _build_full_name(first_name: str | None, last_name: str | None) -> str:
    """Build a display name from first/last name, skipping missing parts."""
    return f"{first_name or ''} {last_name or ''}".strip()


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


async def handle_mailing_user_created(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 27 — Mailing -> CRM: create or attach a Mailing user identity.

    Queue: mailing.user.created | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Create a new Contact when the email and Mailing ID are new.
    - Reuse a unique existing Contact when the Mailing payload is idempotent.
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
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "MailingUserCreated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

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
                contact = await create_contact(sf, _build_mailing_contact_data(xml))
                await sender.publish_user_confirmed(_build_user_data(contact))
                logger.info("Published crm.user.confirmed for new Mailing user %s", email)
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
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for existing Mailing user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserCreated — error processing message: %s", exc)
        await message.reject(requeue=True)


async def handle_mailing_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 28 — Mailing -> CRM: update an existing Mailing-linked user.

    Queue: mailing.user.updated | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Resolve the Contact strictly by Mailing_ID__c.
    - Requeue unknown Mailing identities so out-of-order create/update can recover.
    - Ack ambiguous Mailing identities without retry.
    - Publish crm.user.conflict on email collisions.
    - Update Mailing-owned fields authoritatively in Salesforce.
    - Publish crm.user.updated after a successful update.
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
        mailing_id = xml.findtext("id") or ""
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "MailingUserUpdated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

        if not await has_contact_mailing_id_field(sf):
            logger.error(
                "MailingUserUpdated rejected — Salesforce Contact field Mailing_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        mailing_match_status, existing_contact = await get_contact_match_by_mailing_id(sf, mailing_id)
        if mailing_match_status == "none":
            logger.warning(
                "MailingUserUpdated deferred — no Contact found for Mailing_ID__c %s; retrying for possible out-of-order create/update",
                mailing_id,
            )
            await message.reject(requeue=True)
            return

        if mailing_match_status == "ambiguous":
            logger.warning(
                "MailingUserUpdated ignored — ambiguous Mailing_ID__c %s in Salesforce",
                mailing_id,
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

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            mailing_id=mailing_id,
        )
        contact = await update_mailing_contact(
            sf,
            contact,
            email=email,
            first_name=_normalize_optional_text(xml.findtext("firstName")),
            last_name=_get_mailing_last_name_for_contact(xml),
            company_id=_normalize_optional_text(xml.findtext("companyId")),
        )
        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Mailing user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserUpdated — error processing message: %s", exc)
        await message.reject(requeue=True)


async def handle_facturatie_user_created(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 24 — Facturatie -> CRM: manually created user.

    Queue: facturatie.user.created | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Reuse an existing unique Contact after ensuring canonical identifiers.
    - Create a new Contact when no Contact exists yet.
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
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "FacturatieUserCreated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

        registration_id = xml.findtext("registrationId")
        facturatie_customer_id = xml.findtext("facturatieCustomerId")
        match_status, existing_contact = await get_contact_match_by_email(sf, email)
        if match_status == "unique" and existing_contact is not None:
            contact = await ensure_contact_identifiers(
                sf,
                existing_contact,
                registration_id=registration_id,
                facturatie_customer_id=facturatie_customer_id,
            )
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

        contact_data = await add_facturatie_customer_id_if_supported(
            sf,
            contact_data,
            facturatie_customer_id,
        )

        contact = await create_contact(sf, contact_data)
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for new Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserCreated — error processing message: %s", exc)
        await message.reject(requeue=True)


async def handle_facturatie_user_updated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 25 — Facturatie -> CRM: existing CRM-linked user update.

    Queue: facturatie.user.updated | durable: true

    Behaviour:
    - Validate XML against schema.
    - Reject updates without GDPR consent.
    - Update the unique Contact by CRM UUID (id).
    - Publish crm.user.updated after Salesforce update.
    - Missing/ambiguous CRM UUID: ack without retry.
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
        crm_id = xml.findtext("id") or ""
        email = xml.findtext("email") or ""
        gdpr_text = xml.findtext("gdprConsent")
        if gdpr_text not in ("true", "1"):
            logger.warning(
                "FacturatieUserUpdated refused — gdprConsent=%s for email %s",
                gdpr_text,
                email,
            )
            await message.reject(requeue=False)
            return

        payload = {
            "id": crm_id,
            "email": email,
            "firstName": xml.findtext("firstName"),
            "lastName": xml.findtext("lastName"),
            "phone": xml.findtext("phone"),
            "street": xml.findtext("street"),
            "houseNumber": xml.findtext("houseNumber"),
            "postalCode": xml.findtext("postalCode"),
            "city": xml.findtext("city"),
            "country": xml.findtext("country"),
            "role": xml.findtext("role"),
            "companyId": xml.findtext("companyId"),
            "badgeCode": xml.findtext("badgeCode"),
            "isActive": xml.findtext("isActive") in ("true", "1"),
            "gdprConsent": True,
        }

        contact = await update_contact_by_crm_id(sf, crm_id, payload)
        if contact is None:
            await message.ack()
            return

        await sender.publish_user_updated(_build_updated_user_data(contact))
        logger.info("Published crm.user.updated for Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserUpdated — error processing message: %s", exc)
        await message.reject(requeue=True)


async def handle_facturatie_user_deactivated(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 26 — Facturatie -> CRM: CRM-linked user deactivation.

    Queue: facturatie.user.deactivated | durable: true

    Behaviour:
    - Validate XML against schema.
    - Soft-delete the unique Contact by CRM UUID (id) only.
    - Publish crm.user.deactivated after Salesforce update.
    - Missing/ambiguous CRM UUID: ack without retry.
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
        crm_id = xml.findtext("id") or ""
        email = xml.findtext("email") or ""

        contact = await deactivate_contact_by_crm_id(sf, crm_id)
        if contact is None:
            await message.ack()
            return

        deactivation_data = {
            "id": contact["CRM_ID__c"],
            "email": contact["Email"],
            "deactivatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        await sender.publish_user_deactivated(deactivation_data)
        logger.info("Published crm.user.deactivated for Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserDeactivated — error processing message: %s", exc)
        await message.reject(requeue=True)


async def handle_registration(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 1 — Frontend -> CRM: new registration.

    Queue: frontend.registration.created | durable: true | US-02, US-04, US-05, US-19

    Behaviour:
    - Validate XML against schema.
    - Check if email exists in Salesforce (R1 scope: log only. R2 adds C15 publish).
    - If new, create Contact in Salesforce mapping fields.
    - Publish crm.user.confirmed via sender.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
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

        # TODO: Switch to registrationId-based dedup as primary key (contract spec).
        #       Current R1 approach uses email as lookup; registrationId is secondary.

        role = xml.findtext("role")
        company = xml.findtext("company")
        if role == "COMPANY_CONTACT" and not company:
            logger.warning("COMPANY_CONTACT registration without company field for %s", email)

        existing_contact = await get_contact_by_email(sf, email)

        if existing_contact:
            reg_id_incoming = xml.findtext("registrationId")
            reg_id_existing = existing_contact.get("Registration_ID__c")
            
            if reg_id_incoming == reg_id_existing:
                # Retry na publish failure -> opnieuw publishen
                logger.info("Retry for registrationId %s — republishing", reg_id_incoming)

                await sender.publish_user_confirmed(_build_user_data(existing_contact))
                
                # C6: Publish mail request
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

        # Prepare payload for Salesforce
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

        # Company mapping deferred to Contract 3 (aparte taak)
        # TODO: sessionId mapping needed for Contract 2

        logger.info("Creating new Salesforce Contact for %s", email)
        contact = await create_contact(sf, contact_data)

        # Publish crm.user.confirmed
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for %s", email)

        # Contract 6 (R1 scope) — publish registration_confirmation
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
        updated   → Upsert Contact in Salesforce, publish crm.user.updated (C18).
        cancelled → Soft-delete Contact (IsActive__c=false), publish crm.user.deactivated (C22).
    - Cancellation is always a soft delete — never physically remove (GDPR).
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
            await _handle_cancellation(email, sf, message)
        else:
            # XSD validation should prevent this, but defence-in-depth
            logger.error("Unknown changeType '%s' for email %s — rejecting", change_type, email)
            await message.reject(requeue=False)

    except Exception as exc:  # noqa: BLE001
        logger.error("RegistrationChange — error processing message: %s", exc)
        await message.reject(requeue=True)


async def _handle_update(
    xml: etree._Element, email: str, sf: "Salesforce", message: aio_pika.IncomingMessage
) -> None:
    """Process changeType=updated: upsert Contact and publish crm.user.updated."""
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

    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for %s", email)
    await message.ack()


async def _handle_cancellation(
    email: str, sf: "Salesforce", message: aio_pika.IncomingMessage
) -> None:
    """Process changeType=cancelled: soft-delete Contact and publish crm.user.deactivated."""
    contact = await deactivate_contact(sf, email)

    if contact is None:
        # Contact doesn't exist — nothing to deactivate. Ack to prevent infinite requeue.
        logger.warning("Cancellation for unknown email %s — acking without action", email)
        await message.ack()
        return

    deactivation_data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "deactivatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    await sender.publish_user_deactivated(deactivation_data)
    logger.info("Published crm.user.deactivated for %s", email)
    await message.ack()
