"""RabbitMQ queue consumer — listens on 11 queues from other teams."""

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
    create_contact,
    deactivate_contact,
    ensure_contact_identifiers,
    get_contact_match_by_email,
    get_contact_by_email,
    get_salesforce_client,
    get_unpaid_contacts,
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
    """Consume messages from all inbound queues, validate XML, process in Salesforce.

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
    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": contact.get("Role__c", "VISITOR"),
        "isActive": _get_contact_is_active(contact),
        "gdprConsent": contact.get("GDPR_Consent__c", True),
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
        match_status, existing_contact = await get_contact_match_by_email(sf, email)
        if match_status == "unique" and existing_contact is not None:
            contact = await ensure_contact_identifiers(
                sf,
                existing_contact,
                registration_id=registration_id,
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

        contact = await create_contact(sf, contact_data)
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for new Facturatie user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserCreated — error processing message: %s", exc)
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
