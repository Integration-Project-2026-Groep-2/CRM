"""RabbitMQ queue consumer — listens on inbound queues from other teams."""

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from lxml import etree

from src import sender, xml_validator
from src.config import Config
from src.salesforce_client import (

    add_facturatie_customer_id_if_supported,
    create_contact,
    deactivate_contact,
    deactivate_contact_by_crm_id,
    ensure_contact_identifiers,
    get_contact_by_email,
    get_contact_match_by_email,
    get_salesforce_client,
    get_unpaid_contacts,
    update_contact_by_crm_id,
    update_payment_status,

    upsert_contact_by_email,
)

logger = logging.getLogger(__name__)

# Queue → topic exchange mapping (Infra-beheerd, zie docs/rabbitmq-exchanges.md)
_INBOUND_EXCHANGE: dict[str, str] = {
    "frontend.registration.created": "user.topic",
    "frontend.registration.updated": "user.topic",
    "frontend.company.created": "user.topic",
    "facturatie.user.created": "user.topic",
    "facturatie.user.updated": "user.topic",
    "facturatie.user.deactivated": "user.topic",
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
    """Consume messages from all inbound queues, validate XML, process in Salesforce."""
    from src.salesforce_client import get_salesforce_client

    sf = await get_salesforce_client(config)
    channel = await connection.channel()

    # Contract 9 — Controlroom → CRM: system warning
    # Queue: controlroom.warning.issued | Exchange: planning.topic | durable: false | US-26
    queue_warning = await _declare_and_bind(channel, "controlroom.warning.issued", durable=False)
    await queue_warning.consume(handle_warning)

    # Contract 1 — Frontend → CRM: new registration
    # Queue: frontend.registration.created | Exchange: user.topic | durable: true
    queue_registration = await _declare_and_bind(channel, "frontend.registration.created", durable=True)
    await queue_registration.consume(partial(handle_registration, sf=sf))

    # Contract 2 — Frontend → CRM: update/cancel registration
    # Queue: frontend.registration.updated | Exchange: user.topic | durable: true
    queue_reg_updated = await _declare_and_bind(channel, "frontend.registration.updated", durable=True)
    await queue_reg_updated.consume(partial(handle_registration_updated, sf=sf))

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

    # Contract 3 — Frontend → CRM: create company
    # Queue: frontend.company.created | Exchange: user.topic | durable: true | US-40, US-20
    queue_company = await _declare_and_bind(channel, "frontend.company.created", durable=True)
    await queue_company.consume(partial(handle_company_created, sf=sf))

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


# ---------------------------------------------------------------------------
# Contract 9 — Controlroom → CRM: system warning
# ---------------------------------------------------------------------------

async def handle_warning(message: aio_pika.IncomingMessage) -> None:
    """Contract 9 — Controlroom → CRM: system warning.

    Validates incoming XML, logs as ERROR, explicit ack/reject.
    Invalid XML is rejected (requeue=False) — it will never become valid.
    """
    try:
        xml = xml_validator.validate(message.body)
        logger.error(
            "Controlroom warning received: %s",
            etree.tostring(xml, encoding="unicode"),
        )
        await message.ack()
    except Exception as exc:
        logger.error(
            "Controlroom warning — invalid XML, rejecting message: %s", exc
        )
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

# ---------------------------------------------------------------------------
# Contract 1 — Frontend → CRM: new registration
# ---------------------------------------------------------------------------



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
    # ── Step 1: validate XML ──────────────────────────────────────────────
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:
        logger.error("Contract 1 — invalid XML, rejecting: %s", exc)
        await message.reject(requeue=False)
        return

    registration_id = xml.findtext("registrationId")
    email = xml.findtext("email")
    first_name = xml.findtext("firstName")
    last_name = xml.findtext("lastName")
    role = xml.findtext("role")
    session_id = xml.findtext("sessionId")
    gdpr_raw = xml.findtext("gdprConsent", "false")
    phone = xml.findtext("phone")
    # FIX (Issue 1): company is xs:string in the XSD — read as plain text,
    # not as a complex element with child nodes.
    company = xml.findtext("company")

    # ── Step 2: check gdprConsent ─────────────────────────────────────────
    gdpr_consent = gdpr_raw.lower() in ("true", "1")
    if not gdpr_consent:
        logger.warning(
            "Registration refused — gdprConsent=false for %s", email
        )
        await message.reject(requeue=False)
        return

    if role == "COMPANY_CONTACT" and not company:
        logger.warning(
            "COMPANY_CONTACT registration without company field for %s", email
        )

    # ── Step 3: look up existing contact by email ─────────────────────────
    try:
        existing_contact = await get_contact_by_email(sf, email)
    except Exception as exc:
        logger.error(
            "Contract 1 — Salesforce error during email lookup (email=%s): %s",
            email, exc,
        )
        await message.reject(requeue=True)
        return

    full_name = f"{first_name} {last_name}"
    confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if existing_contact is not None:
        if existing_contact.get("Registration_ID__c") == registration_id:
            # Retry path — same registrationId.
            # FIX (Issue 4): use Salesforce values (authoritative), not XML values.
            # phone is explicitly included from the Salesforce record.
            crm_id = existing_contact["CRM_ID__c"]
            user_data: dict = {
                "id": crm_id,
                "email": existing_contact.get("Email", email),
                "firstName": existing_contact.get("FirstName", first_name),
                "lastName": existing_contact.get("LastName", last_name),
                "role": existing_contact.get("Role__c", role),
                "isActive": existing_contact.get("IsActive__c", True),
                "gdprConsent": existing_contact.get("GDPR_Consent__c", gdpr_consent),
                "confirmedAt": confirmed_at,
            }
            if existing_contact.get("Phone"):
                user_data["phone"] = existing_contact["Phone"]
            try:
                await sender.publish_user_confirmed(user_data)
                await sender.publish_mail_requested(
                    "registration_confirmation",
                    {"email": email, "name": full_name},
                    {"guest_name": full_name},
                )
            except Exception as exc:
                logger.error(
                    "Contract 1 — publish failed during retry (email=%s): %s",
                    email, exc,
                )
                await message.reject(requeue=True)
                return
            await message.ack()
        else:
            # Conflict — different registrationId for the same email
            logger.warning(
                "Conflict: email %s exists with different registrationId", email
            )
            await message.ack()
        return

    # ── Step 4: create Contact in Salesforce ──────────────────────────────
    payload: dict = {
        "FirstName": first_name,
        "LastName": last_name,
        "Email": email,
        "Role__c": role,
        "Session_ID__c": session_id,
        "GDPR_Consent__c": gdpr_consent,
        "Registration_ID__c": registration_id,
    }
    if phone:
        payload["Phone"] = phone
    # FIX (Issue 1): company is a plain string — store directly, no child access.
    if company:
        payload["Company_Name__c"] = company

    try:
        contact = await create_contact(sf, payload)
    except Exception as exc:
        logger.error(
            "Contract 1 — Salesforce error creating contact (email=%s): %s",
            email, exc,
        )
        await message.reject(requeue=True)
        return

    crm_id = contact["CRM_ID__c"]
    confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Step 5: publish crm.user.confirmed (C13) ──────────────────────────
    user_data = {
        "id": crm_id,
        "email": email,
        "firstName": first_name,
        "lastName": last_name,
        "role": role,
        "isActive": True,
        "gdprConsent": gdpr_consent,
        "confirmedAt": confirmed_at,
    }
    if phone:
        user_data["phone"] = phone

    # ── Step 6: publish crm.mail.requested (C6) ───────────────────────────
    try:
        await sender.publish_user_confirmed(user_data)
        await sender.publish_mail_requested(
            "registration_confirmation",
            {"email": email, "name": full_name},
            {"guest_name": full_name},
        )
    except Exception as exc:
        logger.error(
            "Contract 1 — publish failed (email=%s): %s",
            email, exc,
        )
        await message.reject(requeue=True)
        return

    await message.ack()
    logger.info(
        "Contract 1 — Contact created and confirmed (email=%s, crm_id=%s).",
        email, crm_id,
    )


# ---------------------------------------------------------------------------
# Contract 2 — Frontend → CRM: update/cancel registration
# ---------------------------------------------------------------------------

_UPDATED_FIELD_MAP = {
    "firstName": "FirstName",
    "lastName": "LastName",
    "phone": "Phone",
    "role": "Role__c",
    "email": "Email",
    "sessionId": "Session_ID__c",
}


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


async def handle_registration_updated(
    message: aio_pika.IncomingMessage,
    sf: object,
) -> None:
    """Contract 2 — Frontend → CRM: update or cancel a registration.

    Queue: frontend.registration.updated | durable: true

    changeType=updated   → upsert Contact, publish crm.user.updated (C18)
    changeType=cancelled → deactivate Contact, publish crm.user.deactivated (C22)

    Error handling:
    - Invalid XML                      → reject (requeue=False)
    - Unknown changeType               → reject (requeue=False)
    - Salesforce error                 → reject (requeue=True)
    - Publish failure                  → reject (requeue=True)
    - Contact not found (cancelled)    → log warning, ack without publish
    """
    # ── Step 1: validate XML ──────────────────────────────────────────────
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:
        logger.error("Contract 2 — invalid XML, rejecting: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email")
    change_type = xml.findtext("changeType")

    if change_type == "updated":
        # Build update_data from <updatedFields> child elements
        updated_el = xml.find("updatedFields")
        update_data: dict = {}
        if updated_el is not None:
            for child in updated_el:
                sf_field = _UPDATED_FIELD_MAP.get(child.tag)
                if sf_field and child.text:
                    update_data[sf_field] = child.text

        try:
            contact = await upsert_contact_by_email(sf, email, update_data)
        except Exception as exc:
            logger.error(
                "Contract 2 — Salesforce error upserting contact (email=%s): %s",
                email, exc,
            )
            await message.reject(requeue=True)
            return

        updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # FIX (Issue 5): default Role__c to "VISITOR" (valid FacturatieUserRoleType
        # enum value) instead of "" which fails XSD validation on C18 outbound.
        user_data: dict = {
            "id": contact["CRM_ID__c"],
            "email": email,
            "firstName": contact.get("FirstName", ""),
            "lastName": contact.get("LastName", ""),
            "role": contact.get("Role__c", "VISITOR"),
            "isActive": contact.get("IsActive__c", False),
            "gdprConsent": contact.get("GDPR_Consent__c", False),
            "updatedAt": updated_at,
        }
        for sf_field, key in _OPTIONAL_CONTACT_FIELDS:
            value = contact.get(sf_field)
            if value is not None:
                user_data[key] = value

        try:
            await sender.publish_user_updated(user_data)
        except Exception as exc:
            logger.error(
                "Contract 2 — publish_user_updated failed (email=%s): %s",
                email, exc,
            )
            await message.reject(requeue=True)
            return

        await message.ack()
        logger.info("Contract 2 — Contact updated and published (email=%s).", email)

    elif change_type == "cancelled":
        try:
            contact = await deactivate_contact(sf, email)
        except Exception as exc:
            logger.error(
                "Contract 2 — Salesforce error deactivating contact (email=%s): %s",
                email, exc,
            )
            await message.reject(requeue=True)
            return

        if contact is None:
            logger.warning(
                "Contract 2 — Contact not found for cancellation (email=%s), "
                "acking without action.",
                email,
            )
            await message.ack()
            return

        deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        deact_data: dict = {
            "id": contact["CRM_ID__c"],
            "email": email,
            "deactivatedAt": deactivated_at,
        }

        try:
            await sender.publish_user_deactivated(deact_data)
        except Exception as exc:
            logger.error(
                "Contract 2 — publish_user_deactivated failed (email=%s): %s",
                email, exc,
            )
            await message.reject(requeue=True)
            return

        await message.ack()
        logger.info("Contract 2 — Contact deactivated and published (email=%s).", email)

    else:
        # FIX (Issue 3): unknown changeType must be explicitly rejected.
        # Without this branch the message stays unacked → infinite requeue loop
        # when the channel closes.
        logger.error(
            "Contract 2 — unknown changeType '%s' for email %s, rejecting.",
            change_type, email,
        )
        await message.reject(requeue=False)


# ---------------------------------------------------------------------------
# Contract 3 — Frontend → CRM: create company
# ---------------------------------------------------------------------------

async def handle_company_created(
    message: aio_pika.IncomingMessage,
    sf: object,
) -> None:
    """Contract 3 — Frontend → CRM: create company.

    Queue: frontend.company.created | durable: true | US-40, US-20

    Flow:
    1. Validate XML
    2. Reject if email is absent (required for C14 CompanyConfirmed)
    3. Look up existing Account by VAT number (idempotency)
    4. Create Account in Salesforce
    5. Publish crm.company.confirmed (C14)
    6. Ack

    Error handling:
    - Invalid XML          → reject (requeue=False)
    - Email absent         → reject (requeue=False)
    - Salesforce error     → reject (requeue=True)
    - Publish failure      → reject (requeue=True)
    """
    # ── Step 1: validate XML ──────────────────────────────────────────────
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:
        logger.error("Contract 3 — invalid XML, rejecting: %s", exc)
        await message.reject(requeue=False)
        return

    vat_number = xml.findtext("vatNumber")
    name = xml.findtext("name")
    email = xml.findtext("email")
    phone = xml.findtext("phone")
    street = xml.findtext("street")
    house_number = xml.findtext("houseNumber")
    postal_code = xml.findtext("postalCode")
    city = xml.findtext("city")
    country = xml.findtext("country")

    # FIX (Issue 2): email is optional in CompanyCreated (C3) but required in
    # CompanyConfirmed (C14) — XSD EmailType rejects empty strings.
    # Reject early instead of passing "" which would fail XSD validation downstream.
    if not email:
        logger.error(
            "Contract 3 — email is required for C14 (CompanyConfirmed) but absent "
            "(vatNumber=%s), rejecting.",
            vat_number,
        )
        await message.reject(requeue=False)
        return

    # ── Step 2: idempotency check ─────────────────────────────────────────
    try:
        existing_account = await get_account_by_vat(sf, vat_number)
    except Exception as exc:
        logger.error(
            "Contract 3 — Salesforce error during VAT lookup (vatNumber=%s): %s",
            vat_number, exc,
        )
        await message.reject(requeue=True)
        return

    if existing_account:
        logger.info(
            "Contract 3 — Account with vatNumber=%s already exists "
            "(id=%s), skipping (idempotent).",
            vat_number, existing_account.get("CRM_ID__c"),
        )
        await message.ack()
        return

    confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    account_payload: dict = {
        "Name": name,
        "VAT_Number__c": vat_number,
        "IsActive__c": True,
    }
    if email:
        account_payload["Email__c"] = email
    if phone:
        account_payload["Phone"] = phone
    if street:
        account_payload["BillingStreet"] = (
            f"{street} {house_number}".strip() if house_number else street
        )
    if postal_code:
        account_payload["BillingPostalCode"] = postal_code
    if city:
        account_payload["BillingCity"] = city
    if country:
        account_payload["BillingCountry"] = country

    # ── Step 3: create Account in Salesforce ──────────────────────────────
    try:
        account = await create_account(sf, account_payload)
    except Exception as exc:
        logger.error(
            "Contract 3 — Salesforce error creating account (vatNumber=%s): %s",
            vat_number, exc,
        )
        await message.reject(requeue=True)
        return

    crm_id = account["CRM_ID__c"]
    logger.info(
        "Contract 3 — Account created (vatNumber=%s, crm_id=%s).",
        vat_number, crm_id,
    )

    # ── Step 4: publish crm.company.confirmed (C14) ───────────────────────
    # email is guaranteed non-empty here (checked at top of handler).
    company_data: dict = {
        "id": crm_id,
        "vatNumber": vat_number,
        "name": name,
        "email": email,
        "isActive": True,
        "confirmedAt": confirmed_at,
    }
    try:
        await sender.publish_company_confirmed(company_data)
    except Exception as exc:
        logger.error(
            "Contract 3 — publish_company_confirmed failed (vatNumber=%s): %s",
            vat_number, exc,
        )
        await message.reject(requeue=True)
        return

    await message.ack()
    logger.info("Contract 3 — crm.company.confirmed published (crm_id=%s).", crm_id)