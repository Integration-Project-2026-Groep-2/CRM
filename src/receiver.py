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
    create_account,
    create_contact,
    deactivate_contact,
    get_account_by_vat,
    get_contact_by_email,
    upsert_contact_by_email,
)

logger = logging.getLogger(__name__)

# Queue → topic exchange mapping (Infra-beheerd, zie docs/rabbitmq-exchanges.md)
_INBOUND_EXCHANGE: dict[str, str] = {
    "frontend.registration.created": "user.topic",
    "frontend.registration.updated": "user.topic",
    "frontend.company.created": "user.topic",
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
    # queue_payment = await _declare_and_bind(channel, "kassa.payment.confirmed", durable=True)
    # await queue_payment.consume(handle_payment_confirmed)

    # Contract 17a — Kassa → CRM: unpaid persons request
    # queue_unpaid = await _declare_and_bind(channel, "kassa.unpaid.requested", durable=True)
    # await queue_unpaid.consume(handle_unpaid_requested)

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


# ---------------------------------------------------------------------------
# Contract 1 — Frontend → CRM: new registration
# ---------------------------------------------------------------------------

async def handle_registration(
    message: aio_pika.IncomingMessage,
    sf: object,
) -> None:
    """Contract 1 — Frontend → CRM: new registration.

    Flow:
    1. Validate XML
    2. Reject if gdprConsent is not true/1
    3. Look up existing contact by email
       - Same registrationId → retry: republish and ack
       - Different registrationId → conflict: log warning, ack
    4. Create Contact in Salesforce
    5. Publish crm.user.confirmed (C13) and crm.mail.requested (C6)
    6. Ack

    Error handling:
    - Invalid XML          → reject (requeue=False)
    - gdprConsent false    → reject (requeue=False)
    - Salesforce error     → reject (requeue=True)
    - Publish failure      → reject (requeue=True)
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
    company_el = xml.find("company")

    # ── Step 2: check gdprConsent ─────────────────────────────────────────
    gdpr_consent = gdpr_raw.lower() in ("true", "1")
    if not gdpr_consent:
        logger.warning(
            "Registration refused — gdprConsent=false for %s", email
        )
        await message.reject(requeue=False)
        return

    if role == "COMPANY_CONTACT" and company_el is None:
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
            # Retry path — same registrationId, republish and ack
            crm_id = existing_contact["CRM_ID__c"]
            user_data: dict = {
                "id": crm_id,
                "email": email,
                "firstName": first_name,
                "lastName": last_name,
                "role": role,
                "isActive": True,
                "gdprConsent": gdpr_consent,
                "confirmedAt": confirmed_at,
            }
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
            # Conflict — different registrationId
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
    if company_el is not None:
        payload["Company_Name__c"] = company_el.findtext("name")
        payload["Company_VAT__c"] = company_el.findtext("vatNumber")

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

_OPTIONAL_CONTACT_FIELDS = [
    ("Company_ID__c", "companyId"),
    ("Badge_Code__c", "badgeCode"),
    ("MailingStreet", "street"),
    ("House_Number__c", "houseNumber"),
    ("MailingPostalCode", "postalCode"),
    ("MailingCity", "city"),
    ("MailingCountry", "country"),
]


async def handle_registration_updated(
    message: aio_pika.IncomingMessage,
    sf: object,
) -> None:
    """Contract 2 — Frontend → CRM: update or cancel a registration.

    Queue: frontend.registration.updated | durable: true

    changeType=updated  → upsert Contact, publish crm.user.updated (C18)
    changeType=cancelled → deactivate Contact, publish crm.user.deactivated (C22)

    Error handling:
    - Invalid XML          → reject (requeue=False)
    - Salesforce error     → reject (requeue=True)
    - Publish failure      → reject (requeue=True)
    - Contact not found (cancelled) → log warning, ack without publish
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
        user_data: dict = {
            "id": contact["CRM_ID__c"],
            "email": email,
            "firstName": contact.get("FirstName", ""),
            "lastName": contact.get("LastName", ""),
            "role": contact.get("Role__c", ""),
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
    2. Look up existing Account by VAT number (idempotency)
    3. Create Account in Salesforce
    4. Publish crm.company.confirmed (C14)
    5. Ack

    Error handling:
    - Invalid XML          → reject (requeue=False)
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
    company_data: dict = {
        "id": crm_id,
        "vatNumber": vat_number,
        "name": name,
        "email": email or "",
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
