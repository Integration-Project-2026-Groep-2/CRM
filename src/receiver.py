"""RabbitMQ queue consumer — listens on inbound queues from other teams."""

import asyncio
import logging
import uuid
from datetime import datetime, timezone

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from lxml import etree

from src import salesforce, sender, xml_validator
from src.config import Config
from src.salesforce_client import (
    create_contact,
    deactivate_contact,
    get_contact_by_email,
    upsert_contact_by_email,
)

logger = logging.getLogger(__name__)


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume messages from all inbound queues, validate XML, process in Salesforce."""
    from src.salesforce_client import get_salesforce_client

    sf = await get_salesforce_client(config)
    channel = await connection.channel()

    # Contract 9 — Controlroom → CRM: system warning
    # Queue: controlroom.warning.issued | durable: false | US-26
    queue_warning = await channel.declare_queue(
        "controlroom.warning.issued", durable=False
    )
    await queue_warning.consume(handle_warning)

    # Contract 1 — Frontend → CRM: new registration
    # Queue: frontend.registration.created | durable: true | US-02, 03, 04, 05, 19
    queue_registration = await channel.declare_queue(
        "frontend.registration.created", durable=True
    )
    await queue_registration.consume(lambda msg: handle_registration(msg, sf))

    # Contract 2 — Frontend → CRM: update/cancel registration
    # Queue: frontend.registration.updated | durable: true
    queue_reg_updated = await channel.declare_queue(
        "frontend.registration.updated", durable=True
    )
    await queue_reg_updated.consume(lambda msg: handle_registration_updated(msg, sf))

    # Contract 3 — Frontend → CRM: create company
    # Queue: frontend.company.created | durable: true | US-40, US-20
    queue_company = await channel.declare_queue(
        "frontend.company.created", durable=True
    )
    await queue_company.consume(handle_company_created)

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

    await sender.publish_user_confirmed(user_data)

    # ── Step 6: publish crm.mail.requested (C6) ───────────────────────────
    await sender.publish_mail_requested(
        "registration_confirmation",
        {"email": email, "name": full_name},
        {"guest_name": full_name},
    )

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
    ("IsActive__c", "isActive"),
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

async def handle_company_created(message: aio_pika.IncomingMessage) -> None:
    """Contract 3 — Frontend → CRM: create company.

    Queue: frontend.company.created | durable: true | US-40, US-20
    """
    async with message.process(ignore_processed=True):
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

        try:
            existing_account = await salesforce.find_account_by_vat(vat_number)
        except salesforce.SalesforceUnavailableError as exc:
            logger.error(
                "Contract 3 — Salesforce unavailable during VAT lookup "
                "(vatNumber=%s), requeueing: %s",
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
            return

        crm_id = str(uuid.uuid4())
        confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        account_payload: dict = {
            "CRM_ID__c": crm_id,
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

        try:
            await salesforce.create_account(account_payload)
        except salesforce.SalesforceUnavailableError as exc:
            logger.error(
                "Contract 3 — Salesforce unavailable during account creation "
                "(vatNumber=%s), requeueing: %s",
                vat_number, exc,
            )
            await message.reject(requeue=True)
            return

        logger.info(
            "Contract 3 — Account created (vatNumber=%s, crm_id=%s).",
            vat_number, crm_id,
        )

        company_data: dict = {
            "id": crm_id,
            "vatNumber": vat_number,
            "name": name,
            "email": email or "",
            "isActive": True,
            "confirmedAt": confirmed_at,
        }
        await sender.publish_company_confirmed(company_data)
        logger.info("Contract 3 — crm.company.confirmed published (crm_id=%s).", crm_id)
