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

logger = logging.getLogger(__name__)


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume messages from all inbound queues, validate XML, process in Salesforce."""
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
    await queue_registration.consume(handle_registration)

    # Contract 3 — Frontend → CRM: create company
    # Queue: frontend.company.created | durable: true | US-40, US-20
    queue_company = await channel.declare_queue(
        "frontend.company.created", durable=True
    )
    await queue_company.consume(handle_company_created)

    # Future contracts — uncomment and implement per sprint:
    # Contract 2 — Frontend → CRM: update/cancel registration
    # queue_reg_updated = await channel.declare_queue(
    #     "frontend.registration.updated", durable=True
    # )
    # await queue_reg_updated.consume(handle_registration_updated)

    # Contract 5a — Facturatie → CRM: request company data
    # queue_company_req = await channel.declare_queue(
    #     "facturatie.company.requested", durable=True
    # )
    # await queue_company_req.consume(handle_company_requested)

    # Contract 10a — Kassa → CRM: person lookup request
    # queue_person_lookup = await channel.declare_queue(
    #     "kassa.person.lookup.requested", durable=True
    # )
    # await queue_person_lookup.consume(handle_person_lookup)

    # Contract 16 — Kassa → CRM: payment confirmed
    # queue_payment = await channel.declare_queue(
    #     "kassa.payment.confirmed", durable=True
    # )
    # await queue_payment.consume(handle_payment_confirmed)

    # Contract 17a — Kassa → CRM: unpaid persons request
    # queue_unpaid = await channel.declare_queue(
    #     "kassa.unpaid.requested", durable=True
    # )
    # await queue_unpaid.consume(handle_unpaid_requested)

    # Contract 11 — Planning → CRM: session update (Release 2)
    # queue_session = await channel.declare_queue(
    #     "planning.session.updated", durable=True
    # )
    # await queue_session.consume(handle_session_updated)

    # Contract 12 — IoT → CRM: badge linked (Release 2)
    # queue_badge = await channel.declare_queue(
    #     "iot.badge.linked", durable=True
    # )
    # await queue_badge.consume(handle_badge_linked)

    logger.info("Receiver started. Listening on all configured queues.")
    await asyncio.Future()  # run forever


# ---------------------------------------------------------------------------
# Contract 9 — Controlroom → CRM: system warning
# ---------------------------------------------------------------------------

async def handle_warning(message: aio_pika.IncomingMessage) -> None:
    """Contract 9 — Controlroom → CRM: system warning.

    Queue: controlroom.warning.issued | durable: false | US-26

    Behaviour:
    - Validates incoming XML against XSD
    - Logs as logger.error() — no crash
    - No Salesforce action required
    - Invalid XML: caught, logged, rejected (requeue=False — will never become valid)
    """
    async with message.process(ignore_processed=True):
        try:
            xml = xml_validator.validate(message.body)
            logger.error(
                "Controlroom warning received: %s",
                etree.tostring(xml, encoding="unicode"),
            )
        except Exception as exc:
            logger.error(
                "Controlroom warning — invalid XML, rejecting message: %s", exc
            )
            await message.reject(requeue=False)


# ---------------------------------------------------------------------------
# Contract 1 — Frontend → CRM: new registration
# Contracts 13 + 6 published on success
# ---------------------------------------------------------------------------

async def handle_registration(message: aio_pika.IncomingMessage) -> None:
    """Contract 1 — Frontend → CRM: new registration.

    Queue: frontend.registration.created | durable: true | US-02, 03, 04, 05, 19

    Flow:
    1. Validate XML against XSD (<Registration>)
    2. Check idempotency: skip if registrationId already processed
    3. Check if email already exists in Salesforce
       - Exists with conflicting data → log conflict (C15 handling, R2)
       - Does not exist → create Contact + generate UUID v4
    4. Publish crm.user.confirmed (C13) with CRM UUID
    5. Publish crm.mail.requested (C6) with mailType=registration_confirmation

    Error handling:
    - Invalid XML        → log error, reject (requeue=False)
    - Salesforce down    → log error, requeue (requeue=True) for retry
    - Duplicate regId   → log info, ack silently (idempotent)
    - Duplicate email   → log warning (conflict), ack (C15 is R2)
    """
    async with message.process(ignore_processed=True):
        # ── Step 1: validate XML ──────────────────────────────────────────
        try:
            xml = xml_validator.validate(message.body)
        except Exception as exc:
            logger.error(
                "Contract 1 — invalid XML, rejecting: %s", exc
            )
            await message.reject(requeue=False)
            return

        registration_id = xml.findtext("registrationId")
        email = xml.findtext("email")
        first_name = xml.findtext("firstName")
        last_name = xml.findtext("lastName")
        role = xml.findtext("role")
        session_id = xml.findtext("sessionId")
        gdpr_consent = xml.findtext("gdprConsent", "false").lower() == "true"
        phone = xml.findtext("phone")  # optional
        company_el = xml.find("company")  # optional, required if COMPANY_CONTACT

        # ── Step 2: idempotency check ─────────────────────────────────────
        try:
            already_processed = await salesforce.registration_exists(registration_id)
        except salesforce.SalesforceUnavailableError as exc:
            logger.error(
                "Contract 1 — Salesforce unavailable during idempotency check "
                "(registrationId=%s), requeueing: %s",
                registration_id, exc,
            )
            await message.reject(requeue=True)
            return

        if already_processed:
            logger.info(
                "Contract 1 — duplicate registrationId=%s, skipping (idempotent).",
                registration_id,
            )
            return  # auto-ack via process() context manager

        # ── Step 3: check for existing email in Salesforce ────────────────
        try:
            existing_contact = await salesforce.find_contact_by_email(email)
        except salesforce.SalesforceUnavailableError as exc:
            logger.error(
                "Contract 1 — Salesforce unavailable during email lookup "
                "(email=%s), requeueing: %s",
                email, exc,
            )
            await message.reject(requeue=True)
            return

        if existing_contact:
            # C15 conflict handling is R2 — for now just log and ack
            logger.warning(
                "Contract 1 — email conflict detected for %s "
                "(existing id=%s). C15 fanout is Release 2, logging only.",
                email, existing_contact.get("CRM_ID__c"),
            )
            return  # auto-ack

        # ── Step 4: create Contact in Salesforce ──────────────────────────
        crm_id = str(uuid.uuid4())
        confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        contact_payload = {
            "CRM_ID__c": crm_id,
            "Registration_ID__c": registration_id,
            "FirstName": first_name,
            "LastName": last_name,
            "Email": email,
            "Role__c": role,
            "Session_ID__c": session_id,
            "GDPR_Consent__c": gdpr_consent,
        }
        if phone:
            contact_payload["Phone"] = phone
        if company_el is not None:
            contact_payload["Company_Name__c"] = company_el.findtext("name")
            contact_payload["Company_VAT__c"] = company_el.findtext("vatNumber")

        try:
            await salesforce.create_contact(contact_payload)
        except salesforce.SalesforceUnavailableError as exc:
            logger.error(
                "Contract 1 — Salesforce unavailable during contact creation "
                "(registrationId=%s), requeueing: %s",
                registration_id, exc,
            )
            await message.reject(requeue=True)
            return

        logger.info(
            "Contract 1 — Contact created in Salesforce "
            "(registrationId=%s, crm_id=%s).",
            registration_id, crm_id,
        )

        # ── Step 5: publish crm.user.confirmed (C13) ──────────────────────
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
        if phone:
            user_data["phone"] = phone
        if company_el is not None:
            # companyId is only available after the company is confirmed (C14/C3)
            # omit here — consumers will correlate via crm.company.confirmed
            pass

        await sender.publish_user_confirmed(user_data)
        logger.info("Contract 1 — crm.user.confirmed published (crm_id=%s).", crm_id)

        # ── Step 6: publish crm.mail.requested (C6) ───────────────────────
        await sender.publish_mail_requested(
            mail_type="registration_confirmation",
            recipient={"email": email, "name": f"{first_name} {last_name}"},
            dynamic_data={
                "guest_name": first_name,
                "session_name": session_id,   # mailing team resolves name from ID
                "session_time": confirmed_at,  # placeholder — planning owns schedule
            },
        )
        logger.info(
            "Contract 1 — crm.mail.requested published "
            "(registration_confirmation, email=%s).",
            email,
        )


# ---------------------------------------------------------------------------
# Contract 3 — Frontend → CRM: create company
# Contract 14 published on success
# ---------------------------------------------------------------------------

async def handle_company_created(message: aio_pika.IncomingMessage) -> None:
    """Contract 3 — Frontend → CRM: create company.

    Queue: frontend.company.created | durable: true | US-40, US-20

    Flow:
    1. Validate XML against XSD (<CompanyCreated>)
    2. Check idempotency: skip if vatNumber already exists as Account in Salesforce
    3. Create Account in Salesforce + generate UUID v4
    4. Publish crm.company.confirmed (C14) to all consumers

    Error handling:
    - Invalid XML        → log error, reject (requeue=False)
    - Salesforce down    → log error, requeue (requeue=True)
    - Duplicate vatNumber → log info, ack silently (idempotent)
    """
    async with message.process(ignore_processed=True):
        # ── Step 1: validate XML ──────────────────────────────────────────
        try:
            xml = xml_validator.validate(message.body)
        except Exception as exc:
            logger.error(
                "Contract 3 — invalid XML, rejecting: %s", exc
            )
            await message.reject(requeue=False)
            return

        vat_number = xml.findtext("vatNumber")
        name = xml.findtext("name")
        email = xml.findtext("email")          # optional
        phone = xml.findtext("phone")          # optional
        street = xml.findtext("street")        # optional
        house_number = xml.findtext("houseNumber")  # optional
        postal_code = xml.findtext("postalCode")    # optional
        city = xml.findtext("city")            # optional
        country = xml.findtext("country")      # optional

        # ── Step 2: idempotency check (vatNumber as dedup key) ────────────
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
            return  # auto-ack

        # ── Step 3: create Account in Salesforce ──────────────────────────
        crm_id = str(uuid.uuid4())
        confirmed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        account_payload = {
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
            "Contract 3 — Account created in Salesforce "
            "(vatNumber=%s, crm_id=%s).",
            vat_number, crm_id,
        )

        # ── Step 4: publish crm.company.confirmed (C14) ───────────────────
        company_data: dict = {
            "id": crm_id,
            "vatNumber": vat_number,
            "name": name,
            "email": email or "",
            "isActive": True,
            "confirmedAt": confirmed_at,
        }

        await sender.publish_company_confirmed(company_data)
        logger.info(
            "Contract 3 — crm.company.confirmed published (crm_id=%s).", crm_id
        )