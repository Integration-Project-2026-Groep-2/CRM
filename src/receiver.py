"""RabbitMQ queue consumer — listens on 11 queues from other teams."""

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from lxml import etree

from src import sender, xml_validator
from src.config import Config
from src.salesforce_client import create_contact, get_contact_by_email, get_salesforce_client

logger = logging.getLogger(__name__)


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume messages from all inbound queues, validate XML, process in Salesforce.

    Contract 9 is the first implemented handler and establishes the base structure
    for all future contract handlers.
    """
    channel = await connection.channel()
    sf_client = await get_salesforce_client(config)

    # Contract 9 — Controlroom → CRM: system warning
    # Queue: controlroom.warning.issued | durable: false | US-26
    queue_warning = await channel.declare_queue("controlroom.warning.issued", durable=False)
    await queue_warning.consume(handle_warning)

    # Future contracts — uncomment and implement per sprint:
    # Contract 1 — Frontend → CRM: new registration
    queue_registration = await channel.declare_queue("frontend.registration.created", durable=True)
    await queue_registration.consume(partial(handle_registration, sf=sf_client))

    # Contract 2 — Frontend → CRM: update/cancel registration
    # queue_reg_updated = await channel.declare_queue(
    #     "frontend.registration.updated", durable=True
    # )
    # await queue_reg_updated.consume(handle_registration_updated)

    # Contract 3 — Frontend → CRM: create company
    # queue_company = await channel.declare_queue(
    #     "frontend.company.created", durable=True
    # )
    # await queue_company.consume(handle_company_created)

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


async def handle_registration(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 1 — Frontend -> CRM: new registration.

    Queue: frontend.registration.created | durable: true | US-02, US-04

    Behaviour:
    - Validate XML against schema.
    - Check if email exists in Salesforce (R2 conflict handling: log only).
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
        existing_contact = await get_contact_by_email(sf, email)

        if existing_contact:
            logger.warning("Conflict: Registration for existing email %s", email)
            # R2 requires us to only log the conflict, but not propagate a new user confirmation.
            await message.ack()
            return

        # Prepare payload for Salesforce
        contact_data = {
            "FirstName": xml.findtext("firstName"),
            "LastName": xml.findtext("lastName"),
            "Email": email,
            "Role__c": xml.findtext("role"),
            "GDPR_Consent__c": xml.findtext("gdprConsent") == "true",
            "Registration_ID__c": xml.findtext("registrationId"),
        }

        phone = xml.findtext("phone")
        if phone:
            contact_data["Phone"] = phone

        # Company mapping could go to Account, but Contract 1 payload allows string
        # Not creating an Account here as per Contract 1 (only User flow)

        logger.info("Creating new Salesforce Contact for %s", email)
        contact = await create_contact(sf, contact_data)

        # Publish crm.user.confirmed
        user_data = {
            "id": contact["CRM_ID__c"],
            "email": contact["Email"],
            "firstName": contact["FirstName"],
            "lastName": contact["LastName"],
            "role": contact["Role__c"],
            "isActive": True,  # Assuming active immediately
            "gdprConsent": contact["GDPR_Consent__c"],
            "confirmedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        if contact.get("Phone"):
            user_data["phone"] = contact["Phone"]

        await sender.publish_user_confirmed(user_data)
        logger.info("Published crm.user.confirmed for %s", email)
        await message.ack()

    except Exception as exc:  # noqa: BLE001
        logger.error("Registration — error processing message: %s", exc)
        await message.reject(requeue=True)
