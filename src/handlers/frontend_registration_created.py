"""Handler for Contract 1 — Frontend → CRM: new registration.

Queue: crm.frontend.registration.created (routing key: frontend.registration.created)
Exchange: user.topic | durable: true | US-02, US-04, US-05, US-19
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import (
    _build_mail_display_name,
    _build_registration_conflict_data,
    _normalize_registration_role,
    _registration_fields_are_compatible,
)
from src.salesforce.contacts import _build_user_data
from src.salesforce_client import (
    create_contact,
    ensure_contact_identifiers,
    get_contact_by_email,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)
async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 1 — Frontend -> CRM: new registration.

    Behaviour:
    - Validate XML against schema.
    - Check if email exists in Salesforce (R1 scope: log only. R2 adds C15 publish).
    - If new, create Contact in Salesforce mapping fields.
    - Publish crm.user.confirmed via sender.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("Registration — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email")
    registration_id = xml.findtext("registrationId") or ""

    gdpr_text = xml.findtext("gdprConsent")
    if gdpr_text not in ("true", "1"):
        logger.warning("Registration refused — gdprConsent=%s for email %s", gdpr_text, email)
        await message.reject(requeue=False)
        return

    role = _normalize_registration_role(xml.findtext("role"))
    company = xml.findtext("company")
    if role == "COMPANY_CONTACT" and not company:
        logger.warning("COMPANY_CONTACT registration without company field for %s", email)

    existing_contact = await get_contact_by_email(sf, email)

    if existing_contact:
        if not _registration_fields_are_compatible(existing_contact, xml):
            logger.warning(
                "Conflict: email %s exists with incompatible person fields for registrationId %s",
                email,
                registration_id,
            )
            await sender.publish_user_conflict(
                _build_registration_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            registration_id=registration_id,
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
        await message.ack()
        return

    contact_data = {
        "FirstName": xml.findtext("firstName"),
        "LastName": xml.findtext("lastName"),
        "Email": email,
        "Role__c": role,
        "GDPR_Consent__c": xml.findtext("gdprConsent") in ("true", "1"),
        "Registration_ID__c": registration_id,
    }

    phone = xml.findtext("phone")
    if phone:
        contact_data["Phone"] = phone

    logger.info("Creating new Salesforce Contact for %s", email)
    contact = await create_contact(sf, contact_data)

    await sender.publish_user_confirmed(_build_user_data(contact))
    logger.info("Published crm.user.confirmed for %s", email)

    full_name = _build_mail_display_name(
        contact_data.get("FirstName"),
        contact_data.get("LastName"),
        email,
    )

    recipient = {"email": email, "name": full_name}
    dynamic_data = {"guest_name": full_name}
    await sender.publish_mail_requested("registration_confirmation", recipient, dynamic_data)
    logger.info("Published crm.mail.requested for %s", email)

    await message.ack()
