"""Handler for Contract 24 — Facturatie → CRM: manually created user.

Queue: crm.facturatie.user.created (routing key: facturatie.user.created)
Exchange: user.topic | durable: true
"""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.salesforce.contacts import _build_user_data, _build_user_deactivation_data
from src.salesforce_client import (
    apply_is_active,
    create_contact,
    deactivate_contact_record,
    ensure_contact_identifiers,
    get_contact_match_by_email,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 24 — Facturatie -> CRM: manually created user.

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
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

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
    logger.info(
        "FacturatieUserCreated inbound for %s: companyId=%s",
        email,
        "present" if company_id else f"empty-or-missing(raw={company_id!r})",
    )
    if company_id:
        contact_data["Company_ID__c"] = company_id

    if not is_active:
        contact_data = await apply_is_active(sf, contact_data, False)
    contact = await create_contact(sf, contact_data)
    await sender.publish_user_confirmed(_build_user_data(contact))
    logger.info(
        "Published crm.user.confirmed for new Facturatie user %s (isActive=%s, sf_Company_ID__c=%s)",
        email,
        is_active,
        contact.get("Company_ID__c") or "MISSING",
    )
    await message.ack()
