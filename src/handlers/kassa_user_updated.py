"""Handler for Kassa → CRM: update an existing Kassa-linked user.

Queue: crm.kassa.user.updated (routing key: kassa.user.updated)
Exchange: user.topic | durable: true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import (
    _build_conflict_value,
    _normalize_optional_text,
)
from src.salesforce.contacts import (
    _build_updated_user_data,
    _get_contact_is_active,
)
from src.salesforce_client import (
    apply_is_active,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    has_contact_kassa_id_field,
    update_kassa_contact,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 37 — Kassa -> CRM: update an existing Kassa-linked user."""
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    if not await has_contact_kassa_id_field(sf):
        logger.error(
            "KassaUserUpdated rejected — Salesforce Contact field Kassa_ID__c is missing",
        )
        await message.reject(requeue=False)
        return

    kassa_id = xml.findtext("userId") or ""
    email = xml.findtext("email") or ""
    first_name = xml.findtext("firstName") or ""
    last_name = xml.findtext("lastName") or ""
    badge_code = _normalize_optional_text(xml.findtext("badgeCode"))
    role = xml.findtext("role") or ""
    company_id = _normalize_optional_text(xml.findtext("companyId"))

    match_status, existing_contact = await get_contact_match_by_kassa_id(sf, kassa_id)
    if match_status == "none":
        raise MissingDependencyError("Kassa_ID__c", kassa_id)

    if match_status == "ambiguous":
        logger.warning(
            "KassaUserUpdated ignored — ambiguous Kassa_ID__c %s in Salesforce",
            kassa_id,
        )
        await message.ack()
        return

    email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
    if email_match_status == "ambiguous":
        logger.warning(
            "KassaUserUpdated conflict — email %s is ambiguous in Salesforce",
            email,
        )
        await sender.publish_user_conflict(
            {
                "email": email,
                "existingValue": _build_conflict_value(
                    existing_contact.get("FirstName"),
                    existing_contact.get("LastName"),
                    existing_contact.get("Company_ID__c"),
                ),
                "incomingValue": _build_conflict_value(first_name, last_name, company_id),
                "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
        await message.ack()
        return

    if email_match_status == "unique" and existing_by_email is not None and existing_by_email["Id"] != existing_contact["Id"]:
        logger.warning(
            "KassaUserUpdated conflict — email %s already linked to another Contact",
            email,
        )
        await sender.publish_user_conflict(
            {
                "email": email,
                "existingValue": _build_conflict_value(
                    existing_by_email.get("FirstName"),
                    existing_by_email.get("LastName"),
                    existing_by_email.get("Company_ID__c"),
                ),
                "incomingValue": _build_conflict_value(first_name, last_name, company_id),
                "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )
        await message.ack()
        return

    contact = await update_kassa_contact(
        sf,
        existing_contact,
        email=email,
        first_name=first_name,
        last_name=last_name,
        badge_code=badge_code,
        role=role,
        company_id=company_id,
    )

    if not _get_contact_is_active(contact):
        reactivation_update = await apply_is_active(sf, {}, True)
        if reactivation_update:
            contact_id = contact["Id"]
            await asyncio.to_thread(sf.Contact.update, contact_id, reactivation_update)
            contact = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info(
                "Reactivated Contact %s for Kassa user %s (update)",
                contact_id,
                email,
            )

    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for Kassa user %s", email)
    await message.ack()
