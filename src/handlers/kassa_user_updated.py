"""Handler for Kassa → CRM: update an existing CRM-linked user.

Queue: crm.kassa.user.updated (routing key: kassa.user.updated)
Exchange: user.topic | durable: true
"""

import asyncio
import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import (
    _normalize_email_for_compare,
    _normalize_optional_text,
)
from src.handlers._kassa_helpers import _build_kassa_user_conflict_data
from src.salesforce.contacts import (
    _build_updated_user_data,
    _get_contact_is_active,
)
from src.salesforce.contacts.utils import get_full_contact_record
from src.salesforce_client import (
    apply_is_active,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    update_kassa_contact,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 37 — Kassa -> CRM: update an existing CRM-linked user.

    The `<userId>` element carries the CRM master UUID (received by Kassa via
    crm.user.confirmed, Option 2 UUID strategy). CRM resolves the Contact by
    `CRM_ID__c`.

    Kassa-specific extra guard (not in Facturatie/Mailing/Planning peer-sync):
    when the resolved Contact is already owned by a frontend Registration
    (`Registration_ID__c` populated), an email change from Kassa publishes a
    user_conflict instead of overwriting — Frontend remains the registration
    owner.
    """
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    crm_id = xml.findtext("userId") or ""
    email = xml.findtext("email") or ""
    first_name = xml.findtext("firstName") or ""
    last_name = xml.findtext("lastName") or ""
    badge_code = _normalize_optional_text(xml.findtext("badgeCode"))
    role = xml.findtext("role") or ""
    company_id = _normalize_optional_text(xml.findtext("companyId"))

    match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
    if match_status == "none":
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if match_status == "ambiguous":
        logger.warning(
            "KassaUserUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
            crm_id,
        )
        await message.ack()
        return

    existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
    incoming_email = _normalize_email_for_compare(email)
    has_registration_owner = _normalize_optional_text(existing_contact.get("Registration_ID__c")) is not None
    if has_registration_owner and existing_email is not None and incoming_email is not None and existing_email != incoming_email:
        logger.warning(
            "KassaUserUpdated conflict — Contact %s is owned by Registration_ID__c %s and email would change from %s to %s",
            existing_contact.get("Id"),
            existing_contact.get("Registration_ID__c"),
            existing_contact.get("Email"),
            email,
        )
        await sender.publish_user_conflict(
            _build_kassa_user_conflict_data(
                email, existing_contact, first_name, last_name, company_id
            )
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
            _build_kassa_user_conflict_data(
                email, existing_contact, first_name, last_name, company_id
            )
        )
        await message.ack()
        return

    if email_match_status == "unique" and existing_by_email is not None and existing_by_email["Id"] != existing_contact["Id"]:
        logger.warning(
            "KassaUserUpdated conflict — email %s already linked to another Contact",
            email,
        )
        await sender.publish_user_conflict(
            _build_kassa_user_conflict_data(
                email, existing_by_email, first_name, last_name, company_id
            )
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
            contact = await get_full_contact_record(sf, contact_id)
            logger.warning(
                "Reactivated Contact %s for Kassa user %s (update) after previous soft delete/inactive state",
                contact_id,
                email,
            )

    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for Kassa user %s", email)
    await message.ack()
