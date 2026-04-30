"""Handler for Kassa → CRM: deactivate an existing CRM-linked user.

Queue: crm.kassa.user.deactivated (routing key: kassa.user.deactivated)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import _normalize_email_for_compare
from src.salesforce.contacts import _build_user_deactivation_data
from src.salesforce_client import (
    deactivate_contact_record,
    get_contact_match_by_crm_id,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 38 — Kassa -> CRM: deactivate an existing CRM-linked user.

    The `<id>` element carries the CRM master UUID. CRM resolves the Contact by
    `CRM_ID__c` and performs a soft delete only (GDPR audit trail).
    """
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    crm_id = xml.findtext("id") or ""
    email = xml.findtext("email") or ""
    deactivated_at = xml.findtext("deactivatedAt") or ""

    match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
    if match_status == "none":
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if match_status == "ambiguous":
        logger.warning(
            "KassaUserDeactivated ignored — ambiguous CRM_ID__c %s in Salesforce",
            crm_id,
        )
        await message.ack()
        return

    existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
    incoming_email = _normalize_email_for_compare(email)
    if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
        logger.warning(
            "KassaUserDeactivated email mismatch — CRM_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
            crm_id,
            existing_contact.get("Email"),
            email,
        )

    contact = await deactivate_contact_record(
        sf,
        existing_contact,
        log_value=f"CRM_ID__c {crm_id}",
    )
    await sender.publish_user_deactivated(
        _build_user_deactivation_data(contact, deactivated_at)
    )
    logger.info("Published crm.user.deactivated for Kassa CRM_ID__c %s", crm_id)
    await message.ack()
