"""Handler for Kassa → CRM: deactivate an existing Kassa-linked user.

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
    get_contact_match_by_kassa_id,
    has_contact_kassa_id_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 38 — Kassa -> CRM: deactivate an existing Kassa-linked user."""
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    if not await has_contact_kassa_id_field(sf):
        logger.error(
            "KassaUserDeactivated rejected — Salesforce Contact field Kassa_ID__c is missing",
        )
        await message.reject(requeue=False)
        return

    kassa_id = xml.findtext("id") or ""
    email = xml.findtext("email") or ""
    deactivated_at = xml.findtext("deactivatedAt") or ""

    match_status, existing_contact = await get_contact_match_by_kassa_id(sf, kassa_id)
    if match_status == "none":
        raise MissingDependencyError("Kassa_ID__c", kassa_id)

    if match_status == "ambiguous":
        logger.warning(
            "KassaUserDeactivated ignored — ambiguous Kassa_ID__c %s in Salesforce",
            kassa_id,
        )
        await message.ack()
        return

    existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
    incoming_email = _normalize_email_for_compare(email)
    if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
        logger.warning(
            "KassaUserDeactivated email mismatch — Kassa_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
            kassa_id,
            existing_contact.get("Email"),
            email,
        )

    contact = await deactivate_contact_record(
        sf,
        existing_contact,
        log_value=f"Kassa_ID__c {kassa_id}",
    )
    await sender.publish_user_deactivated(
        _build_user_deactivation_data(contact, deactivated_at)
    )
    logger.info("Published crm.user.deactivated for Kassa_ID__c %s", kassa_id)
    await message.ack()
