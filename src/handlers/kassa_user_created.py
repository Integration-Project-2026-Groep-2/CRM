"""Handler for Kassa → CRM: create or attach a Kassa user identity.

Queue: crm.kassa.user.created (routing key: kassa.user.created)
Exchange: user.topic | durable: true
"""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import (
    _build_conflict_value,
    _normalize_email_for_compare,
    _normalize_optional_text,
)
from src.salesforce.contacts import _build_user_data
from src.salesforce_client import (
    backfill_kassa_contact_fields,
    create_contact,
    ensure_contact_identifiers,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    has_contact_kassa_id_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 36 — Kassa -> CRM: create or attach a Kassa user identity."""
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    if not await has_contact_kassa_id_field(sf):
        logger.error(
            "KassaUserCreated rejected — Salesforce Contact field Kassa_ID__c is missing",
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
    if match_status == "ambiguous":
        logger.warning(
            "KassaUserCreated ignored — ambiguous Kassa_ID__c %s in Salesforce",
            kassa_id,
        )
        await message.ack()
        return

    if match_status == "unique" and existing_contact is not None:
        existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
            logger.warning(
                "KassaUserCreated conflict — Kassa ID %s already linked to email %s",
                kassa_id,
                existing_contact.get("Email"),
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

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            kassa_id=kassa_id,
        )
        contact = await backfill_kassa_contact_fields(
            sf,
            contact,
            first_name=first_name,
            last_name=last_name,
            badge_code=badge_code,
            role=role,
            company_id=company_id,
        )
        if contact.get("Kassa_ID__c") != kassa_id:
            contact = await ensure_contact_identifiers(sf, contact, kassa_id=kassa_id)
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for existing Kassa user %s", email)
        await message.ack()
        return

    email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
    if email_match_status == "ambiguous":
        logger.warning(
            "KassaUserCreated ignored — ambiguous email %s in Salesforce",
            email,
        )
        await message.ack()
        return

    if email_match_status == "unique" and existing_by_email is not None:
        existing_kassa_id = _normalize_optional_text(existing_by_email.get("Kassa_ID__c"))
        if existing_kassa_id is not None and existing_kassa_id != kassa_id:
            logger.warning(
                "KassaUserCreated conflict — email %s already linked to Kassa ID %s",
                email,
                existing_kassa_id,
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

        contact = await ensure_contact_identifiers(
            sf,
            existing_by_email,
            kassa_id=kassa_id,
        )
        contact = await backfill_kassa_contact_fields(
            sf,
            contact,
            first_name=first_name,
            last_name=last_name,
            badge_code=badge_code,
            role=role,
            company_id=company_id,
        )
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for linked Kassa user %s", email)
        await message.ack()
        return

    contact_data = {
        "FirstName": first_name,
        "LastName": last_name,
        "Email": email,
        "Role__c": role,
        "Badge_Code__c": badge_code,
        "Kassa_ID__c": kassa_id,
    }
    if company_id:
        contact_data["Company_ID__c"] = company_id
    if not contact_data.get("Role__c"):
        contact_data.pop("Role__c", None)

    if contact_data.get("Badge_Code__c") is None:
        contact_data.pop("Badge_Code__c", None)

    contact = await create_contact(sf, contact_data)

    if contact.get("Kassa_ID__c") != kassa_id:
        contact = await ensure_contact_identifiers(sf, contact, kassa_id=kassa_id)

    if contact.get("Badge_Code__c") != badge_code or contact.get("Company_ID__c") != company_id:
        contact = await backfill_kassa_contact_fields(
            sf,
            contact,
            first_name=first_name,
            last_name=last_name,
            badge_code=badge_code,
            role=role,
            company_id=company_id,
        )

    await sender.publish_user_confirmed(_build_user_data(contact))
    logger.info("Published crm.user.confirmed for new Kassa user %s", email)
    await message.ack()
