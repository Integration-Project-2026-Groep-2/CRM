"""Handler for Contract 30 — Planning → CRM: create/attach a Planning user identity.

Queue: crm.planning.user.created (routing key: planning.user.created)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import _normalize_email_for_compare, _normalize_optional_text
from src.handlers._planning_helpers import (
    _build_planning_contact_data,
    _build_planning_user_conflict_data,
    _planning_user_has_conflicting_data,
)
from src.salesforce.contacts import _build_user_data
from src.salesforce_client import (
    backfill_planning_contact_fields,
    create_contact,
    ensure_contact_identifiers,
    get_contact_match_by_email,
    get_contact_match_by_planning_id,
    has_contact_planning_id_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 30 — Planning -> CRM: create or attach a Planning user identity.

    Behaviour:
    - Validate XML against schema.
    - Reject users without GDPR consent.
    - Resolve users primarily by Planning_ID__c (stable producer identifier).
    - Only use email as a secondary bootstrap key for safe first-time linking.
    - Persist Planning_ID__c on Contact for future idempotent matching.
    - Publish crm.user.confirmed or crm.user.conflict as needed.
    - Invalid XML: rejected without requeue.
    - Ambiguous Contacts: ack without retry.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email") or ""
    planning_id = xml.findtext("id") or ""
    gdpr_text = xml.findtext("gdprConsent")
    if gdpr_text not in ("true", "1"):
        logger.warning(
            "PlanningUserCreated refused — gdprConsent=%s for email %s",
            gdpr_text,
            email,
        )
        await message.reject(requeue=False)
        return

    if not await has_contact_planning_id_field(sf):
        logger.error(
            "PlanningUserCreated rejected — Salesforce Contact field Planning_ID__c is missing",
        )
        await message.reject(requeue=False)
        return

    planning_match_status, existing_by_planning_id = await get_contact_match_by_planning_id(sf, planning_id)
    if planning_match_status == "ambiguous":
        logger.warning(
            "PlanningUserCreated ignored — ambiguous Planning_ID__c %s in Salesforce",
            planning_id,
        )
        await message.ack()
        return

    if planning_match_status == "unique":
        existing_contact = existing_by_planning_id

        existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and existing_email != incoming_email:
            logger.warning(
                "PlanningUserCreated conflict — Planning ID %s already linked to email %s",
                planning_id,
                existing_contact.get("Email"),
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if existing_contact.get("GDPR_Consent__c") is False:
            logger.warning(
                "PlanningUserCreated conflict — email %s already has explicit GDPR opt-out",
                email,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if _planning_user_has_conflicting_data(existing_contact, xml):
            logger.warning(
                "PlanningUserCreated conflict — Planning ID %s exists with conflicting profile data",
                planning_id,
            )
            await sender.publish_user_conflict(
                _build_planning_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            planning_id=planning_id,
        )
        contact = await backfill_planning_contact_fields(
            sf,
            contact,
            first_name=xml.findtext("firstName") or "",
            last_name=xml.findtext("lastName") or "",
            role=xml.findtext("role") or "VISITOR",
            phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
            gdpr_consent=True,
        )
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for existing Planning user %s", email)
        await message.ack()
        return

    email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
    if email_match_status == "ambiguous":
        logger.warning(
            "PlanningUserCreated ignored — ambiguous email %s in Salesforce",
            email,
        )
        await message.ack()
        return

    if email_match_status == "none":
        contact = await create_contact(sf, _build_planning_contact_data(xml))
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for new Planning user %s", email)
        await message.ack()
        return

    existing_contact = existing_by_email
    existing_planning_id = _normalize_optional_text(existing_contact.get("Planning_ID__c"))
    if existing_planning_id is not None and existing_planning_id != planning_id:
        logger.warning(
            "PlanningUserCreated conflict — email %s already linked to Planning ID %s",
            email,
            existing_planning_id,
        )
        await sender.publish_user_conflict(
            _build_planning_user_conflict_data(email, existing_contact, xml)
        )
        await message.ack()
        return

    if existing_contact.get("GDPR_Consent__c") is False:
        logger.warning(
            "PlanningUserCreated conflict — email %s already has explicit GDPR opt-out",
            email,
        )
        await sender.publish_user_conflict(
            _build_planning_user_conflict_data(email, existing_contact, xml)
        )
        await message.ack()
        return

    if _planning_user_has_conflicting_data(existing_contact, xml):
        logger.warning(
            "PlanningUserCreated conflict — email %s already exists with differing data",
            email,
        )
        await sender.publish_user_conflict(
            _build_planning_user_conflict_data(email, existing_contact, xml)
        )
        await message.ack()
        return

    contact = await ensure_contact_identifiers(
        sf,
        existing_contact,
        planning_id=planning_id,
    )
    contact = await backfill_planning_contact_fields(
        sf,
        contact,
        first_name=xml.findtext("firstName") or "",
        last_name=xml.findtext("lastName") or "",
        role=xml.findtext("role") or "VISITOR",
        phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
        gdpr_consent=True,
    )
    await sender.publish_user_confirmed(_build_user_data(contact))
    logger.info("Published crm.user.confirmed for linked Planning user %s", email)
    await message.ack()
