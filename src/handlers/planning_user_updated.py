"""Handler for Contract 31 — Planning → CRM: update an existing Planning-linked user.

Queue: crm.planning.user.updated (routing key: planning.user.updated)
Exchange: user.topic | durable: true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import _normalize_optional_text
from src.handlers._planning_helpers import _build_planning_user_conflict_data
from src.salesforce.contacts import (
    _build_updated_user_data,
    _build_user_deactivation_data,
    _get_contact_is_active,
)
from src.salesforce_client import (
    apply_is_active,
    deactivate_contact_record,
    ensure_contact_identifiers,
    get_contact_match_by_email,
    get_contact_match_by_planning_id,
    has_contact_planning_id_field,
    update_planning_contact,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 31 — Planning -> CRM: update an existing Planning-linked user.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by Planning_ID__c.
    - Raise MissingDependencyError on unknown Planning identities (TTL-DLX deferral).
    - Ack ambiguous Planning identities without retry.
    - Publish crm.user.conflict on email collisions.
    - If `isActive=false`, soft-delete the Contact and publish crm.user.deactivated.
    - Otherwise update Planning-owned fields authoritatively in Salesforce,
      reactivate when needed, and publish crm.user.updated.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email") or ""
    planning_id = xml.findtext("id") or ""
    is_active = xml.findtext("isActive") in ("true", "1")

    if not await has_contact_planning_id_field(sf):
        logger.error(
            "PlanningUserUpdated rejected — Salesforce Contact field Planning_ID__c is missing",
        )
        await message.reject(requeue=False)
        return

    planning_match_status, existing_contact = await get_contact_match_by_planning_id(sf, planning_id)
    if planning_match_status == "none":
        raise MissingDependencyError("Planning_ID__c", planning_id)

    if planning_match_status == "ambiguous":
        logger.warning(
            "PlanningUserUpdated ignored — ambiguous Planning_ID__c %s in Salesforce",
            planning_id,
        )
        await message.ack()
        return

    email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
    if email_match_status == "ambiguous":
        logger.warning(
            "PlanningUserUpdated conflict — email %s is ambiguous in Salesforce",
            email,
        )
        await sender.publish_user_conflict(
            _build_planning_user_conflict_data(email, existing_contact, xml)
        )
        await message.ack()
        return

    if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
        logger.warning(
            "PlanningUserUpdated conflict — email %s already linked to another Contact",
            email,
        )
        await sender.publish_user_conflict(
            _build_planning_user_conflict_data(email, existing_by_email, xml)
        )
        await message.ack()
        return

    contact = await ensure_contact_identifiers(
        sf,
        existing_contact,
        planning_id=planning_id,
    )
    if not is_active:
        contact = await deactivate_contact_record(
            sf, contact, log_value=f"Planning_ID__c {planning_id}",
        )
        deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at),
        )
        logger.info(
            "Published crm.user.deactivated for Planning user %s (isActive=false on update)",
            email,
        )
        await message.ack()
        return

    contact = await update_planning_contact(
        sf,
        contact,
        email=email,
        first_name=xml.findtext("firstName") or "",
        last_name=xml.findtext("lastName") or "",
        role=xml.findtext("role") or "VISITOR",
        phone_number=_normalize_optional_text(xml.findtext("phoneNumber")),
    )
    if not _get_contact_is_active(contact):
        reactivation_update = await apply_is_active(sf, {}, True)
        if reactivation_update:
            contact_id = contact["Id"]
            await asyncio.to_thread(sf.Contact.update, contact_id, reactivation_update)
            contact = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info(
                "Reactivated Contact %s for Planning user %s (isActive=true on update)",
                contact_id,
                email,
            )
    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for Planning user %s", email)
    await message.ack()
