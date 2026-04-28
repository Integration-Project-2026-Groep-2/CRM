"""Handler for Contract 32 — Planning → CRM: deactivate an existing Planning-linked user.

Queue: crm.planning.user.deactivated (routing key: planning.user.deactivated)
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
    ensure_contact_identifiers,
    get_contact_match_by_planning_id,
    has_contact_planning_id_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 32 — Planning -> CRM: deactivate an existing Planning-linked user.

    Behaviour:
    - Validate XML against schema.
    - Require Salesforce support for Planning_ID__c.
    - Resolve the Contact strictly by Planning_ID__c.
    - Raise MissingDependencyError on unknown Planning identities (TTL-DLX deferral).
    - Ack ambiguous Planning identities without retry.
    - Trust Planning_ID__c over a stale payload email, but log the mismatch.
    - Perform a soft delete only and publish crm.user.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PlanningUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email") or ""
    planning_id = xml.findtext("id") or ""
    deactivated_at = xml.findtext("deactivatedAt") or ""

    if not await has_contact_planning_id_field(sf):
        logger.error(
            "PlanningUserDeactivated rejected — Salesforce Contact field Planning_ID__c is missing",
        )
        await message.reject(requeue=False)
        return

    planning_match_status, existing_contact = await get_contact_match_by_planning_id(sf, planning_id)
    if planning_match_status == "none":
        raise MissingDependencyError("Planning_ID__c", planning_id)

    if planning_match_status == "ambiguous":
        logger.warning(
            "PlanningUserDeactivated ignored — ambiguous Planning_ID__c %s in Salesforce",
            planning_id,
        )
        await message.ack()
        return

    contact = await ensure_contact_identifiers(
        sf,
        existing_contact,
        planning_id=planning_id,
    )

    existing_email = _normalize_email_for_compare(contact.get("Email"))
    incoming_email = _normalize_email_for_compare(email)
    if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
        logger.warning(
            "PlanningUserDeactivated email mismatch — Planning_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
            planning_id,
            contact.get("Email"),
            email,
        )

    contact = await deactivate_contact_record(
        sf,
        contact,
        log_value=f"Planning_ID__c {planning_id}",
    )
    await sender.publish_user_deactivated(
        _build_user_deactivation_data(contact, deactivated_at)
    )
    logger.info("Published crm.user.deactivated for Planning_ID__c %s", planning_id)
    await message.ack()
