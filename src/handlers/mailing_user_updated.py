"""Handler for Contract 28 — Mailing → CRM: update an existing Mailing-linked user.

Queue: crm.mailing.user.updated (routing key: mailing.user.updated)
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
from src.handlers._mailing_helpers import (
    _build_mailing_user_conflict_data,
    _get_mailing_last_name_for_contact,
)
from src.salesforce.contacts import (
    _build_updated_user_data,
    _build_user_deactivation_data,
    _get_contact_is_active,
)
from src.salesforce_client import (
    apply_is_active,
    deactivate_contact_record,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    update_mailing_contact,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 28 — Mailing -> CRM: update an existing Mailing-linked user.

    The `<id>` field carries the CRM master UUID (received by Mailing in
    crm.user.confirmed), not the original native Mailing ID. CRM resolves the
    Contact by `CRM_ID__c`; Mailing_ID__c remains on the Contact as a
    provenance record from the create flow.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by CRM_ID__c.
    - Raise MissingDependencyError on unknown CRM identities (TTL-DLX deferral).
    - Ack ambiguous CRM identities without retry.
    - Publish crm.user.conflict on email collisions.
    - If `isActive=false`, soft-delete the Contact and publish crm.user.deactivated.
    - Otherwise update Mailing-owned fields authoritatively in Salesforce,
      reactivate when needed, and publish crm.user.updated.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email") or ""
    crm_id = xml.findtext("id") or ""
    is_active = xml.findtext("isActive") in ("true", "1")

    crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
    if crm_match_status == "none":
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if crm_match_status == "ambiguous":
        logger.warning(
            "MailingUserUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
            crm_id,
        )
        await message.ack()
        return

    email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
    if email_match_status == "ambiguous":
        logger.warning(
            "MailingUserUpdated conflict — email %s is ambiguous in Salesforce",
            email,
        )
        await sender.publish_user_conflict(
            _build_mailing_user_conflict_data(email, existing_contact, xml)
        )
        await message.ack()
        return

    if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
        logger.warning(
            "MailingUserUpdated conflict — email %s already linked to another Contact",
            email,
        )
        await sender.publish_user_conflict(
            _build_mailing_user_conflict_data(email, existing_by_email, xml)
        )
        await message.ack()
        return

    contact = existing_contact
    if not is_active:
        contact = await deactivate_contact_record(
            sf, contact, log_value=f"CRM_ID__c {crm_id}",
        )
        deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at),
        )
        logger.info(
            "Published crm.user.deactivated for Mailing user %s (isActive=false on update)",
            email,
        )
        await message.ack()
        return

    contact = await update_mailing_contact(
        sf,
        contact,
        email=email,
        first_name=_normalize_optional_text(xml.findtext("firstName")),
        last_name=_get_mailing_last_name_for_contact(xml),
        company_id=_normalize_optional_text(xml.findtext("companyId")),
    )
    if not _get_contact_is_active(contact):
        reactivation_update = await apply_is_active(sf, {}, True)
        if reactivation_update:
            contact_id = contact["Id"]
            await asyncio.to_thread(sf.Contact.update, contact_id, reactivation_update)
            contact = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info(
                "Reactivated Contact %s for Mailing user %s (isActive=true on update)",
                contact_id,
                email,
            )
    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for Mailing user %s", email)
    await message.ack()
