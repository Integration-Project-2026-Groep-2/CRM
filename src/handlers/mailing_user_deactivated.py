"""Handler for Contract 29 — Mailing → CRM: deactivate an existing Mailing-linked user.

Queue: crm.mailing.user.deactivated (routing key: mailing.user.deactivated)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import _normalize_email_for_compare
from src.handlers._transport import _handle_out_of_order_deferral, _handle_processing_error
from src.salesforce.contacts import _build_user_deactivation_data
from src.salesforce_client import (
    deactivate_contact_record,
    get_contact_match_by_crm_id,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 29 — Mailing -> CRM: deactivate an existing Mailing-linked user.

    The `<id>` field carries the CRM master UUID (received by Mailing in
    crm.user.confirmed), not the original native Mailing ID. CRM resolves the
    Contact by `CRM_ID__c`.

    Behaviour:
    - Validate XML against schema.
    - Resolve the Contact strictly by CRM_ID__c.
    - Requeue unknown CRM identities so out-of-order create/deactivate can recover.
    - Ack ambiguous CRM identities without retry.
    - Trust CRM_ID__c over a stale payload email, but log the mismatch.
    - Perform a soft delete only and publish crm.user.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        crm_id = xml.findtext("id") or ""
        deactivated_at = xml.findtext("deactivatedAt") or ""

        crm_match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "MailingUserDeactivated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "MailingUserDeactivated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        contact = existing_contact

        existing_email = _normalize_email_for_compare(contact.get("Email"))
        incoming_email = _normalize_email_for_compare(email)
        if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
            logger.warning(
                "MailingUserDeactivated email mismatch — CRM_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
                crm_id,
                contact.get("Email"),
                email,
            )

        contact = await deactivate_contact_record(
            sf,
            contact,
            log_value=f"CRM_ID__c {crm_id}",
        )
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at)
        )
        logger.info("Published crm.user.deactivated for CRM_ID__c %s", crm_id)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("MailingUserDeactivated", message, exc)
