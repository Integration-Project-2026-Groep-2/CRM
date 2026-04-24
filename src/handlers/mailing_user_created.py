"""Handler for Contract 27 — Mailing → CRM: create or attach a Mailing user identity.

Queue: crm.mailing.user.created (routing key: mailing.user.created)
Exchange: user.topic | durable: true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import _normalize_email_for_compare, _normalize_optional_text
from src.handlers._mailing_helpers import (
    _build_mailing_contact_data,
    _build_mailing_user_conflict_data,
    _get_mailing_backfill_kwargs,
    _mailing_user_has_conflicting_data,
)
from src.handlers._transport import _handle_processing_error
from src.salesforce.contacts import _build_user_data, _build_user_deactivation_data
from src.salesforce_client import (
    apply_is_active,
    backfill_mailing_contact_fields,
    create_contact,
    deactivate_contact_record,
    ensure_contact_identifiers,
    get_contact_match_by_email,
    get_contact_match_by_mailing_id,
    has_contact_mailing_id_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 27 — Mailing -> CRM: create or attach a Mailing user identity.

    Behaviour:
    - Validate XML against schema.
    - Mirror Mailing's `isActive` flag to the Contact active field.
    - Create a new Contact when the email and Mailing ID are new.
    - Reuse a unique existing Contact when the Mailing payload is idempotent.
    - When `isActive=false` reaches an existing Contact, deactivate it and
      publish crm.user.deactivated instead of crm.user.confirmed.
    - Publish crm.user.conflict when the email already exists with conflicting data.
    - Invalid XML: rejected without requeue.
    - Ambiguous Contacts: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("MailingUserCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email") or ""
        mailing_id = xml.findtext("id") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        if not await has_contact_mailing_id_field(sf):
            logger.error(
                "MailingUserCreated rejected — Salesforce Contact field Mailing_ID__c is missing",
            )
            await message.reject(requeue=False)
            return

        email_match_status, existing_by_email = await get_contact_match_by_email(sf, email)
        mailing_match_status, existing_by_mailing_id = await get_contact_match_by_mailing_id(sf, mailing_id)

        if mailing_match_status == "ambiguous":
            logger.warning(
                "MailingUserCreated ignored — ambiguous Mailing_ID__c %s in Salesforce",
                mailing_id,
            )
            await message.ack()
            return

        if mailing_match_status == "unique":
            existing_contact = existing_by_mailing_id
        else:
            if email_match_status == "ambiguous":
                logger.warning(
                    "MailingUserCreated ignored — ambiguous email %s in Salesforce",
                    email,
                )
                await message.ack()
                return

            if email_match_status == "none":
                contact_data = _build_mailing_contact_data(xml)
                if not is_active:
                    contact_data = await apply_is_active(sf, contact_data, False)
                contact = await create_contact(sf, contact_data)
                await sender.publish_user_confirmed(_build_user_data(contact))
                logger.info(
                    "Published crm.user.confirmed for new Mailing user %s (isActive=%s)",
                    email,
                    is_active,
                )
                await message.ack()
                return

            existing_contact = existing_by_email

        if email_match_status == "none":
            existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
            incoming_email = _normalize_email_for_compare(email)
            if existing_email != incoming_email:
                logger.warning(
                    "MailingUserCreated conflict — Mailing ID %s already linked to email %s",
                    mailing_id,
                    existing_contact.get("Email"),
                )
                await sender.publish_user_conflict(
                    _build_mailing_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

        if mailing_match_status == "unique":
            existing_email = _normalize_email_for_compare(existing_contact.get("Email"))
            incoming_email = _normalize_email_for_compare(email)
            if existing_email is not None and existing_email != incoming_email:
                logger.warning(
                    "MailingUserCreated conflict — Mailing ID %s already linked to email %s",
                    mailing_id,
                    existing_contact.get("Email"),
                )
                await sender.publish_user_conflict(
                    _build_mailing_user_conflict_data(email, existing_contact, xml)
                )
                await message.ack()
                return

        if email_match_status == "unique" and existing_by_email["Id"] != existing_contact["Id"]:
            logger.warning(
                "MailingUserCreated conflict — email %s and Mailing ID %s point to different Contacts",
                email,
                mailing_id,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        existing_mailing_id = _normalize_optional_text(existing_contact.get("Mailing_ID__c"))

        if existing_mailing_id is not None and existing_mailing_id != mailing_id:
            logger.warning(
                "MailingUserCreated conflict — email %s already linked to Mailing ID %s",
                email,
                existing_mailing_id,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if _mailing_user_has_conflicting_data(existing_contact, xml):
            logger.warning(
                "MailingUserCreated conflict — email %s already exists with differing data",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        if existing_contact.get("GDPR_Consent__c") is False:
            logger.warning(
                "MailingUserCreated conflict — email %s already has explicit GDPR opt-out",
                email,
            )
            await sender.publish_user_conflict(
                _build_mailing_user_conflict_data(email, existing_contact, xml)
            )
            await message.ack()
            return

        contact = await ensure_contact_identifiers(
            sf,
            existing_contact,
            mailing_id=mailing_id,
        )
        contact = await backfill_mailing_contact_fields(
            sf,
            contact,
            **_get_mailing_backfill_kwargs(contact, xml),
        )
        if not is_active:
            contact = await deactivate_contact_record(
                sf, contact, log_value=f"Mailing_ID__c {mailing_id}",
            )
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await sender.publish_user_deactivated(
                _build_user_deactivation_data(contact, deactivated_at),
            )
            logger.info(
                "Published crm.user.deactivated for existing Mailing user %s (isActive=false on create)",
                email,
            )
            await message.ack()
            return
        await sender.publish_user_confirmed(_build_user_data(contact))
        logger.info("Published crm.user.confirmed for existing Mailing user %s", email)
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("MailingUserCreated", message, exc)
