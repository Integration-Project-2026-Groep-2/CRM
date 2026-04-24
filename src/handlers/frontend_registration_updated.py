"""Handler for Contract 2 — Frontend → CRM: registration update/cancellation.

Queue: crm.frontend.registration.updated (routing key: frontend.registration.updated)
Exchange: user.topic | durable: true | US-21 (R1), US-33 (R2)
"""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika
from lxml import etree

from src import sender, xml_validator
from src.handlers._helpers import (
    _build_company_deactivation_data,
    _contact_has_native_identity,
    _normalize_optional_text,
)
from src.handlers._transport import _handle_processing_error
from src.salesforce.contacts import _build_updated_user_data, _build_user_deactivation_data
from src.salesforce_client import (
    count_active_contacts_for_company,
    count_active_session_registrations,
    deactivate_account_by_crm_id,
    deactivate_contact_record,
    deactivate_session_registration,
    ensure_session_registration_active,
    get_account_by_crm_id,
    get_contact_by_email,
    has_session_registration_object,
    upsert_contact_by_email,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 2 — Frontend -> CRM: registration update or cancellation.

    Behaviour:
    - Validate XML against schema (<RegistrationChange>).
    - Branch on changeType:
        updated   → Upsert Contact in Salesforce, keep the session registration active,
                    publish crm.user.updated (C18).
        cancelled → Soft-delete the session registration first; only soft-delete the
                    Contact and publish crm.user.deactivated (C22) when no active
                    registrations remain.
    - Contact removal remains soft delete only — never physically remove (GDPR).
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("RegistrationChange — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        email = xml.findtext("email")
        session_id = xml.findtext("sessionId")
        change_type = xml.findtext("changeType")

        logger.info(
            "Processing registration change: email=%s, sessionId=%s, changeType=%s",
            email, session_id, change_type,
        )

        if change_type == "updated":
            await _handle_update(xml, email, sf, message)
        elif change_type == "cancelled":
            await _handle_cancellation(xml, email, sf, message)
        else:
            # XSD validation should prevent this, but defence-in-depth
            logger.error("Unknown changeType '%s' for email %s — rejecting", change_type, email)
            await message.reject(requeue=False)

    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("RegistrationChange", message, exc)


async def _handle_update(
    xml: etree._Element, email: str, sf: "Salesforce", message: aio_pika.IncomingMessage
) -> None:
    """Process changeType=updated: upsert Contact and publish crm.user.updated."""
    if not await has_session_registration_object(sf):
        logger.error(
            "RegistrationChange updated rejected — Salesforce object Session_Registration__c is missing",
        )
        await message.reject(requeue=False)
        return

    update_data: dict = {}

    updated_fields = xml.find("updatedFields")
    if updated_fields is not None:
        field_mapping = {
            "firstName": "FirstName",
            "lastName": "LastName",
            "email": "Email",
            "phone": "Phone",
            "role": "Role__c",
            # company mapping deferred to Contract 3
        }
        for xml_field, sf_field in field_mapping.items():
            value = updated_fields.findtext(xml_field)
            if value is not None:
                update_data[sf_field] = value

    contact = await upsert_contact_by_email(sf, email, update_data)
    registration_id = _normalize_optional_text(xml.findtext("registrationId"))
    session_id = xml.findtext("sessionId") or ""
    await ensure_session_registration_active(
        sf,
        contact_id=contact["Id"],
        session_id=session_id,
        registration_id=registration_id,
    )

    await sender.publish_user_updated(_build_updated_user_data(contact))
    logger.info("Published crm.user.updated for %s", email)
    await message.ack()


async def _handle_cancellation(
    xml: etree._Element,
    email: str,
    sf: "Salesforce",
    message: aio_pika.IncomingMessage,
) -> None:
    """Process changeType=cancelled: deactivate registration, maybe Contact."""
    if not await has_session_registration_object(sf):
        logger.error(
            "RegistrationChange cancelled rejected — Salesforce object Session_Registration__c is missing",
        )
        await message.reject(requeue=False)
        return

    session_id = xml.findtext("sessionId") or ""
    registration_id = _normalize_optional_text(xml.findtext("registrationId"))
    contact = await get_contact_by_email(sf, email)

    if contact is None:
        # Contact doesn't exist — nothing to deactivate. Ack to prevent infinite requeue.
        logger.warning("Cancellation for unknown email %s — acking without action", email)
        await message.ack()
        return

    deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    session_registration = await deactivate_session_registration(
        sf,
        registration_id=registration_id,
        contact_id=contact["Id"],
        session_id=session_id,
    )
    if session_registration is None:
        remaining_registrations = await count_active_session_registrations(sf, contact["Id"])
        native_identity = _contact_has_native_identity(contact)
        if remaining_registrations > 0 or native_identity:
            logger.warning(
                "Cancellation for %s session %s has no Session_Registration__c row — skipping legacy Contact fallback; remaining_active_registrations=%s native_identity=%s",
                email,
                session_id,
                remaining_registrations,
                native_identity,
            )
            await message.ack()
            return

        logger.warning(
            "Cancellation for %s session %s has no Session_Registration__c row — using legacy Contact fallback",
            email,
            session_id,
        )
    else:
        remaining_registrations = await count_active_session_registrations(sf, contact["Id"])
        native_identity = _contact_has_native_identity(contact)
        if remaining_registrations > 0 or native_identity:
            logger.info(
                "Cancelled registration for %s without deactivating Contact; remaining_active_registrations=%s native_identity=%s",
                email,
                remaining_registrations,
                native_identity,
            )
            await message.ack()
            return

    contact = await deactivate_contact_record(sf, contact, log_value=email)
    deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    deactivation_data = _build_user_deactivation_data(contact, deactivated_at)
    await sender.publish_user_deactivated(deactivation_data)
    logger.info("Published crm.user.deactivated for %s", email)

    company_id = _normalize_optional_text(contact.get("Company_ID__c"))
    if company_id is not None:
        # C1 fix — sibling guard: only deactivate the Account when this is
        # the last active Contact on the company. Multiple contacts can share
        # the same Company_ID__c; one cancellation must not wipe the whole
        # company link.
        #
        # C2 fix — pre-validate: look up the Account *before* mutating SF so
        # we never soft-delete an Account that lacks VAT_Number__c (which
        # would prevent Contract 23 from firing → split-brain).
        #
        # H2 fix — wrap in try/except: if this block raises after
        # publish_user_deactivated already fired, we still ack the message
        # to prevent Contract 22 re-fire on redelivery. The company
        # reconciliation can happen separately.
        try:
            sibling_count = await count_active_contacts_for_company(sf, company_id)
            if sibling_count > 0:
                logger.info(
                    "Skipping Account deactivation for Company_ID__c %s — %d other active contact(s) remain",
                    company_id,
                    sibling_count,
                )
            else:
                account = await get_account_by_crm_id(sf, company_id)
                if account is None:
                    logger.warning(
                        "Contact %s is linked to Company_ID__c %s, but Account was not found",
                        email,
                        company_id,
                    )
                elif not account.get("VAT_Number__c"):
                    logger.warning(
                        "Account %s has no VAT_Number__c; skipping deactivation to avoid split-brain (Contract 23 cannot fire)",
                        company_id,
                    )
                else:
                    account = await deactivate_account_by_crm_id(sf, company_id)
                    if account is not None:
                        company_deactivation_data = _build_company_deactivation_data(account, deactivated_at)
                        await sender.publish_company_deactivated(company_deactivation_data)
                        logger.info(
                            "Published crm.company.deactivated for company CRM_ID__c %s",
                            company_id,
                        )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Company deactivation failed for Company_ID__c %s after user deactivation was already published; acking to prevent Contract 22 re-fire",
                company_id,
            )

    await message.ack()
