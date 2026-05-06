"""Handler for Contract 2 — Frontend → CRM: registration update/cancellation.

Queue: crm.frontend.registration.updated (routing key: frontend.registration.updated)
Exchange: user.topic | durable: true | US-21 (R1), US-33 (R2)

Scope: Contact-only mutations. Sessie-deelname (welke gebruiker zit in welke
sessie) is **Planning's domein** — Frontend publiceert sessiekeuze rechtstreeks
naar Planning. CRM ontvangt mailing-filtering via C11 ``participantIds``.
``Session_Registration__c`` junction wordt door deze handler niet meer geschreven.
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
    _normalize_registration_role,
)
from src.salesforce.contacts import _build_updated_user_data, _build_user_deactivation_data
from src.salesforce_client import (
    count_active_contacts_for_company,
    deactivate_account_by_crm_id,
    deactivate_contact_record,
    get_account_by_crm_id,
    get_contact_by_email,
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
        updated   → Upsert Contact in Salesforce, publish crm.user.updated (C18).
        cancelled → Soft-delete the Contact (with native-identity guard),
                    publish crm.user.deactivated (C22), cascade Account
                    deactivation when applicable (C23).
    - Contact removal remains soft delete only — never physically remove (GDPR).
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("RegistrationChange — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email")
    change_type = xml.findtext("changeType")

    logger.info(
        "Processing registration change: email=%s, changeType=%s",
        email, change_type,
    )

    if change_type == "updated":
        await _handle_update(xml, email, sf, message)
    elif change_type == "cancelled":
        await _handle_cancellation(email, sf, message)
    else:
        # Defence-in-depth — XSD should already enforce the enum.
        logger.error("Unknown changeType '%s' for email %s — rejecting", change_type, email)
        await message.reject(requeue=False)


async def _handle_update(
    xml: etree._Element, email: str, sf: "Salesforce", message: aio_pika.IncomingMessage
) -> None:
    """Process changeType=updated: upsert Contact and publish crm.user.updated.

    MDM Rule: Frontend is authoritative for user profile data.
    """
    update_data = _build_frontend_contact_update_data(xml)

    try:
        # 1. Update de Source of Truth in Salesforce
        contact = await upsert_contact_by_email(sf, email, update_data)
        
        # 2. Synchroniseer alle andere systemen (Kassa, Planning, etc.)
        await sender.publish_user_updated(_build_updated_user_data(contact))
        
        logger.info("Master data updated via Frontend for %s; sync published.", email)
    except Exception:  # noqa: BLE001
        logger.exception("RegistrationChange update failed for %s", email)
        raise

    await message.ack()


def _build_frontend_contact_update_data(xml: etree._Element) -> dict:
    """Map Frontend-owned updatedFields to Salesforce Contact fields."""
    update_data: dict = {}

    updated_fields = xml.find("updatedFields")
    if updated_fields is None:
        return update_data

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
            if xml_field == "role":
                value = _normalize_registration_role(value)
            update_data[sf_field] = value

    return update_data


async def _handle_cancellation(
    email: str,
    sf: "Salesforce",
    message: aio_pika.IncomingMessage,
) -> None:
    """Process changeType=cancelled: deactivate Contact (with guards) and publish C22."""
    contact = await get_contact_by_email(sf, email)

    if contact is None:
        # Contact doesn't exist — nothing to deactivate. Ack to prevent infinite requeue.
        logger.warning("Cancellation for unknown email %s — acking without action", email)
        await message.ack()
        return

    if _contact_has_native_identity(contact):
        # Split-brain bescherming: als een ander team (Planning/Mailing/Facturatie/Kassa)
        # deze persoon ook kent (eigen native ID gestempeld), niet hard
        # deactivaten via C2. Cancel signaleert "Frontend-registratie voorbij",
        # niet "persoon bestaat niet meer voor andere teams".
        logger.info(
            "Skipping Contact deactivation for %s — native identity present (Planning/Mailing/Facturatie/Kassa link)",
            email,
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
        # Sibling guard: alleen Account deactiveren als dit de laatste actieve
        # Contact op de company is. Pre-validate: lookup Account vóór mutatie zodat
        # we geen Account zonder VAT_Number__c soft-deleten (zou C23 blokkeren →
        # split-brain). Wrap in try/except: als dit faalt nadat C22 al verzonden is,
        # toch acken om herhaalde C22-publicatie bij redelivery te voorkomen.
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
