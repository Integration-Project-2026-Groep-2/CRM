"""Handler for Kassa -> CRM: validate Kassa user drift against CRM master data.

Queue: crm.kassa.user.updated (routing key: kassa.user.updated)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import (
    _normalize_email_for_compare,
    _normalize_optional_text,
)
from src.salesforce.contacts import _build_updated_user_data
from src.salesforce_client import get_contact_match_by_crm_id

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 37 - Kassa -> CRM: accept the event, but keep CRM authoritative.

    The `<userId>` element carries the CRM master UUID. CRM resolves the Contact
    by `CRM_ID__c`.

    MDM rule:
    - Kassa is NOT authoritative for personal or company master data.
    - Salesforce is NEVER updated from this message.
    - If Kassa's payload differs from Salesforce, CRM publishes the canonical
      Salesforce data as crm.user.updated so Kassa can repair its local copy.
    """
    try:
        xml = xml_validator.validate_kassa(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("KassaUserUpdated - invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    crm_id = xml.findtext("userId") or ""
    email = xml.findtext("email") or ""
    first_name = xml.findtext("firstName") or ""
    last_name = xml.findtext("lastName") or ""
    badge_code = _normalize_optional_text(xml.findtext("badgeCode"))
    role = xml.findtext("role") or ""
    company_id = _normalize_optional_text(xml.findtext("companyId"))

    # Zoek de persoon op via CRM_ID__c
    match_status, existing_contact = await get_contact_match_by_crm_id(sf, crm_id)
    
    if match_status == "none":
        # Sad path: Kassa kent iemand die we niet (meer) hebben
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if match_status == "ambiguous":
        logger.warning("KassaUserUpdated ignored - ambiguous CRM_ID__c %s", crm_id)
        await message.ack()
        return

    # MDM: Vergelijk data zonder Salesforce te muteren
    if _kassa_payload_differs_from_crm(
        existing_contact,
        email=email,
        first_name=first_name,
        last_name=last_name,
        badge_code=badge_code,
        role=role,
        company_id=company_id,
    ):
        logger.warning("MDM Drift detected for Kassa user %s. Triggering correction.", crm_id)
        # Stuur de 'Single Source of Truth' terug naar de Kassa (Contract 18)
        await sender.publish_user_updated(_build_updated_user_data(existing_contact))
    else:
        logger.info("KassaUserUpdated for CRM_ID__c %s matches CRM master data", crm_id)

    await message.ack()


def _kassa_payload_differs_from_crm(
    contact: dict,
    *,
    email: str,
    first_name: str,
    last_name: str,
    badge_code: str | None,
    role: str,
    company_id: str | None,
) -> bool:
    """Return True when Kassa's local copy differs from the CRM master record."""
    comparisons = {
        "email": (
            _normalize_email_for_compare(email),
            _normalize_email_for_compare(contact.get("Email")),
        ),
        "firstName": (
            _normalize_optional_text(first_name),
            _normalize_optional_text(contact.get("FirstName")),
        ),
        "lastName": (
            _normalize_optional_text(last_name),
            _normalize_optional_text(contact.get("LastName")),
        ),
        "badgeCode": (
            _normalize_optional_text(badge_code),
            _normalize_optional_text(contact.get("Badge_Code__c")),
        ),
        "role": (
            _normalize_optional_text(role),
            _normalize_optional_text(contact.get("Role__c")),
        ),
        "companyId": (
            _normalize_optional_text(company_id),
            _normalize_optional_text(contact.get("Company_ID__c")),
        ),
    }

    for field_name, (incoming_value, crm_value) in comparisons.items():
        if incoming_value != crm_value:
            logger.warning(
                "KassaUserUpdated drift detected for %s on Contact %s: incoming=%r crm=%r",
                field_name,
                contact.get("Id"),
                incoming_value,
                crm_value,
            )
            return True

    return False
