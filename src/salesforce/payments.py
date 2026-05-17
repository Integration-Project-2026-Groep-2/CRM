"""Payment-related Salesforce operations.

Covers Contract 16 (payment confirmed) and Contract 17 (unpaid participants
list). Sinds 2026-04-29 schrijft C16 ``Paid_At__c`` rechtstreeks op het Contact
(geen junction). C17b leunt voorlopig nog op ``Session_Registration__c`` voor
historische rijen — een herontwerp wacht op PM-input (zie ClickUp C17b spec).
"""

import asyncio
import logging
from typing import Any

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import (
    _normalize_uuid_v4,
    _parse_iso_datetime_utc,
    _resolve_contact_active_field_optional,
    has_session_registration_object,
)
from src.salesforce.contacts import (
    find_unique_contact_by_email,
    get_contact_by_crm_id,
)
from src.salesforce.contacts.utils import get_full_contact_record

logger = logging.getLogger(__name__)

_PAYMENT_TIMESTAMP_FIELD = "Paid_At__c"


async def get_unpaid_contacts(sf: Salesforce) -> list[dict[str, Any]]:
    """Return active Contacts with at least one unpaid active registration.

    Records missing CRM_ID__c or Email are skipped because they cannot be
    serialized into the outbound XML contract safely.

    TODO(C17b-redesign): deze query leunt nog op de ``Session_Registration__c``
    junction die per 2026-04-29 niet meer geschreven wordt door C2/C16.
    Voor historische rijen blijft het werken; voor nieuwe registraties
    (zonder junction-rij) levert dit lege resultaten. Twee opties wachten op
    PM-beslissing:
      A) Globaal: alle Contacts met ``Paid_At__c IS NULL`` zonder sessie-context.
      B) Planning levert deelnemers-per-sessie via een nieuw inbound contract;
         CRM filtert op betaalstatus.
    Zie ClickUp doc 2kyr1d3m-6675 voor de actieve discussie.
    """
    if not await has_session_registration_object(sf):
        logger.warning(
            "C17b query against legacy Session_Registration__c junction; "
            "object not present — returning empty list. PM redesign pending.",
        )
        return []

    active_field = await _resolve_contact_active_field_optional(sf)
    select_fields = [
        "Id",
        "Contact__c",
        "Contact__r.CRM_ID__c",
        "Contact__r.FirstName",
        "Contact__r.LastName",
        "Contact__r.Email",
        "Contact__r.AccountId",
        "Contact__r.Account.Name",
    ]
    if active_field is not None:
        select_fields.append(f"Contact__r.{active_field}")

    query = (
        f"SELECT {', '.join(select_fields)} FROM Session_Registration__c "
        "WHERE Is_Active__c = true AND Paid_At__c = NULL"
    )

    try:
        result = await asyncio.to_thread(sf.query_all, query)
    except SalesforceError as e:
        logger.error("Failed to get unpaid contacts: %s", str(e))
        raise

    persons_by_id: dict[str, dict[str, Any]] = {}
    for record in result.get("records", []):
        contact = record.get("Contact__r") or {}
        # Older contacts may not have the active field populated yet; treat
        # missing values as active to avoid hiding valid unpaid participants.
        if active_field is not None and contact.get(active_field) is False:
            continue

        crm_id = contact.get("CRM_ID__c")
        email = contact.get("Email")
        if not crm_id or not email:
            logger.warning(
                "Skipping unpaid contact without required fields: CRM_ID__c=%s Email=%s",
                crm_id,
                email,
            )
            continue
        normalized_crm_id = _normalize_uuid_v4(crm_id)
        if normalized_crm_id is None:
            logger.warning("Skipping unpaid contact with invalid CRM_ID__c: %s", crm_id)
            continue

        if normalized_crm_id in persons_by_id:
            continue

        account = contact.get("Account") or {}
        linked_to_company = bool(contact.get("AccountId"))

        person = {
            "id": normalized_crm_id,
            "firstName": contact.get("FirstName") or "",
            "lastName": contact.get("LastName") or "",
            "email": email,
            "linkedToCompany": linked_to_company,
        }
        if linked_to_company and account.get("Name"):
            # Company linkage currently comes from the standard Account relation.
            person["companyName"] = account["Name"]

        persons_by_id[normalized_crm_id] = person

    persons = list(persons_by_id.values())
    persons.sort(key=lambda person: (person["lastName"], person["firstName"], person["email"]))
    return persons


async def update_payment_status(
    sf: Salesforce,
    user_id: str | None,
    email: str,
    registration_id: str | None,
    paid_at: str,
) -> dict[str, Any] | None:
    """Stamp ``Paid_At__c`` on the Contact for Contract 16.

    Resolution order: ``user_id`` (CRM_ID__c lookup) → ``email`` (unique-email
    lookup). ``registration_id`` wordt geaccepteerd in het bericht maar niet
    gebruikt voor opzoek, want CRM houdt geen sessie-registraties meer bij —
    dat is Planning's domein. Als beide lookups falen retourneren we ``None``;
    de handler ackt zonder retry.

    Tijdstempel-strategie: Contact bewaart de laatste betaling. We schrijven
    alleen wanneer de inkomende ``paid_at`` nieuwer is dan de huidige waarde,
    zodat out-of-order C16-berichten niet terug-in-de-tijd schrijven.
    """
    contact: dict[str, Any] | None
    if user_id:
        contact = await get_contact_by_crm_id(sf, user_id)
        if contact is None:
            logger.warning("PaymentConfirmed ignored — no Contact found for userId %s", user_id)
            return None
    else:
        contact = await find_unique_contact_by_email(sf, email)
        if contact is None:
            return None

    if email and contact.get("Email") and contact.get("Email") != email:
        logger.warning(
            "PaymentConfirmed email mismatch — resolved %s but payload contained %s; proceeding with userId match",
            contact.get("Email"),
            email,
        )

    incoming_paid_at = _parse_iso_datetime_utc(paid_at)
    if incoming_paid_at is None:
        raise ValueError(f"Invalid paid_at timestamp: {paid_at}")

    existing_paid_at = _parse_iso_datetime_utc(contact.get(_PAYMENT_TIMESTAMP_FIELD))
    if contact.get(_PAYMENT_TIMESTAMP_FIELD) and existing_paid_at is None:
        logger.warning(
            "Contact %s has invalid %s value %r; overwriting with %s",
            contact["Id"],
            _PAYMENT_TIMESTAMP_FIELD,
            contact.get(_PAYMENT_TIMESTAMP_FIELD),
            paid_at,
        )

    if existing_paid_at is not None and incoming_paid_at <= existing_paid_at:
        logger.info(
            "Skipped Contact %s payment update — incoming %s not newer than existing %s",
            contact["Id"],
            paid_at,
            contact.get(_PAYMENT_TIMESTAMP_FIELD),
        )
        return await get_full_contact_record(sf, contact["Id"])

    await asyncio.to_thread(
        sf.Contact.update,
        contact["Id"],
        {_PAYMENT_TIMESTAMP_FIELD: paid_at},
    )
    logger.info(
        "Updated Contact %s payment timestamp to %s (registrationId=%s, observed)",
        contact["Id"],
        paid_at,
        registration_id,
    )

    return await get_full_contact_record(sf, contact["Id"])
