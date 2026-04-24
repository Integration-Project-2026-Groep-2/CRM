"""Payment-related Salesforce operations.

Covers Contract 16 (payment confirmed) and Contract 17 (unpaid participants
list). Both contracts derive payment state from Session_Registration__c
records; the compatibility `Contact.Paid_At__c` field is maintained so
legacy Contract 16/17 consumers that query Contact directly keep working.
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
from src.salesforce.sessions import (
    get_session_registration_by_registration_id,
    get_unique_active_session_registration_for_contact,
)

logger = logging.getLogger(__name__)

_PAYMENT_TIMESTAMP_FIELD = "Paid_At__c"


async def get_unpaid_contacts(sf: Salesforce) -> list[dict[str, Any]]:
    """Return active Contacts with at least one unpaid active registration.

    Records missing CRM_ID__c or Email are skipped because they cannot be
    serialized into the outbound XML contract safely.
    """
    if not await has_session_registration_object(sf):
        raise RuntimeError(
            "Session_Registration__c is required to resolve unpaid registrations"
        )

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
    """Update payment state on the canonical session registration for Contract 16."""
    if not await has_session_registration_object(sf):
        raise RuntimeError(
            "Session_Registration__c is required to update payment state"
        )

    session_registration: dict[str, Any] | None = None

    if registration_id:
        session_registration = await get_session_registration_by_registration_id(sf, registration_id)
        if session_registration is None:
            logger.warning(
                "PaymentConfirmed ignored — no Session_Registration__c found for registrationId %s",
                registration_id,
            )
            return None
        if session_registration.get("Is_Active__c") is False:
            logger.warning(
                "PaymentConfirmed ignored — Session_Registration__c %s is inactive",
                session_registration["Id"],
            )
            return None

    contact: dict[str, Any] | None
    if session_registration is not None:
        contact = await asyncio.to_thread(sf.Contact.get, session_registration["Contact__c"])
    elif user_id:
        contact = await get_contact_by_crm_id(sf, user_id)
        if contact is None:
            logger.warning("PaymentConfirmed ignored — no Contact found for userId %s", user_id)
            return None
    else:
        contact = await find_unique_contact_by_email(sf, email)
        if contact is None:
            return None

    if user_id and contact.get("CRM_ID__c") != user_id:
        logger.warning(
            "PaymentConfirmed ignored — userId mismatch for registrationId %s: incoming=%s resolved=%s",
            registration_id,
            user_id,
            contact.get("CRM_ID__c"),
        )
        return None

    if email and contact.get("Email") and contact.get("Email") != email:
        logger.warning(
            "PaymentConfirmed email mismatch — resolved %s but payload contained %s; proceeding with canonical registration match",
            contact.get("Email"),
            email,
        )

    if session_registration is None:
        session_registration = await get_unique_active_session_registration_for_contact(
            sf,
            contact["Id"],
        )
        if session_registration is None:
            return None

    if session_registration.get("Contact__c") and session_registration["Contact__c"] != contact["Id"]:
        logger.warning(
            "PaymentConfirmed ignored — Session_Registration__c %s belongs to Contact %s, not %s",
            session_registration["Id"],
            session_registration["Contact__c"],
            contact["Id"],
        )
        return None

    await asyncio.to_thread(
        sf.Session_Registration__c.update,
        session_registration["Id"],
        {"Paid_At__c": paid_at},
    )
    incoming_paid_at = _parse_iso_datetime_utc(paid_at)
    if incoming_paid_at is None:
        raise ValueError(f"Invalid paid_at timestamp: {paid_at}")

    existing_contact_paid_at = _parse_iso_datetime_utc(contact.get(_PAYMENT_TIMESTAMP_FIELD))
    if contact.get(_PAYMENT_TIMESTAMP_FIELD) and existing_contact_paid_at is None:
        logger.warning(
            "Contact %s has invalid %s value %r; overwriting with %s",
            contact["Id"],
            _PAYMENT_TIMESTAMP_FIELD,
            contact.get(_PAYMENT_TIMESTAMP_FIELD),
            paid_at,
        )

    should_sync_contact_paid_at = (
        existing_contact_paid_at is None or incoming_paid_at > existing_contact_paid_at
    )
    if should_sync_contact_paid_at:
        await asyncio.to_thread(
            sf.Contact.update,
            contact["Id"],
            {_PAYMENT_TIMESTAMP_FIELD: paid_at},
        )
        logger.info(
            "Updated Session_Registration__c %s payment and advanced Contact %s compatibility timestamp",
            session_registration["Id"],
            contact["Id"],
        )
    else:
        logger.info(
            "Updated Session_Registration__c %s payment without moving Contact %s compatibility timestamp backwards",
            session_registration["Id"],
            contact["Id"],
        )

    return await asyncio.to_thread(sf.Contact.get, contact["Id"])
