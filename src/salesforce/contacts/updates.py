"""Contact authoritative updates, backfills, and deactivation.

Covers identifier normalization (ensure_contact_identifiers), producer-
specific backfills (backfill_{mailing,planning}_contact_fields),
authoritative updates per team (update_{mailing,planning,facturatie}_
contact), and soft-deletion (deactivate_contact, deactivate_contact_record).
"""

import asyncio
import logging
import uuid
from typing import Any

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import (
    _normalize_optional_field_value,
    _resolve_contact_active_field,
)
from src.salesforce.contacts.mapping import _SPECIALIZED_ROLES
from src.salesforce.contacts.matching import get_contact_by_email

logger = logging.getLogger(__name__)


async def ensure_contact_identifiers(
    sf: Salesforce,
    contact: dict[str, Any],
    registration_id: str | None = None,
    mailing_id: str | None = None,
    planning_id: str | None = None,
    kassa_id: str | None = None,
) -> dict[str, Any]:
    """Ensure a Contact has the canonical identifiers needed by CRM contracts.

    - Always ensure CRM_ID__c exists.
    - Only set Registration_ID__c when it is currently empty and an inbound
      registration_id is provided.
    - Only set Mailing_ID__c when it is currently empty and an inbound
      mailing_id is provided.
        - Only set Planning_ID__c when it is currently empty and an inbound
            planning_id is provided.
        - Only set Kassa_ID__c when it is currently empty and an inbound
            kassa_id is provided.
    """
    updates: dict[str, Any] = {}

    if not contact.get("CRM_ID__c"):
        updates["CRM_ID__c"] = str(uuid.uuid4())

    if registration_id and not contact.get("Registration_ID__c"):
        updates["Registration_ID__c"] = registration_id

    if mailing_id and not contact.get("Mailing_ID__c"):
        updates["Mailing_ID__c"] = mailing_id

    if planning_id and not contact.get("Planning_ID__c"):
        updates["Planning_ID__c"] = planning_id

    if kassa_id and not contact.get("Kassa_ID__c"):
        updates["Kassa_ID__c"] = kassa_id

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Normalized Contact %s identifiers: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def backfill_mailing_contact_fields(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    company_id: str | None = None,
    role: str | None = None,
    gdpr_consent: bool | None = None,
) -> dict[str, Any]:
    """Backfill Mailing-owned fields on a compatible existing Contact.

    Text fields are only filled when missing. Role enrichment is narrower:
    Contract 27 may promote a blank/VISITOR role to COMPANY_CONTACT when the
    incoming Mailing payload links the user to a company, but it must not
    overwrite unrelated CRM roles such as ADMIN or SPEAKER.
    """
    updates: dict[str, Any] = {}

    if first_name and _normalize_optional_field_value(contact.get("FirstName")) is None:
        updates["FirstName"] = first_name

    if last_name and _normalize_optional_field_value(contact.get("LastName")) is None:
        updates["LastName"] = last_name

    requested_role = _normalize_optional_field_value(role)
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))

    if (
        company_id
        and _normalize_optional_field_value(contact.get("Company_ID__c")) is None
        and existing_role in (None, "VISITOR", "COMPANY_CONTACT")
    ):
        updates["Company_ID__c"] = company_id

    if requested_role == "VISITOR" and existing_role is None:
        updates["Role__c"] = requested_role
    elif requested_role == "COMPANY_CONTACT" and existing_role in (None, "VISITOR"):
        if existing_role != "COMPANY_CONTACT":
            updates["Role__c"] = requested_role

    if gdpr_consent is True and contact.get("GDPR_Consent__c") is None:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Backfilled Mailing Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def backfill_planning_contact_fields(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    first_name: str,
    last_name: str,
    role: str,
    phone_number: str | None = None,
    gdpr_consent: bool | None = None,
) -> dict[str, Any]:
    """Backfill Planning-owned fields on a compatible existing Contact.

    Contract 30 links by native Planning ID, then enriches a matching Contact
    without overwriting non-empty CRM values.
    """
    updates: dict[str, Any] = {}

    if _normalize_optional_field_value(contact.get("FirstName")) is None:
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) is None:
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Role__c")) is None:
        updates["Role__c"] = role

    normalized_phone = _normalize_optional_field_value(phone_number)
    if normalized_phone is not None and _normalize_optional_field_value(contact.get("Phone")) is None:
        updates["Phone"] = normalized_phone

    if gdpr_consent is True and contact.get("GDPR_Consent__c") is None:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Backfilled Planning Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def backfill_kassa_contact_fields(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    badge_code: str | None = None,
    role: str | None = None,
    company_id: str | None = None,
) -> dict[str, Any]:
    """Backfill Kassa-owned fields on a compatible existing Contact."""
    updates: dict[str, Any] = {}

    if first_name and _normalize_optional_field_value(contact.get("FirstName")) is None:
        updates["FirstName"] = first_name

    if last_name and _normalize_optional_field_value(contact.get("LastName")) is None:
        updates["LastName"] = last_name

    if badge_code and _normalize_optional_field_value(contact.get("Badge_Code__c")) is None:
        updates["Badge_Code__c"] = badge_code

    if role and _normalize_optional_field_value(contact.get("Role__c")) is None:
        updates["Role__c"] = role

    if company_id and _normalize_optional_field_value(contact.get("Company_ID__c")) is None:
        updates["Company_ID__c"] = company_id

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Backfilled Kassa Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def update_mailing_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str | None,
    last_name: str,
    company_id: str | None,
) -> dict[str, Any]:
    """Authoritatively update Mailing-owned fields on an existing Contact.

    Contract 28 sends the full Mailing-side object. CRM therefore overwrites the
    Mailing-owned Contact fields to match that payload, while preserving
    CRM-owned fields such as active state, badge data, and registration
    identifiers. Specialized existing roles keep their role and company linkage.
    """
    updates: dict[str, Any] = {}
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))
    can_manage_company_link = existing_role in (None, "VISITOR", "COMPANY_CONTACT")

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if (
        can_manage_company_link
        and _normalize_optional_field_value(contact.get("Company_ID__c"))
        != _normalize_optional_field_value(company_id)
    ):
        updates["Company_ID__c"] = company_id

    if contact.get("GDPR_Consent__c") is not True:
        updates["GDPR_Consent__c"] = True

    if can_manage_company_link:
        desired_role = "COMPANY_CONTACT" if company_id else "VISITOR"
        if existing_role != desired_role:
            updates["Role__c"] = desired_role

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Mailing Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def update_planning_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str,
    last_name: str,
    role: str,
    phone_number: str | None,
) -> dict[str, Any]:
    """Authoritatively update Planning-owned fields on an existing Contact.

    Contract 31 sends the full Planning-side user object. CRM therefore
    overwrites Planning-owned fields to match the payload while preserving
    unrelated CRM-owned fields.
    """
    updates: dict[str, Any] = {}

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Role__c")) != _normalize_optional_field_value(role):
        updates["Role__c"] = role

    if _normalize_optional_field_value(contact.get("Phone")) != _normalize_optional_field_value(phone_number):
        updates["Phone"] = phone_number

    if contact.get("GDPR_Consent__c") is not True:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Planning Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def update_facturatie_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str,
    last_name: str,
    phone: str | None,
    street: str | None,
    house_number: str | None,
    postal_code: str | None,
    city: str | None,
    country: str | None,
    role: str,
    company_id: str | None,
) -> dict[str, Any]:
    """Authoritatively update Facturatie-owned fields on an existing Contact.

    Contract 25 sends the full Facturatie-side user object. CRM overwrites the
    Facturatie-owned fields to match, while preserving CRM-owned identifiers
    and GDPR state. A specialized existing role (ADMIN/SPEAKER/EVENT_MANAGER/
    CASHIER/BAR_STAFF) protects both Role__c and Company_ID__c from being
    overwritten — Facturatie should not demote admins or unlink specialized
    users from their companies.
    """
    updates: dict[str, Any] = {}
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))
    can_manage_role_and_company = existing_role not in _SPECIALIZED_ROLES

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Phone")) != _normalize_optional_field_value(phone):
        updates["Phone"] = phone

    if _normalize_optional_field_value(contact.get("MailingStreet")) != _normalize_optional_field_value(street):
        updates["MailingStreet"] = street

    if _normalize_optional_field_value(contact.get("House_Number__c")) != _normalize_optional_field_value(house_number):
        updates["House_Number__c"] = house_number

    if _normalize_optional_field_value(contact.get("MailingPostalCode")) != _normalize_optional_field_value(postal_code):
        updates["MailingPostalCode"] = postal_code

    if _normalize_optional_field_value(contact.get("MailingCity")) != _normalize_optional_field_value(city):
        updates["MailingCity"] = city

    if _normalize_optional_field_value(contact.get("MailingCountry")) != _normalize_optional_field_value(country):
        updates["MailingCountry"] = country

    if can_manage_role_and_company:
        if _normalize_optional_field_value(contact.get("Role__c")) != _normalize_optional_field_value(role):
            updates["Role__c"] = role
        if (
            _normalize_optional_field_value(contact.get("Company_ID__c"))
            != _normalize_optional_field_value(company_id)
        ):
            updates["Company_ID__c"] = company_id
    elif existing_role != _normalize_optional_field_value(role) or (
        _normalize_optional_field_value(contact.get("Company_ID__c"))
        != _normalize_optional_field_value(company_id)
    ):
        logger.warning(
            "Facturatie update skipped Role__c/Company_ID__c overwrite on Contact %s "
            "(existing role=%s, incoming role=%s, incoming company=%s); specialized "
            "roles are protected from Facturatie changes",
            contact.get("Id"),
            existing_role,
            role,
            company_id,
        )

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Facturatie Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def update_kassa_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str | None,
    last_name: str | None,
    badge_code: str | None,
    role: str | None,
    company_id: str | None,
) -> dict[str, Any]:
    """Authoritatively update Kassa-owned fields on an existing Contact."""
    updates: dict[str, Any] = {}
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))
    can_manage_role_and_company = existing_role not in _SPECIALIZED_ROLES

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if badge_code is not None and _normalize_optional_field_value(contact.get("Badge_Code__c")) != _normalize_optional_field_value(badge_code):
        updates["Badge_Code__c"] = badge_code

    if can_manage_role_and_company and _normalize_optional_field_value(contact.get("Role__c")) != _normalize_optional_field_value(role):
        updates["Role__c"] = role

    if can_manage_role_and_company and _normalize_optional_field_value(contact.get("Company_ID__c")) != _normalize_optional_field_value(company_id):
        updates["Company_ID__c"] = company_id
    elif not can_manage_role_and_company and (
        _normalize_optional_field_value(contact.get("Role__c")) != _normalize_optional_field_value(role)
        or _normalize_optional_field_value(contact.get("Company_ID__c")) != _normalize_optional_field_value(company_id)
    ):
        logger.warning(
            "Kassa update skipped Role__c/Company_ID__c overwrite on Contact %s (existing role=%s, incoming role=%s, incoming company=%s); specialized roles are protected from Kassa changes",
            contact.get("Id"),
            existing_role,
            role,
            company_id,
        )

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Kassa Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def deactivate_contact(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Soft-delete a Contact by setting IsActive__c=False (Contract 2, GDPR).

    NEVER physically delete a Contact — GDPR requires soft delete only.
    If the Contact does not exist, returns None and logs a warning.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to deactivate.

    Returns:
        Updated Contact record as dict, or None if not found.

    Raises:
        SalesforceError: If update fails.
    """
    contact = await get_contact_by_email(sf, email)
    if contact is None:
        logger.warning("Cannot deactivate — Contact not found for email: %s", email)
        return None

    return await deactivate_contact_record(sf, contact, log_value=email)


async def deactivate_contact_record(
    sf: Salesforce, contact: dict[str, Any], *, log_value: str | None = None
) -> dict[str, Any]:
    """Soft-delete an already resolved Contact by setting its active flag to False.

    NEVER physically delete a Contact — GDPR requires soft delete only.

    Args:
        sf: Authenticated Salesforce client.
        contact: The already resolved Salesforce Contact record.
        log_value: Optional identifier to include in logs.

    Returns:
        Updated Contact record as dict.

    Raises:
        SalesforceError: If update fails.
    """
    contact_id = contact["Id"]
    log_target = log_value or contact.get("Email") or contact_id

    try:
        active_field = await _resolve_contact_active_field(sf)

        await asyncio.to_thread(
            sf.Contact.update, contact_id, {active_field: False}
        )
        logger.info("Deactivated Contact %s (%s)", contact_id, log_target)

        # Re-fetch to get the complete updated record
        updated_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        if active_field != "IsActive__c" and "IsActive__c" not in updated_record:
            # Normalize key so downstream payload builders remain stable.
            updated_record["IsActive__c"] = updated_record.get(active_field, False)
        return updated_record
    except SalesforceError as e:
        logger.error("Failed to deactivate contact %s: %s", log_target, str(e))
        raise
