"""Contact-scoped Salesforce operations.

Covers create, upsert, lookup/match, enrichment, deactivation, and the
Contact→dict record-mapping helpers used by both receiver and polling when
building outbound C13/C18/C22 payloads.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from simple_salesforce import Salesforce, SalesforceError

from src.country_code import to_iso_alpha2
from src.salesforce.client import (
    _escape_soql,
    _normalize_optional_field_value,
    _resolve_contact_active_field,
    _resolve_contact_active_field_optional,
    coerce_is_active,
)

logger = logging.getLogger(__name__)

_SPECIALIZED_ROLES = frozenset(
    {"ADMIN", "SPEAKER", "EVENT_MANAGER", "CASHIER", "BAR_STAFF"}
)


# ---------------------------------------------------------------------------
# Record → dict mapping helpers (shared by receiver handlers + polling dispatch)
# ---------------------------------------------------------------------------


def _get_contact_is_active(contact: dict) -> bool:
    """Return the normalized active flag across supported Salesforce field names.

    Delegates to `coerce_is_active` so picklist values ("No"/"Yes") aren't
    misinterpreted by Python truthiness (bool('No') == True).
    """
    for active_field in ("IsActive__c", "Active__c", "Is_Active__c"):
        if active_field in contact:
            return coerce_is_active(contact[active_field])
    return True


def _build_user_data(contact: dict) -> dict:
    """Build user_data payload dict from a Salesforce contact record."""
    role = _normalize_optional_field_value(contact.get("Role__c")) or "VISITOR"
    gdpr_consent = contact.get("GDPR_Consent__c")
    if gdpr_consent is None:
        gdpr_consent = True

    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": role,
        "isActive": _get_contact_is_active(contact),
        "gdprConsent": gdpr_consent,
        "confirmedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if contact.get("Phone"):
        data["phone"] = contact["Phone"]
    if contact.get("Company_ID__c"):
        data["companyId"] = contact["Company_ID__c"]
    return data


def _build_user_deactivation_data(contact: dict, deactivated_at: str) -> dict[str, str]:
    """Build the outbound Contract 22 payload from a Salesforce Contact."""
    return {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "deactivatedAt": deactivated_at,
    }


def _build_updated_user_data(contact: dict) -> dict:
    """Build user_data payload dict for crm.user.updated from a Salesforce record.

    Same structure as _build_user_data but with updatedAt instead of confirmedAt.
    Contract 18 requires the full user profile - consumers replace their local
    copy entirely, so all available fields must be included.
    """
    data = {
        "id": contact["CRM_ID__c"],
        "email": contact["Email"],
        "firstName": contact.get("FirstName", ""),
        "lastName": contact.get("LastName", ""),
        "role": contact.get("Role__c", "VISITOR"),
        "isActive": _get_contact_is_active(contact),
        "gdprConsent": contact.get("GDPR_Consent__c", True),
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if contact.get("Phone"):
        data["phone"] = contact["Phone"]
    if contact.get("Company_ID__c"):
        data["companyId"] = contact["Company_ID__c"]
    if contact.get("Badge_Code__c"):
        data["badgeCode"] = contact["Badge_Code__c"]

    # Address fields - map all available SF fields for full profile
    address_mapping = {
        "MailingStreet": "street",
        "House_Number__c": "houseNumber",
        "MailingPostalCode": "postalCode",
        "MailingCity": "city",
    }
    for sf_field, xml_field in address_mapping.items():
        value = contact.get(sf_field)
        if value:
            data[xml_field] = value

    country = (
        to_iso_alpha2(contact.get("MailingCountryCode"))
        or to_iso_alpha2(contact.get("MailingCountry"))
    )
    if country:
        data["country"] = country

    return data


# ---------------------------------------------------------------------------
# Contact CRUD
# ---------------------------------------------------------------------------


async def _ensure_contact_active(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Ensure active Contacts are explicitly marked active in Salesforce."""
    if any(field in data for field in ("IsActive__c", "Active__c", "Is_Active__c")):
        return data

    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = True
    return data


async def apply_is_active(
    sf: Salesforce, data: dict[str, Any], is_active: bool,
) -> dict[str, Any]:
    """Set the resolved Contact active field on a payload dict.

    Used by producer-sync receivers (Mailing/Facturatie) that carry
    authoritative `isActive` state from the source system.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = is_active
    return data


async def create_contact(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Contact in Salesforce (Contracts 1 and 24).

    Maps XML Registration fields to Contact fields and returns complete record
    for crm.user.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Contact fields (FirstName, LastName, Email, Role__c, GDPR_Consent__c, etc.).

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If Contact creation fails.
    """
    try:
        # Shallow copy to avoid mutating input dict
        data = {**data}
        # Generate UUID v4 for crm.user.confirmed (Contract 13)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id
        data = await _ensure_contact_active(sf, data)

        # Create Contact
        result = await asyncio.to_thread(sf.Contact.create, data)
        contact_id = result["id"]
        logger.info("Created Contact with ID %s (CRM_ID: %s)", contact_id, crm_id)

        # Retrieve and return complete record for XML serialization
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to create contact: %s", str(e))
        raise


async def upsert_contact_by_email(
    sf: Salesforce, email: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update a Contact by email (Contract 2).

    Uses native Salesforce upsert with Email as external ID to avoid race conditions.
    Returns complete record for XML serialization.

    REQUIREMENT: Email must be configured as External ID in Salesforce Setup.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email (lookup key).
        data: Contact fields to create/update.

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    data = {**data}

    existing = await get_contact_by_email(sf, email)
    if existing and existing.get("CRM_ID__c"):
        # Preserve the existing canonical UUID on updates — never regenerate.
        data["CRM_ID__c"] = existing["CRM_ID__c"]
    elif not data.get("CRM_ID__c"):
        # simple-salesforce's SFType.upsert() always returns an HTTP status
        # code (int), never a dict, so post-upsert minting via a "created"
        # signal is dead code. Stamp CRM_ID__c into the body so SF persists
        # it atomically on create and backfills any existing record missing it.
        data["CRM_ID__c"] = str(uuid.uuid4())
    if existing is None:
        data = await _ensure_contact_active(sf, data)

    try:
        result = await asyncio.to_thread(sf.Contact.upsert, f"Email/{email}", data)

        # Defensive dict-guard in case a future simple-salesforce version
        # mirrors SFType.create() and returns a dict with "id".
        contact_id = result.get("id") if isinstance(result, dict) else None

        if contact_id is None and existing is not None:
            contact_id = existing.get("Id")

        if contact_id is None:
            refreshed = await get_contact_by_email(sf, email)
            if refreshed is None:
                raise RuntimeError(f"Upsert succeeded but contact not found for email {email}")
            return refreshed

        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to upsert contact by email %s: %s", email, str(e))
        raise


async def count_active_contacts_for_company(
    sf: Salesforce, company_id: str
) -> int:
    """Return the number of active Contacts that share the given Company_ID__c.

    Used as a sibling guard before deactivating an Account: if other active
    Contacts still reference the same company, the Account must stay active.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    escaped_company_id = _escape_soql(company_id)
    query = (
        "SELECT COUNT() FROM Contact "
        f"WHERE Company_ID__c = '{escaped_company_id}'"
    )
    if active_field is not None:
        query += f" AND {active_field} = true"

    try:
        result = await asyncio.to_thread(sf.query, query)
    except SalesforceError as e:
        logger.error(
            "Failed to count active Contacts for Company_ID__c %s: %s",
            company_id,
            str(e),
        )
        raise

    return int(result["totalSize"])


# ---------------------------------------------------------------------------
# Contact lookups / match classification
# ---------------------------------------------------------------------------


async def get_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email (Contracts 5a, 20).

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Complete Contact record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            contact_id = result["records"][0]["Id"]
            contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info("Found Contact by email: %s", email)
            return contact_record
        else:
            logger.info("Contact not found by email: %s", email)
            return None
    except SalesforceError as e:
        logger.error("Failed to get contact by email %s: %s", email, str(e))
        raise


async def get_contact_for_person_lookup(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email for Contract 10a (kassa.person.lookup.requested).

    Returns Contact with Account relationship fields expanded so the
    handler can build the PersonLookupResponse in one round-trip.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Record with ``Id``, ``CRM_ID__c``, ``AccountId`` and (when the Contact
        is linked to an Account) a nested ``Account`` dict with ``Name`` and
        ``CRM_ID__c``. Returns ``None`` if no Contact matches the email.

    Raises:
        SalesforceError: If the SOQL query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = (
            "SELECT Id, CRM_ID__c, AccountId, Account.Name, Account.CRM_ID__c "
            f"FROM Contact WHERE Email = '{escaped_email}'"
        )
        result = await asyncio.to_thread(sf.query, query)
        if result["totalSize"] == 0:
            logger.info("Person lookup — no Contact for email %s", email)
            return None
        logger.info("Person lookup — found Contact for email %s", email)
        return result["records"][0]
    except SalesforceError as e:
        logger.error("Person lookup query failed for %s: %s", email, str(e))
        raise


async def get_contact_by_crm_id(
    sf: Salesforce, crm_id: str
) -> dict[str, Any] | None:
    """Look up a Contact by CRM UUID."""
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return None
        if result["totalSize"] > 1:
            logger.warning("Multiple Contacts found for CRM_ID__c %s", crm_id)
            return None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found Contact by CRM_ID__c: %s", crm_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def find_unique_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email, but only return a unique match."""
    match_status, contact = await get_contact_match_by_email(sf, email)
    if match_status == "none":
        logger.warning("No Contact found for payment confirmation email %s", email)
        return None
    if match_status == "ambiguous":
        logger.warning("Multiple Contacts found for payment confirmation email %s", email)
        return None
    return contact


async def get_contact_match_by_email(
    sf: Salesforce, email: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify an email lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by email: %s", email)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by email %s: %s", email, str(e))
        raise


async def get_contact_match_by_mailing_id(
    sf: Salesforce, mailing_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Mailing native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_mailing_id = _escape_soql(mailing_id)
        query = f"SELECT Id FROM Contact WHERE Mailing_ID__c = '{escaped_mailing_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by Mailing_ID__c: %s", mailing_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Mailing_ID__c %s: %s", mailing_id, str(e))
        raise


async def get_contact_match_by_planning_id(
    sf: Salesforce, planning_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Planning native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_planning_id = _escape_soql(planning_id)
        query = f"SELECT Id FROM Contact WHERE Planning_ID__c = '{escaped_planning_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by Planning_ID__c: %s", planning_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Planning_ID__c %s: %s", planning_id, str(e))
        raise


async def get_contact_match_by_crm_id(
    sf: Salesforce, crm_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a CRM master UUID lookup as no match, unique match, or ambiguous match.

    Used by update/deactivate handlers where consumers send back the canonical
    CRM UUID received in `crm.user.confirmed` (Option 2 UUID strategy).
    """
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by CRM_ID__c: %s", crm_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by CRM_ID__c %s: %s", crm_id, str(e))
        raise


# ---------------------------------------------------------------------------
# Contact enrichment / updates
# ---------------------------------------------------------------------------


async def ensure_contact_identifiers(
    sf: Salesforce,
    contact: dict[str, Any],
    registration_id: str | None = None,
    mailing_id: str | None = None,
    planning_id: str | None = None,
) -> dict[str, Any]:
    """Ensure a Contact has the canonical identifiers needed by CRM contracts.

    - Always ensure CRM_ID__c exists.
    - Only set Registration_ID__c when it is currently empty and an inbound
      registration_id is provided.
    - Only set Mailing_ID__c when it is currently empty and an inbound
      mailing_id is provided.
        - Only set Planning_ID__c when it is currently empty and an inbound
            planning_id is provided.
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


# ---------------------------------------------------------------------------
# Deactivation (Contract 22, GDPR soft delete)
# ---------------------------------------------------------------------------


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
