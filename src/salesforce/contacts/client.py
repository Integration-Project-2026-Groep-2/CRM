"""Contact CRUD operations and active-field helpers.

Covers create, upsert, and the active-field helpers used by create/update
flows. Separated from matching.py (read-only lookups) and updates.py
(authoritative writes to existing records) for cohesion.
"""

import asyncio
import logging
import uuid
from typing import Any

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import (
    _escape_soql,
    _resolve_contact_active_field_optional,
)
from src.salesforce.contacts.utils import get_full_contact_record

logger = logging.getLogger(__name__)


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

        result = await asyncio.to_thread(sf.Contact.create, data)
        contact_id = result["id"]
        logger.info("Created Contact with ID %s (CRM_ID: %s)", contact_id, crm_id)

        contact_record = await get_full_contact_record(sf, contact_id)
        return contact_record
    except (SalesforceError, RuntimeError) as e:
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
    # Local import to avoid module-load cycle: matching.py → nothing;
    # client.py (this file) → matching.get_contact_by_email at call time.
    from src.salesforce.contacts.matching import get_contact_by_email

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

        contact_record = await get_full_contact_record(sf, contact_id)
        return contact_record
    except (SalesforceError, RuntimeError) as e:
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
