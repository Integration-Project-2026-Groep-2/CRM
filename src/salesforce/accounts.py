"""Account-scoped Salesforce operations (companies).

Covers create, upsert, lookup/match, Facturatie-sync updates, and
soft-deactivation for the Account SObject.
"""

import asyncio
import logging
import uuid
from typing import Any, Literal

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import (
    _escape_soql,
    _normalize_optional_field_value,
    _resolve_account_active_field,
    _resolve_account_active_field_optional,
    _resolve_account_country_field,
    _resolve_account_email_field,
    has_account_house_number_field,
)

logger = logging.getLogger(__name__)


async def apply_account_is_active(
    sf: Salesforce, data: dict[str, Any], is_active: bool,
) -> dict[str, Any]:
    """Set the resolved Account active field on a payload dict.

    Sibling of apply_is_active for the Account object. Orgs may use different
    active-field names on Contact vs Account (e.g. IsActive__c on one and
    Active__c on the other), so resolving per-object is mandatory.
    """
    active_field = await _resolve_account_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = is_active
    return data


async def get_account_by_crm_id(
    sf: Salesforce, crm_id: str
) -> dict[str, Any] | None:
    """Look up an Account by CRM UUID."""
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Account WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return None
        if result["totalSize"] > 1:
            logger.warning("Multiple Accounts found for CRM_ID__c %s", crm_id)
            return None

        account_id = result["records"][0]["Id"]
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        logger.info("Found Account by CRM_ID__c: %s", crm_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to get account by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def deactivate_account_by_crm_id(
    sf: Salesforce, crm_id: str
) -> dict[str, Any] | None:
    """Soft-delete an Account by CRM UUID (Contract 23, US-54)."""
    account = await get_account_by_crm_id(sf, crm_id)
    if account is None:
        logger.warning("Cannot deactivate — Account not found for CRM_ID__c: %s", crm_id)
        return None

    account_id = account["Id"]

    try:
        active_field = await _resolve_account_active_field(sf)

        await asyncio.to_thread(
            sf.Account.update, account_id, {active_field: False}
        )
        logger.info("Deactivated Account %s (CRM_ID__c: %s)", account_id, crm_id)

        updated_record = await asyncio.to_thread(sf.Account.get, account_id)
        if active_field != "IsActive__c" and "IsActive__c" not in updated_record:
            updated_record["IsActive__c"] = updated_record.get(active_field, False)
        return updated_record
    except SalesforceError as e:
        logger.error("Failed to deactivate account %s: %s", crm_id, str(e))
        raise


async def create_account(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Account (company) in Salesforce (Contract 3).

    Maps XML CompanyRequest fields to Account fields and returns complete record
    for crm.company.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Account fields (Name, VAT_Number__c, Email, Phone, etc.).

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If Account creation fails.
    """
    try:
        # Shallow copy to avoid mutating input dict
        data = {**data}
        # Generate UUID v4 for crm.company.confirmed (Contract 14)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id

        # Create Account
        result = await asyncio.to_thread(sf.Account.create, data)
        account_id = result["id"]
        logger.info("Created Account with ID %s (CRM_ID: %s)", account_id, crm_id)

        # Retrieve and return complete record for XML serialization
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to create account: %s", str(e))
        raise


async def upsert_account_by_vat(
    sf: Salesforce, vat_number: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update an Account by VAT number (Contracts 3, 5a).

    Uses native Salesforce upsert with VAT_Number__c as external ID to avoid race conditions.
    Returns complete record for XML serialization.

    REQUIREMENT: VAT_Number__c must be configured as External ID in Salesforce Setup.

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number (lookup key, should be in data as VAT_Number__c).
        data: Account fields to create/update.

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    data = {**data}

    # Salesforce v59+ rejects the external-ID field in the body when it is
    # already specified in the URL path. Strip it — the value on the URL is
    # authoritative for the upsert target.
    data.pop("VAT_Number__c", None)

    existing = await get_account_by_vat(sf, vat_number)
    if existing and existing.get("CRM_ID__c"):
        # Preserve the existing canonical UUID on updates — never regenerate.
        data["CRM_ID__c"] = existing["CRM_ID__c"]
    elif not data.get("CRM_ID__c"):
        # simple-salesforce's SFType.upsert() always returns an HTTP status
        # code (int), never a dict, so there is no "created" signal we can
        # use to mint CRM_ID__c post-upsert. Stamp it in the request body so
        # SF persists it atomically on create. Also backfills existing
        # records whose CRM_ID__c was left blank by an earlier buggy rollout.
        data["CRM_ID__c"] = str(uuid.uuid4())

    try:
        result = await asyncio.to_thread(
            sf.Account.upsert, f"VAT_Number__c/{vat_number}", data
        )

        # Defensive dict-guard in case a future simple-salesforce version
        # mirrors SFType.create() and returns a dict with "id".
        account_id = result.get("id") if isinstance(result, dict) else None

        if account_id is None and existing is not None:
            account_id = existing.get("Id")

        if account_id is None:
            refreshed = await get_account_by_vat(sf, vat_number)
            if refreshed is None:
                raise RuntimeError(
                    f"Upsert succeeded but account not found for VAT {vat_number}"
                )
            return refreshed

        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to upsert account by VAT %s: %s", vat_number, str(e))
        raise


async def get_account_by_vat(
    sf: Salesforce, vat_number: str
) -> dict[str, Any] | None:
    """Look up an Account by VAT number (Contracts 5a, 5b).

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number to search for.

    Returns:
        Complete Account record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_vat = _escape_soql(vat_number)
        query = f"SELECT Id FROM Account WHERE VAT_Number__c = '{escaped_vat}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            account_id = result["records"][0]["Id"]
            account_record = await asyncio.to_thread(sf.Account.get, account_id)
            logger.info("Found Account by VAT: %s", vat_number)
            return account_record
        else:
            logger.info("Account not found by VAT: %s", vat_number)
            return None
    except SalesforceError as e:
        logger.error("Failed to get account by VAT %s: %s", vat_number, str(e))
        raise


async def get_account_match_by_crm_id(
    sf: Salesforce, crm_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify an Account CRM_ID__c lookup as no/unique/ambiguous match.

    Used by Contracts 34/35 (Facturatie company updated/deactivated) where
    Facturatie sends the canonical CRM UUID back in the payload.
    """
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Account WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        account_id = result["records"][0]["Id"]
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        logger.info("Found unique Account by CRM_ID__c: %s", crm_id)
        return "unique", account_record
    except SalesforceError as e:
        logger.error("Failed to get account match by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def get_account_match_by_email(
    sf: Salesforce, email: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify an Account email lookup as no/unique/ambiguous match.

    Probes the Account SObject for either Email__c or standard Email.
    Returns ("none", None) when no email field exists on the org's Account.
    """
    email_field = await _resolve_account_email_field(sf)
    if email_field is None:
        logger.info(
            "Account has no Email__c or Email field; cannot match by email: %s", email,
        )
        return "none", None

    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Account WHERE {email_field} = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        account_id = result["records"][0]["Id"]
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        logger.info("Found unique Account by %s: %s", email_field, email)
        return "unique", account_record
    except SalesforceError as e:
        logger.error("Failed to get account match by email %s: %s", email, str(e))
        raise


async def update_facturatie_account(
    sf: Salesforce,
    account: dict[str, Any],
    *,
    vat_number: str | None,
    name: str,
    email: str,
    phone: str | None,
    street: str | None,
    house_number: str | None,
    postal_code: str | None,
    city: str | None,
    country: str | None,
) -> dict[str, Any]:
    """Authoritatively update Facturatie-owned fields on an existing Account.

    Contract 34 sends the full Facturatie-side company object. CRM overwrites
    the Facturatie-owned fields to match, while preserving CRM-owned identifiers
    (CRM_ID__c). Only writes to Salesforce when at least one field differs.

    `house_number` is persisted when the org exposes Account.House_Number__c.
    """
    email_field = await _resolve_account_email_field(sf)

    updates: dict[str, Any] = {}

    if _normalize_optional_field_value(account.get("Name")) != _normalize_optional_field_value(name):
        updates["Name"] = name

    # VAT_Number__c doubles as the Account external ID / idempotency key
    # (upsert_account_by_vat). Only overwrite when Facturatie explicitly sends
    # a vatNumber — an omission means "no change", NOT "clear the field".
    # Guards against breaking dedup on the next facturatie.company.created.
    if vat_number is not None and _normalize_optional_field_value(
        account.get("VAT_Number__c")
    ) != _normalize_optional_field_value(vat_number):
        updates["VAT_Number__c"] = vat_number

    if email_field is not None and (
        _normalize_optional_field_value(account.get(email_field))
        != _normalize_optional_field_value(email)
    ):
        updates[email_field] = email

    if _normalize_optional_field_value(account.get("Phone")) != _normalize_optional_field_value(phone):
        updates["Phone"] = phone

    if _normalize_optional_field_value(account.get("BillingStreet")) != _normalize_optional_field_value(street):
        updates["BillingStreet"] = street

    if _normalize_optional_field_value(account.get("BillingPostalCode")) != _normalize_optional_field_value(postal_code):
        updates["BillingPostalCode"] = postal_code

    if _normalize_optional_field_value(account.get("BillingCity")) != _normalize_optional_field_value(city):
        updates["BillingCity"] = city

    if await has_account_house_number_field(sf):
        if _normalize_optional_field_value(account.get("House_Number__c")) != _normalize_optional_field_value(house_number):
            updates["House_Number__c"] = house_number

    # Use the org-appropriate country field: BillingCountryCode when State &
    # Country Picklists are enabled (BillingCountry is read-only then),
    # BillingCountry otherwise. Compare against whichever one exists on the
    # fetched record to avoid spurious updates.
    country_field = await _resolve_account_country_field(sf)
    if _normalize_optional_field_value(account.get(country_field)) != _normalize_optional_field_value(country):
        updates[country_field] = country

    if not updates:
        return account

    account_id = account["Id"]
    await asyncio.to_thread(sf.Account.update, account_id, updates)
    logger.info(
        "Updated Facturatie Account %s fields: %s",
        account_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Account.get, account_id)


async def patch_account_fields(
    sf: Salesforce, account: dict[str, Any], fields: dict[str, Any]
) -> dict[str, Any]:
    """Patch an Account, writing only the given fields that actually changed.

    `fields` is already mapped to org-specific Account field names and holds
    exactly what the caller wants to set (a value of None clears that column).
    Fields the caller leaves out of `fields` are never touched — this is what
    makes a partial update safe: omission means "keep", not "clear". A no-op
    returns the account unchanged without hitting Salesforce.
    """
    updates = {
        field: value
        for field, value in fields.items()
        if _normalize_optional_field_value(account.get(field))
        != _normalize_optional_field_value(value)
    }
    if not updates:
        return account

    account_id = account["Id"]
    await asyncio.to_thread(sf.Account.update, account_id, updates)
    logger.info(
        "Patched Account %s fields: %s",
        account_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Account.get, account_id)


async def deactivate_account_record(
    sf: Salesforce, account: dict[str, Any], *, log_value: str | None = None
) -> dict[str, Any]:
    """Soft-delete an already resolved Account by setting its active flag to False.

    NEVER physically delete an Account — company records may be referenced
    by Contacts, invoices, etc. Preserve for audit trail.
    """
    account_id = account["Id"]
    log_target = log_value or account.get("CRM_ID__c") or account.get("VAT_Number__c") or account_id

    try:
        active_field = await _resolve_account_active_field(sf)

        await asyncio.to_thread(
            sf.Account.update, account_id, {active_field: False}
        )
        logger.info("Deactivated Account %s (%s)", account_id, log_target)

        updated_record = await asyncio.to_thread(sf.Account.get, account_id)
        if active_field != "IsActive__c" and "IsActive__c" not in updated_record:
            updated_record["IsActive__c"] = updated_record.get(active_field, False)
        return updated_record
    except SalesforceError as e:
        logger.error("Failed to deactivate account %s: %s", log_target, str(e))
        raise
