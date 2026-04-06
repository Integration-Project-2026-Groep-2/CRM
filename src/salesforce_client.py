"""Salesforce REST API client wrapper using simple-salesforce.

CUSTOM FIELDS REFERENCE (must be created in Salesforce Setup):
- Contact.CRM_ID__c (Text, Unique) — UUID v4 for Contract 13, 18, 22
- Contact.GDPR_Consent__c (Checkbox) — For Contract 1
- Contact.Registration_ID__c (Text) — Deduplication key for Contract 1
- Contact.Mailing_ID__c (Text, Unique) — Native Mailing UUID for Contracts 27-29
- Contact.Role__c (Picklist: VISITOR | COMPANY_CONTACT) — For Contract 1, 13, 18
- Contact.Paid_At__c (DateTime) — Payment timestamp for Contract 16 / Contract 17

- Account.CRM_ID__c (Text, Unique) — UUID v4 for Contract 14, 19, 23
- Account.VAT_Number__c (Text, External ID, Unique) — For Contract 3, 5a, 5b, 14

All functions return complete records as dicts for XML serialization.
All SF calls are wrapped in asyncio.to_thread() to prevent blocking the event loop.
"""

import asyncio
import logging
import uuid
from typing import Any, Literal

from simple_salesforce import Salesforce, SalesforceError

from src.config import Config

logger = logging.getLogger(__name__)

_PAYMENT_TIMESTAMP_FIELD = "Paid_At__c"


def _escape_soql(value: str) -> str:
    """Escape single quotes for SOQL to prevent injection attacks."""
    return value.replace("'", "''")


def _normalize_uuid_v4(value: Any) -> str | None:
    """Return a canonical UUID v4 string or None when invalid."""
    try:
        parsed = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None

    if parsed.version != 4:
        return None
    return str(parsed)


async def get_salesforce_client(config: Config) -> Salesforce:
    """Create an authenticated Salesforce client.

    Args:
        config: Application configuration with SF credentials.

    Returns:
        Authenticated Salesforce instance.
    """
    logger.info("Connecting to Salesforce as %s...", config.salesforce_username)
    sf = await asyncio.to_thread(
        Salesforce,
        username=config.salesforce_username,
        password=config.salesforce_password,
        security_token=config.salesforce_security_token,
        domain=config.salesforce_domain,
    )
    logger.info("Connected to Salesforce.")
    return sf


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
        data["CRM_ID__c"] = existing["CRM_ID__c"]
    if existing is None:
        data = await _ensure_contact_active(sf, data)

    try:
        result = await asyncio.to_thread(sf.Contact.upsert, f"Email/{email}", data)

        # Salesforce upsert may return a dict for creates and an int status code
        # for updates, depending on backend/API behavior.
        created = isinstance(result, dict) and result.get("created", False)
        contact_id = result.get("id") if isinstance(result, dict) else None

        if contact_id is None and existing is not None:
            contact_id = existing.get("Id")

        if created and contact_id and not data.get("CRM_ID__c"):
            crm_id = str(uuid.uuid4())
            await asyncio.to_thread(
                sf.Contact.update, contact_id, {"CRM_ID__c": crm_id}
            )

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


_active_field_cache: str | None = None
_mailing_id_field_supported_cache: bool | None = None


def _normalize_optional_field_value(value: Any) -> str | None:
    """Normalize optional Salesforce/text values so blanks behave like absence."""
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


async def _ensure_contact_active(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Ensure active Contacts are explicitly marked active in Salesforce."""
    if any(field in data for field in ("IsActive__c", "Active__c", "Is_Active__c")):
        return data

    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = True
    return data


async def _resolve_contact_active_field_optional(sf: Salesforce) -> str | None:
    """Resolve the optional Contact active field without requiring the migration."""
    global _active_field_cache  # noqa: PLW0603
    if _active_field_cache is not None:
        return _active_field_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}

    for candidate in ("IsActive__c", "Active__c", "Is_Active__c"):
        if candidate in available_fields:
            _active_field_cache = candidate
            return candidate

    return None


async def _resolve_contact_active_field(sf: Salesforce) -> str:
    """Resolve which custom field is used as contact active flag in this org.

    Result is cached after first call - custom fields don't change at runtime.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is not None:
        return active_field

    raise RuntimeError(
        "No supported Contact active field found. Expected one of: "
        "IsActive__c, Active__c, Is_Active__c"
    )


async def has_contact_mailing_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Mailing_ID__c."""
    global _mailing_id_field_supported_cache  # noqa: PLW0603
    if _mailing_id_field_supported_cache is not None:
        return _mailing_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _mailing_id_field_supported_cache = "Mailing_ID__c" in available_fields
    return _mailing_id_field_supported_cache


async def get_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email (Contracts 5a, 10a, 20).

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


async def ensure_contact_identifiers(
    sf: Salesforce,
    contact: dict[str, Any],
    registration_id: str | None = None,
    mailing_id: str | None = None,
) -> dict[str, Any]:
    """Ensure a Contact has the canonical identifiers needed by CRM contracts.

    - Always ensure CRM_ID__c exists.
    - Only set Registration_ID__c when it is currently empty and an inbound
      registration_id is provided.
    - Only set Mailing_ID__c when it is currently empty and an inbound
      mailing_id is provided.
    """
    updates: dict[str, Any] = {}

    if not contact.get("CRM_ID__c"):
        updates["CRM_ID__c"] = str(uuid.uuid4())

    if registration_id and not contact.get("Registration_ID__c"):
        updates["Registration_ID__c"] = registration_id

    if mailing_id and not contact.get("Mailing_ID__c"):
        updates["Mailing_ID__c"] = mailing_id

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


async def get_unpaid_contacts(sf: Salesforce) -> list[dict[str, Any]]:
    """Return active Contacts without a payment timestamp for Contract 17.

    Records missing CRM_ID__c or Email are skipped because they cannot be
    serialized into the outbound XML contract safely.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    select_fields = [
        "CRM_ID__c",
        "FirstName",
        "LastName",
        "Email",
        "AccountId",
        "Account.Name",
        "Paid_At__c",
    ]
    if active_field is not None:
        select_fields.append(active_field)

    query = f"SELECT {', '.join(select_fields)} FROM Contact WHERE Paid_At__c = NULL"

    try:
        result = await asyncio.to_thread(sf.query_all, query)
    except SalesforceError as e:
        logger.error("Failed to get unpaid contacts: %s", str(e))
        raise

    persons: list[dict[str, Any]] = []
    for record in result.get("records", []):
        # Older contacts may not have the active field populated yet; treat
        # missing values as active to avoid hiding valid unpaid participants.
        if active_field is not None and record.get(active_field) is False:
            continue

        crm_id = record.get("CRM_ID__c")
        email = record.get("Email")
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

        account = record.get("Account") or {}
        linked_to_company = bool(record.get("AccountId"))

        person = {
            "id": normalized_crm_id,
            "firstName": record.get("FirstName") or "",
            "lastName": record.get("LastName") or "",
            "email": email,
            "linkedToCompany": linked_to_company,
        }
        if linked_to_company and account.get("Name"):
            # Company linkage currently comes from the standard Account relation.
            person["companyName"] = account["Name"]

        persons.append(person)

    persons.sort(key=lambda person: (person["lastName"], person["firstName"], person["email"]))
    return persons


async def update_payment_status(
    sf: Salesforce,
    user_id: str | None,
    email: str,
    registration_id: str | None,
    paid_at: str,
) -> dict[str, Any] | None:
    """Update a Contact payment timestamp for Contract 16.

    Lookup order:
    - If user_id is present, search by CRM_ID__c and do not fall back to email.
    - Otherwise search by email, but only if it resolves to exactly one Contact.
    """
    if user_id:
        contact = await get_contact_by_crm_id(sf, user_id)
        if contact is None:
            logger.warning("PaymentConfirmed ignored — no Contact found for userId %s", user_id)
            return None
    else:
        contact = await find_unique_contact_by_email(sf, email)
        if contact is None:
            return None

    existing_registration_id = contact.get("Registration_ID__c")
    if registration_id and existing_registration_id and registration_id != existing_registration_id:
        logger.warning(
            "PaymentConfirmed ignored — registrationId mismatch for %s: incoming=%s existing=%s",
            contact.get("Email", email),
            registration_id,
            existing_registration_id,
        )
        return None

    contact_id = contact["Id"]
    await asyncio.to_thread(
        sf.Contact.update, contact_id, {_PAYMENT_TIMESTAMP_FIELD: paid_at}
    )
    logger.info("Updated %s for Contact %s", _PAYMENT_TIMESTAMP_FIELD, contact_id)
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

    contact_id = contact["Id"]

    try:
        active_field = await _resolve_contact_active_field(sf)

        await asyncio.to_thread(
            sf.Contact.update, contact_id, {active_field: False}
        )
        logger.info("Deactivated Contact %s (email: %s)", contact_id, email)

        # Re-fetch to get the complete updated record
        updated_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        if active_field != "IsActive__c" and "IsActive__c" not in updated_record:
            # Normalize key so downstream payload builders remain stable.
            updated_record["IsActive__c"] = updated_record.get(active_field, False)
        return updated_record
    except SalesforceError as e:
        logger.error("Failed to deactivate contact %s: %s", email, str(e))
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

    existing = await get_account_by_vat(sf, vat_number)
    if existing and existing.get("CRM_ID__c"):
        data["CRM_ID__c"] = existing["CRM_ID__c"]

    try:
        result = await asyncio.to_thread(
            sf.Account.upsert, f"VAT_Number__c/{vat_number}", data
        )
        account_id = result["id"]

        if result.get("created", False):
            if not data.get("CRM_ID__c"):
                crm_id = str(uuid.uuid4())
                await asyncio.to_thread(
                    sf.Account.update, account_id, {"CRM_ID__c": crm_id}
                )

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
