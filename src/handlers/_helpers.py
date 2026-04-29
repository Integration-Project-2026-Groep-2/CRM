"""Shared helpers used across multiple handler modules.

Generic text/naming normalization, conflict-payload primitives, and
Salesforce Account output-builders. All functions are pure (no I/O).
"""

from datetime import datetime, timezone
from typing import Any

from lxml import etree

from src.country_code import to_iso_alpha2
from src.salesforce.client import coerce_is_active

# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------


def _normalize_optional_text(value: str | None) -> str | None:
    """Normalize optional text fields so blank values behave like absence."""
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def _normalize_email_for_compare(value: str | None) -> str | None:
    """Normalize email values for dedupe/conflict comparisons."""
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None

    return normalized.casefold()


# ---------------------------------------------------------------------------
# Name formatting
# ---------------------------------------------------------------------------


def _build_full_name(first_name: str | None, last_name: str | None) -> str:
    """Build a display name from first/last name, skipping missing parts."""
    return f"{first_name or ''} {last_name or ''}".strip()


def _build_mail_display_name(
    first_name: str | None,
    last_name: str | None,
    email: str | None,
) -> str:
    """Build a non-empty display name for outbound mail requests."""
    full_name = _build_full_name(first_name, last_name)
    if full_name:
        return full_name
    return email or ""


# ---------------------------------------------------------------------------
# Conflict payload (Contract 15) primitives
# ---------------------------------------------------------------------------


def _build_conflict_value(
    first_name: str | None,
    last_name: str | None,
    company: str | None = None,
) -> dict[str, str]:
    """Build one side of a Contract 15 payload with required name fields."""
    value = {
        "firstName": _normalize_optional_text(first_name) or "",
        "lastName": _normalize_optional_text(last_name) or "",
    }

    normalized_company = _normalize_optional_text(company)
    if normalized_company is not None:
        value["company"] = normalized_company
    return value


def _has_conflicting_optional_value(existing_value: str | None, incoming_value: str | None) -> bool:
    """Return True when a provided incoming value conflicts with a non-empty existing value."""
    normalized_incoming = _normalize_optional_text(incoming_value)
    if normalized_incoming is None:
        return False

    normalized_existing = _normalize_optional_text(existing_value)
    return normalized_existing is not None and normalized_existing != normalized_incoming


def _build_registration_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Contract 1 registration.

    `detectedAt` is minted server-side: the inbound Registration XSD has no
    `detectedAt` element, so the timestamp must be generated here.
    """
    return {
        "email": email,
        "existingValue": _build_conflict_value(
            contact.get("FirstName"),
            contact.get("LastName"),
            contact.get("Company_ID__c"),
        ),
        "incomingValue": _build_conflict_value(
            xml.findtext("firstName"),
            xml.findtext("lastName"),
            xml.findtext("company"),
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Contact identity / registration compatibility (frontend registration reuse)
# ---------------------------------------------------------------------------


def _contact_has_native_identity(contact: dict) -> bool:
    """Return whether the Contact is also owned by a native producer id."""
    return any(
        _normalize_optional_text(contact.get(field)) is not None
        for field in ("Planning_ID__c", "Mailing_ID__c", "Kassa_ID__c")
    )


def _registration_fields_are_compatible(contact: dict, xml: etree._Element) -> bool:
    """Return whether a Contract 1 payload can safely reuse an existing Contact."""
    comparisons = (
        ("FirstName", xml.findtext("firstName")),
        ("LastName", xml.findtext("lastName")),
        ("Role__c", xml.findtext("role")),
    )
    for sf_field, incoming_value in comparisons:
        normalized_existing = _normalize_optional_text(contact.get(sf_field))
        normalized_incoming = _normalize_optional_text(incoming_value)
        if (
            normalized_existing is not None
            and normalized_incoming is not None
            and normalized_existing != normalized_incoming
        ):
            return False
    return True


# ---------------------------------------------------------------------------
# Account → dict builders (Contract 14, 19, 23)
# ---------------------------------------------------------------------------


def _get_account_is_active(account: dict) -> bool:
    """Normalized active flag across the supported Account active field names."""
    for active_field in ("IsActive__c", "Active__c", "Is_Active__c"):
        if active_field in account:
            return coerce_is_active(account[active_field])
    return True


def _get_account_email(account: dict) -> str | None:
    """Return the Account email via either custom Email__c or standard Email."""
    for candidate in ("Email__c", "Email"):
        value = account.get(candidate)
        if value:
            return value
    return None


def _build_company_data(account: dict) -> dict:
    """Build outbound Contract 14 crm.company.confirmed payload from a Salesforce Account.

    Required XSD fields: id, vatNumber, name, email, street, houseNumber,
    postalCode, city, country, isActive, confirmedAt. Only phone is optional.
    """
    crm_id = account.get("CRM_ID__c")
    if not crm_id:
        raise ValueError(
            f"Account {account.get('Id')} has no CRM_ID__c — refusing to publish "
            "crm.company.confirmed with a null canonical id.",
        )
    email = _get_account_email(account)
    if email is None:
        raise ValueError(
            f"Account {account.get('Id')} has no email; cannot build "
            "crm.company.confirmed payload (Contract 14 requires email).",
        )

    data: dict[str, Any] = {
        "id": crm_id,
        "vatNumber": account["VAT_Number__c"],
        "name": account["Name"],
        "email": email,
        "isActive": _get_account_is_active(account),
        "confirmedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if account.get("Phone"):
        data["phone"] = account["Phone"]
    for sf_field, xml_field in (
        ("BillingStreet", "street"),
        ("House_Number__c", "houseNumber"),
        ("BillingPostalCode", "postalCode"),
        ("BillingCity", "city"),
    ):
        value = account.get(sf_field)
        if value:
            data[xml_field] = value
    country = (
        to_iso_alpha2(account.get("BillingCountryCode"))
        or to_iso_alpha2(account.get("BillingCountry"))
    )
    if country:
        data["country"] = country

    missing = [
        f for f in ("street", "houseNumber", "postalCode", "city", "country")
        if f not in data
    ]
    if missing:
        raise ValueError(
            f"Account {account.get('Id')}: cannot publish crm.company.confirmed — "
            f"missing required address fields: {missing}",
        )
    return data


def _build_updated_company_data(account: dict) -> dict:
    """Build outbound Contract 19 crm.company.updated payload from a Salesforce Account.

    Consumers replace their local copy entirely — include all available fields.
    """
    crm_id = account.get("CRM_ID__c")
    if not crm_id:
        raise ValueError(
            f"Account {account.get('Id')} has no CRM_ID__c — refusing to publish "
            "crm.company.updated with a null canonical id.",
        )
    data: dict[str, Any] = {
        "id": crm_id,
        "vatNumber": account["VAT_Number__c"],
        "name": account["Name"],
        "isActive": _get_account_is_active(account),
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    email = _get_account_email(account)
    if email:
        data["email"] = email
    if account.get("Phone"):
        data["phone"] = account["Phone"]

    address_mapping = {
        "BillingStreet": "street",
        "House_Number__c": "houseNumber",
        "BillingPostalCode": "postalCode",
        "BillingCity": "city",
    }
    for sf_field, xml_field in address_mapping.items():
        value = account.get(sf_field)
        if value:
            data[xml_field] = value

    country = (
        to_iso_alpha2(account.get("BillingCountryCode"))
        or to_iso_alpha2(account.get("BillingCountry"))
    )
    if country:
        data["country"] = country

    return data


def _build_company_deactivation_data(account: dict, deactivated_at: str) -> dict[str, str]:
    """Build the outbound Contract 23 payload from a Salesforce Account."""
    crm_id = account.get("CRM_ID__c")
    if not crm_id:
        raise ValueError(
            f"Account {account.get('Id')} has no CRM_ID__c — refusing to publish "
            "crm.company.deactivated with a null canonical id.",
        )
    return {
        "id": crm_id,
        "vatNumber": account["VAT_Number__c"],
        "deactivatedAt": deactivated_at,
    }
