"""Mailing-specific helpers used by the Mailing user-sync handlers (C27/C28/C29)."""

from datetime import datetime, timezone

from lxml import etree

from src.handlers._helpers import (
    _build_conflict_value,
    _has_conflicting_optional_value,
    _normalize_optional_text,
)


def _derive_mailing_user_role(company_id: str | None) -> str:
    """Derive the CRM role for Mailing user sync payloads."""
    return "COMPANY_CONTACT" if company_id else "VISITOR"


def _get_mailing_last_name_for_contact(xml: etree._Element) -> str:
    """Resolve the Contact last name for Mailing payloads.

    Mailing's XSD allows lastName to be omitted, but Salesforce requires
    Contact.LastName. In that case CRM falls back to the validated email.
    """
    last_name = _normalize_optional_text(xml.findtext("lastName"))
    if last_name is not None:
        return last_name

    return _normalize_optional_text(xml.findtext("email")) or ""


def _get_effective_mailing_company_id(contact: dict, xml: etree._Element) -> str | None:
    """Return the effective company linkage for a Mailing create/reuse flow."""
    inbound_company_id = _normalize_optional_text(xml.findtext("companyId"))
    if inbound_company_id is not None:
        return inbound_company_id

    return _normalize_optional_text(contact.get("Company_ID__c"))


def _mailing_user_has_conflicting_data(contact: dict, xml: etree._Element) -> bool:
    """Detect create-path conflicts for Mailing user sync without mutating CRM data."""
    if _has_conflicting_optional_value(contact.get("FirstName"), xml.findtext("firstName")):
        return True
    if _has_conflicting_optional_value(contact.get("LastName"), xml.findtext("lastName")):
        return True

    incoming_company_id = _normalize_optional_text(xml.findtext("companyId"))
    effective_company_id = _get_effective_mailing_company_id(contact, xml)
    existing_role = _normalize_optional_text(contact.get("Role__c"))
    if effective_company_id is not None and existing_role not in (None, "VISITOR", "COMPANY_CONTACT"):
        return True

    return _has_conflicting_optional_value(contact.get("Company_ID__c"), incoming_company_id)


def _build_mailing_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Mailing payload."""
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
            xml.findtext("companyId"),
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _build_mailing_contact_data(xml: etree._Element) -> dict:
    """Map Contract 27 XML fields to Salesforce Contact fields."""
    company_id = _normalize_optional_text(xml.findtext("companyId"))
    contact_data = {
        "Mailing_ID__c": xml.findtext("id"),
        "Email": xml.findtext("email"),
        "GDPR_Consent__c": True,
        "LastName": _get_mailing_last_name_for_contact(xml),
        "Role__c": _derive_mailing_user_role(company_id),
    }

    first_name = _normalize_optional_text(xml.findtext("firstName"))
    if first_name is not None:
        contact_data["FirstName"] = first_name

    if company_id is not None:
        contact_data["Company_ID__c"] = company_id

    return contact_data


def _get_mailing_backfill_kwargs(contact: dict, xml: etree._Element) -> dict[str, object]:
    """Build the safe backfill fields for a compatible existing Mailing contact."""
    company_id = _get_effective_mailing_company_id(contact, xml)
    kwargs: dict[str, object] = {}

    first_name = _normalize_optional_text(xml.findtext("firstName"))
    if first_name is not None:
        kwargs["first_name"] = first_name

    kwargs["last_name"] = _get_mailing_last_name_for_contact(xml)

    if company_id is not None:
        kwargs["company_id"] = company_id

    kwargs["role"] = _derive_mailing_user_role(company_id)
    kwargs["gdpr_consent"] = True
    return kwargs
