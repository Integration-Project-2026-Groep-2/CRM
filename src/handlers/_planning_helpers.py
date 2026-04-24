"""Planning-specific helpers used by the Planning user-sync handlers (C30/C31/C32)."""

from datetime import datetime, timezone

from lxml import etree

from src.handlers._helpers import (
    _build_conflict_value,
    _has_conflicting_optional_value,
    _normalize_optional_text,
)


def _build_planning_contact_data(xml: etree._Element) -> dict:
    """Map Contract 30 XML fields to Salesforce Contact fields."""
    contact_data = {
        "Planning_ID__c": xml.findtext("id"),
        "Email": xml.findtext("email"),
        "FirstName": xml.findtext("firstName"),
        "LastName": xml.findtext("lastName"),
        "Role__c": xml.findtext("role"),
        "GDPR_Consent__c": True,
    }

    phone_number = _normalize_optional_text(xml.findtext("phoneNumber"))
    if phone_number is not None:
        contact_data["Phone"] = phone_number

    return contact_data


def _planning_user_has_conflicting_data(contact: dict, xml: etree._Element) -> bool:
    """Detect conflicting immutable profile data for Planning create-link logic."""
    if _has_conflicting_optional_value(contact.get("FirstName"), xml.findtext("firstName")):
        return True
    if _has_conflicting_optional_value(contact.get("LastName"), xml.findtext("lastName")):
        return True
    if _has_conflicting_optional_value(contact.get("Role__c"), xml.findtext("role")):
        return True
    return _has_conflicting_optional_value(contact.get("Phone"), xml.findtext("phoneNumber"))


def _build_planning_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Planning payload."""
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
