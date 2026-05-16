"""Contact record → dict mapping helpers.

Pure functions that transform a Salesforce Contact record into the outbound
XML payload dict for C13 (user confirmed), C18 (user updated), and C22
(user deactivated). Used by both receiver handlers and polling dispatch to
keep the wire-format consistent across inbound-triggered and out-of-band
changes.
"""

import logging
from datetime import datetime, timezone

from src.country_code import to_iso_alpha2
from src.salesforce.client import coerce_is_active

logger = logging.getLogger(__name__)

_SPECIALIZED_ROLES = frozenset(
    {"ADMIN", "SPEAKER", "EVENT_MANAGER", "CASHIER", "BAR_STAFF"}
)


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
    logger.debug("_build_user_data received contact: %s", contact)
    # Intentionally permissive: Role__c may be absent on legacy Contacts —
    # default to VISITOR instead of crashing, so out-of-band edits in the
    # Salesforce UI never drop the C13 publish.
    from src.salesforce.client import _normalize_optional_field_value

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
