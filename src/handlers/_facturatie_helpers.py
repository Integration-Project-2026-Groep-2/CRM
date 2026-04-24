"""Facturatie-specific helpers used by the Facturatie user/company handlers (C24-C26, C33-C35)."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from lxml import etree

from src.handlers._helpers import _build_conflict_value, _normalize_optional_text
from src.salesforce.client import (
    _resolve_account_country_field,
    _resolve_account_email_field,
    has_account_house_number_field,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce


def _build_facturatie_user_conflict_data(email: str, contact: dict, xml: etree._Element) -> dict:
    """Build a Contract 15 payload from an existing Contact and incoming Facturatie payload."""
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


async def _build_facturatie_account_data(
    xml: etree._Element, sf: "Salesforce"
) -> dict[str, Any]:
    """Map Facturatie inbound company XML to an Account-create payload.

    Dynamically resolves org-specific field names:
    - Email lands on `Email__c` or standard `Email` depending on what exists.
    - Country lands on `BillingCountryCode` (State & Country Picklists enabled,
      ISO-2 expected) or `BillingCountry` (picklists disabled, free text).

    """
    data: dict[str, Any] = {
        "Name": xml.findtext("name") or "",
    }
    vat_number = _normalize_optional_text(xml.findtext("vatNumber"))
    if vat_number:
        data["VAT_Number__c"] = vat_number

    email = _normalize_optional_text(xml.findtext("email"))
    if email:
        email_field = await _resolve_account_email_field(sf)
        if email_field is not None:
            data[email_field] = email

    phone = _normalize_optional_text(xml.findtext("phone"))
    if phone:
        data["Phone"] = phone

    address_mapping = {
        "street": "BillingStreet",
        "houseNumber": "House_Number__c",
        "postalCode": "BillingPostalCode",
        "city": "BillingCity",
    }
    for xml_field, sf_field in address_mapping.items():
        if sf_field == "House_Number__c" and not await has_account_house_number_field(sf):
            continue
        value = _normalize_optional_text(xml.findtext(xml_field))
        if value:
            data[sf_field] = value

    country = _normalize_optional_text(xml.findtext("country"))
    if country:
        country_field = await _resolve_account_country_field(sf)
        data[country_field] = country

    return data
