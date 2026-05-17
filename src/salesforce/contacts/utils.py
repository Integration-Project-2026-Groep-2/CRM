"""SOQL-based Contact retrieval to bypass Page Layout / FLS filtering.

`simple_salesforce.SFType.get(id)` calls the REST sObject row GET, whose
response shape is governed by the integration user's profile — fields not
readable for the user are silently omitted, leaving callers with dicts
missing custom fields that were just written. Using SOQL with an explicit
field list returns every described field that survives FLS, independent
of layout.
"""

import asyncio
import logging
from typing import Any

from simple_salesforce import Salesforce

from src.salesforce.client import _escape_soql

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = ["Id", "CRM_ID__c", "FirstName", "LastName", "Email"]

_OPTIONAL_FIELDS = [
    "Phone",
    "Role__c",
    "GDPR_Consent__c",
    "Badge_Code__c",
    "Company_ID__c",
    "Registration_ID__c",
    "Mailing_ID__c",
    "Planning_ID__c",
    "Kassa_ID__c",
    "MailingStreet",
    "House_Number__c",
    "MailingPostalCode",
    "MailingCity",
    "MailingCountry",
    "MailingCountryCode",
    "IsActive__c",
    "Active__c",
    "Is_Active__c",
    "CreatedDate",
    "SystemModstamp",
    "LastModifiedById",
]

_describe_cache: dict[str, set[str]] = {}


async def _available_contact_fields(sf: Salesforce) -> set[str]:
    cached = _describe_cache.get("Contact")
    if cached is not None:
        return cached
    describe = await asyncio.to_thread(sf.Contact.describe)
    names = {f["name"] for f in describe.get("fields", [])}
    _describe_cache["Contact"] = names
    return names


async def get_full_contact_record(sf: Salesforce, contact_id: str) -> dict[str, Any]:
    available = await _available_contact_fields(sf)
    missing = [f for f in _REQUIRED_FIELDS if f not in available]
    if missing:
        raise RuntimeError(f"Contact missing required fields on org: {missing}")
    fields = [f for f in (_REQUIRED_FIELDS + _OPTIONAL_FIELDS) if f in available]
    soql = f"SELECT {', '.join(fields)} FROM Contact WHERE Id = '{_escape_soql(contact_id)}'"
    result = await asyncio.to_thread(sf.query, soql)
    total = result.get("totalSize") if isinstance(result, dict) else None
    if total != 1:
        raise RuntimeError(f"Contact {contact_id} not found via SOQL (totalSize={total})")
    return result["records"][0]
