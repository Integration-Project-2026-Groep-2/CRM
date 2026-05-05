"""CRM MCP — Company tools (Salesforce Account).

Implements: search_company, get_company.

The Salesforce object is `Account` but the tool name follows the project's
domain language (`<Company>` in XML Contracts 3, 14, 19, 23). This is an
anti-corruption layer between Salesforce internals and the AI master agent.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .._util import (
    coerce_is_active,
    is_valid_sf_id,
    parse_sf_datetime,
    validate_vat_number,
)
from ..escaping import escape_soql, escape_soql_like
from ..models import CompanyDetails, CompanySummary
from ..salesforce import CrmSalesforceClient

_MIN_QUERY_LENGTH = 2
_MAX_QUERY_LENGTH = 200
_MAX_LIMIT_SEARCH = 100


async def search_company(
    client: CrmSalesforceClient,
    query: str,
    limit: int = 10,
) -> list[CompanySummary]:
    """Fuzzy lookup of companies by name or VAT-number fragment."""
    stripped = query.strip()
    if len(stripped) < _MIN_QUERY_LENGTH:
        raise ValueError("query must be at least 2 characters")
    if len(stripped) > _MAX_QUERY_LENGTH:
        raise ValueError(f"query must be at most {_MAX_QUERY_LENGTH} characters")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    safe_limit = min(limit, _MAX_LIMIT_SEARCH)
    pattern = f"%{escape_soql_like(stripped)}%"

    soql = (
        "SELECT Id, Name, VAT_Number__c, BillingCountryCode "
        "FROM Account "
        f"WHERE (Name LIKE '{pattern}' OR VAT_Number__c LIKE '{pattern}') "
        "ORDER BY Name "
        f"LIMIT {safe_limit}"
    )
    result = await client.query(soql)
    return [
        CompanySummary(
            id=r["Id"],
            name=r.get("Name") or "",
            vat_number=r.get("VAT_Number__c"),
            country=r.get("BillingCountryCode"),
        )
        for r in result.get("records", [])
    ]


async def get_company(
    client: CrmSalesforceClient,
    vat_number: str | None = None,
    company_id: str | None = None,
) -> CompanyDetails | None:
    """Fetch full company details by VAT-number (preferred) or by Account Id.

    VAT-number is the canonical business key per Contract 3 deduplication
    rule. If both arguments are provided, `vat_number` wins.
    """
    if vat_number is None and company_id is None:
        raise ValueError("provide either vat_number or company_id")

    active_field = await client.get_account_active_field()
    fields = (
        "Id, Name, VAT_Number__c, BillingStreet, BillingCity, "
        "BillingPostalCode, BillingCountryCode, CreatedDate, LastModifiedDate"
    )
    if active_field:
        fields = f"{fields}, {active_field}"

    if vat_number is not None:
        validate_vat_number(vat_number)
        soql = (
            f"SELECT {fields} FROM Account "
            f"WHERE VAT_Number__c = '{escape_soql(vat_number)}' LIMIT 1"
        )
    else:
        # company_id is not None here (guarded above).
        if company_id is None or not is_valid_sf_id(company_id, prefix="001"):
            raise ValueError(
                "company_id must be a 15- or 18-character alphanumeric "
                "Salesforce Id starting with '001'"
            )
        soql = (
            f"SELECT {fields} FROM Account "
            f"WHERE Id = '{escape_soql(company_id)}' LIMIT 1"
        )

    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None

    r = records[0]
    now = datetime.now(timezone.utc)
    return CompanyDetails(
        id=r["Id"],
        name=r.get("Name") or "",
        vat_number=r.get("VAT_Number__c"),
        street=r.get("BillingStreet"),
        city=r.get("BillingCity"),
        postal_code=r.get("BillingPostalCode"),
        country=r.get("BillingCountryCode"),
        is_active=coerce_is_active(r.get(active_field)) if active_field else True,
        created_at=parse_sf_datetime(r.get("CreatedDate")) or now,
        last_modified_at=parse_sf_datetime(r.get("LastModifiedDate")) or now,
    )
