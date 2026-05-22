"""CRM MCP — Company tools (Salesforce Account).

Read-only: search_company, get_company.
Write (R2): create_company, update_company, delete_company. Each write does a
Salesforce upsert plus an outbound XSD-validated XML broadcast (`crm.company.*`)
so all consumer teams stay in sync — see `R2_PLANNING_AGENTIC_GATEWAY.md` and
`src/sender.py`.

The Salesforce object is `Account` but the tool name follows the project's
domain language (`<Company>` in XML Contracts 3, 14, 19, 23). This is an
anti-corruption layer between Salesforce internals and the AI master agent.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .._util import (
    VALID_USER_ROLES,
    coerce_is_active,
    format_soql_datetime,
    is_valid_sf_id,
    parse_sf_datetime,
    require_crm_id,
    validate_vat_number,
)
from ..escaping import escape_soql, escape_soql_like
from ..messaging import MessagePublisher
from ..models import CompanyContactSummary, CompanyCount, CompanyDetails, CompanyProfile, CompanySummary, MutationResult
from ..salesforce import CrmSalesforceClient

logger = logging.getLogger(__name__)

_MIN_QUERY_LENGTH = 2
_MAX_QUERY_LENGTH = 200
_MAX_LIMIT_SEARCH = 100
_MAX_LIMIT_COMPANY_CONTACTS = 100
_MAX_LIMIT_LIST_COMPANIES = 100
_MAX_RECENT_HOURS_COMPANIES = 168
_MAX_LIMIT_RECENT_COMPANIES = 100
_CRM_ID_FIELD = "CRM_ID__c"


def _now_iso() -> str:
    """UTC RFC3339 timestamp matching XSD `ISO8601DateTimeType`."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _find_account_by_crm_id(
    client: CrmSalesforceClient, crm_id: str
) -> dict[str, Any] | None:
    soql = (
        f"SELECT Id, Name, VAT_Number__c FROM Account "
        f"WHERE {_CRM_ID_FIELD} = '{escape_soql(crm_id)}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    return records[0] if records else None


async def _resolve_account_record(
    client: CrmSalesforceClient, ref: str
) -> dict[str, Any] | None:
    """Look up an Account by CRM_ID__c UUID OR Salesforce Id (001-prefix).

    Drop-in replacement for `_find_account_by_crm_id` that accepts both formats.
    The return shape additionally includes CRM_ID__c so callers can echo the
    canonical UUID in MutationResult regardless of input format.
    """
    if is_valid_sf_id(ref, prefix="001"):
        where_clause = f"Id = '{escape_soql(ref)}'"
    else:
        where_clause = f"{_CRM_ID_FIELD} = '{escape_soql(ref)}'"
    soql = (
        f"SELECT Id, {_CRM_ID_FIELD}, Name, VAT_Number__c FROM Account "
        f"WHERE {where_clause} LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    return records[0] if records else None


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
        soql = f"SELECT {fields} FROM Account WHERE Id = '{escape_soql(company_id)}' LIMIT 1"

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


async def get_company_contacts(
    client: CrmSalesforceClient,
    vat_number: str | None = None,
    company_id: str | None = None,
    role: str | None = None,
    is_active: bool | None = True,
    limit: int = 20,
) -> list[CompanyContactSummary]:
    """List all contacts linked to a specific company.

    Provide `vat_number` (preferred, canonical key) or `company_id` (CRM_ID__c UUID
    or 001-prefix Salesforce Id). Optional filters: `role` (Role__c picklist value),
    `is_active` (default: active only). Results ordered by LastModifiedDate DESC.
    """
    if vat_number is None and company_id is None:
        raise ValueError("provide either vat_number or company_id")
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if role is not None and role not in VALID_USER_ROLES:
        raise ValueError(
            f"role must be one of {sorted(VALID_USER_ROLES)}"
        )

    safe_limit = min(limit, _MAX_LIMIT_COMPANY_CONTACTS)

    if vat_number is not None:
        validate_vat_number(vat_number)
        result = await client.query(
            f"SELECT Id FROM Account WHERE VAT_Number__c = '{escape_soql(vat_number)}' LIMIT 1"
        )
        records = result.get("records", [])
        if not records:
            raise ValueError(f"no company found with vat_number '{vat_number}'")
        account_sf_id = records[0]["Id"]
    else:
        existing = await _resolve_account_record(client, company_id)  # type: ignore[arg-type]
        if existing is None:
            raise ValueError(f"no company found with id '{company_id}'")
        account_sf_id = str(existing["Id"])

    active_field = await client.get_contact_active_field()
    clauses: list[str] = [f"AccountId = '{escape_soql(account_sf_id)}'"]
    if role is not None:
        clauses.append(f"Role__c = '{escape_soql(role)}'")
    if is_active is True:
        clauses.append(f"{active_field} = true")
    elif is_active is False:
        clauses.append(f"{active_field} = false")

    where = " AND ".join(clauses)
    soql = (
        f"SELECT Id, Name, Email, Role__c, {active_field}, Paid_At__c "
        f"FROM Contact "
        f"WHERE {where} "
        f"ORDER BY LastModifiedDate DESC "
        f"LIMIT {safe_limit}"
    )
    result = await client.query(soql)
    return [
        CompanyContactSummary(
            id=r["Id"],
            name=r.get("Name") or "",
            email=r.get("Email"),
            role=r.get("Role__c"),
            is_active=coerce_is_active(r.get(active_field)),
            paid_at=parse_sf_datetime(r.get("Paid_At__c")),
        )
        for r in result.get("records", [])
    ]


async def list_companies(
    client: CrmSalesforceClient,
    country: str | None = None,
    is_active: bool | None = True,
    limit: int = 20,
    offset: int = 0,
) -> list[CompanySummary]:
    """Browse all companies with optional filters and pagination.

    Unlike `search_company` (which requires a query string), this tool enumerates
    companies. `country` must be ISO 3166-1 alpha-2 (e.g. 'BE'). `is_active`
    defaults to active-only; pass `None` for all. Results ordered by Name.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if country is not None:
        if len(country) != 2 or not country.isalpha():
            raise ValueError("country must be ISO 3166-1 alpha-2 (2 letters, e.g. 'BE')")
        country = country.upper()

    safe_limit = min(limit, _MAX_LIMIT_LIST_COMPANIES)
    country_field = await client.get_account_country_field()
    active_field = await client.get_account_active_field()

    clauses: list[str] = []
    if country is not None:
        clauses.append(f"{country_field} = '{escape_soql(country)}'")
    if is_active is True and active_field:
        clauses.append(f"{active_field} = true")
    elif is_active is False and active_field:
        clauses.append(f"{active_field} = false")

    where_clause = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    soql = (
        f"SELECT Id, Name, VAT_Number__c, BillingCountryCode "
        f"FROM Account "
        f"{where_clause}"
        f"ORDER BY Name "
        f"LIMIT {safe_limit} OFFSET {offset}"
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


async def get_recent_companies(
    client: CrmSalesforceClient,
    mode: Literal["created", "modified"] = "modified",
    since_hours: int = 24,
    limit: int = 20,
) -> list[CompanySummary]:
    """Recently created or modified companies within `since_hours` hours.

    `mode` selects the date field: 'created' uses CreatedDate, 'modified'
    (default) uses LastModifiedDate. since_hours capped at 168 (one week),
    limit capped at 100.
    """
    if since_hours < 1:
        raise ValueError("since_hours must be at least 1")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    safe_hours = min(since_hours, _MAX_RECENT_HOURS_COMPANIES)
    safe_limit = min(limit, _MAX_LIMIT_RECENT_COMPANIES)
    threshold = format_soql_datetime(
        datetime.now(timezone.utc) - timedelta(hours=safe_hours)
    )
    date_field = "CreatedDate" if mode == "created" else "LastModifiedDate"

    soql = (
        f"SELECT Id, Name, VAT_Number__c, BillingCountryCode, {date_field} "
        f"FROM Account "
        f"WHERE {date_field} >= {threshold} "
        f"ORDER BY {date_field} DESC "
        f"LIMIT {safe_limit}"
    )
    result = await client.query(soql)
    return [
        CompanySummary(
            id=r["Id"],
            name=r.get("Name") or "",
            vat_number=r.get("VAT_Number__c"),
            country=r.get("BillingCountryCode"),
            last_modified_at=parse_sf_datetime(r.get(date_field)),
        )
        for r in result.get("records", [])
    ]


async def get_company_profile(
    client: CrmSalesforceClient,
    vat_number: str | None = None,
    company_id: str | None = None,
) -> CompanyProfile | None:
    """Fetch company details plus linked contact count in one call.

    Provide `vat_number` (canonical key) or `company_id` (CRM_ID__c UUID or
    001-prefix Salesforce Id). Returns None if not found.
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
        record = await _resolve_account_record(client, company_id)  # type: ignore[arg-type]
        if record is None:
            return None
        sf_id = str(record["Id"])
        soql = f"SELECT {fields} FROM Account WHERE Id = '{escape_soql(sf_id)}' LIMIT 1"

    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None
    r = records[0]
    sf_id = str(r["Id"])

    contact_count = await client.query_count(
        f"SELECT COUNT() FROM Contact WHERE AccountId = '{escape_soql(sf_id)}'"
    )

    now = datetime.now(timezone.utc)
    return CompanyProfile(
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
        contact_count=contact_count,
    )


async def count_companies(
    client: CrmSalesforceClient,
    country: str | None = None,
) -> CompanyCount:
    """Aggregate company counts with active/inactive breakdown.

    Optional `country` filter (ISO 3166-1 alpha-2, e.g. 'BE'). When the org
    has no Account active-flag field, `active` and `inactive` are both 0.
    """
    if country is not None:
        if len(country) != 2 or not country.isalpha():
            raise ValueError("country must be ISO 3166-1 alpha-2 (2 letters, e.g. 'BE')")
        country = country.upper()

    active_field = await client.get_account_active_field()
    country_field = await client.get_account_country_field()

    base_clauses: list[str] = []
    if country is not None:
        base_clauses.append(f"{country_field} = '{escape_soql(country)}'")
    base_where = " AND ".join(base_clauses)

    total_soql = "SELECT COUNT() FROM Account"
    if base_where:
        total_soql += f" WHERE {base_where}"

    if active_field:
        active_clause = f"{active_field} = true"
        if base_where:
            active_clause = f"{base_where} AND {active_clause}"
        active_soql = f"SELECT COUNT() FROM Account WHERE {active_clause}"
        total, active_count = await asyncio.gather(
            client.query_count(total_soql),
            client.query_count(active_soql),
        )
        inactive_count = max(total - active_count, 0)
    else:
        total = await client.query_count(total_soql)
        active_count = 0
        inactive_count = 0

    return CompanyCount(total=total, active=active_count, inactive=inactive_count)


# ---------------------------------------------------------------------------
# Write tools — R2 actionable agent
# ---------------------------------------------------------------------------


async def create_company(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    name: str,
    vat_number: str,
    email: str,
    street: str,
    house_number: str,
    postal_code: str,
    city: str,
    country: str,
    phone: str | None = None,
) -> MutationResult:
    """Create an Account in Salesforce and broadcast C14 `<CompanyConfirmed>`.

    XSD-required fields all map directly. `country` must be ISO 3166-1 alpha-2.
    VAT-uniqueness is pre-checked (Contract 3 dedup-rule); a duplicate raises
    ValueError without touching Salesforce.
    """
    if not name.strip():
        raise ValueError("name must not be empty")
    validate_vat_number(vat_number)

    existing = await client.query(
        f"SELECT Id FROM Account WHERE VAT_Number__c = '{escape_soql(vat_number)}' LIMIT 1"
    )
    if existing.get("records"):
        raise ValueError(f"vat_number '{vat_number}' already exists")

    crm_id = str(uuid.uuid4())
    country_field = await client.get_account_country_field()
    sf_payload: dict[str, Any] = {
        "Name": name,
        "VAT_Number__c": vat_number,
        _CRM_ID_FIELD: crm_id,
        "BillingStreet": street,
        "BillingPostalCode": postal_code,
        "BillingCity": city,
        country_field: country,
    }
    if await client.has_account_house_number_field():
        sf_payload["House_Number__c"] = house_number
    email_field = await client.get_account_email_field()
    if email_field:
        sf_payload[email_field] = email
    if phone:
        sf_payload["Phone"] = phone

    active_field = await client.get_account_active_field()
    if active_field:
        sf_payload[active_field] = True

    sf_response = await client.create_account(sf_payload)
    sf_id = str(sf_response["id"])

    confirmed_at = _now_iso()
    company_data: dict[str, Any] = {
        "id": crm_id,
        "vatNumber": vat_number,
        "name": name,
        "email": email,
        "street": street,
        "houseNumber": house_number,
        "postalCode": postal_code,
        "city": city,
        "country": country,
        "isActive": True,
        "confirmedAt": confirmed_at,
    }
    if phone:
        company_data["phone"] = phone
    publisher.publish_company_confirmed(company_data)
    logger.info(
        "MCP write-tool create_company succeeded crm_id=%s sf_id=%s", crm_id, sf_id
    )

    return MutationResult(
        id=crm_id,
        success=True,
        routing_key="crm.company.confirmed",
        salesforce_id=sf_id,
    )


async def update_company(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    crm_id: str,
    name: str | None = None,
    vat_number: str | None = None,
    email: str | None = None,
    phone: str | None = None,
    street: str | None = None,
    house_number: str | None = None,
    postal_code: str | None = None,
    city: str | None = None,
    country: str | None = None,
    is_active: bool | None = None,
) -> MutationResult:
    """Patch an Account and broadcast C19 `<CompanyUpdated>`.

    `crm_id` accepts either the CRM_ID__c UUID (canonical, returned by other
    tools as `crm_id`) or the Salesforce Id (001-prefix, returned as `id`).
    The MutationResult always echoes the canonical UUID regardless of input
    format.

    Partial-update friendly: omitted fields are left unchanged in Salesforce.
    XSD-required fields (vatNumber, name, isActive) on the broadcast payload
    fall back to the existing record values when the caller doesn't override.
    """
    if not crm_id.strip():
        raise ValueError("crm_id must not be empty")
    if vat_number is not None:
        validate_vat_number(vat_number)

    existing = await _resolve_account_record(client, crm_id)
    if existing is None:
        raise ValueError(f"no company found with id '{crm_id}'")
    sf_id = str(existing["Id"])
    canonical_crm_id = require_crm_id(existing, _CRM_ID_FIELD, "company", sf_id)
    existing_vat = existing.get("VAT_Number__c")
    existing_name = existing.get("Name") or ""

    sf_payload: dict[str, Any] = {}
    if name is not None:
        sf_payload["Name"] = name
    if vat_number is not None:
        sf_payload["VAT_Number__c"] = vat_number
    if street is not None:
        sf_payload["BillingStreet"] = street
    if house_number is not None and await client.has_account_house_number_field():
        sf_payload["House_Number__c"] = house_number
    if postal_code is not None:
        sf_payload["BillingPostalCode"] = postal_code
    if city is not None:
        sf_payload["BillingCity"] = city
    if country is not None:
        country_field = await client.get_account_country_field()
        sf_payload[country_field] = country
    if email is not None:
        email_field = await client.get_account_email_field()
        if email_field:
            sf_payload[email_field] = email
    if phone is not None:
        sf_payload["Phone"] = phone

    active_field = await client.get_account_active_field()
    if is_active is not None and active_field:
        sf_payload[active_field] = is_active

    if sf_payload:
        await client.update_account(sf_id, sf_payload)

    final_vat = vat_number if vat_number is not None else existing_vat
    if not final_vat:
        raise ValueError("vat_number missing on existing record — cannot publish C19")
    final_name = name if name is not None else existing_name
    final_is_active = is_active if is_active is not None else True
    updated_at = _now_iso()

    company_data: dict[str, Any] = {
        "id": canonical_crm_id,
        "vatNumber": final_vat,
        "name": final_name,
        "isActive": final_is_active,
        "updatedAt": updated_at,
    }
    for key, val in (
        ("email", email),
        ("phone", phone),
        ("street", street),
        ("houseNumber", house_number),
        ("postalCode", postal_code),
        ("city", city),
        ("country", country),
    ):
        if val is not None:
            company_data[key] = val
    publisher.publish_company_updated(company_data)
    logger.info(
        "MCP write-tool update_company succeeded crm_id=%s sf_id=%s",
        canonical_crm_id,
        sf_id,
    )

    return MutationResult(
        id=canonical_crm_id,
        success=True,
        routing_key="crm.company.updated",
        salesforce_id=sf_id,
    )


async def delete_company(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    crm_id: str,
    hard: bool = False,
) -> MutationResult:
    """Soft-delete an Account (IsActive__c=false) and broadcast C23 `<CompanyDeactivated>`.

    `crm_id` accepts either the CRM_ID__c UUID or the Salesforce Id (001-prefix).
    The MutationResult always echoes the canonical UUID.

    `hard=True` is rejected — XSD has no hard-delete contract and the
    project-policy in `src/salesforce/accounts.py:388-390` forbids hard-deletes
    on Accounts to preserve relationship-integrity.
    """
    if hard:
        raise ValueError("hard-delete not supported on Account — use soft-delete (default)")
    if not crm_id.strip():
        raise ValueError("crm_id must not be empty")

    existing = await _resolve_account_record(client, crm_id)
    if existing is None:
        raise ValueError(f"no company found with id '{crm_id}'")
    sf_id = str(existing["Id"])
    canonical_crm_id = require_crm_id(existing, _CRM_ID_FIELD, "company", sf_id)
    vat_number = existing.get("VAT_Number__c")
    if not vat_number:
        raise ValueError("vat_number missing on existing record — cannot publish C23")

    active_field = await client.get_account_active_field()
    if active_field is None:
        raise RuntimeError(
            "Account has no active-flag field — cannot soft-delete. "
            "Org must expose IsActive__c, Active__c, or Is_Active__c."
        )
    await client.update_account(sf_id, {active_field: False})

    company_data = {
        "id": canonical_crm_id,
        "vatNumber": vat_number,
        "deactivatedAt": _now_iso(),
    }
    publisher.publish_company_deactivated(company_data)
    logger.info(
        "MCP write-tool delete_company succeeded crm_id=%s sf_id=%s",
        canonical_crm_id,
        sf_id,
    )

    return MutationResult(
        id=canonical_crm_id,
        success=True,
        routing_key="crm.company.deactivated",
        salesforce_id=sf_id,
    )
