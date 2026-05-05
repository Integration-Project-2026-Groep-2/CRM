"""CRM MCP — Contact tools.

Implements: search_contact, get_contact, count_contacts, recent_contacts.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from .._util import (
    coerce_is_active,
    coerce_known_role,
    format_soql_datetime,
    is_valid_sf_id,
    parse_sf_datetime,
)
from ..escaping import escape_soql, escape_soql_like
from ..models import ContactCount, ContactDetails, ContactSummary
from ..salesforce import CrmSalesforceClient

_MIN_QUERY_LENGTH = 2
_MAX_QUERY_LENGTH = 200
_MAX_LIMIT_SEARCH = 100
_MAX_LIMIT_RECENT = 100
_MAX_RECENT_HOURS = 168  # one week


async def search_contact(
    client: CrmSalesforceClient,
    query: str,
    limit: int = 10,
) -> list[ContactSummary]:
    """Fuzzy lookup of contacts by name, email, or phone fragment."""
    stripped = query.strip()
    if len(stripped) < _MIN_QUERY_LENGTH:
        raise ValueError("query must be at least 2 characters")
    if len(stripped) > _MAX_QUERY_LENGTH:
        raise ValueError(f"query must be at most {_MAX_QUERY_LENGTH} characters")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    safe_limit = min(limit, _MAX_LIMIT_SEARCH)
    pattern = f"%{escape_soql_like(stripped)}%"
    active_field = await client.get_contact_active_field()

    soql = (
        f"SELECT Id, Name, Email, {active_field}, LastModifiedDate "
        "FROM Contact "
        f"WHERE (Name LIKE '{pattern}' "
        f"OR Email LIKE '{pattern}' "
        f"OR Phone LIKE '{pattern}') "
        "ORDER BY LastModifiedDate DESC "
        f"LIMIT {safe_limit}"
    )

    result = await client.query(soql)
    return [
        ContactSummary(
            id=r["Id"],
            name=r.get("Name") or "",
            email=r.get("Email"),
            is_active=coerce_is_active(r.get(active_field)),
            last_modified_at=parse_sf_datetime(r.get("LastModifiedDate")),
        )
        for r in result.get("records", [])
    ]


async def get_contact(
    client: CrmSalesforceClient,
    contact_id: str,
) -> ContactDetails | None:
    """Fetch full contact details by exact Salesforce Contact Id (prefix '003')."""
    if not is_valid_sf_id(contact_id, prefix="003"):
        raise ValueError(
            "contact_id must be a 15- or 18-character alphanumeric "
            "Salesforce Id starting with '003'"
        )

    safe_id = escape_soql(contact_id)
    active_field = await client.get_contact_active_field()

    soql = (
        f"SELECT Id, Name, FirstName, LastName, Email, Phone, "
        f"{active_field}, Role__c, GDPR_Consent__c, Paid_At__c, "
        f"AccountId, Account.Name, CreatedDate, LastModifiedDate "
        f"FROM Contact WHERE Id = '{safe_id}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None

    r = records[0]
    account = r.get("Account") if isinstance(r.get("Account"), dict) else None
    now = datetime.now(timezone.utc)
    return ContactDetails(
        id=r["Id"],
        name=r.get("Name") or "",
        first_name=r.get("FirstName"),
        last_name=r.get("LastName"),
        email=r.get("Email"),
        phone=r.get("Phone"),
        is_active=coerce_is_active(r.get(active_field)),
        role=coerce_known_role(r.get("Role__c")),
        gdpr_consent=bool(r.get("GDPR_Consent__c") or False),
        paid_at=parse_sf_datetime(r.get("Paid_At__c")),
        account_id=r.get("AccountId"),
        account_name=account.get("Name") if account else None,
        created_at=parse_sf_datetime(r.get("CreatedDate")) or now,
        last_modified_at=parse_sf_datetime(r.get("LastModifiedDate")) or now,
    )


async def count_contacts(
    client: CrmSalesforceClient,
    role: Literal["VISITOR", "COMPANY_CONTACT"] | None = None,
    is_active: bool | None = True,
    gdpr_consent: bool | None = None,
    has_paid: bool | None = None,
) -> ContactCount:
    """Aggregate contact counts with optional filters; returns total + breakdowns."""
    active_field = await client.get_contact_active_field()
    base_where = _build_contact_where(active_field, role, is_active, gdpr_consent, has_paid)

    total = await client.query_count(_compose("SELECT COUNT() FROM Contact", base_where))

    by_role = await _resolve_role_breakdown(client, base_where, role, total)
    by_active = await _resolve_active_breakdown(
        client, active_field, base_where, is_active, total
    )

    return ContactCount(total=total, by_role=by_role, by_active=by_active)


async def recent_contacts(
    client: CrmSalesforceClient,
    mode: Literal["created", "modified"] = "modified",
    since_hours: int = 24,
    limit: int = 20,
) -> list[ContactSummary]:
    """Recently created or modified contacts within `since_hours` hours."""
    if since_hours < 1:
        raise ValueError("since_hours must be at least 1")
    if limit < 1:
        raise ValueError("limit must be at least 1")
    safe_hours = min(since_hours, _MAX_RECENT_HOURS)
    safe_limit = min(limit, _MAX_LIMIT_RECENT)

    threshold_dt = datetime.now(timezone.utc) - timedelta(hours=safe_hours)
    threshold = format_soql_datetime(threshold_dt)
    date_field = "CreatedDate" if mode == "created" else "LastModifiedDate"
    active_field = await client.get_contact_active_field()

    soql = (
        f"SELECT Id, Name, Email, {active_field}, {date_field} "
        "FROM Contact "
        f"WHERE {date_field} >= {threshold} "
        f"ORDER BY {date_field} DESC "
        f"LIMIT {safe_limit}"
    )
    result = await client.query(soql)
    return [
        ContactSummary(
            id=r["Id"],
            name=r.get("Name") or "",
            email=r.get("Email"),
            is_active=coerce_is_active(r.get(active_field)),
            last_modified_at=parse_sf_datetime(r.get(date_field)),
        )
        for r in result.get("records", [])
    ]


# ---- helpers ----

def _build_contact_where(
    active_field: str,
    role: Literal["VISITOR", "COMPANY_CONTACT"] | None,
    is_active: bool | None,
    gdpr_consent: bool | None,
    has_paid: bool | None,
) -> str:
    clauses: list[str] = []
    if role is not None:
        clauses.append(f"Role__c = '{escape_soql(role)}'")
    if is_active is True:
        clauses.append(f"{active_field} = true")
    elif is_active is False:
        clauses.append(f"{active_field} = false")
    if gdpr_consent is True:
        clauses.append("GDPR_Consent__c = true")
    elif gdpr_consent is False:
        clauses.append("GDPR_Consent__c = false")
    if has_paid is True:
        clauses.append("Paid_At__c != null")
    elif has_paid is False:
        clauses.append("Paid_At__c = null")
    return " AND ".join(clauses)


def _compose(select_clause: str, where: str) -> str:
    return f"{select_clause} WHERE {where}" if where else select_clause


def _and_combine(base_where: str, extra: str) -> str:
    return f"{base_where} AND {extra}" if base_where else extra


async def _resolve_role_breakdown(
    client: CrmSalesforceClient,
    base_where: str,
    role_filter: Literal["VISITOR", "COMPANY_CONTACT"] | None,
    total: int,
) -> dict[str, int]:
    if role_filter is not None:
        return {
            "VISITOR": total if role_filter == "VISITOR" else 0,
            "COMPANY_CONTACT": total if role_filter == "COMPANY_CONTACT" else 0,
            "UNKNOWN": 0,
        }

    breakdown: dict[str, int] = {}
    for role_value in ("VISITOR", "COMPANY_CONTACT"):
        composed = _and_combine(base_where, f"Role__c = '{role_value}'")
        soql = _compose("SELECT COUNT() FROM Contact", composed)
        breakdown[role_value] = await client.query_count(soql)
    breakdown["UNKNOWN"] = max(
        total - breakdown["VISITOR"] - breakdown["COMPANY_CONTACT"], 0
    )
    return breakdown


async def _resolve_active_breakdown(
    client: CrmSalesforceClient,
    active_field: str,
    base_where: str,
    is_active_filter: bool | None,
    total: int,
) -> dict[str, int]:
    if is_active_filter is True:
        return {"active": total, "inactive": 0}
    if is_active_filter is False:
        return {"active": 0, "inactive": total}

    composed = _and_combine(base_where, f"{active_field} = true")
    soql = _compose("SELECT COUNT() FROM Contact", composed)
    active_count = await client.query_count(soql)
    return {"active": active_count, "inactive": max(total - active_count, 0)}
