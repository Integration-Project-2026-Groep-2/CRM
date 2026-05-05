"""CRM MCP — Registration tools (Salesforce Session_Registration__c).

Implements: count_registrations.
"""

from __future__ import annotations

from datetime import datetime

from .._util import format_soql_datetime
from ..escaping import escape_soql
from ..models import RegistrationCount
from ..salesforce import CrmSalesforceClient


async def count_registrations(
    client: CrmSalesforceClient,
    session_id: str | None = None,
    is_active: bool | None = True,
    paid: bool | None = None,
    since: datetime | None = None,
) -> RegistrationCount:
    """Aggregate Session_Registration__c counts with optional filters.

    `since` must be timezone-aware (rejected if naive to avoid silent
    off-by-hours bugs).
    """
    base_where = _build_where(session_id, is_active, since)

    # Branch on the `paid` filter so we issue only the queries we need.
    # When paid is fixed, the breakdown collapses and 1 query suffices.
    if paid is True:
        paid_count = await client.query_count(_compose(base_where, "Paid_At__c != null"))
        return RegistrationCount(total=paid_count, paid=paid_count, unpaid=0)
    if paid is False:
        unpaid_count = await client.query_count(_compose(base_where, "Paid_At__c = null"))
        return RegistrationCount(total=unpaid_count, paid=0, unpaid=unpaid_count)

    # No paid filter → return full breakdown (3 queries).
    total = await client.query_count(_compose(base_where))
    paid_count = await client.query_count(_compose(base_where, "Paid_At__c != null"))
    unpaid_count = await client.query_count(_compose(base_where, "Paid_At__c = null"))
    return RegistrationCount(total=total, paid=paid_count, unpaid=unpaid_count)


def _build_where(
    session_id: str | None,
    is_active: bool | None,
    since: datetime | None,
) -> str:
    clauses: list[str] = []
    if session_id:
        clauses.append(f"Session_ID__c = '{escape_soql(session_id)}'")
    if is_active is True:
        clauses.append("Is_Active__c = true")
    elif is_active is False:
        clauses.append("Is_Active__c = false")
    if since is not None:
        clauses.append(f"LastModifiedDate >= {format_soql_datetime(since)}")
    return " AND ".join(clauses)


def _compose(base_where: str, extra: str | None = None) -> str:
    parts = [base_where]
    if extra:
        parts.append(extra)
    where = " AND ".join(p for p in parts if p)
    base = "SELECT COUNT() FROM Session_Registration__c"
    return f"{base} WHERE {where}" if where else base
