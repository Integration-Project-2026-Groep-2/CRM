"""CRM MCP — Contact tools.

Read-only: search_contact, get_contact, count_contacts, recent_contacts.
Write (R2): create_contact, update_contact, delete_contact. Each write does a
Salesforce upsert plus an outbound XSD-validated XML broadcast (`crm.user.*`)
so all consumer teams stay in sync — see `R2_PLANNING_AGENTIC_GATEWAY.md` and
`src/sender.py`.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from .._util import (
    VALID_USER_ROLES,
    coerce_is_active,
    coerce_known_role,
    format_soql_datetime,
    is_valid_sf_id,
    parse_sf_datetime,
)
from ..escaping import escape_soql, escape_soql_like
from ..messaging import MessagePublisher
from ..models import ContactActivitySummary, ContactCount, ContactDetails, ContactSummary, MutationResult
from ..salesforce import CrmSalesforceClient

logger = logging.getLogger(__name__)

_MIN_QUERY_LENGTH = 2
_MAX_QUERY_LENGTH = 200
_MAX_LIMIT_SEARCH = 100
_MAX_LIMIT_RECENT = 100
_MAX_RECENT_HOURS = 168  # one week
_CRM_ID_FIELD = "CRM_ID__c"

_VALID_USER_ROLES = VALID_USER_ROLES


def _now_iso() -> str:
    """UTC RFC3339 timestamp matching XSD `ISO8601DateTimeType`."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _find_contact_by_crm_id(
    client: CrmSalesforceClient, crm_id: str
) -> dict[str, Any] | None:
    """Fetch the columns needed for partial-update fallbacks per XSD C18."""
    active_field = await client.get_contact_active_field()
    soql = (
        "SELECT Id, Email, FirstName, LastName, Role__c, GDPR_Consent__c, "
        f"Company_ID__c, {active_field} FROM Contact "
        f"WHERE {_CRM_ID_FIELD} = '{escape_soql(crm_id)}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    return records[0] if records else None


async def _resolve_contact_record(
    client: CrmSalesforceClient, ref: str
) -> dict[str, Any] | None:
    """Look up a Contact by CRM_ID__c UUID OR Salesforce Id (003-prefix).

    Drop-in replacement for `_find_contact_by_crm_id` that accepts both formats.
    The return shape additionally includes CRM_ID__c so callers can echo the
    canonical UUID in MutationResult regardless of input format.
    """
    if is_valid_sf_id(ref, prefix="003"):
        where_clause = f"Id = '{escape_soql(ref)}'"
    else:
        where_clause = f"{_CRM_ID_FIELD} = '{escape_soql(ref)}'"
    active_field = await client.get_contact_active_field()
    soql = (
        f"SELECT Id, {_CRM_ID_FIELD}, Email, FirstName, LastName, Role__c, "
        f"GDPR_Consent__c, Company_ID__c, {active_field} "
        f"FROM Contact WHERE {where_clause} LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    return records[0] if records else None


async def _resolve_account_record_for_contact(
    client: CrmSalesforceClient, ref: str
) -> dict[str, Any] | None:
    """Resolve a Company reference (CRM_ID__c UUID or Salesforce Id 001-prefix).

    Returns a dict with `Id` and `CRM_ID__c`, or None when not found. Used by
    create/update_contact when linking a Contact to an Account: the
    Company_ID__c custom field stores the canonical CRM_ID__c UUID regardless
    of which format the caller provided.
    """
    if is_valid_sf_id(ref, prefix="001"):
        where_clause = f"Id = '{escape_soql(ref)}'"
    else:
        where_clause = f"{_CRM_ID_FIELD} = '{escape_soql(ref)}'"
    soql = f"SELECT Id, {_CRM_ID_FIELD} FROM Account WHERE {where_clause} LIMIT 1"
    result = await client.query(soql)
    records = result.get("records", [])
    return records[0] if records else None


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


def _map_contact_record(r: dict[str, Any], active_field: str) -> ContactDetails:
    """Map a raw Salesforce Contact record to ContactDetails."""
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


def _contact_detail_fields(active_field: str) -> str:
    """SOQL field list shared by all ContactDetails lookups."""
    return (
        f"Id, Name, FirstName, LastName, Email, Phone, "
        f"{active_field}, Role__c, GDPR_Consent__c, Paid_At__c, "
        f"AccountId, Account.Name, CreatedDate, LastModifiedDate"
    )


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
        f"SELECT {_contact_detail_fields(active_field)} "
        f"FROM Contact WHERE Id = '{safe_id}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None
    return _map_contact_record(records[0], active_field)


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
    by_active = await _resolve_active_breakdown(client, active_field, base_where, is_active, total)

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
    breakdown["UNKNOWN"] = max(total - breakdown["VISITOR"] - breakdown["COMPANY_CONTACT"], 0)
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


# ---------------------------------------------------------------------------
# Write tools — R2 actionable agent
# ---------------------------------------------------------------------------


async def create_contact(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    email: str,
    first_name: str,
    last_name: str,
    role: str,
    gdpr_consent: bool,
    phone: str | None = None,
    company_id: str | None = None,
    badge_code: str | None = None,
) -> MutationResult:
    """Create a Contact in Salesforce and broadcast C13 `<UserConfirmed>`.

    `role` is the C13 `UserRoleType` enum (VISITOR, COMPANY_CONTACT, SPEAKER,
    EVENT_MANAGER, CASHIER, BAR_STAFF, ADMIN). `company_id` accepts either the
    Account's CRM_ID__c UUID or its Salesforce Id (001-prefix); the canonical
    UUID is stored in the Contact's `Company_ID__c` custom field (matches the
    project's Frontend/Facturatie/Mailing handler convention, not the standard
    `AccountId` foreign-key). Email-uniqueness is pre-checked.
    """
    if not email.strip():
        raise ValueError("email must not be empty")
    if not first_name.strip() or not last_name.strip():
        raise ValueError("first_name and last_name must not be empty")
    if role not in _VALID_USER_ROLES:
        raise ValueError(f"role must be one of {sorted(_VALID_USER_ROLES)}, got '{role}'")

    existing = await client.query(
        f"SELECT Id FROM Contact WHERE Email = '{escape_soql(email)}' LIMIT 1"
    )
    if existing.get("records"):
        raise ValueError(f"email '{email}' already exists")

    company_uuid: str | None = None
    if company_id is not None:
        company_record = await _resolve_account_record_for_contact(client, company_id)
        if company_record is None:
            raise ValueError(f"no company found with id '{company_id}'")
        company_uuid = company_record[_CRM_ID_FIELD]

    crm_id = str(uuid.uuid4())
    sf_payload: dict[str, Any] = {
        "FirstName": first_name,
        "LastName": last_name,
        "Email": email,
        "Role__c": role,
        "GDPR_Consent__c": gdpr_consent,
        _CRM_ID_FIELD: crm_id,
    }
    if phone:
        sf_payload["Phone"] = phone
    if company_uuid:
        sf_payload["Company_ID__c"] = company_uuid
    if badge_code:
        sf_payload["Badge_Code__c"] = badge_code

    active_field = await client.get_contact_active_field()
    sf_payload[active_field] = True

    sf_response = await client.create_contact(sf_payload)
    sf_id = str(sf_response["id"])

    user_data: dict[str, Any] = {
        "id": crm_id,
        "email": email,
        "firstName": first_name,
        "lastName": last_name,
        "role": role,
        "isActive": True,
        "gdprConsent": gdpr_consent,
        "confirmedAt": _now_iso(),
    }
    if phone:
        user_data["phone"] = phone
    if company_uuid:
        user_data["companyId"] = company_uuid
    if badge_code:
        user_data["badgeCode"] = badge_code

    publisher.publish_user_confirmed(user_data)
    logger.info(
        "MCP write-tool create_contact succeeded crm_id=%s sf_id=%s", crm_id, sf_id
    )

    return MutationResult(
        id=crm_id,
        success=True,
        routing_key="crm.user.confirmed",
        salesforce_id=sf_id,
    )


async def update_contact(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    crm_id: str,
    email: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    role: str | None = None,
    phone: str | None = None,
    company_id: str | None = None,
    badge_code: str | None = None,
    gdpr_consent: bool | None = None,
    is_active: bool | None = None,
) -> MutationResult:
    """Patch a Contact and broadcast C18 `<UserUpdated>`.

    `crm_id` accepts either the CRM_ID__c UUID (canonical, returned by other
    tools as `crm_id`) or the Salesforce Id (003-prefix, returned as `id`).
    `company_id` accepts the Account's CRM_ID__c UUID or its Salesforce Id
    (001-prefix). The MutationResult always echoes the canonical UUID
    regardless of input format.

    Partial-update friendly: omitted fields are left unchanged in Salesforce.
    XSD-required broadcast fields (email/firstName/lastName/role/isActive)
    fall back to existing record values when the caller doesn't override.
    """
    if not crm_id.strip():
        raise ValueError("crm_id must not be empty")
    if role is not None and role not in _VALID_USER_ROLES:
        raise ValueError(f"role must be one of {sorted(_VALID_USER_ROLES)}, got '{role}'")

    existing = await _resolve_contact_record(client, crm_id)
    if existing is None:
        raise ValueError(f"no contact found with id '{crm_id}'")
    sf_id = str(existing["Id"])
    canonical_crm_id = str(existing[_CRM_ID_FIELD])

    sf_payload: dict[str, Any] = {}
    if first_name is not None:
        sf_payload["FirstName"] = first_name
    if last_name is not None:
        sf_payload["LastName"] = last_name
    if email is not None:
        sf_payload["Email"] = email
    if role is not None:
        sf_payload["Role__c"] = role
    if phone is not None:
        sf_payload["Phone"] = phone
    if gdpr_consent is not None:
        sf_payload["GDPR_Consent__c"] = gdpr_consent
    if badge_code is not None:
        sf_payload["Badge_Code__c"] = badge_code
    company_uuid: str | None = None
    if company_id is not None:
        company_record = await _resolve_account_record_for_contact(client, company_id)
        if company_record is None:
            raise ValueError(f"no company found with id '{company_id}'")
        company_uuid = company_record[_CRM_ID_FIELD]
        sf_payload["Company_ID__c"] = company_uuid

    active_field = await client.get_contact_active_field()
    if is_active is not None:
        sf_payload[active_field] = is_active

    if sf_payload:
        await client.update_contact(sf_id, sf_payload)

    final_email = email if email is not None else existing.get("Email")
    final_first = first_name if first_name is not None else existing.get("FirstName")
    final_last = last_name if last_name is not None else existing.get("LastName")
    final_role = role if role is not None else existing.get("Role__c")
    if not (final_email and final_first and final_last and final_role):
        raise ValueError(
            "existing record missing one of email/firstName/lastName/role — "
            "cannot publish C18 without all required fields"
        )
    # XSD C18 requires gdprConsent + isActive — fall back to existing record
    # when caller omits, else we silently flip booleans / break XSD validation.
    final_gdpr_consent = (
        gdpr_consent
        if gdpr_consent is not None
        else bool(existing.get("GDPR_Consent__c"))
    )
    final_is_active = (
        is_active
        if is_active is not None
        else coerce_is_active(existing.get(active_field))
    )

    user_data: dict[str, Any] = {
        "id": canonical_crm_id,
        "email": final_email,
        "firstName": final_first,
        "lastName": final_last,
        "role": final_role,
        "isActive": final_is_active,
        "gdprConsent": final_gdpr_consent,
        "updatedAt": _now_iso(),
    }
    if phone is not None:
        user_data["phone"] = phone
    if company_uuid is not None:
        user_data["companyId"] = company_uuid
    if badge_code is not None:
        user_data["badgeCode"] = badge_code

    publisher.publish_user_updated(user_data)
    logger.info(
        "MCP write-tool update_contact succeeded crm_id=%s sf_id=%s",
        canonical_crm_id,
        sf_id,
    )

    return MutationResult(
        id=canonical_crm_id,
        success=True,
        routing_key="crm.user.updated",
        salesforce_id=sf_id,
    )


async def delete_contact(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    crm_id: str,
    hard: bool = False,
) -> MutationResult:
    """Soft-delete a Contact (IsActive__c=false) and broadcast C22 `<UserDeactivated>`.

    `crm_id` accepts either the CRM_ID__c UUID or the Salesforce Id (003-prefix).
    The MutationResult always echoes the canonical UUID.

    `hard=True` is rejected — XSD has no hard-delete contract and project policy
    is GDPR soft-delete (cf. `src/salesforce/contacts/`).
    """
    if hard:
        raise ValueError("hard-delete not supported on Contact — use soft-delete (default)")
    if not crm_id.strip():
        raise ValueError("crm_id must not be empty")

    existing = await _resolve_contact_record(client, crm_id)
    if existing is None:
        raise ValueError(f"no contact found with id '{crm_id}'")
    sf_id = str(existing["Id"])
    canonical_crm_id = str(existing[_CRM_ID_FIELD])
    email = existing.get("Email")
    if not email:
        raise ValueError("email missing on existing record — cannot publish C22")

    active_field = await client.get_contact_active_field()
    await client.update_contact(sf_id, {active_field: False})

    user_data = {
        "id": canonical_crm_id,
        "email": email,
        "deactivatedAt": _now_iso(),
    }
    publisher.publish_user_deactivated(user_data)
    logger.info(
        "MCP write-tool delete_contact succeeded crm_id=%s sf_id=%s",
        canonical_crm_id,
        sf_id,
    )

    return MutationResult(
        id=canonical_crm_id,
        success=True,
        routing_key="crm.user.deactivated",
        salesforce_id=sf_id,
    )


# ---------------------------------------------------------------------------
# Additional read-only analytics tools
# ---------------------------------------------------------------------------

_MAX_LIMIT_ORPHAN_CONTACTS = 100


async def find_contacts_without_company(
    client: CrmSalesforceClient,
    role: str | None = None,
    is_active: bool | None = True,
    limit: int = 20,
) -> list[ContactSummary]:
    """Find contacts that have no linked company (AccountId is null).

    Useful for data-quality checks and onboarding flows. Optional filters:
    `role` (Role__c picklist value), `is_active` (default: active only).
    Results ordered by LastModifiedDate DESC.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if role is not None and role not in _VALID_USER_ROLES:
        raise ValueError(f"role must be one of {sorted(_VALID_USER_ROLES)}")

    safe_limit = min(limit, _MAX_LIMIT_ORPHAN_CONTACTS)
    active_field = await client.get_contact_active_field()

    clauses: list[str] = ["AccountId = null"]
    if role is not None:
        clauses.append(f"Role__c = '{escape_soql(role)}'")
    if is_active is True:
        clauses.append(f"{active_field} = true")
    elif is_active is False:
        clauses.append(f"{active_field} = false")

    where = " AND ".join(clauses)
    soql = (
        f"SELECT Id, Name, Email, {active_field}, LastModifiedDate "
        f"FROM Contact "
        f"WHERE {where} "
        f"ORDER BY LastModifiedDate DESC "
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


async def contact_activity_summary(
    client: CrmSalesforceClient,
) -> ContactActivitySummary:
    """Org-wide analytics dashboard.

    Returns total, active/inactive split, GDPR consent count, paid count,
    per-role breakdown, new contacts in the last 7 days, and contacts
    modified in the last 24 hours. All counts are computed in a single
    parallel batch of SOQL queries.
    """
    import asyncio

    active_field = await client.get_contact_active_field()
    now = datetime.now(timezone.utc)
    threshold_7d = format_soql_datetime(now - timedelta(days=7))
    threshold_24h = format_soql_datetime(now - timedelta(hours=24))

    (
        total,
        active_count,
        gdpr_count,
        paid_count,
        new_7d,
        modified_24h,
        visitor_count,
        company_contact_count,
    ) = await asyncio.gather(
        client.query_count("SELECT COUNT() FROM Contact"),
        client.query_count(f"SELECT COUNT() FROM Contact WHERE {active_field} = true"),
        client.query_count("SELECT COUNT() FROM Contact WHERE GDPR_Consent__c = true"),
        client.query_count("SELECT COUNT() FROM Contact WHERE Paid_At__c != null"),
        client.query_count(
            f"SELECT COUNT() FROM Contact WHERE CreatedDate >= {threshold_7d}"
        ),
        client.query_count(
            f"SELECT COUNT() FROM Contact WHERE LastModifiedDate >= {threshold_24h}"
        ),
        client.query_count("SELECT COUNT() FROM Contact WHERE Role__c = 'VISITOR'"),
        client.query_count(
            "SELECT COUNT() FROM Contact WHERE Role__c = 'COMPANY_CONTACT'"
        ),
    )

    inactive_count = max(total - active_count, 0)
    unknown_role_count = max(total - visitor_count - company_contact_count, 0)

    return ContactActivitySummary(
        total=total,
        active=active_count,
        inactive=inactive_count,
        gdpr_consent=gdpr_count,
        paid=paid_count,
        by_role={
            "VISITOR": visitor_count,
            "COMPANY_CONTACT": company_contact_count,
            "UNKNOWN": unknown_role_count,
        },
        new_last_7_days=new_7d,
        modified_last_24h=modified_24h,
    )


async def get_contact_by_email(
    client: CrmSalesforceClient,
    email: str,
) -> ContactDetails | None:
    """Fetch full contact details by exact email address.

    Returns None when no contact has that email. Use `search_contact` for
    fuzzy matching; this function requires an exact address.
    """
    stripped = email.strip()
    if not stripped:
        raise ValueError("email must not be empty")

    active_field = await client.get_contact_active_field()
    soql = (
        f"SELECT {_contact_detail_fields(active_field)} "
        f"FROM Contact WHERE Email = '{escape_soql(stripped)}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None
    return _map_contact_record(records[0], active_field)


_MAX_LIMIT_LIST_CONTACTS = 100


async def list_contacts(
    client: CrmSalesforceClient,
    role: str | None = None,
    is_active: bool | None = True,
    gdpr_consent: bool | None = None,
    has_paid: bool | None = None,
    company_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[ContactSummary]:
    """Browse contacts with optional filters and pagination.

    `company_id` accepts a Salesforce AccountId (001-prefix) to filter contacts
    linked to a specific company. Results are ordered by LastModifiedDate DESC.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if role is not None and role not in _VALID_USER_ROLES:
        raise ValueError(f"role must be one of {sorted(_VALID_USER_ROLES)}")

    safe_limit = min(limit, _MAX_LIMIT_LIST_CONTACTS)
    active_field = await client.get_contact_active_field()

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
    if company_id is not None:
        clauses.append(f"AccountId = '{escape_soql(company_id)}'")

    where_clause = f"WHERE {' AND '.join(clauses)} " if clauses else ""
    soql = (
        f"SELECT Id, Name, Email, {active_field}, LastModifiedDate "
        f"FROM Contact "
        f"{where_clause}"
        f"ORDER BY LastModifiedDate DESC "
        f"LIMIT {safe_limit} OFFSET {offset}"
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


async def get_contact_by_badge(
    client: CrmSalesforceClient,
    badge_code: str,
) -> ContactDetails | None:
    """Fetch full contact details by badge code (Badge_Code__c).

    Designed for event gate scanning: given a scanned badge code, returns who
    the badge belongs to. Returns None when the badge code is not found.
    """
    stripped = badge_code.strip()
    if not stripped:
        raise ValueError("badge_code must not be empty")

    active_field = await client.get_contact_active_field()
    soql = (
        f"SELECT {_contact_detail_fields(active_field)} "
        f"FROM Contact WHERE Badge_Code__c = '{escape_soql(stripped)}' LIMIT 1"
    )
    result = await client.query(soql)
    records = result.get("records", [])
    if not records:
        return None
    return _map_contact_record(records[0], active_field)
