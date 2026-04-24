"""Salesforce polling task — publishes out-of-band Contact/Account changes.

Runs as the 4th long-running asyncio task alongside heartbeat, status, and
receiver. Its job: detect changes made directly in Salesforce (e.g. an admin
creates a Contact via the Salesforce UI, not via RabbitMQ) and publish the
matching outbound event on the `contact.topic` exchange so other teams stay
in sync with CRM as the master data owner.

Published contracts:
    Contract 13 — crm.user.confirmed
    Contract 18 — crm.user.updated
    Contract 22 — crm.user.deactivated
    Contract 14 — crm.company.confirmed
    Contract 19 — crm.company.updated
    Contract 23 — crm.company.deactivated

Double-publish avoidance:
    The receiver task already publishes these contracts when it processes
    inbound messages. Polling must not re-publish those same events. We filter
    on `LastModifiedById != integration_user_id` — receiver writes under the
    integration user, admin UI edits under the admin user. This requires a
    dedicated Salesforce integration user in production (see CLAUDE.md §5).

Checkpoint state:
    `last_seen` per object type is persisted to a JSON file (default
    `/tmp/polling_checkpoint.json`). The container is intentionally stateless
    across restarts — on cold start the checkpoint is seeded from the
    Salesforce server clock (`MAX(SystemModstamp)`) so no events from before
    the restart are republished. Any events that occurred during container
    downtime are lost (consumers are idempotent on `id`, so a later edit
    will re-emit the latest state).
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from src import sender
from src.config import Config
from src.country_code import to_iso_alpha2
from src.salesforce.contacts import (
    _build_updated_user_data,
    _build_user_data,
    _build_user_deactivation_data,
    _get_contact_is_active,
)
from src.salesforce_client import (
    SalesforceSession,
    coerce_is_active,
    escape_soql,
    is_rate_limit_error,
    sf_call,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# Re-resolve the integration user id once an hour so a rotated username
# doesn't silently let polling re-publish receiver-induced events.
_USER_ID_REFRESH_SECONDS = 3600

# Deduplicate ERROR logs for records that fail dispatch deterministically:
# skip-and-cap still re-processes them every cycle, but we only want ONE log
# per (id, modstamp). Admin fix → new modstamp → new tuple → one more log.
_REPORTED_FAILURES_CAP = 1000
_reported_failures: "collections.OrderedDict[tuple[str, str], None]" = (
    collections.OrderedDict()
)


def _should_log_failure(record: dict) -> bool:
    key = (str(record.get("Id")), str(record.get("SystemModstamp")))
    if key in _reported_failures:
        _reported_failures.move_to_end(key)
        return False
    _reported_failures[key] = None
    if len(_reported_failures) > _REPORTED_FAILURES_CAP:
        _reported_failures.popitem(last=False)
    return True

# SOQL-safe timestamp format. Salesforce accepts ISO-8601 with Z suffix.
_SF_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


# ---------------------------------------------------------------------------
# Checkpoint state
# ---------------------------------------------------------------------------

@dataclass
class PollingState:
    """Per-object last-seen timestamps persisted across restarts."""

    contact_last_seen: datetime
    account_last_seen: datetime


def _parse_sf_timestamp(value: str) -> datetime:
    """Parse a Salesforce SystemModstamp string into a tz-aware UTC datetime.

    Salesforce returns timestamps like '2026-04-21T12:34:56.000+0000' or
    '2026-04-21T12:34:56.789Z' depending on the API path. Both are handled.
    """
    normalized = value.replace("Z", "+0000")
    # Python's %z handles '+0000' natively. Fall back to fromisoformat for
    # already-colon-suffixed zones (e.g. '+00:00').
    try:
        return datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone.utc)
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _format_sf_timestamp(value: datetime) -> str:
    """Format a datetime as SOQL-literal ISO-8601 with millisecond precision."""
    utc = value.astimezone(timezone.utc)
    return utc.strftime(_SF_TIMESTAMP_FORMAT)


async def _seed_last_seen_from_sf(session: SalesforceSession, sobject: str) -> datetime:
    """Query Salesforce for the most-recent SystemModstamp on a given object.

    Falls back to `now()` in UTC when the org is empty (so cold start on an
    empty demo org still works). Always returns tz-aware UTC.
    """
    query = f"SELECT SystemModstamp FROM {sobject} ORDER BY SystemModstamp DESC LIMIT 1"
    result = await sf_call(session, lambda sf: sf.query(query))
    records = result.get("records") or []
    if not records:
        logger.warning("Polling: %s table is empty, seeding last_seen = now(UTC).", sobject)
        return datetime.now(timezone.utc)
    return _parse_sf_timestamp(records[0]["SystemModstamp"])


def _try_load_checkpoint_file(state_path: str) -> PollingState | None:
    """Attempt to load the on-disk checkpoint. Returns None when absent or
    unreadable so the caller can fall through to per-object SF seeding.
    A corrupted file never blocks startup — it's logged and treated as a
    cold start.
    """
    try:
        with open(state_path, encoding="utf-8") as fh:
            raw = json.load(fh)
        state = PollingState(
            contact_last_seen=_parse_sf_timestamp(raw["contact_last_seen"]),
            account_last_seen=_parse_sf_timestamp(raw["account_last_seen"]),
        )
        logger.info(
            "Polling: loaded checkpoint — contact=%s, account=%s",
            state.contact_last_seen.isoformat(),
            state.account_last_seen.isoformat(),
        )
        return state
    except FileNotFoundError:
        logger.info("Polling: no checkpoint file at %s, seeding from Salesforce.", state_path)
        return None
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.warning(
            "Polling: checkpoint file %s unreadable (%s); seeding from Salesforce.",
            state_path, exc,
        )
        return None


async def _load_or_seed_state(session: SalesforceSession, state_path: str) -> PollingState:
    """Load persisted checkpoint or seed from Salesforce on cold start.

    NOTE: for bootstrap resilience, `run_polling` does NOT call this directly;
    it calls `_try_load_checkpoint_file` + `_seed_last_seen_from_sf` separately
    so a failure on Account seeding doesn't invalidate a successful Contact
    seed (which would otherwise get re-seeded at a newer MAX(SystemModstamp)
    on the next retry, silently dropping events). Kept for backwards
    compatibility with existing tests.
    """
    file_state = _try_load_checkpoint_file(state_path)
    if file_state is not None:
        return file_state
    contact_ts = await _seed_last_seen_from_sf(session, "Contact")
    account_ts = await _seed_last_seen_from_sf(session, "Account")
    return PollingState(contact_last_seen=contact_ts, account_last_seen=account_ts)


async def _persist_state(state: PollingState, state_path: str) -> None:
    """Atomically write the checkpoint to disk (temp + rename)."""
    payload = {
        "contact_last_seen": _format_sf_timestamp(state.contact_last_seen),
        "account_last_seen": _format_sf_timestamp(state.account_last_seen),
    }
    parent = os.path.dirname(state_path) or "."
    os.makedirs(parent, exist_ok=True)

    def _write_atomic() -> None:
        fd, tmp_path = tempfile.mkstemp(prefix=".polling_", dir=parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp_path, state_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    await asyncio.to_thread(_write_atomic)


# ---------------------------------------------------------------------------
# Integration user resolution
# ---------------------------------------------------------------------------

async def _resolve_integration_user_id(session: SalesforceSession, config: Config) -> str:
    """Look up the integration account's Salesforce User Id.

    Used to exclude receiver-induced writes from the polling cycle via a
    `LastModifiedById != <uid>` SOQL filter.

    When `config.polling_integration_user_id` is set, skip the SOQL lookup
    and return the configured value. This is the escape-hatch for E2E tests
    (use a fake-but-valid-format ID like '005000000000000AAA' so real writes
    are not filtered) and for dev/sandbox operators whose admin account
    doubles as the integration account (set to a fake ID to disable filter).
    """
    if config.polling_integration_user_id:
        logger.info(
            "Polling: using configured integration user override id=%s (skipping SOQL lookup)",
            config.polling_integration_user_id,
        )
        return config.polling_integration_user_id

    username = config.salesforce_username
    soql = f"SELECT Id FROM User WHERE Username = '{escape_soql(username)}' LIMIT 1"
    result = await sf_call(session, lambda sf: sf.query(soql))
    records = result.get("records") or []
    if not records:
        raise RuntimeError(
            f"Polling: cannot resolve integration user Id for username={username!r}; "
            "ensure a dedicated SF integration user exists.",
        )
    uid = str(records[0]["Id"])

    # Warn operators when admin == integration user. In that case the filter
    # `LastModifiedById != uid` will also exclude admin UI edits → polling
    # detects nothing. Best-effort; never fails startup on this check.
    await _warn_if_admin_is_integration_user(session, uid)
    return uid


async def _warn_if_admin_is_integration_user(session: SalesforceSession, uid: str) -> None:
    """Best-effort: emit a WARNING when the resolved integration user id is
    the same user the SF session is logged in as. This typically happens in
    dev/sandbox where the admin account IS the integration account — polling
    cannot distinguish receiver writes from admin UI writes and will silently
    detect zero out-of-band changes.

    The probe itself is best-effort (swallow transient network / 5xx errors)
    but permanent credential failures must NOT be swallowed — otherwise a
    rotated/revoked integration user would run the polling task forever with
    a dead client, hiding the real root cause until the first real query.
    """
    from simple_salesforce.exceptions import SalesforceAuthenticationFailed

    try:
        userinfo = await sf_call(session, lambda sf: sf.restful("chatter/users/me"))
    except (SalesforceAuthenticationFailed, asyncio.CancelledError):
        raise  # Permanent auth fail / shutdown — operator-visible, not swallowable
    except Exception:  # noqa: BLE001
        return  # transient probe failure — best-effort, skip warning
    session_uid = (userinfo or {}).get("id") or ""
    # SF returns 18-char canonical ids; compare the stable 15-char prefix.
    if session_uid[:15] and session_uid[:15] == uid[:15]:
        logger.warning(
            "Polling: resolved integration_user_id (%s) equals the current SF "
            "session user. If admin also logs in as this user, polling will "
            "not detect admin UI edits (they'll be filtered out as "
            "receiver-induced writes). Set POLLING_INTEGRATION_USER_ID to a "
            "fake-but-valid-format id like 005000000000000AAA to bypass.",
            uid,
        )


# ---------------------------------------------------------------------------
# Field lists for polling SELECT
# ---------------------------------------------------------------------------

# Fields we WILL ALWAYS request — either standard SF fields or required custom.
# Missing any of these is a real deployment problem, not a polling edge case.
_CONTACT_REQUIRED_FIELDS = [
    "Id", "CRM_ID__c", "FirstName", "LastName", "Email",
    "CreatedDate", "SystemModstamp", "LastModifiedById",
]

# Optional — included in SELECT only when `describe` reports them. A missing
# field here is a dev/sandbox org that hasn't deployed all contracts yet.
_CONTACT_OPTIONAL_FIELDS = [
    "Phone", "Role__c", "GDPR_Consent__c", "Badge_Code__c", "Company_ID__c",
    "MailingStreet", "House_Number__c", "MailingPostalCode", "MailingCity",
    "MailingCountry", "MailingCountryCode",
    "Mailing_ID__c", "Planning_ID__c",
]

_ACCOUNT_REQUIRED_FIELDS = [
    "Id", "CRM_ID__c", "Name", "VAT_Number__c",
    "CreatedDate", "SystemModstamp", "LastModifiedById",
]

_ACCOUNT_OPTIONAL_FIELDS = [
    "Phone", "Email__c", "Email",
    "BillingStreet", "House_Number__c", "BillingPostalCode", "BillingCity",
    "BillingCountry", "BillingCountryCode",
]


def _pick_boolean_active_field(describe: dict) -> str:
    """Return the first active-style custom field that is typed as boolean.

    Some orgs have BOTH `Active__c` (picklist with values Yes/No) AND
    `Is_Active__c` (boolean) on the same SObject. A picklist value `'No'`
    is truthy under `bool()`, so picking the picklist field would invert
    the active/inactive logic. Always prefer the boolean-typed field.
    """
    fields_by_name = {f["name"]: f for f in describe.get("fields", [])}
    candidates = ("IsActive__c", "Active__c", "Is_Active__c")

    # Pass 1: boolean-typed candidates.
    for name in candidates:
        field = fields_by_name.get(name)
        if field is not None and field.get("type") == "boolean":
            return name
    # Pass 2: fall back to any existing candidate (picklist, text, …).
    for name in candidates:
        if name in fields_by_name:
            return name
    raise RuntimeError("Polling: no IsActive/Active/Is_Active custom field on this SObject.")


async def _resolve_contact_active_field(session: SalesforceSession) -> str:
    """Probe which active-style custom field Contact uses, preferring boolean type."""
    describe = await _describe_cached(session, "Contact")
    return _pick_boolean_active_field(describe)


async def _resolve_account_active_field(session: SalesforceSession) -> str:
    """Probe which active-style custom field Account uses, preferring boolean type."""
    describe = await _describe_cached(session, "Account")
    return _pick_boolean_active_field(describe)


async def _resolve_account_email_field(session: SalesforceSession) -> str | None:
    """Probe which email field exists on Account, if any."""
    describe = await _describe_cached(session, "Account")
    available = {field["name"] for field in describe.get("fields", [])}
    for name in ("Email__c", "Email"):
        if name in available:
            return name
    return None


# Describe caches — SObject metadata is stable during container uptime,
# hitting it once per boot saves ~4 REST calls per polling cycle (the two
# *_active_field resolvers plus the two _available_fields probes).
_describe_cache: dict[str, dict] = {}


async def _describe_cached(session: SalesforceSession, sobject: str) -> dict:
    cached = _describe_cache.get(sobject)
    if cached is not None:
        return cached
    describe = await sf_call(session, lambda sf: getattr(sf, sobject).describe())
    _describe_cache[sobject] = describe
    return describe


async def _available_fields(session: SalesforceSession, sobject: str) -> set[str]:
    """Return the set of field API names that actually exist on the SObject.

    Used to filter the polling SELECT list so that missing optional custom
    fields (Company_ID__c, Mailing_ID__c, …) do not cause SOQL INVALID_FIELD
    errors — the CRM_UUID field spec varies per org and release.
    """
    describe = await _describe_cached(session, sobject)
    return {field["name"] for field in describe.get("fields", [])}


async def _build_contact_select_fields(session: SalesforceSession) -> list[str]:
    available = await _available_fields(session, "Contact")
    fields = [name for name in _CONTACT_REQUIRED_FIELDS if name in available]
    missing_required = set(_CONTACT_REQUIRED_FIELDS) - available
    if missing_required:
        raise RuntimeError(
            f"Polling: required Contact fields missing from Salesforce: {sorted(missing_required)}",
        )
    for name in _CONTACT_OPTIONAL_FIELDS:
        if name in available and name not in fields:
            fields.append(name)
    active = await _resolve_contact_active_field(session)
    if active not in fields:
        fields.append(active)
    return fields


async def _build_account_select_fields(session: SalesforceSession) -> list[str]:
    available = await _available_fields(session, "Account")
    fields = [name for name in _ACCOUNT_REQUIRED_FIELDS if name in available]
    missing_required = set(_ACCOUNT_REQUIRED_FIELDS) - available
    if missing_required:
        raise RuntimeError(
            f"Polling: required Account fields missing from Salesforce: {sorted(missing_required)}",
        )
    for name in _ACCOUNT_OPTIONAL_FIELDS:
        if name in available and name not in fields:
            fields.append(name)
    active = await _resolve_account_active_field(session)
    if active not in fields:
        fields.append(active)
    return fields


# ---------------------------------------------------------------------------
# Account → company_data builders (equivalent of receiver._build_user_data for Contacts)
# ---------------------------------------------------------------------------

def _get_account_is_active(account: dict) -> bool:
    """Read Account active flag, handling boolean/picklist/string/None uniformly."""
    for key in ("IsActive__c", "Active__c", "Is_Active__c"):
        if key in account:
            return coerce_is_active(account[key])
    return True


def _account_email(account: dict) -> str | None:
    return account.get("Email__c") or account.get("Email")


def _account_company_fields(account: dict) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if account.get("Phone"):
        data["phone"] = account["Phone"]

    address_mapping = {
        "BillingStreet": "street",
        "House_Number__c": "houseNumber",
        "BillingPostalCode": "postalCode",
        "BillingCity": "city",
    }
    for sf_field, xml_field in address_mapping.items():
        value = account.get(sf_field)
        if value:
            data[xml_field] = value

    # When State & Country Picklists are enabled, BillingCountry is the
    # read-only localized label ("Belgium"); the ISO-2 lives in the *Code field.
    country = (
        to_iso_alpha2(account.get("BillingCountryCode"))
        or to_iso_alpha2(account.get("BillingCountry"))
    )
    if country:
        data["country"] = country
    return data


_C14_REQUIRED_ADDRESS_FIELDS = ("street", "houseNumber", "postalCode", "city", "country")


def _build_company_confirmed_data(account: dict) -> dict:
    data = {
        "id": account["CRM_ID__c"],
        "vatNumber": account["VAT_Number__c"],
        "name": account["Name"],
        "email": _account_email(account) or "",
        "isActive": _get_account_is_active(account),
        "confirmedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    data.update(_account_company_fields(account))
    missing = [f for f in _C14_REQUIRED_ADDRESS_FIELDS if f not in data]
    if missing:
        raise ValueError(
            f"Account {account.get('Id')}: cannot publish crm.company.confirmed — "
            f"missing required address fields: {missing}",
        )
    return data


def _build_company_updated_data(account: dict) -> dict:
    data: dict[str, Any] = {
        "id": account["CRM_ID__c"],
        "vatNumber": account["VAT_Number__c"],
        "name": account["Name"],
        "isActive": _get_account_is_active(account),
        "updatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    email = _account_email(account)
    if email:
        data["email"] = email
    data.update(_account_company_fields(account))
    return data


def _build_company_deactivation_data(account: dict, deactivated_at: str) -> dict:
    return {
        "id": account["CRM_ID__c"],
        "vatNumber": account["VAT_Number__c"],
        "deactivatedAt": deactivated_at,
    }


# ---------------------------------------------------------------------------
# Ensure CRM_ID__c is stamped on records created directly in Salesforce
# ---------------------------------------------------------------------------

# In-memory map of SF record Id → generated CRM UUID for records whose
# post-publish SF write has not yet succeeded. Persists across polling cycles
# within a container lifetime so a transient SF error on the back-write
# doesn't cause the same record to be published twice with different
# canonical ids.
_pending_crm_ids: dict[str, str] = {}


def _assign_local_crm_id(record: dict) -> tuple[dict, bool]:
    """Populate `CRM_ID__c` on the in-memory record without writing to Salesforce.

    Returns `(record, needs_stamp)` — `needs_stamp=True` signals that caller
    MUST persist the UUID to Salesforce *after* a successful publish.

    If a prior cycle already generated a UUID for this SF record but the
    back-write to Salesforce failed, we reuse that same UUID from the
    `_pending_crm_ids` cache. Without this cache a transient SF error would
    cause the same Contact/Account to be published twice under different
    canonical ids — breaking consumers that key on that UUID.

    Writing the UUID to SF before publish would bump `LastModifiedById` to the
    integration user even if the publish then fails. Next poll would filter
    the record out (`LastModifiedById != integration_user_id`) and the change
    would be silently lost. Order of operations: assign → publish → stamp.
    """
    if record.get("CRM_ID__c"):
        return record, False
    sf_id = record.get("Id")
    cached = _pending_crm_ids.get(sf_id) if sf_id else None
    record["CRM_ID__c"] = cached or str(uuid.uuid4())
    if sf_id:
        _pending_crm_ids[sf_id] = record["CRM_ID__c"]
    return record, True


def _clear_pending_crm_id(sf_id: str | None) -> None:
    """Called after a successful post-publish SF stamp completes."""
    if sf_id:
        _pending_crm_ids.pop(sf_id, None)


async def _persist_contact_crm_id(session: SalesforceSession, contact: dict) -> None:
    """Write `CRM_ID__c` back to Salesforce after a successful Contact publish."""
    contact_id = contact["Id"]
    crm_id = contact["CRM_ID__c"]
    await sf_call(session, lambda sf: sf.Contact.update(contact_id, {"CRM_ID__c": crm_id}))
    _clear_pending_crm_id(contact.get("Id"))
    logger.info(
        "Polling: stamped CRM_ID__c=%s on Contact %s (post-publish)",
        contact["CRM_ID__c"], contact["Id"],
    )


async def _persist_account_crm_id(session: SalesforceSession, account: dict) -> None:
    """Write `CRM_ID__c` back to Salesforce after a successful Account publish."""
    account_id = account["Id"]
    crm_id = account["CRM_ID__c"]
    await sf_call(session, lambda sf: sf.Account.update(account_id, {"CRM_ID__c": crm_id}))
    _clear_pending_crm_id(account.get("Id"))
    logger.info(
        "Polling: stamped CRM_ID__c=%s on Account %s (post-publish)",
        account["CRM_ID__c"], account["Id"],
    )


# ---------------------------------------------------------------------------
# Dispatch logic per record
# ---------------------------------------------------------------------------

async def _dispatch_contact(
    contact: dict,
    previous_last_seen: datetime,
) -> bool:
    """Publish the correct Contract for a single Contact record.

    Returns True if a message was published, False if the record was skipped
    (e.g. validation failure). Skipped records intentionally do not advance
    the checkpoint so the next cycle retries them on the same SystemModstamp.
    """
    is_active = _get_contact_is_active(contact)
    created = _parse_sf_timestamp(contact["CreatedDate"])
    modstamp = _parse_sf_timestamp(contact["SystemModstamp"])

    if not is_active:
        deactivated_at = modstamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        await sender.publish_user_deactivated(
            _build_user_deactivation_data(contact, deactivated_at),
        )
        return True

    # Contracts 13 and 18 both require a valid email (XSD EmailType). If the
    # Contact has no email yet, we skip publishing and leave the checkpoint
    # held below this record so the next edit (adding an email) gets routed
    # correctly. Without this guard the sender's XSD validation would raise,
    # cascade into "failed to dispatch" exception handling, and cause every
    # later Contact change to be reprocessed on every poll until the email is
    # filled in.
    if not contact.get("Email"):
        logger.warning(
            "Polling: skipping Contact %s — no email on record; checkpoint "
            "not advanced, will retry when record is re-edited",
            contact.get("Id"),
        )
        return False

    if created > previous_last_seen:
        await sender.publish_user_confirmed(_build_user_data(contact))
    else:
        await sender.publish_user_updated(_build_updated_user_data(contact))
    return True


async def _dispatch_account(
    account: dict,
    previous_last_seen: datetime,
) -> bool:
    """Publish the correct Contract for a single Account record.

    Returns True if a message was published, False if skipped. Skipped records
    intentionally do NOT advance the checkpoint so a later edit that makes
    the record publishable still routes to CONFIRMED — consumers need the
    CREATE event to materialise local state. Without this, an Account whose
    email is filled in after creation would only emit an UPDATE, never a
    CONFIRMED, and consumers keyed on that create event would miss the record.

    Contract 14 (CompanyConfirmed) requires a non-empty `email` field. When
    an Account has no email, we log a warning and return False.
    """
    is_active = _get_account_is_active(account)
    created = _parse_sf_timestamp(account["CreatedDate"])
    modstamp = _parse_sf_timestamp(account["SystemModstamp"])

    if not is_active:
        deactivated_at = modstamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        await sender.publish_company_deactivated(
            _build_company_deactivation_data(account, deactivated_at),
        )
        return True

    if created > previous_last_seen:
        if not _account_email(account):
            logger.warning(
                "Polling: skipping CompanyConfirmed for Account %s — no email; "
                "checkpoint not advanced, will retry when record is re-edited",
                account.get("Id"),
            )
            return False
        await sender.publish_company_confirmed(_build_company_confirmed_data(account))
    else:
        await sender.publish_company_updated(_build_company_updated_data(account))
    return True


# ---------------------------------------------------------------------------
# Per-object poll cycles
# ---------------------------------------------------------------------------

async def _poll_contacts(
    session: SalesforceSession,
    state: PollingState,
    integration_user_id: str,
) -> PollingState:
    """Query Contact changes since last_seen and dispatch each record."""
    fields = await _build_contact_select_fields(session)
    since = _format_sf_timestamp(state.contact_last_seen)
    soql = (
        f"SELECT {', '.join(fields)} FROM Contact "
        f"WHERE SystemModstamp > {since} "
        f"AND LastModifiedById != '{escape_soql(integration_user_id)}' "
        f"ORDER BY SystemModstamp ASC"
    )
    result = await sf_call(session, lambda sf: sf.query_all(soql))
    records = result.get("records") or []
    if not records:
        return state

    previous_last_seen = state.contact_last_seen
    latest = state.contact_last_seen
    earliest_skipped: datetime | None = None
    for record in records:
        record_ts = _parse_sf_timestamp(record["SystemModstamp"])
        try:
            # Order matters: assign UUID IN MEMORY, publish, then persist UUID
            # to Salesforce. Stamping before publish would bump
            # LastModifiedById to the integration user even if the publish
            # fails; the next poll's filter would then drop the record
            # forever.
            record, needs_stamp = _assign_local_crm_id(record)
            dispatched = await _dispatch_contact(record, previous_last_seen)
            if dispatched and needs_stamp:
                await _persist_contact_crm_id(session, record)
            if dispatched:
                if record_ts > latest:
                    latest = record_ts
            else:
                if earliest_skipped is None or record_ts < earliest_skipped:
                    earliest_skipped = record_ts
        except Exception as exc:
            # Rate-limit errors must surface to the outer run_polling handler so
            # the whole cycle backs off; swallowing them here would produce a
            # full stack trace per record instead of the concise "skipping cycle"
            # log, and would keep hammering SF while it throttles us.
            if is_rate_limit_error(exc):
                raise
            if _should_log_failure(record):
                logger.exception(
                    "Polling: failed to dispatch Contact %s", record.get("Id"),
                )
            # Treat as a skip so the checkpoint cap stays below this record's
            # timestamp and a fixed version is re-processed on the next cycle.
            # Without this, a later successful record would advance the
            # checkpoint past this one and any admin fix would be mis-routed
            # as user.updated instead of user.confirmed.
            if earliest_skipped is None or record_ts < earliest_skipped:
                earliest_skipped = record_ts

    latest = _cap_checkpoint_below_skips(latest, earliest_skipped)
    return PollingState(
        contact_last_seen=latest,
        account_last_seen=state.account_last_seen,
    )


async def _poll_accounts(
    session: SalesforceSession,
    state: PollingState,
    integration_user_id: str,
) -> PollingState:
    """Query Account changes since last_seen and dispatch each record."""
    fields = await _build_account_select_fields(session)
    since = _format_sf_timestamp(state.account_last_seen)
    soql = (
        f"SELECT {', '.join(fields)} FROM Account "
        f"WHERE SystemModstamp > {since} "
        f"AND LastModifiedById != '{escape_soql(integration_user_id)}' "
        f"ORDER BY SystemModstamp ASC"
    )
    result = await sf_call(session, lambda sf: sf.query_all(soql))
    records = result.get("records") or []
    if not records:
        return state

    previous_last_seen = state.account_last_seen
    latest = state.account_last_seen
    earliest_skipped: datetime | None = None
    for record in records:
        record_ts = _parse_sf_timestamp(record["SystemModstamp"])
        try:
            record, needs_stamp = _assign_local_crm_id(record)
            dispatched = await _dispatch_account(record, previous_last_seen)
            if dispatched and needs_stamp:
                await _persist_account_crm_id(session, record)
            if dispatched:
                if record_ts > latest:
                    latest = record_ts
            else:
                if earliest_skipped is None or record_ts < earliest_skipped:
                    earliest_skipped = record_ts
        except Exception as exc:
            if is_rate_limit_error(exc):
                raise
            if _should_log_failure(record):
                logger.exception(
                    "Polling: failed to dispatch Account %s", record.get("Id"),
                )
            if earliest_skipped is None or record_ts < earliest_skipped:
                earliest_skipped = record_ts

    latest = _cap_checkpoint_below_skips(latest, earliest_skipped)
    return PollingState(
        contact_last_seen=state.contact_last_seen,
        account_last_seen=latest,
    )


def _cap_checkpoint_below_skips(
    latest: datetime, earliest_skipped: datetime | None,
) -> datetime:
    """Ensure the new checkpoint never sits at-or-above an unpublished record.

    Scenario the review flagged: record X at T dispatched, record Y at same T
    skipped (no email). Without this cap, `latest = T`, next cycle's SOQL uses
    `SystemModstamp > T` and Y is filtered out forever. By capping `latest` at
    `min(earliest_skipped - 1ms, max_dispatched)`, next cycle's WHERE picks up Y
    (and may re-process X — consumers are idempotent on `id`, so no harm).
    """
    if earliest_skipped is None:
        return latest
    cap = earliest_skipped - timedelta(milliseconds=1)
    return cap if cap < latest else latest


# ---------------------------------------------------------------------------
# Top-level task
# ---------------------------------------------------------------------------

async def run_polling(
    config: Config,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Contract 13/14/18/19/22/23 — poll Salesforce for out-of-band changes.

    Loop cadence controlled by `config.polling_interval_seconds` (default 60s).
    Graceful shutdown: external `task.cancel()` from main.py.

    Creates its own Salesforce client (same pattern as run_receiver). Doing
    this inside the task — instead of before task creation in main.py — keeps
    heartbeat/status/receiver startup independent of SF reachability.
    """
    # Lazy import avoids a load-time circular dependency with main.py imports.
    from src.salesforce_client import get_salesforce_client

    sf = await get_salesforce_client(config, shutdown_event=shutdown_event)
    # Wrap in a SalesforceSession so sf_call can auto-reauth on expired
    # sessions (401 INVALID_SESSION_ID or observed 302→404 on /query endpoint).
    # All helper calls take `session` and route through sf_call; the underlying
    # sf client is swapped in-place on reauth.
    session = SalesforceSession(sf, config, shutdown_event)

    # Bootstrap (state + integration user) lives INSIDE the retry loop so a
    # transient SF outage during startup doesn't permanently kill the polling
    # task. _supervised_task only logs exceptions; it does not restart.
    #
    # Each bootstrap step caches independently so a partial-success retry
    # doesn't re-run already-completed steps. Without this, e.g. Contact
    # seeds at T1, Account seed fails, next cycle re-seeds Contact at T2 →
    # any Contact changes between T1 and T2 are silently dropped.
    state: PollingState | None = None
    contact_seed: datetime | None = None
    account_seed: datetime | None = None
    integration_user_id: str | None = None
    user_id_resolved_at = 0.0

    while True:
        try:
            if state is None:
                # Prefer on-disk checkpoint (atomic, both objects together).
                state = _try_load_checkpoint_file(config.polling_state_path)
            if state is None:
                # Cold-start SF seed, per-object. Each call is cached on its
                # own so a failure on Account doesn't force Contact to re-seed.
                if contact_seed is None:
                    contact_seed = await _seed_last_seen_from_sf(session, "Contact")
                if account_seed is None:
                    account_seed = await _seed_last_seen_from_sf(session, "Account")
                state = PollingState(
                    contact_last_seen=contact_seed,
                    account_last_seen=account_seed,
                )
            if integration_user_id is None:
                integration_user_id = await _resolve_integration_user_id(session, config)
                user_id_resolved_at = time.monotonic()
                logger.info(
                    "Polling started: interval=%ss, integration_user_id=%s, checkpoint=%s",
                    config.polling_interval_seconds,
                    integration_user_id, config.polling_state_path,
                )

            if time.monotonic() - user_id_resolved_at > _USER_ID_REFRESH_SECONDS:
                integration_user_id = await _resolve_integration_user_id(session, config)
                user_id_resolved_at = time.monotonic()

            # Persist after EACH object's cycle — a crash between
            # _poll_contacts and _poll_accounts would otherwise discard the
            # Contact advance and re-publish every Contact on restart.
            assert state is not None and integration_user_id is not None  # bootstrapped
            state = await _poll_contacts(session, state, integration_user_id)
            await _persist_state(state, config.polling_state_path)
            state = await _poll_accounts(session, state, integration_user_id)
            await _persist_state(state, config.polling_state_path)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_rate_limit_error(exc):
                logger.error("Polling: Salesforce rate limit hit; skipping cycle.")
            elif state is None or integration_user_id is None:
                logger.exception(
                    "Polling bootstrap failed; will retry after interval. Error: %s", exc,
                )
            else:
                logger.exception("Polling cycle failed; continuing. Error: %s", exc)

        await asyncio.sleep(config.polling_interval_seconds)
