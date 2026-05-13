"""Salesforce session, auth, retry, and org-describe helpers.

Thin core of the salesforce package: everything in this module is
SObject-agnostic. Per-SObject operations live in sibling modules
(`contacts`, `accounts`, `sessions`, `payments`).

CUSTOM FIELDS REFERENCE (must be created in Salesforce Setup):
- Contact.CRM_ID__c (Text, Unique) — UUID v4 for Contract 13, 18, 22
- Contact.GDPR_Consent__c (Checkbox) — For Contract 1
- Contact.Registration_ID__c (Text) — Deduplication key for Contract 1
- Contact.Paid_At__c (DateTime) — Compatibility timestamp derived from the latest
  paid session registration for Contracts 16 / 17 legacy consumers
- Contact.Mailing_ID__c (Text, Unique) — Native Mailing UUID for Contracts 27-29
- Contact.Planning_ID__c (Text, Unique) — Native Planning UUID for Contracts 30-32
- Contact.Kassa_ID__c (Text, Unique) — Native Kassa UUID for Contracts 36-38
- Contact.Role__c (Picklist: VISITOR | COMPANY_CONTACT) — For Contract 1, 13, 18

- Session_Registration__c.Registration_ID__c (Text, External ID, Unique) —
  Canonical registration identifier for Contracts 1, 2, 11, 16
- Session_Registration__c.Session_ID__c (Text) — Planning session identifier
- Session_Registration__c.Contact__c (Lookup(Contact)) — Canonical Contact link
- Session_Registration__c.Is_Active__c (Checkbox) — Soft delete flag per registration
- Session_Registration__c.Paid_At__c (DateTime) — Payment timestamp per registration

- Account.CRM_ID__c (Text, Unique) — UUID v4 for Contract 14, 19, 23
- Account.VAT_Number__c (Text, External ID, Unique) — For Contract 3, 5a, 5b, 14

All SObject functions return complete records as dicts for XML serialization.
All SF calls are wrapped in asyncio.to_thread() to prevent blocking the event loop.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import requests
from requests.adapters import HTTPAdapter
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import (
    SalesforceAuthenticationFailed,
    SalesforceExpiredSession,
    SalesforceResourceNotFound,
)
from urllib3.util.retry import Retry

from src.config import Config

_T = TypeVar("_T")

logger = logging.getLogger(__name__)

_PAYMENT_TIMESTAMP_FIELD = "Paid_At__c"

# Startup retry configuration for Salesforce login. Mirrors the RabbitMQ
# connect retry pattern in src/connection.py so a transient Salesforce
# outage (SERVER_UNAVAILABLE / network blip) does not leave the receiver
# task silently dead while heartbeat keeps the container "alive".
_SF_STARTUP_DELAY: float = 1.0
_SF_STARTUP_MAX_DELAY: float = 60.0
_TRANSIENT_SF_AUTH_CODES = frozenset({"SERVER_UNAVAILABLE", "SERVICE_UNAVAILABLE"})

# Default per-request timeout for the Salesforce HTTP session. Without this
# `requests` blocks indefinitely on stalled sockets, which manifests as a
# silently-hung receiver handler (see incident 2026-05-09: stuck unacked
# messages on crm.frontend.registration.created).
_SF_HTTP_TIMEOUT_SECONDS: float = 30.0


def _build_retry_adapter() -> HTTPAdapter:
    # Keep in sync with crm-mcp/crm_mcp/salesforce.py::_build_retry_adapter.
    # 401 stays out of status_forcelist so sf_call reauth fires instead.
    retry = Retry(
        total=3,
        connect=3,
        backoff_factor=0.5,
        status_forcelist=(502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH", "DELETE"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    return HTTPAdapter(max_retries=retry)


class _TimeoutSession(requests.Session):
    """`requests.Session` that injects a default timeout on every call."""

    def __init__(self, timeout: float = _SF_HTTP_TIMEOUT_SECONDS) -> None:
        super().__init__()
        self._default_timeout = timeout
        adapter = _build_retry_adapter()
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def request(self, method, url, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._default_timeout)
        return super().request(method, url, **kwargs)


def escape_soql(value: str) -> str:
    """Escape SOQL string-literal metacharacters to prevent injection.

    Salesforce SOQL string literals honour `\\` as escape, so a lone
    backslash followed by the doubling-quote trick (`\\''`) would close
    the literal early. Escape backslashes first (ordering matters), then
    single quotes per the official Salesforce SOQL docs.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


# Backwards-compatibility alias — existing callers in this module use the
# private form; remove once everyone has migrated.
_escape_soql = escape_soql


def coerce_is_active(raw_value: Any) -> bool:
    """Normalize an active-flag value from Salesforce into a bool.

    Salesforce custom active fields come in multiple forms across orgs:
    - Boolean (`IsActive__c` / `Is_Active__c`) → True / False / None
    - Picklist (`Active__c`) → "Yes" / "No"
    - Text → "true" / "false" / empty string

    Python's `bool()` treats any non-empty string as True, so `bool("No")`
    is True — exactly the opposite of what we want for a picklist field.
    Callers should always route active-field values through this helper.

    Missing (`None`) defaults to True: records without a flag are treated as
    active. This mirrors the receiver's legacy behaviour.
    """
    if raw_value is None:
        return True
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return bool(raw_value)
    text = str(raw_value).strip().lower()
    if text in ("yes", "true", "1", "y"):
        return True
    if text in ("no", "false", "0", "n", ""):
        return False
    # Unknown non-empty string → best-effort fall-through.
    return bool(raw_value)


def is_rate_limit_error(exc: Exception) -> bool:
    """Detect Salesforce REQUEST_LIMIT_EXCEEDED via content attribute or message.

    Shared between the receiver (drop-and-sleep on rate limit) and the polling
    task (skip cycle on rate limit). The Salesforce REST API surfaces the error
    both as a structured `content` attribute on SalesforceError subclasses and
    occasionally as a plain message, hence both checks.
    """
    content = getattr(exc, "content", None)
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("errorCode") == "REQUEST_LIMIT_EXCEEDED":
                return True
    return "REQUEST_LIMIT_EXCEEDED" in str(exc)


def is_expired_session_error(exc: Exception) -> bool:
    """Detect an expired Salesforce session from the exception shape.

    Keep in sync with crm-mcp/crm_mcp/salesforce.py::_is_expired_session_error.

    Catches two documented patterns:
    1. Native 401 → `SalesforceExpiredSession`.
    2. 404 on /query with `INVALID_SESSION_ID` in the content list — the
       documented API response when the session is invalid.

    Empty-body 404s on /query are NOT classified as expired here — they are
    a transient SF edge response handled by `is_transient_query_404` and
    the polling caller (see commit 5686f19 for the false-positive reauth
    loop incident that this split protects against).
    """
    if isinstance(exc, SalesforceExpiredSession):
        return True
    content = getattr(exc, "content", None)
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("errorCode") == "INVALID_SESSION_ID":
                return True
    return "INVALID_SESSION_ID" in str(exc)


def is_transient_query_404(exc: Exception) -> bool:
    """Detect the transient empty-body 404 SF emits on /query during polling.

    Salesforce occasionally returns a bare 404 with an empty body on the
    REST `/query` endpoint. The next polling cycle recovers naturally;
    callers should skip-and-log, NOT reauthenticate. Reauth on this signal
    caused a documented ~40min false-positive reauth loop in production —
    see commit 5686f19.

    Distinct from `is_expired_session_error`: a real expired session is
    surfaced as `SalesforceExpiredSession` or as `INVALID_SESSION_ID` in
    the response content list.
    """
    if not isinstance(exc, SalesforceResourceNotFound):
        return False

    if getattr(exc, "status", None) != 404:
        return False

    resource_name = str(
        getattr(exc, "resource_name", None)
        or getattr(exc, "name", None)
        or "",
    ).strip("/")
    url = str(getattr(exc, "url", "")).rstrip("/")
    if resource_name != "query" and not url.endswith("/query"):
        return False

    content = getattr(exc, "content", None)
    return content in (None, "", b"", [])


class SalesforceSession:
    """Mutable container for a Salesforce client that can re-authenticate.

    Used by long-lived tasks (polling) where the underlying SF session can
    expire. `sf_call()` is the companion helper that wraps a call and
    triggers `reauth()` on expired-session errors.

    Kept intentionally small: this is a container, not a proxy. Callers use
    `session.sf.query(...)` directly or — preferably — go through `sf_call`
    which handles the retry.

    Concurrency: safe for multiple coroutines on a single event loop — the
    `_reauth_lock` serialises overlapping `reauth()` calls so a burst of
    expired-session errors only triggers ONE new login. NOT safe across
    processes or threads; SF clients should not be shared between them.
    """

    def __init__(
        self,
        sf: Salesforce,
        config: Config,
        shutdown_event: asyncio.Event | None = None,
    ) -> None:
        self.sf = sf
        self._config = config
        self._shutdown_event = shutdown_event
        self._reauth_lock = asyncio.Lock()

    async def reauth(self) -> None:
        """Swap the internal sf client for a freshly authenticated instance.

        Delegates to `get_salesforce_client` so all retry-on-transient and
        shutdown-abort logic is reused. The `_reauth_lock` ensures that if
        two coroutines race into `reauth()` simultaneously (future refactor
        with parallel sf_calls), only the first actually re-authenticates;
        the second waits on the lock and then finds a fresh `self.sf`.
        """
        async with self._reauth_lock:
            # Re-check staleness after acquiring the lock: a previous caller
            # may have already refreshed the session while we were waiting,
            # in which case we should skip the redundant login.
            # (Detection hook reserved for a future "is_session_fresh" probe;
            # current impl always reauths — single-task polling never races.)
            logger.warning("Salesforce session expired; reauthenticating...")
            # Route through the facade so tests that patch
            # `src.salesforce_client.get_salesforce_client` take effect here
            # too. Lazy import avoids a load-time cycle with the facade shim.
            from src import salesforce_client

            self.sf = await salesforce_client.get_salesforce_client(
                self._config, shutdown_event=self._shutdown_event,
            )
            logger.info("Salesforce session reauthenticated.")


async def sf_call(
    session: SalesforceSession,
    fn: Callable[[Salesforce], _T],
    *,
    max_reauths: int = 1,
) -> _T:
    """Run `fn(session.sf)` in a thread; reauth + retry on expired session.

    Behaviour:
    - Rate-limit errors propagate immediately (outer handler skips cycle).
    - Expired-session errors trigger at most `max_reauths` reauth attempts,
      each followed by one retry. Default max_reauths=1 is enough for the
      common "session just expired" case; anything more indicates the
      problem is not session-related.
    - All other errors propagate directly.

    The lambda takes `sf` as an argument (not a closure) so that after
    `session.reauth()` swaps the internal client, the retry uses the
    new instance.
    """
    attempt = 0
    while True:
        try:
            return await asyncio.to_thread(fn, session.sf)
        except Exception as exc:  # noqa: BLE001
            if is_rate_limit_error(exc):
                raise
            if not is_expired_session_error(exc) or attempt >= max_reauths:
                raise
            attempt += 1
            try:
                await session.reauth()
            except RuntimeError as reauth_exc:
                # get_salesforce_client signals shutdown via RuntimeError
                # ("...cancelled by shutdown signal"). Convert to
                # CancelledError so run_polling's outer handler re-raises
                # (exit cleanly) instead of logging "cycle failed" and
                # sleeping for the full polling interval.
                if "shutdown" in str(reauth_exc).lower():
                    raise asyncio.CancelledError() from reauth_exc
                raise
            # SalesforceAuthenticationFailed (bad creds) and other reauth
            # errors propagate unchanged — caller's outer handler logs them
            # so operators can distinguish "session expired + creds rotated"
            # from a routine cycle failure.


def _normalize_uuid_v4(value: Any) -> str | None:
    """Return a canonical UUID v4 string or None when invalid."""
    try:
        parsed = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None

    if parsed.version != 4:
        return None
    return str(parsed)


def _normalize_optional_field_value(value: Any) -> str | None:
    """Normalize optional Salesforce/text values so blanks behave like absence."""
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _parse_iso_datetime_utc(value: Any) -> datetime | None:
    """Parse an ISO-8601 datetime into UTC, or return None when invalid/missing."""
    normalized = _normalize_optional_field_value(value)
    if normalized is None:
        return None

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _wait_retry_or_shutdown(
    delay: float, shutdown_event: asyncio.Event | None = None,
) -> bool:
    """Sleep for `delay` seconds unless shutdown fires first.

    Returns True when shutdown was requested during the wait, False otherwise.
    Mirrors the helper in src/connection.py so both retry loops behave the
    same way (see plan: feature/sf-startup-retry).
    """
    if shutdown_event is None:
        await asyncio.sleep(delay)
        return False

    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
        return True
    except asyncio.TimeoutError:
        return False


async def get_salesforce_client(
    config: Config, shutdown_event: asyncio.Event | None = None,
) -> Salesforce:
    """Create an authenticated Salesforce client, retrying transient failures.

    Retries with exponential backoff (1s → 60s cap) on transient Salesforce
    auth codes (SERVER_UNAVAILABLE, SERVICE_UNAVAILABLE) and on generic
    network errors. Honours `shutdown_event` so a graceful shutdown during
    the retry backoff does not hang the container.

    Permanent authentication errors (INVALID_LOGIN, PASSWORD_LOCKOUT, bad
    security token) are re-raised immediately so the receiver task crashes
    visibly and the operator can tell the difference between "Salesforce is
    flaky" and "credentials are wrong".

    Args:
        config: Application configuration with SF credentials.
        shutdown_event: Optional event that aborts the retry loop when set.

    Returns:
        Authenticated Salesforce instance.

    Raises:
        SalesforceAuthenticationFailed: Permanent auth failure (bad creds).
        RuntimeError: Shutdown fired during the retry backoff.
    """
    delay = _SF_STARTUP_DELAY
    while True:
        if shutdown_event is not None and shutdown_event.is_set():
            raise RuntimeError(
                "Salesforce connection cancelled by shutdown signal",
            )

        try:
            logger.info(
                "Connecting to Salesforce as %s...", config.salesforce_username,
            )
            sf = await asyncio.to_thread(
                Salesforce,
                username=config.salesforce_username,
                password=config.salesforce_password,
                security_token=config.salesforce_security_token,
                domain=config.salesforce_domain,
                session=_TimeoutSession(),
            )
            logger.info("Connected to Salesforce.")
            return sf
        except SalesforceAuthenticationFailed as exc:
            if exc.code not in _TRANSIENT_SF_AUTH_CODES:
                logger.error(
                    "Salesforce authentication failed permanently "
                    "(code=%s): %s",
                    exc.code, exc,
                )
                raise
            logger.warning(
                "Salesforce transient auth failure (code=%s); "
                "retrying in %.1fs",
                exc.code, delay,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Salesforce connection failed (%s); retrying in %.1fs",
                exc, delay,
            )

        shutdown_requested = await _wait_retry_or_shutdown(
            delay, shutdown_event,
        )
        if shutdown_requested:
            raise RuntimeError(
                "Salesforce connection cancelled by shutdown signal",
            )
        delay = min(delay * 2, _SF_STARTUP_MAX_DELAY)


# ---------------------------------------------------------------------------
# Org-describe caches — custom field layouts don't change at runtime.
# ---------------------------------------------------------------------------

_active_field_cache: str | None = None
_mailing_id_field_supported_cache: bool | None = None
_kassa_id_field_supported_cache: bool | None = None
_account_active_field_cache: str | None = None
_planning_id_field_supported_cache: bool | None = None
_session_registration_object_supported_cache: bool | None = None
_account_email_field_cache: str | None = None
_account_country_field_cache: str | None = None
_account_house_number_field_supported_cache: bool | None = None


async def _resolve_contact_active_field_optional(sf: Salesforce) -> str | None:
    """Resolve the optional Contact active field without requiring the migration."""
    global _active_field_cache  # noqa: PLW0603
    if _active_field_cache is not None:
        return _active_field_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}

    for candidate in ("IsActive__c", "Active__c", "Is_Active__c"):
        if candidate in available_fields:
            _active_field_cache = candidate
            return candidate

    return None


async def _resolve_contact_active_field(sf: Salesforce) -> str:
    """Resolve which custom field is used as contact active flag in this org.

    Result is cached after first call - custom fields don't change at runtime.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is not None:
        return active_field

    raise RuntimeError(
        "No supported Contact active field found. Expected one of: "
        "IsActive__c, Active__c, Is_Active__c"
    )


async def has_contact_mailing_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Mailing_ID__c."""
    global _mailing_id_field_supported_cache  # noqa: PLW0603
    if _mailing_id_field_supported_cache is not None:
        return _mailing_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _mailing_id_field_supported_cache = "Mailing_ID__c" in available_fields
    return _mailing_id_field_supported_cache


async def has_contact_kassa_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Kassa_ID__c."""
    global _kassa_id_field_supported_cache  # noqa: PLW0603
    if _kassa_id_field_supported_cache is not None:
        return _kassa_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _kassa_id_field_supported_cache = "Kassa_ID__c" in available_fields
    return _kassa_id_field_supported_cache


async def _resolve_account_active_field_optional(sf: Salesforce) -> str | None:
    """Resolve the optional Account active field without requiring migration."""
    global _account_active_field_cache  # noqa: PLW0603
    if _account_active_field_cache is not None:
        return _account_active_field_cache

    describe = await asyncio.to_thread(sf.Account.describe)
    if asyncio.iscoroutine(describe):
        describe = await describe
    if not isinstance(describe, dict):
        return None
    available_fields = {field["name"] for field in describe.get("fields", [])}

    for candidate in ("IsActive__c", "Active__c", "Is_Active__c"):
        if candidate in available_fields:
            _account_active_field_cache = candidate
            return candidate

    return None


async def _resolve_account_active_field(sf: Salesforce) -> str:
    """Resolve which custom field is used as account active flag in this org."""
    active_field = await _resolve_account_active_field_optional(sf)
    if active_field is not None:
        return active_field

    raise RuntimeError(
        "No supported Account active field found. Expected one of: "
        "IsActive__c, Active__c, Is_Active__c"
    )


async def has_contact_planning_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Planning_ID__c."""
    global _planning_id_field_supported_cache  # noqa: PLW0603
    if _planning_id_field_supported_cache is not None:
        return _planning_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _planning_id_field_supported_cache = "Planning_ID__c" in available_fields
    return _planning_id_field_supported_cache


async def has_session_registration_object(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Session_Registration__c."""
    global _session_registration_object_supported_cache  # noqa: PLW0603
    if _session_registration_object_supported_cache is not None:
        return _session_registration_object_supported_cache

    describe = await asyncio.to_thread(sf.describe)
    available_objects = {
        sobject.get("name")
        for sobject in describe.get("sobjects", [])
        if isinstance(sobject, dict)
    }
    _session_registration_object_supported_cache = "Session_Registration__c" in available_objects
    return _session_registration_object_supported_cache


async def has_account_house_number_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Account.House_Number__c."""
    global _account_house_number_field_supported_cache  # noqa: PLW0603
    if _account_house_number_field_supported_cache is not None:
        return _account_house_number_field_supported_cache

    describe = await asyncio.to_thread(sf.Account.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _account_house_number_field_supported_cache = "House_Number__c" in available_fields
    return _account_house_number_field_supported_cache


async def _resolve_account_email_field(sf: Salesforce) -> str | None:
    """Probe which email field exists on Account, preferring Email__c over Email.

    Cached after first call — custom field layouts don't change at runtime.
    """
    global _account_email_field_cache  # noqa: PLW0603
    if _account_email_field_cache is not None:
        return _account_email_field_cache

    describe = await asyncio.to_thread(sf.Account.describe)
    available = {field["name"] for field in describe.get("fields", [])}
    for candidate in ("Email__c", "Email"):
        if candidate in available:
            _account_email_field_cache = candidate
            return candidate
    return None


async def _resolve_account_country_field(sf: Salesforce) -> str:
    """Probe whether this Salesforce org has State & Country Picklists enabled.

    When picklists are enabled, `BillingCountry` is a read-only derived label
    and writes must target `BillingCountryCode` (ISO-2 code). When picklists
    are disabled, only `BillingCountry` exists as a free-text field.

    Writing the wrong one yields `FIELD_INTEGRITY_EXCEPTION` — regression
    caught in production 2026-04-22. Cached after first call.
    """
    global _account_country_field_cache  # noqa: PLW0603
    if _account_country_field_cache is not None:
        return _account_country_field_cache

    describe = await asyncio.to_thread(sf.Account.describe)
    available = {field["name"] for field in describe.get("fields", [])}
    for candidate in ("BillingCountryCode", "BillingCountry"):
        if candidate in available:
            _account_country_field_cache = candidate
            return candidate
    # Safe default — every standard Account has BillingCountry even without picklists.
    return "BillingCountry"
