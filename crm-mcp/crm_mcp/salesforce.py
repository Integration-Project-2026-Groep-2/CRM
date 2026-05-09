"""Async Salesforce client wrapper for the CRM MCP server."""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any

import requests
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

from .config import SalesforceConfig

logger = logging.getLogger(__name__)


# Without a default per-request timeout, `requests` blocks forever on stalled
# sockets. Symptom in productie 2026-05-09: cold-start tool-call hung 794s
# before the receiver-side fix landed (`src/salesforce/client.py`). MCP needed
# the same shield — that's this class.
_DEFAULT_SF_HTTP_TIMEOUT_SECONDS = 30.0
_LOGIN_RETRY_ATTEMPTS = 3
_LOGIN_RETRY_BASE_DELAY = 1.0
_LOGIN_RETRY_MAX_DELAY = 8.0

# Per-call retry: a single `REQUEST_LIMIT_EXCEEDED` from SF or a transient
# network blip should not bubble up as a tool-failure to the LLM. We retry
# transparently on transient errors only — validation/auth errors propagate
# immediately so callers see real problems (and don't waste API quota).
_CALL_RETRY_ATTEMPTS = 3
_CALL_RETRY_BASE_DELAY = 0.5
_CALL_RETRY_MAX_DELAY = 4.0


def _is_rate_limit_error(exc: Exception) -> bool:
    """Detect Salesforce REQUEST_LIMIT_EXCEEDED via content attribute or message.

    Mirrors `src/salesforce/client.py:127-140` — duplicated here because the
    crm-mcp package is structurally independent from the parent receiver.
    Consolidation into a shared module is Tier-3's L3 PR.
    """
    content = getattr(exc, "content", None)
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("errorCode") == "REQUEST_LIMIT_EXCEEDED":
                return True
    return "REQUEST_LIMIT_EXCEEDED" in str(exc)


def _is_transient_call_error(exc: Exception) -> bool:
    """True for transient errors worth a retry (connection / timeout)."""
    if _is_rate_limit_error(exc):
        return True
    # `requests`-layer flakes: peer reset, DNS blip, read timeout, chunked
    # encoding mid-response, generic IO failure on the SF socket.
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    return False


class _TimeoutSession(requests.Session):
    """`requests.Session` that injects a default timeout on every call."""

    def __init__(self, timeout: float = _DEFAULT_SF_HTTP_TIMEOUT_SECONDS) -> None:
        super().__init__()
        self._default_timeout = timeout

    def request(self, method, url, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._default_timeout)
        return super().request(method, url, **kwargs)


def _resolve_timeout_seconds() -> float:
    raw = os.getenv("CRM_MCP_SF_TIMEOUT_SECONDS")
    if raw is None:
        return _DEFAULT_SF_HTTP_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "CRM_MCP_SF_TIMEOUT_SECONDS=%r not a float; using default %.1fs",
            raw,
            _DEFAULT_SF_HTTP_TIMEOUT_SECONDS,
        )
        return _DEFAULT_SF_HTTP_TIMEOUT_SECONDS
    return value if value > 0 else _DEFAULT_SF_HTTP_TIMEOUT_SECONDS


class CrmSalesforceClient:
    """Async Salesforce client for read + write MCP access.

    Wraps simple-salesforce so all blocking calls run on a thread pool.
    Connects lazily on first query and caches both the connection and the
    custom-field describe results (active-flag, address, email, country).

    Concurrency model: a single `_lock` serialises connection establishment
    AND describe-cache reads, ensuring concurrent first-time callers don't
    duplicate the SF roundtrip.
    """

    def __init__(self, config: SalesforceConfig) -> None:
        self._config = config
        self._sf: Salesforce | None = None
        self._lock = asyncio.Lock()

        # Describe-cache. The bool flags signal "already resolved" so we can
        # distinguish "not looked up" from "looked up and no field exists".
        self._contact_active_field: str | None = None
        self._contact_active_field_resolved: bool = False
        self._account_active_field: str | None = None
        self._account_active_field_resolved: bool = False
        self._account_has_house_number: bool = False
        self._account_house_number_resolved: bool = False
        self._account_email_field: str | None = None
        self._account_email_field_resolved: bool = False
        self._account_country_field: str = "BillingCountryCode"
        self._account_country_field_resolved: bool = False

    async def connect(self) -> Salesforce:
        """Lazily establish and cache a Salesforce session."""
        async with self._lock:
            return await self._ensure_sf_locked()

    async def _ensure_sf_locked(self) -> Salesforce:
        """Caller must already hold `self._lock`."""
        if self._sf is None:
            self._sf = await asyncio.to_thread(self._connect_with_retry)
            logger.info(
                "Connected to Salesforce instance %s",
                self._config.domain,
            )
        return self._sf

    def _connect_with_retry(self) -> Salesforce:
        """Login with exponential-backoff retry on transient failures."""
        last_exc: Exception | None = None
        for attempt in range(1, _LOGIN_RETRY_ATTEMPTS + 1):
            try:
                return self._safe_connect()
            except RuntimeError as exc:
                last_exc = exc
                if attempt == _LOGIN_RETRY_ATTEMPTS:
                    break
                delay = min(
                    _LOGIN_RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                    _LOGIN_RETRY_MAX_DELAY,
                ) + random.uniform(0, 0.5)
                logger.warning(
                    "Salesforce login attempt %d/%d failed; retrying in %.1fs",
                    attempt,
                    _LOGIN_RETRY_ATTEMPTS,
                    delay,
                )
                # Synchronous sleep — caller already on a worker-thread via
                # asyncio.to_thread, so blocking here doesn't stall the event
                # loop. Using asyncio.sleep would require restructuring.
                import time
                time.sleep(delay)
        assert last_exc is not None
        raise last_exc

    def _safe_connect(self) -> Salesforce:
        """Connect with credential-leak protection + bounded HTTP timeouts.

        `simple_salesforce.Salesforce(...)` may include keyword arguments
        (password / security_token) in the chained traceback. `from None`
        breaks the chain so credentials never reach the JSON-RPC error layer.

        The injected `_TimeoutSession` enforces a per-request read-timeout so
        a stalled socket can't hang a tool-call indefinitely (cf. 2026-05-09
        cold-start incident — see module-docstring on `_TimeoutSession`).
        """
        try:
            return Salesforce(
                username=self._config.username,
                password=self._config.password,
                security_token=self._config.security_token,
                domain=self._config.domain,
                session=_TimeoutSession(timeout=_resolve_timeout_seconds()),
            )
        except Exception as exc:
            raise RuntimeError(f"Salesforce login failed: {type(exc).__name__}") from None

    async def _call_with_retry(self, label: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking SF call on a worker-thread; retry on transient errors.

        Used by `query`, `create_*`, `update_*` so a single rate-limit / network
        blip becomes an invisible retry instead of a tool-failure to the LLM.
        Validation, auth, and other deterministic errors propagate immediately
        (re-trying them wastes API quota and delays the real failure).

        `label` is used only for the warning log so operators can correlate
        retries with which SF operation was flaky.
        """
        last_exc: Exception | None = None
        for attempt in range(1, _CALL_RETRY_ATTEMPTS + 1):
            try:
                return await asyncio.to_thread(fn, *args, **kwargs)
            except Exception as exc:
                if not _is_transient_call_error(exc) or attempt == _CALL_RETRY_ATTEMPTS:
                    raise
                last_exc = exc
                delay = min(
                    _CALL_RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                    _CALL_RETRY_MAX_DELAY,
                ) + random.uniform(0, 0.25)
                logger.warning(
                    "Salesforce %s attempt %d/%d failed (%s); retrying in %.1fs",
                    label,
                    attempt,
                    _CALL_RETRY_ATTEMPTS,
                    type(exc).__name__,
                    delay,
                )
                await asyncio.sleep(delay)
        # Unreachable: loop either returns or raises. Defensive fallback.
        assert last_exc is not None
        raise last_exc

    async def query(self, soql: str) -> dict[str, Any]:
        sf = await self.connect()
        return await self._call_with_retry("query", sf.query, soql)

    async def query_count(self, soql_count: str) -> int:
        result = await self.query(soql_count)
        return int(result.get("totalSize", 0))

    async def create_account(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a new Account; map SF duplicate-errors to ValueError.

        Caller is expected to have set CRM_ID__c and any other required custom
        fields. Returns ``{"id": "001...", "success": true, "errors": []}``.
        Raises ``ValueError`` if SF rejects the create on a uniqueness rule
        (atomic safety-net for the SELECT-then-INSERT race) — re-raises other
        SF errors unchanged.
        """
        sf = await self.connect()
        try:
            return await self._call_with_retry("Account.create", sf.Account.create, data)
        except SalesforceMalformedRequest as exc:
            if _is_duplicate_error(exc):
                raise ValueError(f"Account create rejected as duplicate: {exc}") from None
            raise

    async def update_account(self, account_id: str, data: dict[str, Any]) -> int:
        """Patch an existing Account by Salesforce Id. Returns HTTP status (204)."""
        sf = await self.connect()
        return await self._call_with_retry("Account.update", sf.Account.update, account_id, data)

    async def create_contact(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a new Contact; map SF duplicate-errors to ValueError. See `create_account`."""
        sf = await self.connect()
        try:
            return await self._call_with_retry("Contact.create", sf.Contact.create, data)
        except SalesforceMalformedRequest as exc:
            if _is_duplicate_error(exc):
                raise ValueError(f"Contact create rejected as duplicate: {exc}") from None
            raise

    async def update_contact(self, contact_id: str, data: dict[str, Any]) -> int:
        """Patch an existing Contact by Salesforce Id. Returns HTTP status (204)."""
        sf = await self.connect()
        return await self._call_with_retry("Contact.update", sf.Contact.update, contact_id, data)

    async def get_contact_active_field(self) -> str:
        """Return the Contact active-flag field; cached after first describe."""
        async with self._lock:
            if self._contact_active_field_resolved:
                if self._contact_active_field is None:
                    raise RuntimeError(_CONTACT_ACTIVE_NOT_FOUND_MSG)
                return self._contact_active_field

            sf = await self._ensure_sf_locked()
            describe = await asyncio.to_thread(sf.Contact.describe)
            names = {f["name"] for f in describe.get("fields", [])}
            for cand in ("IsActive__c", "Active__c", "Is_Active__c"):
                if cand in names:
                    self._contact_active_field = cand
                    self._contact_active_field_resolved = True
                    return cand
            self._contact_active_field_resolved = True
            raise RuntimeError(_CONTACT_ACTIVE_NOT_FOUND_MSG)

    async def get_account_active_field(self) -> str | None:
        """Return Account active-field name or None if no field exists.

        Account active-flag is optional in this org — None signals all
        accounts should be treated as active.
        """
        async with self._lock:
            if self._account_active_field_resolved:
                return self._account_active_field

            sf = await self._ensure_sf_locked()
            describe = await asyncio.to_thread(sf.Account.describe)
            names = {f["name"] for f in describe.get("fields", [])}
            for cand in ("IsActive__c", "Active__c", "Is_Active__c"):
                if cand in names:
                    self._account_active_field = cand
                    self._account_active_field_resolved = True
                    return cand
            self._account_active_field = None
            self._account_active_field_resolved = True
            return None

    async def has_account_house_number_field(self) -> bool:
        """Return True if Account has a `House_Number__c` custom field.

        Frontend handler `_build_frontend_account_data` only populates the
        custom field when present, otherwise lets the receiver fall back to a
        BillingStreet-only address (lossy). MCP write-tools must mirror that
        behaviour to avoid producing payloads with inconsistent address shape.
        """
        async with self._lock:
            if self._account_house_number_resolved:
                return self._account_has_house_number

            sf = await self._ensure_sf_locked()
            describe = await asyncio.to_thread(sf.Account.describe)
            names = {f["name"] for f in describe.get("fields", [])}
            self._account_has_house_number = "House_Number__c" in names
            self._account_house_number_resolved = True
            return self._account_has_house_number

    async def get_account_email_field(self) -> str | None:
        """Return the Account email-field name (`Email__c` preferred, else `Email`).

        Returns None if neither exists — callers should skip writing email to SF
        in that case. The outbound C14/C19 broadcast still carries email since
        XSD requires it.
        """
        async with self._lock:
            if self._account_email_field_resolved:
                return self._account_email_field

            sf = await self._ensure_sf_locked()
            describe = await asyncio.to_thread(sf.Account.describe)
            names = {f["name"] for f in describe.get("fields", [])}
            for cand in ("Email__c", "Email"):
                if cand in names:
                    self._account_email_field = cand
                    self._account_email_field_resolved = True
                    return cand
            self._account_email_field = None
            self._account_email_field_resolved = True
            return None

    async def get_account_country_field(self) -> str:
        """Return the Account country-field name.

        Probes for `BillingCountryCode` (State+Country picklists enabled) and
        falls back to `BillingCountry` (text). Defaults to `BillingCountryCode`
        if neither describe-resolves — XSD validation surfaces the failure.
        """
        async with self._lock:
            if self._account_country_field_resolved:
                return self._account_country_field

            sf = await self._ensure_sf_locked()
            describe = await asyncio.to_thread(sf.Account.describe)
            names = {f["name"] for f in describe.get("fields", [])}
            for cand in ("BillingCountryCode", "BillingCountry"):
                if cand in names:
                    self._account_country_field = cand
                    self._account_country_field_resolved = True
                    return cand
            self._account_country_field_resolved = True
            return self._account_country_field


def _is_duplicate_error(exc: SalesforceMalformedRequest) -> bool:
    """Detect SF duplicate-uniqueness rejection in a SalesforceMalformedRequest."""
    text = str(exc).upper()
    return any(
        marker in text
        for marker in ("DUPLICATE_VALUE", "DUPLICATES_DETECTED", "DUPLICATE_EXTERNAL_ID")
    )


_CONTACT_ACTIVE_NOT_FOUND_MSG = (
    "No supported Contact active field found. "
    "Expected one of: IsActive__c, Active__c, Is_Active__c."
)
