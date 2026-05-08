"""Async Salesforce client wrapper for the CRM MCP server."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest

from .config import SalesforceConfig

logger = logging.getLogger(__name__)


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
            self._sf = await asyncio.to_thread(self._safe_connect)
            logger.info(
                "Connected to Salesforce instance %s",
                self._config.domain,
            )
        return self._sf

    def _safe_connect(self) -> Salesforce:
        """Connect with credential-leak protection.

        `simple_salesforce.Salesforce(...)` may include keyword arguments
        (password / security_token) in the chained traceback. `from None`
        breaks the chain so credentials never reach the JSON-RPC error layer.
        """
        try:
            return Salesforce(
                username=self._config.username,
                password=self._config.password,
                security_token=self._config.security_token,
                domain=self._config.domain,
            )
        except Exception as exc:
            raise RuntimeError(f"Salesforce login failed: {type(exc).__name__}") from None

    async def query(self, soql: str) -> dict[str, Any]:
        sf = await self.connect()
        return await asyncio.to_thread(sf.query, soql)

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
            return await asyncio.to_thread(sf.Account.create, data)
        except SalesforceMalformedRequest as exc:
            if _is_duplicate_error(exc):
                raise ValueError(f"Account create rejected as duplicate: {exc}") from None
            raise

    async def update_account(self, account_id: str, data: dict[str, Any]) -> int:
        """Patch an existing Account by Salesforce Id. Returns HTTP status (204)."""
        sf = await self.connect()
        return await asyncio.to_thread(sf.Account.update, account_id, data)

    async def create_contact(self, data: dict[str, Any]) -> dict[str, Any]:
        """Insert a new Contact; map SF duplicate-errors to ValueError. See `create_account`."""
        sf = await self.connect()
        try:
            return await asyncio.to_thread(sf.Contact.create, data)
        except SalesforceMalformedRequest as exc:
            if _is_duplicate_error(exc):
                raise ValueError(f"Contact create rejected as duplicate: {exc}") from None
            raise

    async def update_contact(self, contact_id: str, data: dict[str, Any]) -> int:
        """Patch an existing Contact by Salesforce Id. Returns HTTP status (204)."""
        sf = await self.connect()
        return await asyncio.to_thread(sf.Contact.update, contact_id, data)

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
