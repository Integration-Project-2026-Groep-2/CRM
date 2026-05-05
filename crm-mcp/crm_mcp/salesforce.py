"""Read-only Salesforce client wrapper for the CRM MCP server."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from simple_salesforce import Salesforce

from .config import SalesforceConfig

logger = logging.getLogger(__name__)


class CrmSalesforceClient:
    """Async Salesforce client tailored for read-only MCP access.

    Wraps simple-salesforce so all blocking calls run on a thread pool.
    Connects lazily on first query and caches both the connection and the
    custom-field describe results (active-flag detection).

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
            raise RuntimeError(
                f"Salesforce login failed: {type(exc).__name__}"
            ) from None

    async def query(self, soql: str) -> dict[str, Any]:
        sf = await self.connect()
        return await asyncio.to_thread(sf.query, soql)

    async def query_count(self, soql_count: str) -> int:
        result = await self.query(soql_count)
        return int(result.get("totalSize", 0))

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


_CONTACT_ACTIVE_NOT_FOUND_MSG = (
    "No supported Contact active field found. "
    "Expected one of: IsActive__c, Active__c, Is_Active__c."
)
