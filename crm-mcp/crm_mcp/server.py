"""CRM MCP server entrypoint.

Registers all CRM tools against a single FastMCP instance and starts the server
on the configured transport (stdio for local dev, streamable-http for the
deployed agent).

Tools split:
    - 7 read-only (search/get/count/recent on Contact, Company, Registration)
    - 6 write (create/update/delete on Contact, Company) — R2 actionable agent

Write-tools require a bound `MessagePublisher` (see `messaging.py`) to broadcast
XSD-validated XML events on RabbitMQ. They are annotated with
`requires_approval=True` so MCP-clients (mcp-master) can route them through an
approval-flow before execution.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import SalesforceConfig, ServerConfig
from .messaging import MessagePublisher
from .models import (
    CompanyDetails,
    CompanySummary,
    ContactCount,
    ContactDetails,
    ContactSummary,
    MutationResult,
    RegistrationCount,
)
from .salesforce import CrmSalesforceClient
from .tools import companies as company_tools
from .tools import contacts as contact_tools
from .tools import registrations as registration_tools

logger = logging.getLogger(__name__)


_READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

# Lightweight SOQL probe for the /health endpoint. Cheap (LIMIT 1) and avoids
# describe-cache side effects. Cached for _SF_PROBE_CACHE_SECONDS so a flood of
# healthcheck calls (Docker, k8s, monitoring) doesn't burn API quota.
_SF_PROBE_SOQL = "SELECT Id FROM Contact LIMIT 1"
_SF_PROBE_CACHE_SECONDS = 30.0
_SF_PROBE_TIMEOUT_SECONDS = 5.0


class _HealthState:
    """Probe-state for the /health endpoint.

    Caches the SF reachability check so healthcheck-burst doesn't multiply
    Salesforce API calls. Cache TTL is short on success (30s) and shorter on
    failure (5s) so recovery is detected quickly.
    """

    def __init__(self, client: CrmSalesforceClient, publisher: MessagePublisher) -> None:
        self._client = client
        self._publisher = publisher
        self._start_time = time.monotonic()
        self._sf_cache: tuple[bool, float] | None = None
        self._sf_lock = asyncio.Lock()

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._start_time

    async def check_sf(self) -> bool:
        async with self._sf_lock:
            now = time.monotonic()
            if self._sf_cache is not None and now < self._sf_cache[1]:
                return self._sf_cache[0]
            try:
                await asyncio.wait_for(
                    self._client.query(_SF_PROBE_SOQL),
                    timeout=_SF_PROBE_TIMEOUT_SECONDS,
                )
                self._sf_cache = (True, now + _SF_PROBE_CACHE_SECONDS)
                return True
            except Exception as exc:
                logger.warning("Salesforce health-probe failed: %s", type(exc).__name__)
                self._sf_cache = (False, now + 5.0)
                return False

    def check_publisher(self) -> bool:
        return self._publisher.is_ready()


def build_server(
    client: CrmSalesforceClient,
    publisher: MessagePublisher,
    *,
    host: str = "0.0.0.0",
    port: int = 7001,
) -> FastMCP:
    """Build the FastMCP server with all CRM tools registered.

    `publisher` may be unbound at registration time — write-tools tolerate this
    via `MessagePublisher.is_ready()` and raise `PublisherNotReadyError` only
    when actually invoked before the publisher is bound.
    """
    mcp = FastMCP(
        "crm-mcp",
        instructions=(
            "CRM team's MCP server for the Desideriushogeschool ShiftFestival "
            "integration project. Provides read access to Salesforce Contacts, "
            "Accounts (Companies), and Session_Registration__c records, plus "
            "CRUD write-tools for Contact and Company. Write operations broadcast "
            "XSD-validated XML on RabbitMQ (`crm.user.*` / `crm.company.*`) so all "
            "consumer teams (Mailing, Facturatie, Kassa, Planning, IoT, "
            "Controlroom) stay in sync — there is no team-private state. "
            "Write-tools are annotated `requires_approval=true` and should be "
            "routed through approval-flow by MCP-clients."
        ),
        host=host,
        port=port,
    )

    health_state = _HealthState(client, publisher)

    @mcp.custom_route("/health", methods=["GET"])
    async def health(request: Request) -> JSONResponse:
        sf_connected = await health_state.check_sf()
        publisher_bound = health_state.check_publisher()
        all_ok = sf_connected and publisher_bound
        body = {
            "status": "ok" if all_ok else "degraded",
            "uptime_seconds": round(health_state.uptime_seconds, 1),
            "checks": {
                "sf_connected": sf_connected,
                "publisher_bound": publisher_bound,
            },
        }
        return JSONResponse(body, status_code=200 if all_ok else 503)

    # ---- Read-only tools ----

    @mcp.tool(annotations=_READ_ONLY)
    async def search_contact(query: str, limit: int = 10) -> list[ContactSummary]:
        """Find candidate contacts (deelnemers) by name, email, or phone fragment.

        Returns lightweight summaries (id, name, email). Use `get_contact` to
        fetch full details for a chosen Id. Minimum query length is 2 chars.
        """
        return await contact_tools.search_contact(client, query=query, limit=limit)

    @mcp.tool(annotations=_READ_ONLY)
    async def get_contact(contact_id: str) -> ContactDetails | None:
        """Retrieve full contact details by exact Salesforce Contact Id (starts with '003').

        Returns None if the contact does not exist.
        """
        return await contact_tools.get_contact(client, contact_id=contact_id)

    @mcp.tool(annotations=_READ_ONLY)
    async def count_contacts(
        role: Literal["VISITOR", "COMPANY_CONTACT"] | None = None,
        is_active: bool | None = True,
        gdpr_consent: bool | None = None,
        has_paid: bool | None = None,
    ) -> ContactCount:
        """Count contacts with optional filters; returns totals plus role/active breakdowns.

        - role: VISITOR or COMPANY_CONTACT (default: any)
        - is_active: true (default) / false / null=any
        - gdpr_consent: filter on GDPR_Consent__c
        - has_paid: filter on Paid_At__c being non-null (derived field)
        """
        return await contact_tools.count_contacts(
            client,
            role=role,
            is_active=is_active,
            gdpr_consent=gdpr_consent,
            has_paid=has_paid,
        )

    @mcp.tool(annotations=_READ_ONLY)
    async def recent_contacts(
        mode: Literal["created", "modified"] = "modified",
        since_hours: int = 24,
        limit: int = 20,
    ) -> list[ContactSummary]:
        """Recently created or modified contacts within `since_hours` hours.

        Use `get_contact` for full details on any returned Id. since_hours is
        capped at 168 (one week) and limit at 100.
        """
        return await contact_tools.recent_contacts(
            client, mode=mode, since_hours=since_hours, limit=limit
        )

    @mcp.tool(annotations=_READ_ONLY)
    async def search_company(query: str, limit: int = 10) -> list[CompanySummary]:
        """Find candidate companies by name or VAT-number fragment.

        Returns lightweight summaries; use `get_company` for full details.
        """
        return await company_tools.search_company(client, query=query, limit=limit)

    @mcp.tool(annotations=_READ_ONLY)
    async def get_company(
        vat_number: str | None = None,
        company_id: str | None = None,
    ) -> CompanyDetails | None:
        """Retrieve full company details by VAT-number or by Salesforce Account Id.

        Provide one of `vat_number` or `company_id`. VAT-number is the canonical
        business key per Contract 3 deduplication rule.
        """
        return await company_tools.get_company(client, vat_number=vat_number, company_id=company_id)

    @mcp.tool(annotations=_READ_ONLY)
    async def count_registrations(
        session_id: str | None = None,
        is_active: bool | None = True,
        paid: bool | None = None,
        since: datetime | None = None,
    ) -> RegistrationCount:
        """Count Session_Registration__c records with optional filters.

        - session_id: Planning-side session identifier (Session_ID__c)
        - is_active: default true; soft-delete filter
        - paid: derived from Paid_At__c != null
        - since: filter on LastModifiedDate (ISO-8601 datetime)
        """
        return await registration_tools.count_registrations(
            client,
            session_id=session_id,
            is_active=is_active,
            paid=paid,
            since=since,
        )

    # ---- Write tools (R2) — each broadcasts an XSD-validated event ----

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def create_company(
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
        """Create a Company (Account) and broadcast C14 `<CompanyConfirmed>`.

        VAT-uniqueness is pre-checked (Contract 3 dedup-rule). `country` is
        ISO 3166-1 alpha-2 (e.g. 'BE'). All consumer teams subscribe to
        `crm.company.confirmed` and will create their own copy automatically.
        """
        return await company_tools.create_company(
            client,
            publisher,
            name=name,
            vat_number=vat_number,
            email=email,
            street=street,
            house_number=house_number,
            postal_code=postal_code,
            city=city,
            country=country,
            phone=phone,
        )

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def update_company(
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
        """Patch a Company by CRM_ID__c and broadcast C19 `<CompanyUpdated>`.

        Partial-update: omitted fields stay as-is in Salesforce. `crm_id` is
        the UUID v4 stamped on the record at create-time (not the Salesforce
        Id). `street` and `house_number` must be updated together.
        """
        return await company_tools.update_company(
            client,
            publisher,
            crm_id=crm_id,
            name=name,
            vat_number=vat_number,
            email=email,
            phone=phone,
            street=street,
            house_number=house_number,
            postal_code=postal_code,
            city=city,
            country=country,
            is_active=is_active,
        )

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=True,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def delete_company(crm_id: str, hard: bool = False) -> MutationResult:
        """Soft-delete a Company (IsActive__c=false) and broadcast C23 `<CompanyDeactivated>`.

        `hard=True` is rejected — XSD has no hard-delete contract and project
        policy is GDPR soft-delete only.
        """
        return await company_tools.delete_company(client, publisher, crm_id=crm_id, hard=hard)

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def create_contact(
        email: str,
        first_name: str,
        last_name: str,
        role: Literal[
            "VISITOR",
            "COMPANY_CONTACT",
            "SPEAKER",
            "EVENT_MANAGER",
            "CASHIER",
            "BAR_STAFF",
            "ADMIN",
        ],
        gdpr_consent: bool,
        phone: str | None = None,
        company_id: str | None = None,
        badge_code: str | None = None,
    ) -> MutationResult:
        """Create a Contact and broadcast C13 `<UserConfirmed>`.

        `company_id` is the linked Company's CRM_ID__c (UUID v4) — resolved
        server-side to the Salesforce AccountId. Email-uniqueness pre-checked.
        All consumer teams subscribe to `crm.user.confirmed`.
        """
        return await contact_tools.create_contact(
            client,
            publisher,
            email=email,
            first_name=first_name,
            last_name=last_name,
            role=role,
            gdpr_consent=gdpr_consent,
            phone=phone,
            company_id=company_id,
            badge_code=badge_code,
        )

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=True,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def update_contact(
        crm_id: str,
        email: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        role: Literal[
            "VISITOR",
            "COMPANY_CONTACT",
            "SPEAKER",
            "EVENT_MANAGER",
            "CASHIER",
            "BAR_STAFF",
            "ADMIN",
        ]
        | None = None,
        phone: str | None = None,
        company_id: str | None = None,
        badge_code: str | None = None,
        gdpr_consent: bool | None = None,
        is_active: bool | None = None,
    ) -> MutationResult:
        """Patch a Contact by CRM_ID__c and broadcast C18 `<UserUpdated>`.

        Partial-update: omitted fields stay as-is. `crm_id` is the UUID v4
        stamped at create-time (not the Salesforce Id).
        """
        return await contact_tools.update_contact(
            client,
            publisher,
            crm_id=crm_id,
            email=email,
            first_name=first_name,
            last_name=last_name,
            role=role,
            phone=phone,
            company_id=company_id,
            badge_code=badge_code,
            gdpr_consent=gdpr_consent,
            is_active=is_active,
        )

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=True,
            openWorldHint=False,
            requires_approval=True,
        )
    )
    async def delete_contact(crm_id: str, hard: bool = False) -> MutationResult:
        """Soft-delete a Contact (IsActive__c=false) and broadcast C22 `<UserDeactivated>`.

        `hard=True` is rejected — GDPR soft-delete only.
        """
        return await contact_tools.delete_contact(client, publisher, crm_id=crm_id, hard=hard)

    return mcp


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sf_config = SalesforceConfig.from_env()
    server_config = ServerConfig.from_env()
    client = CrmSalesforceClient(sf_config)
    publisher = MessagePublisher()  # standalone-mode: never bound, write-tools error
    mcp = build_server(client, publisher, host=server_config.host, port=server_config.port)

    logger.info(
        "Starting CRM MCP server (transport=%s host=%s port=%d)",
        server_config.transport,
        server_config.host,
        server_config.port,
    )
    mcp.run(transport=server_config.transport)


if __name__ == "__main__":
    main()
