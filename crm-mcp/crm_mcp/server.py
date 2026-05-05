"""CRM MCP server entrypoint.

Registers all seven tools against a single FastMCP instance and starts the
server on the configured transport (stdio for local dev, streamable-http for
the deployed agent).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

from mcp.server.fastmcp import FastMCP

from .config import SalesforceConfig, ServerConfig
from .models import (
    CompanyDetails,
    CompanySummary,
    ContactCount,
    ContactDetails,
    ContactSummary,
    RegistrationCount,
)
from .salesforce import CrmSalesforceClient
from .tools import companies as company_tools
from .tools import contacts as contact_tools
from .tools import registrations as registration_tools

logger = logging.getLogger(__name__)


def build_server(client: CrmSalesforceClient, *, host: str = "0.0.0.0", port: int = 7001) -> FastMCP:
    """Build the FastMCP server with all CRM tools registered."""
    mcp = FastMCP(
        "crm-mcp",
        instructions=(
            "CRM team's MCP server for the Desideriushogeschool ShiftFestival "
            "integration project. Provides read-only access to Salesforce "
            "Contacts, Accounts (Companies), and Session_Registration__c records. "
            "Write flows happen via XML contracts on RabbitMQ — never via this server."
        ),
        host=host,
        port=port,
    )

    @mcp.tool()
    async def search_contact(query: str, limit: int = 10) -> list[ContactSummary]:
        """Find candidate contacts (deelnemers) by name, email, or phone fragment.

        Returns lightweight summaries (id, name, email). Use `get_contact` to
        fetch full details for a chosen Id. Minimum query length is 2 chars.
        """
        return await contact_tools.search_contact(client, query=query, limit=limit)

    @mcp.tool()
    async def get_contact(contact_id: str) -> ContactDetails | None:
        """Retrieve full contact details by exact Salesforce Contact Id (starts with '003').

        Returns None if the contact does not exist.
        """
        return await contact_tools.get_contact(client, contact_id=contact_id)

    @mcp.tool()
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

    @mcp.tool()
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

    @mcp.tool()
    async def search_company(query: str, limit: int = 10) -> list[CompanySummary]:
        """Find candidate companies by name or VAT-number fragment.

        Returns lightweight summaries; use `get_company` for full details.
        """
        return await company_tools.search_company(client, query=query, limit=limit)

    @mcp.tool()
    async def get_company(
        vat_number: str | None = None,
        company_id: str | None = None,
    ) -> CompanyDetails | None:
        """Retrieve full company details by VAT-number or by Salesforce Account Id.

        Provide one of `vat_number` or `company_id`. VAT-number is the canonical
        business key per Contract 3 deduplication rule.
        """
        return await company_tools.get_company(
            client, vat_number=vat_number, company_id=company_id
        )

    @mcp.tool()
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

    return mcp


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    sf_config = SalesforceConfig.from_env()
    server_config = ServerConfig.from_env()
    client = CrmSalesforceClient(sf_config)
    mcp = build_server(client, host=server_config.host, port=server_config.port)

    logger.info(
        "Starting CRM MCP server (transport=%s host=%s port=%d)",
        server_config.transport,
        server_config.host,
        server_config.port,
    )
    mcp.run(transport=server_config.transport)


if __name__ == "__main__":
    main()
