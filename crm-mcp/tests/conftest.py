"""Shared pytest fixtures for the CRM MCP test suite."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crm_mcp.salesforce import CrmSalesforceClient


@pytest.fixture
def fake_sf_client() -> CrmSalesforceClient:
    """Return a CrmSalesforceClient with `query`/`query_count` mocked.

    Tests should set `client.query.return_value` or use `side_effect` on
    `client.query_count` directly.
    """
    client = AsyncMock(spec=CrmSalesforceClient)
    client.get_contact_active_field = AsyncMock(return_value="IsActive__c")
    client.get_account_active_field = AsyncMock(return_value="IsActive__c")
    return client


@pytest.fixture
def make_query_response() -> Callable[..., dict[str, Any]]:
    """Helper to build a fake Salesforce query response from a list of records."""
    def _build(records: list[dict[str, Any]]) -> dict[str, Any]:
        return {"totalSize": len(records), "done": True, "records": records}

    return _build
