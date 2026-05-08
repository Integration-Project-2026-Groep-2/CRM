"""Shared pytest fixtures for the CRM MCP test suite."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from crm_mcp.messaging import MessagePublisher
from crm_mcp.salesforce import CrmSalesforceClient


@pytest.fixture
def fake_sf_client() -> CrmSalesforceClient:
    """Return a CrmSalesforceClient with all read+write methods mocked.

    Tests should set `client.query.return_value` or use `side_effect` on
    `client.query_count` directly. Write methods (`create_account`,
    `update_account`, `create_contact`, `update_contact`) default to AsyncMocks
    returning a Salesforce-style success response — override per test as needed.
    """
    client = AsyncMock(spec=CrmSalesforceClient)
    client.get_contact_active_field = AsyncMock(return_value="IsActive__c")
    client.get_account_active_field = AsyncMock(return_value="IsActive__c")
    client.has_account_house_number_field = AsyncMock(return_value=True)
    client.get_account_email_field = AsyncMock(return_value="Email__c")
    client.get_account_country_field = AsyncMock(return_value="BillingCountryCode")
    client.create_account = AsyncMock(
        return_value={"id": "001gK00000NewAcc1AAA", "success": True, "errors": []}
    )
    client.update_account = AsyncMock(return_value=204)
    client.create_contact = AsyncMock(
        return_value={"id": "003gK00000NewCon1AAA", "success": True, "errors": []}
    )
    client.update_contact = AsyncMock(return_value=204)
    return client


@pytest.fixture
def make_query_response() -> Callable[..., dict[str, Any]]:
    """Helper to build a fake Salesforce query response from a list of records."""

    def _build(records: list[dict[str, Any]]) -> dict[str, Any]:
        return {"totalSize": len(records), "done": True, "records": records}

    return _build


@pytest.fixture
def fake_publisher() -> MessagePublisher:
    """Return a MessagePublisher with all 6 publish_* methods mocked.

    Uses a MagicMock with spec=MessagePublisher so attribute typos still fail.
    Tests assert on `publisher.publish_*.call_args` to verify the broadcast
    payload matches the XSD-required keys.
    """
    publisher = MagicMock(spec=MessagePublisher)
    publisher.is_ready = MagicMock(return_value=True)
    return publisher
