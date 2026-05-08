"""Unit tests for the Salesforce client wrapper.

Focus: lazy connection, describe-cache correctness, concurrency safety.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from crm_mcp.config import SalesforceConfig
from crm_mcp.salesforce import CrmSalesforceClient


def _make_config() -> SalesforceConfig:
    return SalesforceConfig(
        username="u@example.com",
        password="pw",
        security_token="tok",
        domain="login",
    )


def _make_sf_mock(contact_fields: list[str], account_fields: list[str]) -> MagicMock:
    sf = MagicMock()
    sf.Contact.describe.return_value = {
        "fields": [{"name": n} for n in contact_fields]
    }
    sf.Account.describe.return_value = {
        "fields": [{"name": n} for n in account_fields]
    }
    return sf


@pytest.mark.asyncio
async def test_get_contact_active_field_resolves_from_describe() -> None:
    client = CrmSalesforceClient(_make_config())
    sf_mock = _make_sf_mock(["IsActive__c", "Email"], ["Name"])

    with patch.object(client, "_safe_connect", return_value=sf_mock):
        field = await client.get_contact_active_field()

    assert field == "IsActive__c"


@pytest.mark.asyncio
async def test_get_contact_active_field_raises_when_missing() -> None:
    client = CrmSalesforceClient(_make_config())
    sf_mock = _make_sf_mock(["Email", "Name"], ["Name"])  # no active flag

    with patch.object(client, "_safe_connect", return_value=sf_mock):
        with pytest.raises(RuntimeError, match="No supported Contact active field"):
            await client.get_contact_active_field()


@pytest.mark.asyncio
async def test_get_account_active_field_returns_none_when_absent() -> None:
    client = CrmSalesforceClient(_make_config())
    sf_mock = _make_sf_mock(["IsActive__c"], ["Name", "VAT_Number__c"])  # no Account active flag

    with patch.object(client, "_safe_connect", return_value=sf_mock):
        result = await client.get_account_active_field()

    assert result is None


@pytest.mark.asyncio
async def test_describe_cache_concurrent_calls_describe_once() -> None:
    """Regression: concurrent first-time callers must share one describe roundtrip.

    Two parallel coroutines hitting `get_contact_active_field` must result in
    `Contact.describe` being called exactly once thanks to the shared lock.
    """
    client = CrmSalesforceClient(_make_config())
    sf_mock = _make_sf_mock(["IsActive__c"], ["Name"])

    with patch.object(client, "_safe_connect", return_value=sf_mock):
        results = await asyncio.gather(
            client.get_contact_active_field(),
            client.get_contact_active_field(),
            client.get_contact_active_field(),
        )

    assert results == ["IsActive__c", "IsActive__c", "IsActive__c"]
    assert sf_mock.Contact.describe.call_count == 1


@pytest.mark.asyncio
async def test_describe_cache_persists_after_first_call() -> None:
    client = CrmSalesforceClient(_make_config())
    sf_mock = _make_sf_mock(["Active__c"], ["IsActive__c"])

    with patch.object(client, "_safe_connect", return_value=sf_mock):
        await client.get_contact_active_field()
        await client.get_contact_active_field()
        await client.get_account_active_field()
        await client.get_account_active_field()

    assert sf_mock.Contact.describe.call_count == 1
    assert sf_mock.Account.describe.call_count == 1


@pytest.mark.asyncio
async def test_safe_connect_strips_credential_chain() -> None:
    """Regression: SF login errors must not leak credentials in chained traceback."""
    client = CrmSalesforceClient(_make_config())

    def boom(**_: Any) -> None:
        raise ValueError("password=secret123 in error")

    with patch("crm_mcp.salesforce.Salesforce", side_effect=boom):
        with pytest.raises(RuntimeError, match="Salesforce login failed") as excinfo:
            await client.connect()

    # `from None` breaks the chain — original ValueError must not be __cause__.
    assert excinfo.value.__cause__ is None
    assert "password" not in str(excinfo.value)
    assert "secret123" not in str(excinfo.value)


@pytest.mark.asyncio
async def test_create_account_maps_duplicate_to_value_error() -> None:
    """C5 — SF DUPLICATE_VALUE error → ValueError; race-condition safety net."""
    from simple_salesforce.exceptions import SalesforceMalformedRequest

    client = CrmSalesforceClient(_make_config())
    sf_mock = MagicMock()
    sf_mock.Account.create.side_effect = SalesforceMalformedRequest(
        "url",
        400,
        "Account",
        [{"errorCode": "DUPLICATE_VALUE", "message": "duplicate value"}],
    )

    with (
        patch.object(client, "_safe_connect", return_value=sf_mock),
        pytest.raises(ValueError, match="duplicate"),
    ):
        await client.create_account({"Name": "X", "VAT_Number__c": "BE0123456789"})


@pytest.mark.asyncio
async def test_create_account_passes_through_non_duplicate_errors() -> None:
    """SF errors that are not duplicates must NOT be silently downgraded."""
    from simple_salesforce.exceptions import SalesforceMalformedRequest

    client = CrmSalesforceClient(_make_config())
    sf_mock = MagicMock()
    sf_mock.Account.create.side_effect = SalesforceMalformedRequest(
        "url",
        400,
        "Account",
        [{"errorCode": "REQUIRED_FIELD_MISSING", "message": "Name required"}],
    )

    with (
        patch.object(client, "_safe_connect", return_value=sf_mock),
        pytest.raises(SalesforceMalformedRequest),
    ):
        await client.create_account({})
