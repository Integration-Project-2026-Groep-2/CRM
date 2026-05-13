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


def test_safe_connect_passes_timeout_session() -> None:
    """A1 — Salesforce(...) must receive a `_TimeoutSession` so HTTP calls
    can't hang indefinitely. This is the fix for the 794s cold-start incident.
    """
    from crm_mcp.salesforce import _TimeoutSession

    client = CrmSalesforceClient(_make_config())
    captured: dict[str, Any] = {}

    def fake_salesforce(**kwargs):
        captured.update(kwargs)
        return MagicMock()

    with patch("crm_mcp.salesforce.Salesforce", side_effect=fake_salesforce):
        client._safe_connect()

    assert "session" in captured
    assert isinstance(captured["session"], _TimeoutSession)


def test_timeout_session_injects_default_timeout() -> None:
    """The session must inject `timeout=` on every request unless caller sets it."""
    from crm_mcp.salesforce import _TimeoutSession

    session = _TimeoutSession(timeout=12.0)
    captured_kwargs: dict[str, Any] = {}

    def fake_send(self, *args, **kwargs):
        # `requests.Session.request` calls `self.send`. We intercept higher up
        # via patching the parent's `request` method instead.
        return MagicMock()

    with patch.object(
        type(session).__mro__[1],  # requests.Session
        "request",
        autospec=True,
        side_effect=lambda self, method, url, **kw: captured_kwargs.update(kw) or MagicMock(),
    ):
        session.request("GET", "http://example.com/x")

    assert captured_kwargs.get("timeout") == 12.0


@pytest.mark.asyncio
async def test_connect_with_retry_succeeds_after_transient_failures() -> None:
    """A1 — login retries with backoff and eventually succeeds."""
    client = CrmSalesforceClient(_make_config())
    success_sf = MagicMock()
    attempts = {"count": 0}

    def flaky_safe_connect():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("Salesforce login failed: ConnectionError")
        return success_sf

    with (
        patch.object(client, "_safe_connect", side_effect=flaky_safe_connect),
        patch("time.sleep"),  # zero-out backoff
    ):
        sf = await client.connect()

    assert sf is success_sf
    assert attempts["count"] == 3


@pytest.mark.asyncio
async def test_connect_with_retry_gives_up_after_max_attempts() -> None:
    """A1 — after 3 failed attempts, the original error propagates."""
    client = CrmSalesforceClient(_make_config())

    def always_fail():
        raise RuntimeError("Salesforce login failed: AuthError")

    with (
        patch.object(client, "_safe_connect", side_effect=always_fail),
        patch("time.sleep"),
    ):
        with pytest.raises(RuntimeError, match="login failed"):
            await client.connect()


def test_resolve_timeout_seconds_reads_env_var(monkeypatch) -> None:
    from crm_mcp.salesforce import _resolve_timeout_seconds

    monkeypatch.setenv("CRM_MCP_SF_TIMEOUT_SECONDS", "45")
    assert _resolve_timeout_seconds() == 45.0


def test_resolve_timeout_seconds_falls_back_on_garbage(monkeypatch) -> None:
    from crm_mcp.salesforce import _DEFAULT_SF_HTTP_TIMEOUT_SECONDS, _resolve_timeout_seconds

    monkeypatch.setenv("CRM_MCP_SF_TIMEOUT_SECONDS", "not-a-number")
    assert _resolve_timeout_seconds() == _DEFAULT_SF_HTTP_TIMEOUT_SECONDS


class TestSfCall:
    """sf_call wraps SF calls with one-shot reauth on expired session."""

    @pytest.mark.asyncio
    async def test_returns_result_on_first_try(self) -> None:
        from crm_mcp.salesforce import sf_call

        sf = MagicMock()
        sf.query.return_value = {"records": [{"Id": "001"}]}

        client = CrmSalesforceClient(_make_config())
        client._sf = sf  # pre-cache to skip the login round-trip

        result = await sf_call(client, lambda s: s.query("SELECT Id FROM Contact"))

        assert result == {"records": [{"Id": "001"}]}
        sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_reauths_once_on_expired_session(self) -> None:
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from crm_mcp.salesforce import sf_call

        old_sf = MagicMock(name="old")
        old_sf.query.side_effect = SalesforceResourceNotFound(
            "https://example/query/", 404, "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}],
        )
        new_sf = MagicMock(name="new")
        new_sf.query.return_value = {"records": []}

        client = CrmSalesforceClient(_make_config())
        client._sf = old_sf
        with patch.object(
            CrmSalesforceClient, "_connect_with_retry", return_value=new_sf,
        ):
            result = await sf_call(client, lambda s: s.query("SELECT Id"))

        assert result == {"records": []}
        assert client._sf is new_sf
        old_sf.query.assert_called_once()
        new_sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_propagates_non_session_errors(self) -> None:
        from crm_mcp.salesforce import sf_call

        sf = MagicMock()
        sf.query.side_effect = ValueError("not a session issue")

        client = CrmSalesforceClient(_make_config())
        client._sf = sf

        with pytest.raises(ValueError, match="not a session issue"):
            await sf_call(client, lambda s: s.query("any"))

    @pytest.mark.asyncio
    async def test_propagates_after_max_reauths_exhausted(self) -> None:
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from crm_mcp.salesforce import sf_call

        expired = SalesforceResourceNotFound(
            "https://example/query/", 404, "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "expired"}],
        )

        sf_a = MagicMock()
        sf_a.query.side_effect = expired
        sf_b = MagicMock()
        sf_b.query.side_effect = expired

        client = CrmSalesforceClient(_make_config())
        client._sf = sf_a
        with patch.object(
            CrmSalesforceClient, "_connect_with_retry", return_value=sf_b,
        ):
            with pytest.raises(SalesforceResourceNotFound):
                await sf_call(client, lambda s: s.query("any"))

    @pytest.mark.asyncio
    async def test_reset_session_drops_cached_sf(self) -> None:
        client = CrmSalesforceClient(_make_config())
        client._sf = MagicMock()

        await client.reset_session()

        assert client._sf is None


class TestClientCallSitesReauth:
    """Verify the public methods route through sf_call so reauth actually fires."""

    @pytest.mark.asyncio
    async def test_query_reauths_on_expired_session(self) -> None:
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        old_sf = MagicMock(name="old")
        old_sf.query.side_effect = SalesforceResourceNotFound(
            "https://example/query/", 404, "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "expired"}],
        )
        new_sf = MagicMock(name="new")
        new_sf.query.return_value = {"records": [{"Id": "001"}], "totalSize": 1}

        client = CrmSalesforceClient(_make_config())
        client._sf = old_sf
        with patch.object(
            CrmSalesforceClient, "_connect_with_retry", return_value=new_sf,
        ):
            result = await client.query("SELECT Id FROM Contact")

        assert result["records"] == [{"Id": "001"}]
        assert client._sf is new_sf

    @pytest.mark.asyncio
    async def test_create_account_preserves_duplicate_value_error(self) -> None:
        from simple_salesforce.exceptions import SalesforceMalformedRequest

        sf = MagicMock()
        sf.Account.create.side_effect = SalesforceMalformedRequest(
            "https://example/Account/", 400, "Account",
            [{"errorCode": "DUPLICATE_VALUE", "message": "duplicate"}],
        )
        client = CrmSalesforceClient(_make_config())
        client._sf = sf

        with pytest.raises(ValueError, match="duplicate"):
            await client.create_account({"Name": "Acme"})

    @pytest.mark.asyncio
    async def test_update_contact_returns_status_through_sf_call(self) -> None:
        sf = MagicMock()
        sf.Contact.update.return_value = 204
        client = CrmSalesforceClient(_make_config())
        client._sf = sf

        result = await client.update_contact("003x", {"Phone": "+32 1"})

        assert result == 204


class TestTimeoutSessionRetryConfig:
    """MCP-side `_TimeoutSession` matches the receiver-side urllib3 Retry config."""

    def test_mounts_retry_adapter_on_https(self) -> None:
        from crm_mcp.salesforce import _TimeoutSession

        sess = _TimeoutSession()
        adapter = sess.get_adapter("https://example.salesforce.com")
        assert adapter.max_retries.total == 3
        assert adapter.max_retries.connect == 3

    def test_401_not_in_status_forcelist(self) -> None:
        """Expired-session (401) must surface so sf_call reauth fires."""
        from crm_mcp.salesforce import _TimeoutSession

        sess = _TimeoutSession()
        adapter = sess.get_adapter("https://example.salesforce.com")
        assert 401 not in adapter.max_retries.status_forcelist

    def test_default_timeout_preserved(self) -> None:
        from crm_mcp.salesforce import _DEFAULT_SF_HTTP_TIMEOUT_SECONDS, _TimeoutSession

        sess = _TimeoutSession()
        assert sess._default_timeout == _DEFAULT_SF_HTTP_TIMEOUT_SECONDS
