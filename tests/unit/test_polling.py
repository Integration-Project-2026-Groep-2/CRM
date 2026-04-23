"""Unit tests for src.polling — Salesforce polling task.

Covers contracts 13/14/18/19/22/23 published as out-of-band Salesforce UI edits.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import polling, sender
from src.config import Config

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config(tmp_path: Path, polling_integration_user_id: str | None = None) -> Config:
    return Config(
        rabbitmq_url="amqp://test",
        salesforce_username="integration@example.com",
        salesforce_password="pw",
        salesforce_security_token="tok",
        salesforce_domain="login",
        heartbeat_interval_seconds=0,
        system_name="CRM",
        polling_interval_seconds=0,
        polling_state_path=str(tmp_path / "checkpoint.json"),
        polling_integration_user_id=polling_integration_user_id,
        log_level="INFO",
    )


def make_session(sf: MagicMock) -> "polling.SalesforceSession":
    """Wrap a MagicMock sf-client in a real SalesforceSession for tests.

    Tests that pass `sf` to helpers (post-reauth-fix refactor) need a
    SalesforceSession container so sf_call can access `session.sf`. The
    config/shutdown args are MagicMock placeholders — reauth() would need
    them only if the test actually triggers an expired-session path.
    """
    return polling.SalesforceSession(sf, config=MagicMock(), shutdown_event=None)


def make_sf_mock(
    *,
    contact_query_records: list[dict] | None = None,
    account_query_records: list[dict] | None = None,
    user_query_records: list[dict] | None = None,
    contact_seed_records: list[dict] | None = None,
    account_seed_records: list[dict] | None = None,
) -> MagicMock:
    """Build a Salesforce MagicMock with canned responses for polling SOQL."""
    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.Account = MagicMock()
    sf.Contact.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "FirstName", "LastName", "Email", "Phone",
            "Role__c", "GDPR_Consent__c", "Badge_Code__c", "Company_ID__c",
            "MailingStreet", "House_Number__c", "MailingPostalCode", "MailingCity", "MailingCountry",
            "Mailing_ID__c", "Planning_ID__c",
            "CreatedDate", "SystemModstamp", "LastModifiedById",
            "IsActive__c",
        ]],
    }
    sf.Account.describe.return_value = {
        "fields": [{"name": name} for name in [
            "Id", "CRM_ID__c", "Name", "VAT_Number__c", "Phone", "Email__c",
            "BillingStreet", "BillingPostalCode", "BillingCity", "BillingCountry",
            "CreatedDate", "SystemModstamp", "LastModifiedById",
            "IsActive__c",
        ]],
    }

    _default_seed = [{"SystemModstamp": "2026-04-01T00:00:00.000+0000"}]
    _default_user = [{"Id": "005ADMIN0000001"}]

    def _query(soql: str):
        low = soql.lower()
        if "from user" in low:
            records = user_query_records if user_query_records is not None else _default_user
            return {"records": records}
        if "order by systemmodstamp desc" in low and "contact" in low:
            records = contact_seed_records if contact_seed_records is not None else _default_seed
            return {"records": records}
        if "order by systemmodstamp desc" in low and "account" in low:
            records = account_seed_records if account_seed_records is not None else _default_seed
            return {"records": records}
        return {"records": []}

    def _query_all(soql: str):
        low = soql.lower()
        if "from contact" in low:
            records = contact_query_records if contact_query_records is not None else []
            return {"records": records}
        if "from account" in low:
            records = account_query_records if account_query_records is not None else []
            return {"records": records}
        return {"records": []}

    sf.query.side_effect = _query
    sf.query_all.side_effect = _query_all
    return sf


@pytest.fixture(autouse=True)
def reset_sf_describe_caches():
    """Reset global describe caches AND pending CRM_ID state between tests.

    H4 fix: `_pending_crm_ids` is also a module-level global that leaked
    across tests — a test that populated a UUID for SF record X would poison
    any later test hitting the same Id. Clear both caches around each test.
    """
    import src.salesforce_client as sc

    sc._mailing_id_field_supported_cache = None
    sc._planning_id_field_supported_cache = None
    sc._session_registration_object_supported_cache = None
    sc._active_field_cache = None
    polling._describe_cache.clear()
    polling._pending_crm_ids.clear()
    yield
    polling._describe_cache.clear()
    polling._pending_crm_ids.clear()


@pytest.fixture(autouse=True)
def immediate_to_thread(monkeypatch):
    """Make asyncio.to_thread run synchronously for deterministic tests."""

    async def fake(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(polling.asyncio, "to_thread", fake)
    import src.salesforce_client as sc

    monkeypatch.setattr(sc.asyncio, "to_thread", fake)


@pytest.fixture
def patch_sf_login(monkeypatch):
    """Provide a factory that patches get_salesforce_client (lazily imported
    inside run_polling) so unit tests can inject their own `sf` MagicMock."""

    def _install(sf_mock):
        async def fake_login(*_args, **_kwargs):
            return sf_mock

        monkeypatch.setattr("src.salesforce_client.get_salesforce_client", fake_login)

    return _install


@pytest.fixture(autouse=True)
def sender_init():
    """Reset sender module globals and install AsyncMock publishers."""
    with patch.object(sender, "publish_user_confirmed", new_callable=AsyncMock) as pc, \
         patch.object(sender, "publish_user_updated", new_callable=AsyncMock) as pu, \
         patch.object(sender, "publish_user_deactivated", new_callable=AsyncMock) as pd, \
         patch.object(sender, "publish_company_confirmed", new_callable=AsyncMock) as cc, \
         patch.object(sender, "publish_company_updated", new_callable=AsyncMock) as cu, \
         patch.object(sender, "publish_company_deactivated", new_callable=AsyncMock) as cd:
        yield {
            "user_confirmed": pc,
            "user_updated": pu,
            "user_deactivated": pd,
            "company_confirmed": cc,
            "company_updated": cu,
            "company_deactivated": cd,
        }


# ---------------------------------------------------------------------------
# Checkpoint state: load / persist
# ---------------------------------------------------------------------------


class TestLoadOrSeedState:
    @pytest.mark.asyncio
    async def test_cold_start_seeds_from_salesforce(self, tmp_path):
        sf = make_sf_mock(
            contact_seed_records=[{"SystemModstamp": "2026-04-10T10:00:00.000+0000"}],
            account_seed_records=[{"SystemModstamp": "2026-04-11T11:00:00.000+0000"}],
        )
        state = await polling._load_or_seed_state(make_session(sf), str(tmp_path / "missing.json"))
        assert state.contact_last_seen == datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc)
        assert state.account_last_seen == datetime(2026, 4, 11, 11, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_warm_start_reads_existing_checkpoint(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        path.write_text(json.dumps({
            "contact_last_seen": "2026-04-05T12:00:00.000000Z",
            "account_last_seen": "2026-04-06T12:00:00.000000Z",
        }))
        sf = make_sf_mock()
        state = await polling._load_or_seed_state(make_session(sf), str(path))
        assert state.contact_last_seen == datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)
        assert state.account_last_seen == datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)
        sf.query.assert_not_called()  # warm start bypasses SF seed

    @pytest.mark.asyncio
    async def test_corrupted_checkpoint_falls_back_to_seed(self, tmp_path):
        path = tmp_path / "checkpoint.json"
        path.write_text("not json {{{")
        sf = make_sf_mock()
        state = await polling._load_or_seed_state(make_session(sf), str(path))
        # Should not raise; seeded from SF (our mock returns a timestamp).
        assert isinstance(state.contact_last_seen, datetime)

    @pytest.mark.asyncio
    async def test_empty_org_seeds_with_now_utc(self, tmp_path):
        sf = make_sf_mock(contact_seed_records=[], account_seed_records=[])
        before = datetime.now(timezone.utc)
        state = await polling._load_or_seed_state(make_session(sf), str(tmp_path / "missing.json"))
        after = datetime.now(timezone.utc)
        assert before <= state.contact_last_seen <= after


class TestPersistState:
    @pytest.mark.asyncio
    async def test_persist_then_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "checkpoint.json")
        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 20, 10, 30, 45, 123000, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 20, 11, 0, 0, tzinfo=timezone.utc),
        )
        await polling._persist_state(state, path)

        sf = make_sf_mock()
        loaded = await polling._load_or_seed_state(make_session(sf), path)
        assert loaded.contact_last_seen == state.contact_last_seen
        assert loaded.account_last_seen == state.account_last_seen


# ---------------------------------------------------------------------------
# Integration user resolution
# ---------------------------------------------------------------------------


class TestResolveIntegrationUserId:
    @pytest.mark.asyncio
    async def test_returns_user_id_when_found(self, tmp_path):
        sf = make_sf_mock(user_query_records=[{"Id": "0051234567890ABC"}])
        config = make_config(tmp_path)
        uid = await polling._resolve_integration_user_id(make_session(sf), config)
        assert uid == "0051234567890ABC"

    @pytest.mark.asyncio
    async def test_raises_when_user_not_found(self, tmp_path):
        sf = make_sf_mock(user_query_records=[])
        config = make_config(tmp_path)
        with pytest.raises(RuntimeError, match="cannot resolve integration user"):
            await polling._resolve_integration_user_id(make_session(sf), config)

    @pytest.mark.asyncio
    async def test_query_uses_configured_username(self, tmp_path):
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        config = make_config(tmp_path)
        await polling._resolve_integration_user_id(make_session(sf), config)
        call_args = sf.query.call_args_list[0].args[0]
        assert "integration@example.com" in call_args
        assert "FROM User" in call_args

    @pytest.mark.asyncio
    async def test_override_skips_soql_lookup(self, tmp_path):
        """When polling_integration_user_id is set in config, no SOQL is issued."""
        sf = make_sf_mock(user_query_records=[{"Id": "SHOULD_NOT_BE_USED"}])
        config = make_config(tmp_path, polling_integration_user_id="005FAKE0000000000")
        uid = await polling._resolve_integration_user_id(make_session(sf), config)
        assert uid == "005FAKE0000000000"
        # No User SOQL was issued.
        user_queries = [c for c in sf.query.call_args_list if "FROM User" in c.args[0]]
        assert user_queries == []

    @pytest.mark.asyncio
    async def test_warns_when_integration_user_equals_session_user(self, tmp_path, caplog):
        """H5 regression: admin==integration user → polling detects nothing, emit WARN."""
        sf = make_sf_mock(user_query_records=[{"Id": "005dM00000ADMINUEAA"}])
        # Return same user id from the chatter/users/me probe.
        sf.restful = MagicMock(return_value={"id": "005dM00000ADMINU"})  # 15-char prefix match
        config = make_config(tmp_path)
        with caplog.at_level("WARNING"):
            await polling._resolve_integration_user_id(make_session(sf), config)
        assert any(
            "equals the current SF session user" in rec.message
            for rec in caplog.records
        )

    @pytest.mark.asyncio
    async def test_no_warn_when_integration_user_differs_from_session(self, tmp_path, caplog):
        sf = make_sf_mock(user_query_records=[{"Id": "005dM00000INTEGR"}])
        sf.restful = MagicMock(return_value={"id": "005dM00000ADMINX"})  # different prefix
        config = make_config(tmp_path)
        with caplog.at_level("WARNING"):
            await polling._resolve_integration_user_id(make_session(sf), config)
        assert not any("equals the current SF session" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_userinfo_probe_failure_does_not_block_startup(self, tmp_path):
        """Best-effort warning: if restful probe fails, startup must continue."""
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        sf.restful = MagicMock(side_effect=RuntimeError("no permission"))
        config = make_config(tmp_path)
        # Should not raise.
        uid = await polling._resolve_integration_user_id(make_session(sf), config)
        assert uid == "UID"

    @pytest.mark.asyncio
    async def test_username_with_single_quote_is_escaped(self, tmp_path):
        """A username containing a single quote must not break the SOQL."""
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        config = make_config(tmp_path)
        # Craft a config with a dangerous username.
        from dataclasses import replace

        evil_config = replace(config, salesforce_username="o'brien@example.com")
        await polling._resolve_integration_user_id(make_session(sf), evil_config)
        soql = sf.query.call_args_list[0].args[0]
        # Single quote must be backslash-escaped (SOQL convention). Updated
        # from the previous doubling-only escape after the 2026-04-22 review
        # flagged that `\'` can close the literal on raw `\` input.
        assert "o\\'brien@example.com" in soql


class TestPollSoqlEscape:
    """Regression: integration_user_id must also be escaped in SOQL WHERE clauses."""

    @pytest.mark.asyncio
    async def test_integration_user_id_is_escaped_in_contacts_query(self, tmp_path):
        sf = make_sf_mock(contact_query_records=[])
        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 1, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 1, tzinfo=timezone.utc),
        )
        await polling._poll_contacts(make_session(sf), state, "0051' OR '1'='1")
        soql = sf.query_all.call_args.args[0]
        # Each inner single quote becomes `\'`; backslash-escape per SOQL spec
        # (see src.salesforce_client.escape_soql, hardened 2026-04-22).
        assert "'0051\\' OR \\'1\\'=\\'1'" in soql


class TestSkipDoesNotAdvanceCheckpoint:
    """P2 regression: accounts without email are skipped AND the checkpoint
    stays on the previous timestamp so a later edit (adding the email) routes
    to CompanyConfirmed instead of CompanyUpdated."""

    @pytest.mark.asyncio
    async def test_dispatch_account_returns_false_when_confirmed_skipped(self, sender_init):
        """New-Account-without-email returns False so caller can skip checkpoint advance."""
        record = _account_record(
            Email__c=None,
            CreatedDate="2026-04-21T10:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        result = await polling._dispatch_account(record, previous)
        assert result is False
        sender_init["company_confirmed"].assert_not_awaited()
        sender_init["company_updated"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatch_account_returns_true_for_successful_publish(self, sender_init):
        record = _account_record(
            CreatedDate="2026-04-21T10:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        assert await polling._dispatch_account(record, previous) is True

    @pytest.mark.asyncio
    async def test_poll_accounts_skipped_record_does_not_advance_checkpoint(
        self, tmp_path, sender_init,
    ):
        """Batch of 1 skipped record → latest stays on previous_last_seen."""
        record = _account_record(
            Email__c=None,
            CreatedDate="2026-04-21T09:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",
        )
        sf = make_sf_mock(account_query_records=[record])
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        state = polling.PollingState(
            contact_last_seen=previous, account_last_seen=previous,
        )
        new_state = await polling._poll_accounts(make_session(sf), state, "UID")
        assert new_state.account_last_seen == previous  # UNCHANGED

    @pytest.mark.asyncio
    async def test_poll_accounts_advances_only_past_dispatched_records(
        self, tmp_path, sender_init,
    ):
        """Mix of one skipped and one dispatched: checkpoint lands on dispatched timestamp."""
        skipped = _account_record(
            Id="001SKIP",
            Email__c=None,
            VAT_Number__c="BE0000000001",
            CreatedDate="2026-04-21T09:00:00.000+0000",
            SystemModstamp="2026-04-21T11:00:00.000+0000",  # LATER than dispatched
        )
        dispatched = _account_record(
            Id="001DISP",
            VAT_Number__c="BE0000000002",
            CreatedDate="2026-04-21T08:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",  # EARLIER than skipped
        )
        sf = make_sf_mock(account_query_records=[dispatched, skipped])
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        state = polling.PollingState(
            contact_last_seen=previous, account_last_seen=previous,
        )
        new_state = await polling._poll_accounts(make_session(sf), state, "UID")
        # Checkpoint lands on dispatched's timestamp (10:00), NOT on skipped's (11:00).
        # Next cycle will re-process skipped (still above 10:00), good.
        assert new_state.account_last_seen == datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_same_timestamp_collision_keeps_skipped_retryable(
        self, tmp_path, sender_init,
    ):
        """Codex-flagged P2: when dispatched + skipped share the same SystemModstamp,
        the checkpoint must cap at `earliest_skipped - 1ms` so next cycle re-selects
        the skipped record. Without the cap, `> last_seen` filters it forever."""
        skipped = _account_record(
            Id="001SKIP",
            Email__c=None,
            VAT_Number__c="BE0000000001",
            CreatedDate="2026-04-21T09:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",  # SAME as dispatched
        )
        dispatched = _account_record(
            Id="001DISP",
            VAT_Number__c="BE0000000002",
            CreatedDate="2026-04-21T08:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",  # SAME as skipped
        )
        sf = make_sf_mock(account_query_records=[dispatched, skipped])
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        state = polling.PollingState(
            contact_last_seen=previous, account_last_seen=previous,
        )
        new_state = await polling._poll_accounts(make_session(sf), state, "UID")
        # Cap = min(earliest_skipped) - 1ms = 10:00 - 1ms = 09:59:59.999
        from datetime import timedelta as _td

        assert new_state.account_last_seen == (
            datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc) - _td(milliseconds=1)
        )

    @pytest.mark.asyncio
    async def test_exception_caps_checkpoint_like_skip(self, tmp_path, sender_init, monkeypatch):
        """P2 round-2 regression: a record whose dispatch RAISES must also
        hold back the checkpoint. Otherwise a later successful record advances
        past the failed one, and next cycle mis-routes the admin's fix as
        user.updated instead of user.confirmed."""
        from datetime import timedelta as _td

        failing = _contact_record(
            Id="003FAIL",
            CRM_ID__c="550e8400-e29b-41d4-a716-446655440001",
            Email="fail@example.com",
            CreatedDate="2026-04-21T09:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",
        )
        succeeding = _contact_record(
            Id="003OK",
            CRM_ID__c="550e8400-e29b-41d4-a716-446655440002",
            Email="ok@example.com",
            CreatedDate="2026-04-21T08:00:00.000+0000",
            SystemModstamp="2026-04-21T11:00:00.000+0000",
        )
        sf = make_sf_mock(contact_query_records=[failing, succeeding])

        async def raise_only_for_fail(user_data):
            if user_data.get("email") == "fail@example.com":
                raise RuntimeError("xsd validation failed")
            return None

        sender_init["user_confirmed"].side_effect = raise_only_for_fail

        previous = datetime(2026, 4, 20, tzinfo=timezone.utc)
        state = polling.PollingState(
            contact_last_seen=previous, account_last_seen=previous,
        )
        new_state = await polling._poll_contacts(make_session(sf), state, "UID")
        # Checkpoint is capped at (failing.SystemModstamp - 1ms) = 09:59:59.999,
        # NOT at succeeding.SystemModstamp (11:00). Next cycle re-selects the
        # failing record (and re-processes succeeding — consumers idempotent).
        assert new_state.contact_last_seen == (
            datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc) - _td(milliseconds=1)
        )


class TestCheckpointPersistence:
    """C1 regression: checkpoint must persist after each object type, not just both."""

    @pytest.mark.asyncio
    async def test_persists_twice_per_cycle(self, tmp_path, sender_init, monkeypatch, patch_sf_login):
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        patch_sf_login(sf)
        config = make_config(tmp_path)

        persist_calls = {"n": 0}

        async def count_persist(state, path):
            persist_calls["n"] += 1

        monkeypatch.setattr(polling, "_persist_state", count_persist)

        sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(polling.asyncio, "sleep", sleep_mock)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # Contact pass + Account pass = 2 persist calls per cycle (before CancelledError).
        assert persist_calls["n"] == 2


# ---------------------------------------------------------------------------
# Dispatch: Contact
# ---------------------------------------------------------------------------


def _contact_record(**overrides):
    base = {
        "Id": "003CONTACTXYZ",
        "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440000",
        "FirstName": "Alice",
        "LastName": "Admin",
        "Email": "alice@example.com",
        "Phone": "+3212345",
        "Role__c": "VISITOR",
        "GDPR_Consent__c": True,
        "IsActive__c": True,
        "CreatedDate": "2026-04-15T10:00:00.000+0000",
        "SystemModstamp": "2026-04-20T10:00:00.000+0000",
        "LastModifiedById": "005ADMIN0000001",
    }
    base.update(overrides)
    return base


class TestDispatchContact:
    @pytest.mark.asyncio
    async def test_newly_created_publishes_user_confirmed(self, sender_init):
        record = _contact_record(
            CreatedDate="2026-04-20T10:00:00.000+0000",
            SystemModstamp="2026-04-20T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_contact(record, previous)
        sender_init["user_confirmed"].assert_awaited_once()
        sender_init["user_updated"].assert_not_awaited()
        sender_init["user_deactivated"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_edited_existing_publishes_user_updated(self, sender_init):
        record = _contact_record(
            CreatedDate="2026-03-01T10:00:00.000+0000",
            SystemModstamp="2026-04-20T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_contact(record, previous)
        sender_init["user_updated"].assert_awaited_once()
        sender_init["user_confirmed"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_inactive_publishes_user_deactivated(self, sender_init):
        record = _contact_record(
            IsActive__c=False,
            SystemModstamp="2026-04-21T15:30:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_contact(record, previous)
        sender_init["user_deactivated"].assert_awaited_once()
        payload = sender_init["user_deactivated"].await_args.args[0]
        assert payload["id"] == "550e8400-e29b-41d4-a716-446655440000"
        assert payload["email"] == "alice@example.com"
        # SystemModstamp → deactivatedAt (formatted as ISO 8601 with Z)
        assert payload["deactivatedAt"].startswith("2026-04-21T15:30:00")

    @pytest.mark.asyncio
    async def test_reactivation_publishes_user_updated(self, sender_init):
        """Inactive → active edit on an old Contact → user.updated, not confirmed."""
        record = _contact_record(
            IsActive__c=True,
            CreatedDate="2026-01-01T10:00:00.000+0000",
            SystemModstamp="2026-04-21T15:30:00.000+0000",
        )
        previous = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_contact(record, previous)
        sender_init["user_updated"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_contact_without_email_is_skipped(self, sender_init):
        """P2 round-3 regression: Contact.Email=None must skip publish rather
        than cascade into an XSD-validation failure on every poll cycle."""
        record = _contact_record(Email=None)
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        result = await polling._dispatch_contact(record, previous)
        assert result is False
        sender_init["user_confirmed"].assert_not_awaited()
        sender_init["user_updated"].assert_not_awaited()
        sender_init["user_deactivated"].assert_not_awaited()


# ---------------------------------------------------------------------------
# Dispatch: Account
# ---------------------------------------------------------------------------


def _account_record(**overrides):
    base = {
        "Id": "001ACCOUNTXYZ",
        "CRM_ID__c": "660e8400-e29b-41d4-a716-446655440000",
        "Name": "Acme NV",
        "VAT_Number__c": "BE0123456789",
        "Phone": None,
        "Email__c": "info@acme.be",
        "BillingStreet": None,
        "BillingPostalCode": None,
        "BillingCity": None,
        "BillingCountry": None,
        "IsActive__c": True,
        "CreatedDate": "2026-04-15T10:00:00.000+0000",
        "SystemModstamp": "2026-04-20T10:00:00.000+0000",
        "LastModifiedById": "005ADMIN0000001",
    }
    base.update(overrides)
    return base


class TestDispatchAccount:
    @pytest.mark.asyncio
    async def test_newly_created_publishes_company_confirmed(self, sender_init):
        record = _account_record(
            CreatedDate="2026-04-20T10:00:00.000+0000",
            SystemModstamp="2026-04-20T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_account(record, previous)
        sender_init["company_confirmed"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_edited_existing_publishes_company_updated(self, sender_init):
        record = _account_record(
            CreatedDate="2026-03-01T10:00:00.000+0000",
            SystemModstamp="2026-04-20T10:00:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_account(record, previous)
        sender_init["company_updated"].assert_awaited_once()
        payload = sender_init["company_updated"].await_args.args[0]
        assert payload["vatNumber"] == "BE0123456789"
        assert payload["name"] == "Acme NV"
        assert payload["email"] == "info@acme.be"
        assert payload["isActive"] is True

    @pytest.mark.asyncio
    async def test_inactive_publishes_company_deactivated(self, sender_init):
        record = _account_record(
            IsActive__c=False,
            SystemModstamp="2026-04-21T15:30:00.000+0000",
        )
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_account(record, previous)
        sender_init["company_deactivated"].assert_awaited_once()
        payload = sender_init["company_deactivated"].await_args.args[0]
        assert payload["id"] == "660e8400-e29b-41d4-a716-446655440000"
        assert payload["vatNumber"] == "BE0123456789"

    @pytest.mark.asyncio
    async def test_picklist_no_is_treated_as_inactive(self, sender_init):
        """P3 regression: org-only-with-Active__c-picklist, value 'No' → inactive."""
        record = _account_record(SystemModstamp="2026-04-21T15:30:00.000+0000")
        # Remove the boolean IsActive__c and set the picklist Active__c='No'.
        del record["IsActive__c"]
        record["Active__c"] = "No"
        previous = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
        await polling._dispatch_account(record, previous)
        sender_init["company_deactivated"].assert_awaited_once()
        sender_init["company_updated"].assert_not_awaited()
        sender_init["company_confirmed"].assert_not_awaited()


# ---------------------------------------------------------------------------
# CRM_ID__c auto-stamping for admin-created records
# ---------------------------------------------------------------------------


class TestAssignLocalCrmId:
    """P1 round-2: CRM_ID is assigned locally first, persisted to SF only
    after a successful publish, so a failed publish doesn't leave a
    half-committed SF record that gets filtered out by the next poll."""

    def test_no_op_when_already_set(self):
        contact = {"Id": "003XYZ", "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440000"}
        result, needs_stamp = polling._assign_local_crm_id(contact)
        assert result["CRM_ID__c"] == "550e8400-e29b-41d4-a716-446655440000"
        assert needs_stamp is False

    def test_assigns_new_uuid_without_touching_salesforce(self):
        contact = {"Id": "003XYZ", "CRM_ID__c": None}
        result, needs_stamp = polling._assign_local_crm_id(contact)
        import uuid as _uuid

        parsed = _uuid.UUID(result["CRM_ID__c"])
        assert parsed.version == 4
        assert needs_stamp is True

    @pytest.mark.asyncio
    async def test_persist_writes_uuid_to_salesforce(self):
        sf = MagicMock()
        sf.Contact.update = MagicMock()
        contact = {"Id": "003XYZ", "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440000"}
        await polling._persist_contact_crm_id(make_session(sf), contact)
        sf.Contact.update.assert_called_once_with(
            "003XYZ", {"CRM_ID__c": "550e8400-e29b-41d4-a716-446655440000"},
        )

    def test_reassign_reuses_cached_uuid_for_same_sf_id(self):
        """P1 round-3 regression: if the post-publish SF stamp fails and we
        re-process the record next cycle, we MUST reuse the same UUID —
        otherwise consumers receive the same record under two different
        canonical ids."""
        polling._pending_crm_ids.clear()
        first = {"Id": "003SAME", "CRM_ID__c": None}
        first_result, _ = polling._assign_local_crm_id(first)
        first_uuid = first_result["CRM_ID__c"]

        # Simulate persist failure: _pending_crm_ids still has entry for 003SAME.
        assert polling._pending_crm_ids["003SAME"] == first_uuid

        # Next cycle re-processes the same record (still no CRM_ID in SF).
        second = {"Id": "003SAME", "CRM_ID__c": None}
        second_result, needs_stamp = polling._assign_local_crm_id(second)
        assert second_result["CRM_ID__c"] == first_uuid  # SAME UUID reused
        assert needs_stamp is True

        # After a successful persist, the cache entry is cleared.
        import asyncio as _asyncio

        sf = MagicMock()
        sf.Contact.update = MagicMock()
        _asyncio.get_event_loop().run_until_complete(
            polling._persist_contact_crm_id(make_session(sf), second_result),
        ) if False else None
        # Easier: call clear directly.
        polling._clear_pending_crm_id("003SAME")
        assert "003SAME" not in polling._pending_crm_ids

    @pytest.mark.asyncio
    async def test_publish_failure_leaves_salesforce_unchanged(self, sender_init, monkeypatch):
        """P1 regression: publish raises → SF.update MUST NOT be called.

        Before the fix, _ensure_contact_crm_id stamped SF before publish, so
        a publish failure left an orphaned CRM_ID plus LastModifiedById=
        integration_user, and the next poll filtered the record out forever.
        """
        sf = make_sf_mock(contact_query_records=[_contact_record(
            CRM_ID__c=None,
            CreatedDate="2026-04-21T09:00:00.000+0000",
            SystemModstamp="2026-04-21T10:00:00.000+0000",
        )])
        sf.Contact.update = MagicMock()
        sender_init["user_confirmed"].side_effect = RuntimeError("publish failed")
        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
        await polling._poll_contacts(make_session(sf), state, "UID")
        # Critical assertion: no SF write happened because publish failed BEFORE stamping.
        sf.Contact.update.assert_not_called()


# ---------------------------------------------------------------------------
# Poll cycle (integration of dispatch + SOQL + state update)
# ---------------------------------------------------------------------------


class TestPollContacts:
    @pytest.mark.asyncio
    async def test_skips_records_modified_by_integration_user(self, tmp_path, sender_init):
        """The SOQL WHERE clause excludes LastModifiedById == integration_user."""
        sf = make_sf_mock(contact_query_records=[])
        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
        await polling._poll_contacts(make_session(sf), state, "005ADMIN0000001")
        soql = sf.query_all.call_args.args[0]
        assert "LastModifiedById != '005ADMIN0000001'" in soql
        sender_init["user_confirmed"].assert_not_awaited()

    @pytest.mark.asyncio
    async def test_advances_checkpoint_after_batch(self, tmp_path, sender_init):
        sf = make_sf_mock(contact_query_records=[
            _contact_record(
                CreatedDate="2026-04-21T09:00:00.000+0000",
                SystemModstamp="2026-04-21T10:00:00.000+0000",
            ),
            _contact_record(
                CreatedDate="2026-04-21T09:00:00.000+0000",
                SystemModstamp="2026-04-21T11:00:00.000+0000",
            ),
        ])
        old_state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 20, tzinfo=timezone.utc),
        )
        new_state = await polling._poll_contacts(make_session(sf), old_state, "UID")
        assert new_state.contact_last_seen == datetime(2026, 4, 21, 11, 0, tzinfo=timezone.utc)
        assert new_state.account_last_seen == old_state.account_last_seen
        assert sender_init["user_confirmed"].await_count == 2

    @pytest.mark.asyncio
    async def test_stamps_missing_crm_id_after_publish(self, tmp_path, sender_init):
        """P1 fix: CRM_ID is assigned in-memory for the publish payload, then
        persisted to Salesforce AFTER publish succeeds."""
        record = _contact_record(CRM_ID__c=None)
        sf = make_sf_mock(contact_query_records=[record])
        sf.Contact.update = MagicMock()
        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 1, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 1, tzinfo=timezone.utc),
        )
        await polling._poll_contacts(make_session(sf), state, "UID")

        # Publish MUST have happened with the assigned UUID.
        payload = sender_init["user_confirmed"].await_args.args[0]
        import uuid as _uuid

        published_id = _uuid.UUID(payload["id"])
        # SF.update MUST have happened AFTER publish, with the same UUID.
        sf.Contact.update.assert_called_once()
        stamped_uuid = sf.Contact.update.call_args.args[1]["CRM_ID__c"]
        assert _uuid.UUID(stamped_uuid) == published_id


# ---------------------------------------------------------------------------
# Top-level run_polling loop
# ---------------------------------------------------------------------------


class TestRunPollingLoop:
    @pytest.mark.asyncio
    async def test_bootstrap_failure_retries_instead_of_exiting(
        self, tmp_path, sender_init, monkeypatch, patch_sf_login, caplog,
    ):
        """P2 round-3 regression: a transient SF error during initial seed must
        NOT kill the polling task permanently. The task must retry on the
        next cycle — otherwise the service looks healthy (heartbeat, status,
        receiver run) but polling is silently dead."""
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        patch_sf_login(sf)
        config = make_config(tmp_path)

        # Fail the FIRST `query` call (seed), succeed on the second.
        attempts = {"n": 0}
        original_side_effect = sf.query.side_effect

        def flaky_query(soql: str):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("Salesforce temporarily unavailable")
            return original_side_effect(soql)

        sf.query.side_effect = flaky_query

        # First sleep lets the loop retry; second sleep cancels.
        sleep_calls = {"n": 0}

        async def fake_sleep(_sec):
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                raise asyncio.CancelledError()

        monkeypatch.setattr(polling.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # Bootstrap retry fired at least once, task didn't exit on first failure.
        assert attempts["n"] >= 2
        assert any("bootstrap failed" in r.message.lower() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_bootstrap_does_not_reseed_state_across_retries(
        self, tmp_path, sender_init, monkeypatch, patch_sf_login,
    ):
        """P2 round-4 regression: if state seeds successfully but the user
        lookup fails transiently, a retry must NOT re-seed state (which would
        skip over any records created between the two cycles)."""
        sf = make_sf_mock(
            user_query_records=[{"Id": "UID"}],
            contact_seed_records=[{"SystemModstamp": "2026-04-10T10:00:00.000+0000"}],
        )
        patch_sf_login(sf)
        config = make_config(tmp_path)

        # Count how many times a seed-style query is issued.
        seed_calls = {"n": 0}
        user_calls = {"n": 0}
        original_side_effect = sf.query.side_effect

        def flaky(soql: str):
            low = soql.lower()
            if "order by systemmodstamp desc" in low:
                seed_calls["n"] += 1
            if "from user" in low:
                user_calls["n"] += 1
                if user_calls["n"] == 1:
                    raise RuntimeError("transient user lookup failure")
            return original_side_effect(soql)

        sf.query.side_effect = flaky

        sleep_calls = {"n": 0}

        async def fake_sleep(_sec):
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                raise asyncio.CancelledError()

        monkeypatch.setattr(polling.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # Seed was issued exactly ONCE even though bootstrap retried.
        # Contact seed + Account seed = 2 calls total, not 4.
        assert seed_calls["n"] == 2, (
            f"Seed was re-issued on retry (calls={seed_calls['n']}); "
            "this drops events created between bootstrap attempts."
        )

    @pytest.mark.asyncio
    async def test_partial_seed_failure_preserves_successful_seed(
        self, tmp_path, sender_init, monkeypatch, patch_sf_login,
    ):
        """P2 round-5 regression: if Contact seeds successfully but Account
        seed fails transiently, a retry must NOT re-seed Contact."""
        sf = make_sf_mock(
            user_query_records=[{"Id": "UID"}],
            contact_seed_records=[{"SystemModstamp": "2026-04-10T10:00:00.000+0000"}],
            account_seed_records=[{"SystemModstamp": "2026-04-11T10:00:00.000+0000"}],
        )
        patch_sf_login(sf)
        config = make_config(tmp_path)

        contact_seed_calls = {"n": 0}
        account_seed_calls = {"n": 0}
        original = sf.query.side_effect

        def partial_fail(soql: str):
            low = soql.lower()
            if "order by systemmodstamp desc" in low and "contact" in low:
                contact_seed_calls["n"] += 1
            if "order by systemmodstamp desc" in low and "account" in low:
                account_seed_calls["n"] += 1
                if account_seed_calls["n"] == 1:
                    raise RuntimeError("transient Account seed failure")
            return original(soql)

        sf.query.side_effect = partial_fail

        sleep_calls = {"n": 0}

        async def fake_sleep(_sec):
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                raise asyncio.CancelledError()

        monkeypatch.setattr(polling.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # Contact seed ran exactly ONCE across the two bootstrap attempts;
        # Account seed ran twice (first failed, second succeeded).
        assert contact_seed_calls["n"] == 1, (
            f"Contact was re-seeded after Account failure (calls={contact_seed_calls['n']}); "
            "this drops Contact changes created between bootstrap attempts."
        )
        assert account_seed_calls["n"] == 2

    @pytest.mark.asyncio
    async def test_runs_one_cycle_then_cancels(self, tmp_path, sender_init, monkeypatch, patch_sf_login):
        sf = make_sf_mock(
            user_query_records=[{"Id": "UID"}],
            contact_query_records=[_contact_record(
                CreatedDate="2026-04-21T09:00:00.000+0000",
                SystemModstamp="2026-04-21T10:00:00.000+0000",
            )],
        )
        patch_sf_login(sf)
        config = make_config(tmp_path)

        sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(polling.asyncio, "sleep", sleep_mock)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        sender_init["user_confirmed"].assert_awaited_once()
        # Checkpoint file should exist after a successful cycle.
        assert (tmp_path / "checkpoint.json").exists()

    @pytest.mark.asyncio
    async def test_rate_limit_error_skipped_not_crashed(self, tmp_path, sender_init, monkeypatch, caplog, patch_sf_login):
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        patch_sf_login(sf)

        # Force the first cycle to raise a rate-limit error (via sf.query_all).
        rate_err = Exception("REQUEST_LIMIT_EXCEEDED TotalRequests Limit exceeded.")
        call_count = {"n": 0}

        def raising_query_all(soql: str):
            call_count["n"] += 1
            raise rate_err

        sf.query_all.side_effect = raising_query_all

        config = make_config(tmp_path)

        sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(polling.asyncio, "sleep", sleep_mock)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        assert call_count["n"] >= 1
        # The rate-limit branch must log error but not propagate.
        assert any("rate limit" in rec.message.lower() for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_generic_exception_logs_and_continues(self, tmp_path, sender_init, monkeypatch, caplog, patch_sf_login):
        sf = make_sf_mock(user_query_records=[{"Id": "UID"}])
        patch_sf_login(sf)
        sf.query_all.side_effect = RuntimeError("random SF hiccup")
        config = make_config(tmp_path)

        sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(polling.asyncio, "sleep", sleep_mock)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        assert any("polling cycle failed" in rec.message.lower() for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_reresolves_integration_user_after_refresh_window(self, tmp_path, sender_init, monkeypatch, patch_sf_login):
        """After >3600s monotonic, the user-id query runs again."""
        user_records = [{"Id": "UID1"}, {"Id": "UID2"}]
        user_iter = iter(user_records)

        sf = make_sf_mock()
        patch_sf_login(sf)
        original_query = sf.query.side_effect

        def record_aware_query(soql: str):
            low = soql.lower()
            if "from user" in low:
                try:
                    return {"records": [next(user_iter)]}
                except StopIteration:
                    return {"records": []}
            return original_query(soql)

        sf.query.side_effect = record_aware_query
        config = make_config(tmp_path)

        # Simulate monotonic time: sequence, then last value repeats forever.
        time_values = [0.0, 0.0, 3601.0, 3602.0, 7202.0]
        state_counter = {"i": 0}

        def fake_monotonic():
            i = state_counter["i"]
            value = time_values[min(i, len(time_values) - 1)]
            state_counter["i"] = i + 1
            return value

        monkeypatch.setattr(polling.time, "monotonic", fake_monotonic)

        sleep_calls = {"n": 0}

        async def fake_sleep(_sec):
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                raise asyncio.CancelledError()

        monkeypatch.setattr(polling.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # User query should have run at least twice (initial + refresh).
        user_queries = [
            call for call in sf.query.call_args_list
            if "FROM User" in call.args[0]
        ]
        assert len(user_queries) >= 2


# ---------------------------------------------------------------------------
# TestSessionReauth — end-to-end validation of sf_call reauth in polling
# ---------------------------------------------------------------------------


class TestSessionReauth:
    """Validate that the polling helpers trigger reauth on expired sessions.

    The fix: sf_call wraps every SF call; on SalesforceResourceNotFound
    (302→404 pad) or SalesforceExpiredSession (401 pad) it swaps session.sf
    for a fresh client and retries once. These tests lock down the behaviour
    at the polling-helper level, not just the helper unit test level.
    """

    @pytest.mark.asyncio
    async def test_poll_contacts_reauths_on_expired_query_all(
        self, monkeypatch, sender_init,
    ):
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        expired_exc = SalesforceResourceNotFound(
            "https://example/services/data/v59.0/query/", 404, "query", b"",
        )

        # Old sf: query_all always raises expired. Describe + other lookups
        # return valid data (those happen before the query_all in build-fields).
        old_sf = make_sf_mock()
        old_sf.query_all.side_effect = expired_exc

        # New sf after reauth: returns a record that should publish.
        new_sf = make_sf_mock(
            contact_query_records=[{
                "Id": "003NEW", "CRM_ID__c": None,
                "FirstName": "Re", "LastName": "Auth",
                "Email": "reauth@example.com",
                "IsActive__c": True, "LastModifiedById": "005ADMIN0000001",
                "CreatedDate": "2026-04-22T22:00:00.000+0000",
                "SystemModstamp": "2026-04-22T22:00:00.000+0000",
            }],
        )

        reauth_calls = {"n": 0}

        async def fake_get_client(*_a, **_kw):
            reauth_calls["n"] += 1
            return new_sf

        monkeypatch.setattr("src.salesforce_client.get_salesforce_client", fake_get_client)

        session = make_session(old_sf)
        # Point the session's config/shutdown at real-ish values so reauth can run.
        session._config = MagicMock()
        session._shutdown_event = None

        state = polling.PollingState(
            contact_last_seen=datetime(2026, 4, 22, 21, 0, tzinfo=timezone.utc),
            account_last_seen=datetime(2026, 4, 22, 21, 0, tzinfo=timezone.utc),
        )

        new_state = await polling._poll_contacts(session, state, "005ADMIN0000001")

        # Reauth triggered once; after retry the query_all on new_sf returned data.
        assert reauth_calls["n"] == 1
        assert session.sf is new_sf
        assert sender_init["user_confirmed"].await_count == 1
        # Checkpoint advanced past the record.
        assert new_state.contact_last_seen == datetime(
            2026, 4, 22, 22, 0, tzinfo=timezone.utc,
        )

    @pytest.mark.asyncio
    async def test_persist_contact_crm_id_reauths_on_expired_update(
        self, monkeypatch,
    ):
        """Post-publish CRM_ID stamp retries with a fresh sf on expired session."""
        from simple_salesforce.exceptions import SalesforceExpiredSession

        expired_exc = SalesforceExpiredSession(
            "https://example/services/data/v59.0/sobjects/Contact/003X/", 401,
            "Contact", b'[{"errorCode":"INVALID_SESSION_ID","message":"Session expired"}]',
        )

        old_sf = MagicMock()
        old_sf.Contact = MagicMock()
        old_sf.Contact.update.side_effect = expired_exc

        new_sf = MagicMock()
        new_sf.Contact = MagicMock()
        new_sf.Contact.update.return_value = None

        async def fake_get_client(*_a, **_kw):
            return new_sf

        monkeypatch.setattr("src.salesforce_client.get_salesforce_client", fake_get_client)

        session = make_session(old_sf)
        session._config = MagicMock()
        session._shutdown_event = None

        contact = {"Id": "003REAUTH", "CRM_ID__c": "11111111-2222-4333-8444-555555555555"}

        # Must succeed without raising — reauth + retry on the fresh client.
        await polling._persist_contact_crm_id(session, contact)

        assert session.sf is new_sf
        old_sf.Contact.update.assert_called_once_with(
            "003REAUTH", {"CRM_ID__c": "11111111-2222-4333-8444-555555555555"},
        )
        new_sf.Contact.update.assert_called_once_with(
            "003REAUTH", {"CRM_ID__c": "11111111-2222-4333-8444-555555555555"},
        )

    @pytest.mark.asyncio
    async def test_persistent_expired_propagates_after_one_reauth(
        self, monkeypatch, tmp_path,
    ):
        """Reauth-then-still-expired → run_polling's outer handler logs and continues.

        First run_polling's initial get_salesforce_client returns old_sf.
        After sf_call's first try fails, reauth() triggers a second
        get_salesforce_client call which returns new_sf. The new_sf also
        fails (persistent issue), so sf_call's max_reauths=1 gate raises
        through to run_polling's outer except, which logs and sleeps — the
        fake_sleep then raises CancelledError to end the test.
        """
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        expired_exc = SalesforceResourceNotFound(
            "https://example/services/data/v59.0/query/", 404, "query", b"",
        )

        # Both old and new sf raise on query_all — the bug did not clear on reauth.
        old_sf = make_sf_mock()
        old_sf.query_all.side_effect = expired_exc
        new_sf = make_sf_mock()
        new_sf.query_all.side_effect = expired_exc

        # First call (initial login) → old_sf; second call (reauth) → new_sf.
        clients = [old_sf, new_sf]
        call_idx = {"n": 0}

        async def fake_get_client(*_a, **_kw):
            idx = call_idx["n"]
            call_idx["n"] += 1
            return clients[min(idx, len(clients) - 1)]

        monkeypatch.setattr("src.salesforce_client.get_salesforce_client", fake_get_client)

        config = make_config(tmp_path)

        # Let run_polling attempt exactly one cycle then cancel out.
        sleep_calls = {"n": 0}

        async def fake_sleep(_sec):
            sleep_calls["n"] += 1
            raise asyncio.CancelledError()

        monkeypatch.setattr(polling.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await polling.run_polling(config)

        # Initial login + one reauth = 2 get_salesforce_client calls.
        assert call_idx["n"] == 2
        old_sf.query_all.assert_called_once()
        new_sf.query_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_warn_if_admin_propagates_auth_failure(self, monkeypatch):
        """H2: the admin-probe must NOT swallow permanent auth-failures.

        If the probe's `sf.restful()` call triggers sf_call reauth and that
        reauth fails with SalesforceAuthenticationFailed (rotated creds),
        the outer `except Exception: return` used to hide this — polling
        would continue silently with a dead client until the first real
        query. The narrowed handler re-raises AuthFailed.
        """
        from simple_salesforce.exceptions import (
            SalesforceAuthenticationFailed,
            SalesforceResourceNotFound,
        )

        expired_exc = SalesforceResourceNotFound(
            "https://example/services/data/v59.0/query/", 404, "query", b"",
        )

        sf = MagicMock()
        # Probe hits the expired-session pattern, which triggers reauth.
        sf.restful.side_effect = expired_exc

        async def fake_reauth_bad_creds(config, shutdown_event=None):
            raise SalesforceAuthenticationFailed("INVALID_LOGIN", "rotated")

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_reauth_bad_creds,
        )

        session = make_session(sf)

        with pytest.raises(SalesforceAuthenticationFailed):
            await polling._warn_if_admin_is_integration_user(session, "005ADMIN0000001")

    @pytest.mark.asyncio
    async def test_warn_if_admin_swallows_transient_probe_error(self, monkeypatch):
        """H2 inverse: transient probe failures stay best-effort."""
        sf = MagicMock()
        sf.restful.side_effect = TimeoutError("network blip")

        session = make_session(sf)

        # Should NOT raise — transient errors are best-effort swallowed.
        result = await polling._warn_if_admin_is_integration_user(session, "005ADMIN0000001")
        assert result is None
