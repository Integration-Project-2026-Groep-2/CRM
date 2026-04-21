"""E2E tests — Salesforce polling task publishes changes via a running CRM stack.

Simulates admin actions in the Salesforce UI by writing directly to SF via the
REST API, then verifies that the `polling` task inside the running CRM
container picks up the change and publishes the matching contract on
`contact.topic`.

Requirements:
    - A running CRM stack (use `docker compose up --build` or pass
      `E2E_AUTO_START_LOCAL_STACK=1` to pytest).
    - **CRITICAL:** the running CRM container must have
      `POLLING_INTEGRATION_USER_ID` set to a valid-format-but-non-existent SF
      user Id like `005000000000000AAA` (prefix `005` = User object,
      checksum `AAA` = impossible ID). Otherwise the polling task filters
      out *all* changes made via these tests (they use the same SF
      credentials as the CRM container, so `LastModifiedById` always matches
      the real integration user and gets excluded). Salesforce strictly
      validates ID format — random strings like `005FAKE0000000000` will
      cause `INVALID_QUERY_FILTER_OPERATOR`.
      Set it in `.env` before bringing up the stack:
          POLLING_INTEGRATION_USER_ID=005000000000000AAA
    - A short polling interval for quick feedback. Recommended for E2E runs:
          POLLING_INTERVAL_SECONDS=5
    - Real Salesforce credentials in the environment (SALESFORCE_USERNAME,
      SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN).

The tests are all marked with `@pytest.mark.salesforce` so they skip cleanly
when credentials are not configured.

Usage:
    POLLING_INTEGRATION_USER_ID=005FAKE0000000000 POLLING_INTERVAL_SECONDS=5 \\
        docker compose up --build -d
    python -m pytest tests/e2e/test_polling_e2e.py -v
"""

from __future__ import annotations

import asyncio
import os
import random
import string
import uuid
from typing import Any

import aio_pika
import pytest
from lxml import etree
from simple_salesforce.exceptions import SalesforceError

# Polling cycle poll interval + safety margin. The running CRM container should
# have POLLING_INTERVAL_SECONDS set to ~5s for E2E; we wait POLL_WAIT seconds
# which is roughly 2.5× that to give the task time to detect + publish.
POLL_WAIT = float(os.getenv("E2E_POLL_WAIT_SECONDS", "15"))


def _unique_email() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"e2e.polling.{suffix}@example.com"


def _unique_vat() -> str:
    """Generate a unique Belgian-format VAT number (BE + 10 digits)."""
    digits = "".join(random.choices(string.digits, k=10))
    return f"BE{digits}"


async def _wait_for_polling_outbound(
    queue,
    *,
    matcher,
    timeout: float = POLL_WAIT,
) -> etree._Element | None:
    """Poll the queue for a message matching `matcher(xml_element)`.

    Many polling cycles may publish along the way; we filter to the message
    we actually care about rather than asserting on the first arrival.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            msg = await queue.get(no_ack=False, fail=False)
            if msg is not None:
                parsed = etree.fromstring(msg.body)
                await msg.ack()
                if matcher(parsed):
                    return parsed
                continue
        except aio_pika.exceptions.QueueEmpty:
            pass
        await asyncio.sleep(0.5)
    return None


async def _require_polling_override_configured(channel) -> None:
    """Skip if the running CRM container has not set POLLING_INTEGRATION_USER_ID.

    We can't introspect the container's env directly, but we CAN check that
    the polling task is actually publishing *something* out of band — the
    simplest heuristic: verify the test's own SF credentials are configured
    and trust the ops instruction. If the override is missing, the test will
    timeout waiting for a message; caller will see a clear failure.
    """
    # No authoritative check possible — documented prerequisite.
    # Keep the function to make the prerequisite visible in test output.
    return


# ---------------------------------------------------------------------------
# Helpers: SF record lifecycle (create / update / deactivate / delete)
# ---------------------------------------------------------------------------


async def _create_sf_contact(sf_client, email: str, **extra: Any) -> str:
    """Create a Contact in Salesforce and return its Id."""
    payload = {
        "FirstName": "E2EPoll",
        "LastName": "Test",
        "Email": email,
        "Role__c": "VISITOR",
        "GDPR_Consent__c": True,
    }
    active_field, field_type = await _resolve_active_field(sf_client, "Contact")
    payload[active_field] = _encode_active(True, field_type)
    payload.update(extra)
    result = await asyncio.to_thread(sf_client.Contact.create, payload)
    return result["id"]


async def _create_sf_account(sf_client, vat_number: str, **extra: Any) -> str:
    payload = {
        "Name": f"Acme-E2E-{vat_number[-4:]}",
        "VAT_Number__c": vat_number,
    }
    # Contract 14 (CompanyConfirmed) requires a non-empty email. Populate whichever
    # email field the org exposes; fall back silently if none exists.
    email_field = await _resolve_account_email_field(sf_client)
    if email_field:
        payload[email_field] = f"acme-{vat_number.lower()}@example.com"
    active_field, field_type = await _resolve_active_field(sf_client, "Account")
    payload[active_field] = _encode_active(True, field_type)
    payload.update(extra)
    result = await asyncio.to_thread(sf_client.Account.create, payload)
    return result["id"]


async def _resolve_account_email_field(sf_client) -> str | None:
    describe = await asyncio.to_thread(sf_client.Account.describe)
    available = {f["name"] for f in describe.get("fields", [])}
    for name in ("Email__c", "Email"):
        if name in available:
            return name
    return None


async def _update_sf_contact(sf_client, contact_id: str, **fields: Any) -> None:
    await asyncio.to_thread(sf_client.Contact.update, contact_id, fields)


async def _update_sf_account(sf_client, account_id: str, **fields: Any) -> None:
    await asyncio.to_thread(sf_client.Account.update, account_id, fields)


async def _delete_sf_contact(sf_client, contact_id: str) -> None:
    """Best-effort cleanup — never fail the test on delete errors."""
    try:
        await asyncio.to_thread(sf_client.Contact.delete, contact_id)
    except SalesforceError:
        pass


async def _delete_sf_account(sf_client, account_id: str) -> None:
    try:
        await asyncio.to_thread(sf_client.Account.delete, account_id)
    except SalesforceError:
        pass


async def _resolve_active_field(sf_client, sobject: str) -> tuple[str, str]:
    """Pick an active-flag field and tell the caller how to encode bool values.

    Returns (field_name, field_type). Some orgs have both a picklist
    `Active__c` (Yes/No) and a boolean `Is_Active__c` — picking the picklist
    inverts truthiness because `bool('No')` is True. If only a picklist is
    available, the caller must encode bools as "Yes"/"No" before writing to
    Salesforce (SF rejects a Python bool against a picklist schema).
    """
    describe = await asyncio.to_thread(getattr(sf_client, sobject).describe)
    fields_by_name = {f["name"]: f for f in describe.get("fields", [])}
    candidates = ("IsActive__c", "Active__c", "Is_Active__c")
    for name in candidates:
        field = fields_by_name.get(name)
        if field is not None and field.get("type") == "boolean":
            return name, "boolean"
    for name in candidates:
        field = fields_by_name.get(name)
        if field is not None:
            return name, field.get("type", "unknown")
    pytest.skip(f"No IsActive-style custom field on {sobject} — cannot drive polling deactivation")


def _encode_active(is_active: bool, field_type: str):
    """Coerce a Python bool to the correct wire-format for the given SF field type."""
    if field_type == "boolean":
        return is_active
    # Picklist / text → SF-recognised literal strings.
    return "Yes" if is_active else "No"


# ---------------------------------------------------------------------------
# Contract 13 — admin creates Contact in SF → crm.user.confirmed
# ---------------------------------------------------------------------------


@pytest.mark.salesforce
class TestPollingContact:
    """Polling task picks up direct SF writes to Contact and publishes contracts 13/18/22."""

    @pytest.mark.asyncio
    async def test_admin_created_contact_produces_user_confirmed(
        self, channel, outbound_exchange, sf_client,
    ):
        """A Contact created directly in Salesforce is published as C13."""
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        await _require_polling_override_configured(channel)

        email = _unique_email()
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        await _drain_queue(q_confirmed)

        contact_id = await _create_sf_contact(sf_client, email)
        try:
            result = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "UserConfirmed" and xml.findtext("email") == email,
            )

            assert result is not None, (
                f"No crm.user.confirmed received within {POLL_WAIT}s. "
                "Verify POLLING_INTEGRATION_USER_ID override is set on the running CRM container."
            )
            assert result.findtext("firstName") == "E2EPoll"
            assert result.findtext("lastName") == "Test"
            assert result.findtext("role") == "VISITOR"
            assert result.findtext("isActive") == "true"
            assert result.findtext("gdprConsent") == "true"
            # CRM_ID__c stamped by the polling task when missing.
            crm_id = result.findtext("id")
            assert crm_id is not None
            parsed = uuid.UUID(crm_id)
            assert parsed.version == 4
        finally:
            await _delete_sf_contact(sf_client, contact_id)

    @pytest.mark.asyncio
    async def test_admin_edited_contact_produces_user_updated(
        self, channel, outbound_exchange, sf_client,
    ):
        """An existing Contact edited in SF is published as C18."""
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        email = _unique_email()
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        q_updated = await _create_temp_queue(channel, outbound_exchange, "crm.user.updated")

        contact_id = await _create_sf_contact(sf_client, email)
        try:
            # First cycle: expect UserConfirmed (sets up state).
            confirmed = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "UserConfirmed" and xml.findtext("email") == email,
            )
            assert confirmed is not None, "Prerequisite: polling must publish UserConfirmed first"

            await _drain_queue(q_updated)

            # Admin edit: change first name. New SystemModstamp but CreatedDate unchanged
            # → polling logic should route to UserUpdated (not Confirmed).
            await _update_sf_contact(sf_client, contact_id, FirstName="EditedByAdmin")

            result = await _wait_for_polling_outbound(
                q_updated,
                matcher=lambda xml: xml.tag == "UserUpdated" and xml.findtext("email") == email,
            )

            assert result is not None, "No crm.user.updated received within timeout"
            assert result.findtext("firstName") == "EditedByAdmin"
            assert result.findtext("id") == confirmed.findtext("id")
            assert result.findtext("updatedAt") is not None
        finally:
            await _delete_sf_contact(sf_client, contact_id)

    @pytest.mark.asyncio
    async def test_admin_deactivated_contact_produces_user_deactivated(
        self, channel, outbound_exchange, sf_client,
    ):
        """Flipping IsActive__c=false on a Contact is published as C22."""
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        email = _unique_email()
        active_field, field_type = await _resolve_active_field(sf_client, "Contact")

        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.user.confirmed")
        q_deactivated = await _create_temp_queue(channel, outbound_exchange, "crm.user.deactivated")

        contact_id = await _create_sf_contact(sf_client, email)
        try:
            confirmed = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "UserConfirmed" and xml.findtext("email") == email,
            )
            assert confirmed is not None
            expected_id = confirmed.findtext("id")

            await _drain_queue(q_deactivated)
            await _update_sf_contact(sf_client, contact_id, **{active_field: _encode_active(False, field_type)})

            result = await _wait_for_polling_outbound(
                q_deactivated,
                matcher=lambda xml: xml.tag == "UserDeactivated" and xml.findtext("email") == email,
            )

            assert result is not None, "No crm.user.deactivated received within timeout"
            assert result.findtext("id") == expected_id
            assert result.findtext("deactivatedAt") is not None
        finally:
            await _delete_sf_contact(sf_client, contact_id)


# ---------------------------------------------------------------------------
# Contracts 14/19/23 — Account polling
# ---------------------------------------------------------------------------


@pytest.mark.salesforce
class TestPollingAccount:
    """Polling task picks up direct SF writes to Account and publishes contracts 14/19/23."""

    @pytest.mark.asyncio
    async def test_admin_created_account_produces_company_confirmed(
        self, channel, outbound_exchange, sf_client,
    ):
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        vat = _unique_vat()
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.company.confirmed")
        await _drain_queue(q_confirmed)

        account_id = await _create_sf_account(sf_client, vat)
        try:
            result = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "CompanyConfirmed" and xml.findtext("vatNumber") == vat,
            )

            assert result is not None, (
                f"No crm.company.confirmed received within {POLL_WAIT}s. "
                "Verify POLLING_INTEGRATION_USER_ID override is set on the running CRM container."
            )
            assert result.findtext("name").startswith("Acme-E2E-")
            assert result.findtext("isActive") == "true"
            crm_id = result.findtext("id")
            assert crm_id is not None
            parsed = uuid.UUID(crm_id)
            assert parsed.version == 4
        finally:
            await _delete_sf_account(sf_client, account_id)

    @pytest.mark.asyncio
    async def test_admin_edited_account_produces_company_updated(
        self, channel, outbound_exchange, sf_client,
    ):
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        vat = _unique_vat()
        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.company.confirmed")
        q_updated = await _create_temp_queue(channel, outbound_exchange, "crm.company.updated")

        account_id = await _create_sf_account(sf_client, vat)
        try:
            confirmed = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "CompanyConfirmed" and xml.findtext("vatNumber") == vat,
            )
            assert confirmed is not None, "Prerequisite: polling must publish CompanyConfirmed first"

            await _drain_queue(q_updated)
            await _update_sf_account(sf_client, account_id, Name="Edited-NV")

            result = await _wait_for_polling_outbound(
                q_updated,
                matcher=lambda xml: xml.tag == "CompanyUpdated" and xml.findtext("vatNumber") == vat,
            )

            assert result is not None, "No crm.company.updated received within timeout"
            assert result.findtext("name") == "Edited-NV"
            assert result.findtext("id") == confirmed.findtext("id")
            assert result.findtext("isActive") == "true"
            assert result.findtext("updatedAt") is not None
        finally:
            await _delete_sf_account(sf_client, account_id)

    @pytest.mark.asyncio
    async def test_admin_deactivated_account_produces_company_deactivated(
        self, channel, outbound_exchange, sf_client,
    ):
        from tests.e2e.test_contracts_e2e import _create_temp_queue, _drain_queue

        vat = _unique_vat()
        active_field, field_type = await _resolve_active_field(sf_client, "Account")

        q_confirmed = await _create_temp_queue(channel, outbound_exchange, "crm.company.confirmed")
        q_deactivated = await _create_temp_queue(channel, outbound_exchange, "crm.company.deactivated")

        account_id = await _create_sf_account(sf_client, vat)
        try:
            confirmed = await _wait_for_polling_outbound(
                q_confirmed,
                matcher=lambda xml: xml.tag == "CompanyConfirmed" and xml.findtext("vatNumber") == vat,
            )
            assert confirmed is not None
            expected_id = confirmed.findtext("id")

            await _drain_queue(q_deactivated)
            await _update_sf_account(sf_client, account_id, **{active_field: _encode_active(False, field_type)})

            result = await _wait_for_polling_outbound(
                q_deactivated,
                matcher=lambda xml: xml.tag == "CompanyDeactivated" and xml.findtext("vatNumber") == vat,
            )

            assert result is not None, "No crm.company.deactivated received within timeout"
            assert result.findtext("id") == expected_id
            assert result.findtext("deactivatedAt") is not None
        finally:
            await _delete_sf_account(sf_client, account_id)
