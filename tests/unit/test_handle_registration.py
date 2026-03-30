"""
Unit tests — handle_company_created()
Contract 3: frontend.company.created  (US-40, US-20)

Contract 1 tests (handle_registration) live in test_receiver.py
which uses the current dependency-injection API (handle_registration(msg, sf)).
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

# ---------------------------------------------------------------------------
# Test XML fixtures
# ---------------------------------------------------------------------------

VALID_COMPANY_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
</CompanyCreated>"""

VALID_COMPANY_MINIMAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>Minimal BV</name>
    <vatNumber>BE0987654321</vatNumber>
</CompanyCreated>"""

INVALID_XML = b"dit is geen xml <<<"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    msg.process = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))
    return msg


# ---------------------------------------------------------------------------
# Contract 3 — handle_company_created
# ---------------------------------------------------------------------------

class TestHandleCompanyCreated:

    # ── Invalid XML ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_invalid_xml_is_rejected(self):
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")):
            from src.receiver import handle_company_created
            msg = _make_message(INVALID_XML)
            await handle_company_created(msg)
        msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_invalid_xml_does_not_crash(self):
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")):
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(INVALID_XML))
        # Reaching here means no crash

    # ── Idempotency ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_duplicate_vat_is_skipped(self, caplog):
        parsed = etree.fromstring(VALID_COMPANY_XML)
        existing = {"CRM_ID__c": "old-uuid", "VAT_Number__c": "BE0123456789"}
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=existing)), \
             patch("src.salesforce.create_account", new=AsyncMock()) as sf_create, \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()) as pub, \
             caplog.at_level(logging.INFO):
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_XML))

        sf_create.assert_not_called()
        pub.assert_not_called()
        assert any("idempotent" in r.message for r in caplog.records)

    # ── Salesforce unavailable → requeue ─────────────────────────────────

    @pytest.mark.asyncio
    async def test_sf_down_during_vat_lookup_requeues(self):
        from src.salesforce import SalesforceUnavailableError
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat",
                   new=AsyncMock(side_effect=SalesforceUnavailableError("down"))):
            from src.receiver import handle_company_created
            msg = _make_message(VALID_COMPANY_XML)
            await handle_company_created(msg)
        msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_sf_down_during_account_creation_requeues(self):
        from src.salesforce import SalesforceUnavailableError
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account",
                   new=AsyncMock(side_effect=SalesforceUnavailableError("down"))):
            from src.receiver import handle_company_created
            msg = _make_message(VALID_COMPANY_XML)
            await handle_company_created(msg)
        msg.reject.assert_called_once_with(requeue=True)

    # ── Happy path ────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_happy_path_creates_account_in_salesforce(self):
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account", new=AsyncMock(return_value="sf-acct-001")) as sf_create, \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()):
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_XML))

        sf_create.assert_called_once()
        payload = sf_create.call_args[0][0]
        assert payload["Name"] == "Acme NV"
        assert payload["VAT_Number__c"] == "BE0123456789"
        assert payload["IsActive__c"] is True
        assert "CRM_ID__c" in payload

    @pytest.mark.asyncio
    async def test_happy_path_publishes_company_confirmed(self):
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account", new=AsyncMock(return_value="sf-acct")), \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()) as pub:
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_XML))

        pub.assert_called_once()
        company_data = pub.call_args[0][0]
        assert company_data["vatNumber"] == "BE0123456789"
        assert company_data["name"] == "Acme NV"
        assert company_data["isActive"] is True
        assert "id" in company_data
        assert "confirmedAt" in company_data

    @pytest.mark.asyncio
    async def test_crm_id_is_uuid_v4(self):
        import uuid as uuid_module
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account", new=AsyncMock(return_value="sf-acct")), \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()) as pub:
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_XML))

        crm_id = pub.call_args[0][0]["id"]
        parsed_uuid = uuid_module.UUID(crm_id, version=4)
        assert str(parsed_uuid) == crm_id

    @pytest.mark.asyncio
    async def test_optional_email_included_when_present(self):
        parsed = etree.fromstring(VALID_COMPANY_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account", new=AsyncMock(return_value="sf-acct")) as sf_create, \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()):
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_XML))

        payload = sf_create.call_args[0][0]
        assert payload.get("Email__c") == "info@acme.be"

    @pytest.mark.asyncio
    async def test_optional_email_absent_when_not_in_xml(self):
        parsed = etree.fromstring(VALID_COMPANY_MINIMAL_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.find_account_by_vat", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_account", new=AsyncMock(return_value="sf-acct")) as sf_create, \
             patch("src.sender.publish_company_confirmed", new=AsyncMock()):
            from src.receiver import handle_company_created
            await handle_company_created(_make_message(VALID_COMPANY_MINIMAL_XML))

        payload = sf_create.call_args[0][0]
        assert "Email__c" not in payload
