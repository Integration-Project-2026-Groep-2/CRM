"""
Unit tests — handle_registration() en handle_company_created()
Contract 1: frontend.registration.created  (US-02, 03, 04, 05, 19)
Contract 3: frontend.company.created       (US-40, US-20)
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

# ---------------------------------------------------------------------------
# Test XML fixtures
# ---------------------------------------------------------------------------

VALID_REGISTRATION_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>reg-001</registrationId>
    <firstName>Jan</firstName>
    <lastName>Janssen</lastName>
    <email>jan@example.com</email>
    <sessionId>session-42</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
</Registration>"""

VALID_REGISTRATION_WITH_PHONE_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>reg-002</registrationId>
    <firstName>Anna</firstName>
    <lastName>Peeters</lastName>
    <email>anna@example.com</email>
    <sessionId>session-42</sessionId>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>0499123456</phone>
</Registration>"""

VALID_COMPANY_CONTACT_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>reg-003</registrationId>
    <firstName>Bob</firstName>
    <lastName>Smeets</lastName>
    <email>bob@acme.be</email>
    <sessionId>session-42</sessionId>
    <role>COMPANY_CONTACT</role>
    <gdprConsent>true</gdprConsent>
    <company>
        <name>Acme NV</name>
        <vatNumber>BE0123456789</vatNumber>
    </company>
</Registration>"""

INVALID_XML = b"dit is geen xml <<<"

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.reject = AsyncMock()
    msg.process = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=None),
        __aexit__=AsyncMock(return_value=False),
    ))
    return msg


# ---------------------------------------------------------------------------
# Contract 1 — handle_registration
# ---------------------------------------------------------------------------

class TestHandleRegistration:

    # ── Invalid XML ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_invalid_xml_is_rejected(self):
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")):
            from src.receiver import handle_registration
            msg = _make_message(INVALID_XML)
            await handle_registration(msg)
        msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_invalid_xml_does_not_crash(self):
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")):
            from src.receiver import handle_registration
            await handle_registration(_make_message(INVALID_XML))
        # Reaching here means no crash

    @pytest.mark.asyncio
    async def test_invalid_xml_logged_as_error(self, caplog):
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")), \
             caplog.at_level(logging.ERROR):
            from src.receiver import handle_registration
            await handle_registration(_make_message(INVALID_XML))
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    # ── Idempotency ───────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_duplicate_registration_id_is_skipped(self, caplog):
        """Duplicate registrationId → silent ack, no SF write, no publish."""
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=True)) as sf, \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock()) as sf_email, \
             patch("src.salesforce.create_contact", new=AsyncMock()) as sf_create, \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()) as pub_user, \
             patch("src.sender.publish_mail_requested", new=AsyncMock()) as pub_mail, \
             caplog.at_level(logging.INFO):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_XML))

        sf.assert_called_once_with("reg-001")
        sf_email.assert_not_called()
        sf_create.assert_not_called()
        pub_user.assert_not_called()
        pub_mail.assert_not_called()
        assert any("idempotent" in r.message for r in caplog.records)

    # ── Email conflict ────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_duplicate_email_is_logged_and_acked(self, caplog):
        """Duplicate email → log warning, ack (C15 is R2 — no fanout yet)."""
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        existing = {"CRM_ID__c": "existing-uuid", "Email": "jan@example.com"}
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=existing)), \
             patch("src.salesforce.create_contact", new=AsyncMock()) as sf_create, \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()) as pub_user, \
             caplog.at_level(logging.WARNING):
            from src.receiver import handle_registration
            msg = _make_message(VALID_REGISTRATION_XML)
            await handle_registration(msg)

        sf_create.assert_not_called()
        pub_user.assert_not_called()
        msg.reject.assert_not_called()  # acked, not rejected
        assert any("conflict" in r.message.lower() for r in caplog.records)

    # ── Salesforce unavailable → requeue ─────────────────────────────────

    @pytest.mark.asyncio
    async def test_sf_down_during_idempotency_check_requeues(self):
        from src.salesforce import SalesforceUnavailableError
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists",
                   new=AsyncMock(side_effect=SalesforceUnavailableError("down"))):
            from src.receiver import handle_registration
            msg = _make_message(VALID_REGISTRATION_XML)
            await handle_registration(msg)
        msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_sf_down_during_email_lookup_requeues(self):
        from src.salesforce import SalesforceUnavailableError
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email",
                   new=AsyncMock(side_effect=SalesforceUnavailableError("down"))):
            from src.receiver import handle_registration
            msg = _make_message(VALID_REGISTRATION_XML)
            await handle_registration(msg)
        msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_sf_down_during_contact_creation_requeues(self):
        from src.salesforce import SalesforceUnavailableError
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact",
                   new=AsyncMock(side_effect=SalesforceUnavailableError("down"))):
            from src.receiver import handle_registration
            msg = _make_message(VALID_REGISTRATION_XML)
            await handle_registration(msg)
        msg.reject.assert_called_once_with(requeue=True)

    # ── Happy path ────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_happy_path_creates_contact_in_salesforce(self):
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id-001")) as sf_create, \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()), \
             patch("src.sender.publish_mail_requested", new=AsyncMock()):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_XML))

        sf_create.assert_called_once()
        payload = sf_create.call_args[0][0]
        assert payload["Email"] == "jan@example.com"
        assert payload["FirstName"] == "Jan"
        assert payload["LastName"] == "Janssen"
        assert payload["Role__c"] == "VISITOR"
        assert "CRM_ID__c" in payload
        assert "Registration_ID__c" in payload

    @pytest.mark.asyncio
    async def test_happy_path_publishes_user_confirmed(self):
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id")), \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()) as pub_user, \
             patch("src.sender.publish_mail_requested", new=AsyncMock()):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_XML))

        pub_user.assert_called_once()
        user_data = pub_user.call_args[0][0]
        assert user_data["email"] == "jan@example.com"
        assert user_data["firstName"] == "Jan"
        assert user_data["role"] == "VISITOR"
        assert user_data["isActive"] is True
        assert user_data["gdprConsent"] is True
        assert "id" in user_data       # CRM UUID was generated
        assert "confirmedAt" in user_data

    @pytest.mark.asyncio
    async def test_happy_path_publishes_mail_requested(self):
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id")), \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()), \
             patch("src.sender.publish_mail_requested", new=AsyncMock()) as pub_mail:
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_XML))

        pub_mail.assert_called_once()
        args = pub_mail.call_args[0]
        assert args[0] == "registration_confirmation"   # mail_type
        assert args[1]["email"] == "jan@example.com"    # recipient
        assert args[2]["guest_name"] == "Jan"           # dynamic_data

    @pytest.mark.asyncio
    async def test_crm_id_is_uuid_v4(self):
        import uuid as uuid_module
        parsed = etree.fromstring(VALID_REGISTRATION_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id")), \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()) as pub_user, \
             patch("src.sender.publish_mail_requested", new=AsyncMock()):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_XML))

        user_data = pub_user.call_args[0][0]
        crm_id = user_data["id"]
        parsed_uuid = uuid_module.UUID(crm_id, version=4)
        assert str(parsed_uuid) == crm_id

    @pytest.mark.asyncio
    async def test_optional_phone_forwarded_to_salesforce(self):
        parsed = etree.fromstring(VALID_REGISTRATION_WITH_PHONE_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id")) as sf_create, \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()), \
             patch("src.sender.publish_mail_requested", new=AsyncMock()):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_REGISTRATION_WITH_PHONE_XML))

        payload = sf_create.call_args[0][0]
        assert payload.get("Phone") == "0499123456"

    @pytest.mark.asyncio
    async def test_company_contact_stores_company_fields(self):
        parsed = etree.fromstring(VALID_COMPANY_CONTACT_XML)
        with patch("src.xml_validator.validate", return_value=parsed), \
             patch("src.salesforce.registration_exists", new=AsyncMock(return_value=False)), \
             patch("src.salesforce.find_contact_by_email", new=AsyncMock(return_value=None)), \
             patch("src.salesforce.create_contact", new=AsyncMock(return_value="sf-id")) as sf_create, \
             patch("src.sender.publish_user_confirmed", new=AsyncMock()), \
             patch("src.sender.publish_mail_requested", new=AsyncMock()):
            from src.receiver import handle_registration
            await handle_registration(_make_message(VALID_COMPANY_CONTACT_XML))

        payload = sf_create.call_args[0][0]
        assert payload.get("Company_Name__c") == "Acme NV"
        assert payload.get("Company_VAT__c") == "BE0123456789"


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