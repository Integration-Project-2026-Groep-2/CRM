"""
Unit tests — receiver.py
Contract 9: controlroom.warning.issued
Contract 1 + 13: frontend.registration.created → crm.user.confirmed
Contract 24: facturatie.user.created → crm.user.confirmed
Contract 25 + 18 + 15: facturatie.user.updated → crm.user.updated / crm.user.conflict
Contract 26 + 22: facturatie.user.deactivated → crm.user.deactivated
Contract 27 + 15: mailing.user.created → crm.user.confirmed / crm.user.conflict
Contract 28: mailing.user.updated → crm.user.updated / crm.user.conflict
Contract 30 + 13 + 15: planning.user.created → crm.user.confirmed / crm.user.conflict
Contract 31 + 18 + 15: planning.user.updated → crm.user.updated / crm.user.conflict
Contract 32 + 22: planning.user.deactivated → crm.user.deactivated
"""

import logging
import re
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

VALID_WARNING_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Warning>
    <serviceId>CRM</serviceId>
    <message>CPU load boven threshold</message>
    <type>statusCheck</type>
</Warning>"""

INVALID_XML = b"dit is geen xml <<<"


def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    # Required by _republish_with_retry_count: the low-level aiormq channel's
    # basic_publish is awaited. Tests that care about republish behaviour assert
    # on it explicitly; this stub just prevents TypeErrors for the rest.
    msg.headers = None
    msg.content_type = "application/xml"
    msg.content_encoding = None
    msg.delivery_mode = 2
    msg.exchange = "user.topic"
    msg.routing_key = "test.rk"
    msg.channel = MagicMock()
    msg.channel.basic_publish = AsyncMock()
    return msg


class TestHandleWarning:
    @pytest.mark.asyncio
    async def test_valid_warning_is_logged_as_error(self, caplog):
        parsed_xml = etree.fromstring(VALID_WARNING_XML)
        with patch("src.xml_validator.validate", return_value=parsed_xml), caplog.at_level(logging.ERROR):
            from src.receiver import handle_warning

            msg = _make_message(VALID_WARNING_XML)
            await handle_warning(msg)
            msg.ack.assert_called_once()
            msg.reject.assert_not_called()
        assert any("Controlroom warning received" in r.message for r in caplog.records)
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    @pytest.mark.asyncio
    async def test_xml_content_included_in_log(self, caplog):
        parsed_xml = etree.fromstring(VALID_WARNING_XML)
        with patch("src.xml_validator.validate", return_value=parsed_xml), caplog.at_level(logging.ERROR):
            from src.receiver import handle_warning

            await handle_warning(_make_message(VALID_WARNING_XML))
        log_text = " ".join(r.message for r in caplog.records)
        assert "Warning" in log_text or "serviceId" in log_text

    @pytest.mark.asyncio
    async def test_invalid_xml_does_not_crash_container(self, caplog):
        """Invalid XML must be caught, logged as error, and rejected — no crash."""
        with (
            patch("src.xml_validator.validate", side_effect=ValueError("Ongeldige XML")),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_warning

            msg = _make_message(INVALID_XML)
            await handle_warning(msg)

        # Container did not crash — we got here
        # Error was logged
        assert any(r.levelno == logging.ERROR for r in caplog.records)
        # Message was explicitly rejected
        msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_invalid_xml_is_rejected_not_requeued(self):
        """A structurally invalid message must be rejected (requeue=False),
        not requeued — it will never become valid."""
        with patch("src.xml_validator.validate", side_effect=ValueError("bad xml")):
            from src.receiver import handle_warning

            msg = _make_message(INVALID_XML)
            await handle_warning(msg)
        msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_xml_validator_called_with_message_body(self):
        parsed_xml = etree.fromstring(VALID_WARNING_XML)
        with patch("src.xml_validator.validate", return_value=parsed_xml) as mock_validate:
            from src.receiver import handle_warning

            await handle_warning(_make_message(VALID_WARNING_XML))
        mock_validate.assert_called_once_with(VALID_WARNING_XML)


VALID_REG_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration>
    <registrationId>REG-12345</registrationId>
    <firstName>John</firstName>
    <lastName>Doe</lastName>
    <email>john.doe@example.com</email>
    <role>VISITOR</role>
    <gdprConsent>true</gdprConsent>
    <phone>+32412345678</phone>
    <company>Acme Corp</company>
</Registration>"""

CONTACT_RETURN = {
    "Id": "003000000000001",
    "CRM_ID__c": "123e4567-e89b-12d3-a456-426614174000",
    "Email": "john.doe@example.com",
    "FirstName": "John",
    "LastName": "Doe",
    "Role__c": "VISITOR",
    "GDPR_Consent__c": True,
    "Phone": "+32412345678",
}

VALID_FACTURATIE_USER_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UserCreated>
    <facturatieCustomerId>FB-1024</facturatieCustomerId>
    <registrationId>REG-20260415-010</registrationId>
    <firstName>Els</firstName>
    <lastName>Peeters</lastName>
    <email>els.peeters@example.com</email>
    <phone>+32470111222</phone>
    <role>COMPANY_CONTACT</role>
    <companyId>c3d4e5f6-a7b8-4901-8d23-ef4567ab8901</companyId>
    <isActive>true</isActive>
    <createdAt>2026-04-15T09:30:00Z</createdAt>
</UserCreated>"""

FACTURATIE_CONTACT_RETURN = {
    "Id": "003000000000024",
    "CRM_ID__c": "223e4567-e89b-12d3-a456-426614174024",
    "Email": "els.peeters@example.com",
    "FirstName": "Els",
    "LastName": "Peeters",
    "Role__c": "COMPANY_CONTACT",
    "GDPR_Consent__c": True,
    "Phone": "+32470111222",
    "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
    "Registration_ID__c": "REG-20260415-010",
}

VALID_FACTURATIE_USER_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>223e4567-e89b-12d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <phone>+32470999888</phone>
    <street>Nieuwe straat</street>
    <houseNumber>42</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussel</city>
    <country>BE</country>
    <role>COMPANY_CONTACT</role>
    <companyId>f4e5d6c7-b8a9-4012-8f34-ab5678cd9012</companyId>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</UserUpdated>"""

VALID_FACTURATIE_USER_UPDATED_MINIMAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>223e4567-e89b-12d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <role>VISITOR</role>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</UserUpdated>"""

VALID_FACTURATIE_USER_DEACTIVATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>223e4567-e89b-12d3-a456-426614174024</id>
    <email>els.peeters@example.com</email>
    <deactivatedAt>2026-04-21T16:00:00Z</deactivatedAt>
</UserDeactivated>"""

FACTURATIE_UPDATED_CONTACT_RETURN = {
    "Id": "003000000000024",
    "CRM_ID__c": "223e4567-e89b-12d3-a456-426614174024",
    "Email": "els.updated@example.com",
    "FirstName": "Els",
    "LastName": "Updated",
    "Phone": "+32470999888",
    "MailingStreet": "Nieuwe straat",
    "House_Number__c": "42",
    "MailingPostalCode": "1000",
    "MailingCity": "Brussel",
    "MailingCountry": "BE",
    "Role__c": "COMPANY_CONTACT",
    "GDPR_Consent__c": True,
    "Company_ID__c": "f4e5d6c7-b8a9-4012-8f34-ab5678cd9012",
    "Registration_ID__c": "REG-20260415-010",
    "Is_Active__c": True,
}

VALID_MAILING_USER_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserCreated>
    <id>323e4567-e89b-42d3-a456-426614174027</id>
    <email>mia.mail@example.com</email>
    <firstName>Mia</firstName>
    <lastName>Mail</lastName>
    <isActive>true</isActive>
    <companyId>c3d4e5f6-a7b8-4901-8d23-ef4567ab8901</companyId>
</MailingUserCreated>"""

VALID_MAILING_USER_CREATED_MINIMAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserCreated>
    <id>323e4567-e89b-42d3-a456-426614174027</id>
    <email>mia.mail@example.com</email>
    <isActive>true</isActive>
</MailingUserCreated>"""

VALID_MAILING_USER_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserUpdated>
    <id>323e4567-e89b-42d3-a456-426614174027</id>
    <email>mia.updated@example.com</email>
    <firstName>Mila</firstName>
    <lastName>Updated</lastName>
    <isActive>true</isActive>
    <companyId>f4e5d6c7-b8a9-4012-8f34-ab5678cd9012</companyId>
</MailingUserUpdated>"""

VALID_MAILING_USER_UPDATED_MINIMAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserUpdated>
    <id>323e4567-e89b-42d3-a456-426614174027</id>
    <email>mia.updated@example.com</email>
    <isActive>true</isActive>
</MailingUserUpdated>"""

VALID_MAILING_USER_DEACTIVATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<MailingUserDeactivated>
    <id>323e4567-e89b-42d3-a456-426614174027</id>
    <email>mia.mail@example.com</email>
    <deactivatedAt>2026-04-15T16:00:00Z</deactivatedAt>
</MailingUserDeactivated>"""

VALID_PLANNING_USER_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserCreated>
    <id>423e4567-e89b-42d3-a456-426614174030</id>
    <email>sofie.declercq@example.com</email>
    <firstName>Sofie</firstName>
    <lastName>Declercq</lastName>
    <role>SPEAKER</role>
    <gdprConsent>true</gdprConsent>
    <phoneNumber>+32470123456</phoneNumber>
    <company>Desideriushogeschool</company>
</PlanningUserCreated>"""

VALID_PLANNING_USER_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserUpdated>
    <id>423e4567-e89b-42d3-a456-426614174030</id>
    <email>sofie.updated@example.com</email>
    <firstName>Sofie</firstName>
    <lastName>Updated</lastName>
    <role>SPEAKER</role>
    <gdprConsent>true</gdprConsent>
    <phoneNumber>+32470999999</phoneNumber>
    <company>Desideriushogeschool</company>
</PlanningUserUpdated>"""

VALID_PLANNING_USER_DEACTIVATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserDeactivated>
    <id>423e4567-e89b-42d3-a456-426614174030</id>
    <email>sofie.declercq@example.com</email>
    <deactivatedAt>2026-04-15T16:00:00Z</deactivatedAt>
</PlanningUserDeactivated>"""

MAILING_CONTACT_RETURN = {
    "Id": "003000000000027",
    "CRM_ID__c": "323e4567-e89b-42d3-a456-426614174127",
    "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
    "Email": "mia.mail@example.com",
    "FirstName": "Mia",
    "LastName": "Mail",
    "Role__c": "COMPANY_CONTACT",
    "GDPR_Consent__c": True,
    "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
}

MAILING_MINIMAL_CONTACT_RETURN = {
    "Id": "003000000000035",
    "CRM_ID__c": "323e4567-e89b-42d3-a456-426614174128",
    "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
    "Email": "mia.mail@example.com",
    "LastName": "mia.mail@example.com",
    "Role__c": "VISITOR",
    "GDPR_Consent__c": True,
}

MAILING_UPDATED_CONTACT_RETURN = {
    "Id": "003000000000027",
    "CRM_ID__c": "323e4567-e89b-42d3-a456-426614174127",
    "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
    "Email": "mia.updated@example.com",
    "FirstName": "Mila",
    "LastName": "Updated",
    "Role__c": "COMPANY_CONTACT",
    "GDPR_Consent__c": True,
    "Company_ID__c": "f4e5d6c7-b8a9-4012-8f34-ab5678cd9012",
}

MAILING_UPDATED_MINIMAL_CONTACT_RETURN = {
    "Id": "003000000000027",
    "CRM_ID__c": "323e4567-e89b-42d3-a456-426614174127",
    "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
    "Email": "mia.updated@example.com",
    "FirstName": None,
    "LastName": "mia.updated@example.com",
    "Role__c": "VISITOR",
    "GDPR_Consent__c": True,
    "Company_ID__c": None,
}

PLANNING_CONTACT_RETURN = {
    "Id": "003000000000030",
    "CRM_ID__c": "423e4567-e89b-42d3-a456-426614174130",
    "Planning_ID__c": "423e4567-e89b-42d3-a456-426614174030",
    "Email": "sofie.declercq@example.com",
    "FirstName": "Sofie",
    "LastName": "Declercq",
    "Role__c": "SPEAKER",
    "GDPR_Consent__c": True,
    "Phone": "+32470123456",
}

PLANNING_UPDATED_CONTACT_RETURN = {
    "Id": "003000000000030",
    "CRM_ID__c": "423e4567-e89b-42d3-a456-426614174130",
    "Planning_ID__c": "423e4567-e89b-42d3-a456-426614174030",
    "Email": "sofie.updated@example.com",
    "FirstName": "Sofie",
    "LastName": "Updated",
    "Role__c": "SPEAKER",
    "GDPR_Consent__c": True,
    "Phone": "+32470999999",
}

def _registration_patches(
    parsed_xml=None, existing_contact=None, created_contact=None
):
    """Return a combined context manager with standard registration patches."""
    if parsed_xml is None:
        parsed_xml = etree.fromstring(VALID_REG_XML)
    if created_contact is None:
        created_contact = CONTACT_RETURN

    return (
        patch("src.xml_validator.validate", return_value=parsed_xml),
        patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=existing_contact),
        patch("src.handlers.frontend_registration_created.create_contact", return_value=created_contact),
        patch("src.sender.publish_user_confirmed"),
        patch("src.sender.publish_mail_requested"),
    )


class TestHandleRegistration:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    # ------------------------------------------------------------------
    # #7 — Split: each test asserts one thing
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_registration_publishes_user_confirmed(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            mock_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_registration_publishes_mail_requested_with_correct_args(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish, p_mail as mock_mail_publish:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)

            mock_mail_publish.assert_called_once_with(
                "registration_confirmation",
                {"email": "john.doe@example.com", "name": "John Doe"},
                {"guest_name": "John Doe"},
            )

    @pytest.mark.asyncio
    async def test_registration_user_data_id(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["id"] == "123e4567-e89b-12d3-a456-426614174000"

    @pytest.mark.asyncio
    async def test_registration_user_data_email(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["email"] == "john.doe@example.com"

    @pytest.mark.asyncio
    async def test_registration_user_data_names(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["firstName"] == "John"
            assert user_data["lastName"] == "Doe"

    @pytest.mark.asyncio
    async def test_registration_user_data_role(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["role"] == "VISITOR"

    @pytest.mark.asyncio
    async def test_registration_user_data_gdpr_consent(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["gdprConsent"] is True

    @pytest.mark.asyncio
    async def test_registration_user_data_confirmed_at(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert "confirmedAt" in user_data

    @pytest.mark.asyncio
    async def test_registration_user_data_is_active(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["isActive"] is True

    @pytest.mark.asyncio
    async def test_registration_acks_message(self, sf_mock):
        p_val, p_get, p_create, p_publish, p_mail = _registration_patches()
        with p_val, p_get, p_create, p_publish, p_mail:
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_registration_does_not_require_session_registration_object(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", side_effect=AssertionError("should not be called")),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", return_value=CONTACT_RETURN),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail,
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_publish.assert_called_once()
            mock_mail.assert_called_once()
            msg.ack.assert_called_once()

    # ------------------------------------------------------------------
    # #6 — gdprConsent="1" must be accepted (covers bug #1)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_gdpr_consent_numeric_one_accepted(self, sf_mock):
        """XSD xs:boolean allows '1' — must be stored as GDPR_Consent__c=True."""
        xml_one = VALID_REG_XML.replace(b"<gdprConsent>true</gdprConsent>", b"<gdprConsent>1</gdprConsent>")
        parsed_xml = etree.fromstring(xml_one)

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", return_value=CONTACT_RETURN) as mock_create,
            patch("src.sender.publish_user_confirmed"),
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(xml_one)
            await handle_registration(msg, sf_mock)

            # Must NOT be rejected
            msg.reject.assert_not_called()
            # Salesforce payload must have True
            create_payload = mock_create.call_args[0][1]
            assert create_payload["GDPR_Consent__c"] is True

    # ------------------------------------------------------------------
    # Existing tests (kept as-is)
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_duplicate_email_publishes_user_conflict(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.frontend_registration_created.get_contact_by_email",
                return_value={
                    "Id": "003xxx",
                    "Registration_ID__c": "OTHER",
                    "FirstName": "Other",
                    "LastName": "Person",
                    "Role__c": "VISITOR",
                    "Company_ID__c": "Old Co",
                },
            ),
            patch("src.handlers.frontend_registration_created.create_contact") as mock_create,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()
            assert "incompatible person fields" in caplog.text
            payload = mock_conflict.call_args.args[0]
            assert payload["email"] == "john.doe@example.com"
            assert payload["existingValue"] == {
                "firstName": "Other",
                "lastName": "Person",
                "company": "Old Co",
            }
            assert payload["incomingValue"] == {
                "firstName": "John",
                "lastName": "Doe",
                "company": "Acme Corp",
            }
            assert re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
                payload["detectedAt"],
            ), f"detectedAt={payload['detectedAt']!r} is not a UTC ISO-8601 timestamp"

    @pytest.mark.asyncio
    async def test_existing_contact_with_new_registration_reuses_contact_and_creates_session_link(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        existing_contact = {
            **CONTACT_RETURN,
            "Id": "003000000000001",
            "Registration_ID__c": "REG-OLD",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail,
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                registration_id="REG-12345",
            )
            mock_publish.assert_called_once()
            mock_mail.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_inactive_existing_registration_reactivates_instead_of_retry(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        existing_contact = {
            **CONTACT_RETURN,
            "Id": "003000000000001",
            "Registration_ID__c": "REG-12345",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.handlers.frontend_registration_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail,
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                registration_id="REG-12345",
            )
            mock_publish.assert_called_once()
            mock_mail.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_salesforce_create_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", side_effect=Exception("SF Create Down")),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            with pytest.raises(Exception, match="SF Create Down"):
                await handle_registration(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected(self, sf_mock, caplog):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")), caplog.at_level(logging.ERROR):
            from src.receiver import handle_registration

            msg = _make_message(INVALID_XML)
            await handle_registration(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_gdpr_consent_false_rejected(self, sf_mock, caplog):
        invalid_gdpr_xml = VALID_REG_XML.replace(b"<gdprConsent>true</gdprConsent>", b"<gdprConsent>false</gdprConsent>")
        parsed_xml = etree.fromstring(invalid_gdpr_xml)
        with patch("src.xml_validator.validate", return_value=parsed_xml), caplog.at_level(logging.WARNING):
            from src.receiver import handle_registration

            msg = _make_message(invalid_gdpr_xml)
            await handle_registration(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Registration refused — gdprConsent=false" in caplog.text

    @pytest.mark.asyncio
    async def test_retry_publish_user_confirmed_on_existing_registration_id(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
            patch("src.handlers.frontend_registration_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail_publish,
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_called_once()
            mock_mail_publish.assert_called_once()
            msg.ack.assert_called_once()

    # ------------------------------------------------------------------
    # #8 — Coverage gap tests
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_company_contact_without_company_logs_warning(self, sf_mock, caplog):
        """COMPANY_CONTACT without <company> should log a warning but still process."""
        xml_company = VALID_REG_XML.replace(
            b"<role>VISITOR</role>", b"<role>COMPANY_CONTACT</role>"
        ).replace(b"<company>Acme Corp</company>", b"")
        parsed_xml = etree.fromstring(xml_company)

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", return_value=CONTACT_RETURN),
            patch("src.sender.publish_user_confirmed"),
            patch("src.sender.publish_mail_requested"),
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration

            msg = _make_message(xml_company)
            await handle_registration(msg, sf_mock)

            assert "COMPANY_CONTACT registration without company field" in caplog.text
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_registration_without_phone_succeeds(self, sf_mock):
        """Phone is optional — registration should succeed without it."""
        xml_no_phone = VALID_REG_XML.replace(b"    <phone>+32412345678</phone>\n", b"")
        parsed_xml = etree.fromstring(xml_no_phone)

        contact_no_phone = {k: v for k, v in CONTACT_RETURN.items() if k != "Phone"}

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", return_value=contact_no_phone),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(xml_no_phone)
            await handle_registration(msg, sf_mock)

            mock_publish.assert_called_once()
            user_data = mock_publish.call_args[0][0]
            assert "phone" not in user_data
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_registration_lowercase_visitor_role_normalizes_to_picklist(self, sf_mock):
        xml_lower_role = VALID_REG_XML.replace(b"<role>VISITOR</role>", b"<role>visitor</role>")
        parsed_xml = etree.fromstring(xml_lower_role)

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value=None),
            patch("src.handlers.frontend_registration_created.create_contact", return_value=CONTACT_RETURN) as mock_create,
            patch("src.sender.publish_user_confirmed"),
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(xml_lower_role)
            await handle_registration(msg, sf_mock)

            create_payload = mock_create.call_args.args[1]
            assert create_payload["Role__c"] == "VISITOR"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_path_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        """If publish fails during retry (same registrationId), exception must bubble."""
        parsed_xml = etree.fromstring(VALID_REG_XML)

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_created.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
            patch("src.handlers.frontend_registration_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed", side_effect=Exception("Publish failed")),
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            with pytest.raises(Exception, match="Publish failed"):
                await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 24 + 13: facturatie.user.created
# ==========================================================================


class TestHandleFacturatieUserCreated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_new_facturatie_user_creates_contact_and_publishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_created.create_contact", return_value=FACTURATIE_CONTACT_RETURN) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail,
            patch("src.receiver.get_contact_by_email") as mock_fallback_lookup,
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            mock_create.assert_called_once()
            create_payload = mock_create.call_args.args[1]
            assert create_payload["FirstName"] == "Els"
            assert create_payload["LastName"] == "Peeters"
            assert create_payload["Email"] == "els.peeters@example.com"
            assert create_payload["Role__c"] == "COMPANY_CONTACT"
            assert create_payload["Phone"] == "+32470111222"
            assert create_payload["Company_ID__c"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            assert create_payload["Registration_ID__c"] == "REG-20260415-010"
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            mock_mail.assert_not_called()
            mock_fallback_lookup.assert_not_called()
            msg.ack.assert_called_once()


# ===========================================================================
# Contract 30 + 13 + 15: planning.user.created
# ===========================================================================


class TestHandlePlanningUserCreated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_new_planning_user_creates_contact_and_publishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.handlers.planning_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.planning_user_created.create_contact", return_value=PLANNING_CONTACT_RETURN) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            mock_create.assert_called_once()
            create_payload = mock_create.call_args.args[1]
            assert create_payload["Planning_ID__c"] == "423e4567-e89b-42d3-a456-426614174030"
            assert create_payload["Email"] == "sofie.declercq@example.com"
            assert create_payload["FirstName"] == "Sofie"
            assert create_payload["LastName"] == "Declercq"
            assert create_payload["Role__c"] == "SPEAKER"
            assert create_payload["Phone"] == "+32470123456"
            mock_publish.assert_called_once()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_planning_id_is_reused_without_email_lookup(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
            "Id": "003000000000031",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_created.get_contact_match_by_email") as mock_email_lookup,
            patch("src.handlers.planning_user_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.handlers.planning_user_created.backfill_planning_contact_fields", return_value=existing_contact) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            mock_email_lookup.assert_not_called()
            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                planning_id="423e4567-e89b-42d3-a456-426614174030",
            )
            mock_backfill.assert_called_once()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_unique_email_is_safely_linked_to_planning_id(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000032",
            "Email": "sofie.declercq@example.com",
            "FirstName": "Sofie",
            "LastName": "Declercq",
            "Role__c": "SPEAKER",
            "Planning_ID__c": None,
            "GDPR_Consent__c": True,
        }
        normalized_contact = {
            **PLANNING_CONTACT_RETURN,
            "Id": "003000000000032",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.handlers.planning_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.handlers.planning_user_created.backfill_planning_contact_fields", return_value=normalized_contact) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.handlers.planning_user_created.create_contact") as mock_create,
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                planning_id="423e4567-e89b-42d3-a456-426614174030",
            )
            mock_backfill.assert_called_once()
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_conflicting_existing_email_publishes_user_conflict(self, sf_mock):
        conflicting_xml = VALID_PLANNING_USER_CREATED_XML.replace(
            b"<lastName>Declercq</lastName>",
            b"<lastName>Different</lastName>",
        )
        parsed_xml = etree.fromstring(conflicting_xml)
        existing_contact = {
            "Id": "003000000000033",
            "Email": "sofie.declercq@example.com",
            "FirstName": "Sofie",
            "LastName": "Declercq",
            "Role__c": "SPEAKER",
            "Planning_ID__c": None,
            "GDPR_Consent__c": True,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.handlers.planning_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_created.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.planning_user_created.backfill_planning_contact_fields") as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(conflicting_xml)
            await handle_planning_user_created(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_backfill.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_planning_id_with_different_email_publishes_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
            "Email": "other@example.com",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_planning_id_is_acked_without_retry(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_created.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            msg.reject.assert_not_called()
            assert "ambiguous Planning_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_planning_user_created_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_planning_user_created

            msg = _make_message(INVALID_XML)
            await handle_planning_user_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_planning_user_created_without_planning_id_field_rejected(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_created.has_contact_planning_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_planning_user_created

            msg = _make_message(VALID_PLANNING_USER_CREATED_XML)
            await handle_planning_user_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Planning_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_existing_unique_facturatie_user_is_reused_and_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000025",
            "Email": "els.peeters@example.com",
        }
        normalized_contact = {
            **FACTURATIE_CONTACT_RETURN,
            "Id": "003000000000025",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.handlers.facturatie_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested") as mock_mail,
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                registration_id="REG-20260415-010",
            )
            mock_create.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            mock_mail.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_contact_without_crm_id_still_publishes_after_normalization(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000026",
            "Email": "els.peeters@example.com",
            "Registration_ID__c": None,
        }
        normalized_contact = {
            **FACTURATIE_CONTACT_RETURN,
            "Id": "003000000000026",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.sender.publish_user_confirmed") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            mock_ensure.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == FACTURATIE_CONTACT_RETURN["CRM_ID__c"]
            assert published_user["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_inactive_facturatie_user_keeps_fallback_active_field_state(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000029",
            "Email": "els.peeters@example.com",
        }
        normalized_contact = {
            **FACTURATIE_CONTACT_RETURN,
            "Id": "003000000000029",
            "Active__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_created.ensure_contact_identifiers", return_value=normalized_contact),
            patch("src.sender.publish_user_confirmed") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            published_user = mock_publish.call_args.args[0]
            assert published_user["isActive"] is False
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_facturatie_user_email_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.facturatie_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous email els.peeters@example.com" in caplog.text

    @pytest.mark.asyncio
    async def test_facturatie_user_created_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(INVALID_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_facturatie_user_created_inactive_publishes_deactivated(self, sf_mock, caplog):
        inactive_xml = VALID_FACTURATIE_USER_CREATED_XML.replace(
            b"<isActive>true</isActive>",
            b"<isActive>false</isActive>",
        )
        parsed_xml = etree.fromstring(inactive_xml)
        inactive_contact = {**FACTURATIE_CONTACT_RETURN, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_created.apply_is_active", side_effect=lambda _sf, data, flag: {**data, "IsActive__c": flag}),
            patch("src.handlers.facturatie_user_created.create_contact", return_value=inactive_contact) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_confirmed,
            caplog.at_level(logging.INFO),
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(inactive_xml)
            await handle_facturatie_user_created(msg, sf_mock)

            create_payload = mock_create.call_args.args[1]
            assert create_payload["IsActive__c"] is False
            mock_confirmed.assert_called_once()
            confirmed_payload = mock_confirmed.call_args.args[0]
            assert confirmed_payload["isActive"] is False
            msg.ack.assert_called_once()
            assert "isActive=False" in caplog.text

    @pytest.mark.asyncio
    async def test_facturatie_user_created_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_created.create_contact", return_value=FACTURATIE_CONTACT_RETURN),
            patch("src.sender.publish_user_confirmed", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_facturatie_user_created(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unique_match_state_is_not_treated_as_ambiguous(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000028",
            "Email": "els.peeters@example.com",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_created.ensure_contact_identifiers", return_value=FACTURATIE_CONTACT_RETURN),
            patch("src.handlers.facturatie_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.receiver.get_contact_by_email") as mock_fallback_lookup,
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_called_once()
            mock_fallback_lookup.assert_not_called()
            msg.ack.assert_called_once()


# ==========================================================================
# Contract 25 + 18 + 15: facturatie.user.updated
# ==========================================================================


class TestHandleFacturatieUserUpdated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_facturatie_user_updates_contact_and_publishes_user_updated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact", return_value={**FACTURATIE_UPDATED_CONTACT_RETURN, "Email": "els.peeters@example.com", "LastName": "Peeters"}) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="els.peeters@example.com",
                first_name="Els",
                last_name="Peeters",
                phone="+32470999888",
                street="Nieuwe straat",
                house_number="42",
                postal_code="1000",
                city="Brussel",
                country="BE",
                role="COMPANY_CONTACT",
                company_id="f4e5d6c7-b8a9-4012-8f34-ab5678cd9012",
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == FACTURATIE_UPDATED_CONTACT_RETURN["CRM_ID__c"]
            assert published_user["email"] == "els.peeters@example.com"
            assert published_user["firstName"] == "Els"
            assert published_user["lastName"] == "Peeters"
            assert published_user["role"] == "COMPANY_CONTACT"
            assert published_user["phone"] == "+32470999888"
            assert published_user["companyId"] == "f4e5d6c7-b8a9-4012-8f34-ab5678cd9012"
            assert published_user["street"] == "Nieuwe straat"
            assert published_user["houseNumber"] == "42"
            assert published_user["postalCode"] == "1000"
            assert published_user["city"] == "Brussel"
            assert published_user["country"] == "BE"
            assert "updatedAt" in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_optional_fields_clear_address_in_update(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_MINIMAL_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        minimal_updated_contact = {
            "Id": "003000000000024",
            "CRM_ID__c": "223e4567-e89b-12d3-a456-426614174024",
            "Email": "els.peeters@example.com",
            "FirstName": "Els",
            "LastName": "Peeters",
            "Phone": None,
            "MailingStreet": None,
            "House_Number__c": None,
            "MailingPostalCode": None,
            "MailingCity": None,
            "MailingCountry": None,
            "Role__c": "VISITOR",
            "GDPR_Consent__c": True,
            "Company_ID__c": None,
            "Is_Active__c": True,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact", return_value=minimal_updated_contact) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_MINIMAL_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="els.peeters@example.com",
                first_name="Els",
                last_name="Peeters",
                phone=None,
                street=None,
                house_number=None,
                postal_code=None,
                city=None,
                country=None,
                role="VISITOR",
                company_id=None,
            )
            published_user = mock_publish.call_args.args[0]
            assert published_user["role"] == "VISITOR"
            assert "companyId" not in published_user
            assert "phone" not in published_user
            assert "street" not in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_specialized_role_preserves_company_link_via_update_helper(self, sf_mock):
        """The role-guard lives in update_facturatie_contact; the handler just calls it.

        This test asserts the handler forwards the authoritative incoming payload,
        and that the helper's return dict (which reflects guard behaviour) flows
        through into the C18 outbound payload unchanged.
        """
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {
            **FACTURATIE_CONTACT_RETURN,
            "Role__c": "ADMIN",
            "Company_ID__c": "preserved-admin-company",
        }
        # Simulate update_facturatie_contact skipping Role + Company_ID overwrites
        # because existing role is ADMIN.
        guarded_contact = {
            **existing_contact,
            "Email": "els.updated@example.com",
            "LastName": "Updated",
            "MailingStreet": "Nieuwe straat",
            "House_Number__c": "42",
            "MailingPostalCode": "1000",
            "MailingCity": "Brussel",
            "MailingCountry": "BE",
            "Phone": "+32470999888",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact", return_value=guarded_contact) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            # Handler forwards the authoritative Facturatie role — the helper
            # decides whether to apply it. The handler itself does not short-circuit.
            assert mock_update.call_args.kwargs["role"] == "COMPANY_CONTACT"
            published_user = mock_publish.call_args.args[0]
            # Guarded contact preserves ADMIN role → outbound payload reflects that.
            assert published_user["role"] == "ADMIN"
            assert published_user["companyId"] == "preserved-admin-company"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_facturatie_user_updated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.reject.assert_not_called()
            msg.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_conflicting_existing_email_publishes_user_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        conflicting_contact = {
            "Id": "003000000000099",
            "Email": "els.updated@example.com",
            "FirstName": "Other",
            "LastName": "Owner",
            "Company_ID__c": "other-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact") as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["email"] == "els.updated@example.com"
            assert conflict_payload["existingValue"]["firstName"] == "Other"
            assert conflict_payload["existingValue"]["company"] == "other-company-id"
            assert conflict_payload["incomingValue"]["firstName"] == "Els"
            assert conflict_payload["incomingValue"]["company"] == "f4e5d6c7-b8a9-4012-8f34-ab5678cd9012"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_email_publishes_user_conflict(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact") as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()
            assert "email els.updated@example.com is ambiguous" in caplog.text

    @pytest.mark.asyncio
    async def test_facturatie_user_updated_inactive_deactivates_contact(self, sf_mock, caplog):
        inactive_xml = VALID_FACTURATIE_USER_UPDATED_XML.replace(
            b"<isActive>true</isActive>",
            b"<isActive>false</isActive>",
        )
        parsed_xml = etree.fromstring(inactive_xml)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        deactivated_contact = {**existing_contact, "Is_Active__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_updated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact") as mock_update,
            patch("src.sender.publish_user_deactivated") as mock_deactivated_publish,
            patch("src.sender.publish_user_updated") as mock_updated_publish,
            caplog.at_level(logging.INFO),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(inactive_xml)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_deactivate.assert_called_once()
            mock_update.assert_not_called()
            mock_deactivated_publish.assert_called_once()
            mock_updated_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "isActive=false on update" in caplog.text

    @pytest.mark.asyncio
    async def test_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_user_updated.update_facturatie_contact", return_value=FACTURATIE_UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_facturatie_user_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected_without_requeue(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(INVALID_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)


# ==========================================================================
# Contract 26 + 22: facturatie.user.deactivated
# ==========================================================================


class TestHandleFacturatieUserDeactivated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_facturatie_user_deactivates_and_publishes_c22(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        deactivated_contact = {**existing_contact, "Is_Active__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_deactivated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            mock_deactivate.assert_called_once_with(
                sf_mock,
                existing_contact,
                log_value="CRM_ID__c 223e4567-e89b-12d3-a456-426614174024",
            )
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == FACTURATIE_CONTACT_RETURN["CRM_ID__c"]
            assert payload["email"] == "els.peeters@example.com"
            assert payload["deactivatedAt"] == "2026-04-21T16:00:00Z"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_deactivated.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_facturatie_user_deactivated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()
            msg.reject.assert_not_called()
            msg.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_deactivated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_email_mismatch_logs_warning_but_deactivates(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        existing_contact = {
            **FACTURATIE_CONTACT_RETURN,
            "Email": "renamed@example.com",
        }
        deactivated_contact = {**existing_contact, "Is_Active__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_deactivated.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            payload = mock_publish.call_args.args[0]
            assert payload["email"] == "renamed@example.com"
            msg.ack.assert_called_once()
            assert "email mismatch" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected_without_requeue(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(INVALID_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_salesforce_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.facturatie_user_deactivated.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_facturatie_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contracts 33/34/35: facturatie.company.{created,updated,deactivated}
# ==========================================================================


VALID_FACTURATIE_COMPANY_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>billing@acme.example</email>
    <phone>+32 2 123 45 67</phone>
    <street>Kerkstraat</street>
    <houseNumber>12</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussels</city>
    <country>BE</country>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

VALID_FACTURATIE_COMPANY_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyUpdated>
    <id>d1e2f3a4-b5c6-4701-8d23-ef4567ab8501</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme Updated NV</name>
    <email>new@acme.example</email>
    <isActive>true</isActive>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</FacturatieCompanyUpdated>"""

VALID_FACTURATIE_COMPANY_DEACTIVATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyDeactivated>
    <id>d1e2f3a4-b5c6-4701-8d23-ef4567ab8501</id>
    <email>billing@acme.example</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</FacturatieCompanyDeactivated>"""

FACTURATIE_ACCOUNT_RETURN = {
    "Id": "001000000000500",
    "CRM_ID__c": "d1e2f3a4-b5c6-4701-8d23-ef4567ab8501",
    "Name": "Acme NV",
    "VAT_Number__c": "BE0123456789",
    "Email__c": "billing@acme.example",
    "BillingStreet": "Kerkstraat",
    "House_Number__c": "12",
    "BillingPostalCode": "1000",
    "BillingCity": "Brussel",
    "BillingCountryCode": "BE",
    "BillingCountry": "Belgium",
    "IsActive__c": True,
}


class TestBuildCompanyPayloadGuards:
    """Regression — 2026-04-22 production. Defensive guards in the three
    company builders so a stray None CRM_ID__c never reaches the outbound
    sender (where `str(None) == "None"` would fail XSD UUID validation and
    trigger a 5-retry loop).
    """

    def test_build_company_data_raises_on_missing_crm_id(self):
        from src.receiver import _build_company_data

        account = {**FACTURATIE_ACCOUNT_RETURN, "CRM_ID__c": None}
        with pytest.raises(ValueError, match="no CRM_ID__c"):
            _build_company_data(account)

    def test_build_updated_company_data_raises_on_missing_crm_id(self):
        from src.receiver import _build_updated_company_data

        account = {**FACTURATIE_ACCOUNT_RETURN, "CRM_ID__c": None}
        with pytest.raises(ValueError, match="no CRM_ID__c"):
            _build_updated_company_data(account)

    def test_build_company_deactivation_data_raises_on_missing_crm_id(self):
        from src.receiver import _build_company_deactivation_data

        account = {**FACTURATIE_ACCOUNT_RETURN, "CRM_ID__c": None}
        with pytest.raises(ValueError, match="no CRM_ID__c"):
            _build_company_deactivation_data(account, "2026-04-22T11:00:00Z")

    def test_build_updated_company_data_includes_house_number_when_present(self):
        from src.receiver import _build_updated_company_data

        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "House_Number__c": "12A",
            "BillingStreet": "Kerkstraat",
            "BillingPostalCode": "1000",
            "BillingCity": "Brussels",
            "BillingCountry": "BE",
        }

        payload = _build_updated_company_data(account)

        assert payload["houseNumber"] == "12A"

    def test_build_updated_company_data_prefers_billing_country_code(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_company_data

        to_iso_alpha2.cache_clear()
        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "BillingCountryCode": "BE",
            "BillingCountry": "United States",  # stale derived label
        }

        payload = _build_updated_company_data(account)

        assert payload["country"] == "BE"

    def test_build_updated_company_data_falls_back_to_billing_country(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_company_data

        to_iso_alpha2.cache_clear()
        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "BillingCountryCode": None,
            "BillingCountry": "Belgium",
        }

        payload = _build_updated_company_data(account)

        assert payload["country"] == "BE"

    def test_build_updated_company_data_omits_country_when_unresolvable(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_company_data

        to_iso_alpha2.cache_clear()
        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "BillingCountryCode": "",
            "BillingCountry": "Atlantis",
        }

        payload = _build_updated_company_data(account)

        assert "country" not in payload

    def test_build_company_data_includes_address_fields(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_company_data

        to_iso_alpha2.cache_clear()
        payload = _build_company_data(FACTURATIE_ACCOUNT_RETURN)

        assert payload["street"] == "Kerkstraat"
        assert payload["houseNumber"] == "12"
        assert payload["postalCode"] == "1000"
        assert payload["city"] == "Brussel"
        assert payload["country"] == "BE"

    def test_build_company_data_prefers_billing_country_code(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_company_data

        to_iso_alpha2.cache_clear()
        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "BillingCountryCode": "BE",
            "BillingCountry": "United States",
        }

        payload = _build_company_data(account)

        assert payload["country"] == "BE"

    def test_build_company_data_raises_when_street_missing(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_company_data

        to_iso_alpha2.cache_clear()
        account = {**FACTURATIE_ACCOUNT_RETURN, "BillingStreet": None}

        with pytest.raises(ValueError, match="street"):
            _build_company_data(account)

    def test_build_company_data_raises_when_country_unresolvable(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_company_data

        to_iso_alpha2.cache_clear()
        account = {
            **FACTURATIE_ACCOUNT_RETURN,
            "BillingCountryCode": "",
            "BillingCountry": "Atlantis",
        }

        with pytest.raises(ValueError, match="country"):
            _build_company_data(account)


class TestHandleFacturatieCompanyCreated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.fixture(autouse=True)
    def _stub_field_resolvers(self):
        """Stub the async field resolvers so tests don't have to mock describe().

        _build_facturatie_account_data probes the org via resolvers; default
        returns here match the patterns seen in prod (Email__c + BillingCountryCode
        for picklist-enabled orgs).
        """
        with (
            patch("src.handlers._facturatie_helpers._resolve_account_email_field", new_callable=AsyncMock, return_value="Email__c"),
            patch("src.handlers._facturatie_helpers._resolve_account_country_field", new_callable=AsyncMock, return_value="BillingCountryCode"),
            patch("src.handlers._facturatie_helpers.has_account_house_number_field", new_callable=AsyncMock, return_value=False),
        ):
            yield

    @pytest.mark.asyncio
    async def test_upserts_by_vat_and_publishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_created.upsert_account_by_vat", return_value=FACTURATIE_ACCOUNT_RETURN) as mock_upsert,
            patch("src.handlers.facturatie_company_created.get_account_match_by_email") as mock_email_match,
            patch("src.handlers.facturatie_company_created.create_account") as mock_create,
            patch("src.sender.publish_company_confirmed") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(VALID_FACTURATIE_COMPANY_CREATED_XML)
            await handle_facturatie_company_created(msg, sf_mock)

            mock_upsert.assert_called_once()
            vat_arg = mock_upsert.call_args.args[1]
            assert vat_arg == "BE0123456789"
            mock_email_match.assert_not_called()
            mock_create.assert_not_called()
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == FACTURATIE_ACCOUNT_RETURN["CRM_ID__c"]
            assert payload["vatNumber"] == "BE0123456789"
            assert payload["email"] == "billing@acme.example"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_upsert_payload_includes_house_number_when_account_field_supported(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers._facturatie_helpers.has_account_house_number_field", new_callable=AsyncMock, return_value=True),
            patch("src.handlers.facturatie_company_created.upsert_account_by_vat", return_value=FACTURATIE_ACCOUNT_RETURN) as mock_upsert,
            patch("src.sender.publish_company_confirmed"),
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(VALID_FACTURATIE_COMPANY_CREATED_XML)
            await handle_facturatie_company_created(msg, sf_mock)

            upsert_payload = mock_upsert.call_args.args[2]
            assert upsert_payload["House_Number__c"] == "12"

    @pytest.mark.asyncio
    async def test_no_vat_falls_back_to_email_match_create(self, sf_mock):
        no_vat_xml = VALID_FACTURATIE_COMPANY_CREATED_XML.replace(
            b"<vatNumber>BE0123456789</vatNumber>\n    ",
            b"",
        )
        parsed_xml = etree.fromstring(no_vat_xml)
        created_account = {**FACTURATIE_ACCOUNT_RETURN, "VAT_Number__c": None}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_created.upsert_account_by_vat") as mock_upsert,
            patch("src.handlers.facturatie_company_created.get_account_match_by_email", return_value=("none", None)),
            patch("src.handlers.facturatie_company_created.create_account", return_value=created_account) as mock_create,
            patch("src.sender.publish_company_confirmed") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(no_vat_xml)
            await handle_facturatie_company_created(msg, sf_mock)

            mock_upsert.assert_not_called()
            mock_create.assert_called_once()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_vat_ambiguous_email_acked_without_publish(self, sf_mock, caplog):
        no_vat_xml = VALID_FACTURATIE_COMPANY_CREATED_XML.replace(
            b"<vatNumber>BE0123456789</vatNumber>\n    ",
            b"",
        )
        parsed_xml = etree.fromstring(no_vat_xml)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_created.get_account_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.facturatie_company_created.create_account") as mock_create,
            patch("src.sender.publish_company_confirmed") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(no_vat_xml)
            await handle_facturatie_company_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous email billing@acme.example" in caplog.text

    @pytest.mark.asyncio
    async def test_no_vat_email_match_refuses_to_hijack_vat_linked_account(self, sf_mock, caplog):
        """Security regression — H3 from 2026-04-22 review.

        When Facturatie sends a company without vatNumber and email matches
        an existing Account that DOES have a VAT_Number__c, refuse to bind.
        Binding would let Facturatie take over the CRM_ID__c of an unrelated
        company originally registered via Frontend C3.
        """
        no_vat_xml = VALID_FACTURATIE_COMPANY_CREATED_XML.replace(
            b"<vatNumber>BE0123456789</vatNumber>\n    ",
            b"",
        )
        parsed_xml = etree.fromstring(no_vat_xml)
        # Existing Account has a VAT — created via authoritative path.
        existing_vat_linked = {
            **FACTURATIE_ACCOUNT_RETURN,
            "Name": "Big Unrelated Corp",
            "VAT_Number__c": "BE0999999999",
            "Email__c": "billing@acme.example",  # collision with payload
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.facturatie_company_created.get_account_match_by_email",
                return_value=("unique", existing_vat_linked),
            ),
            patch("src.handlers.facturatie_company_created.create_account") as mock_create,
            patch("src.sender.publish_company_confirmed") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(no_vat_xml)
            await handle_facturatie_company_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "refusing to bind" in caplog.text
            assert "BE0999999999" in caplog.text

    @pytest.mark.asyncio
    async def test_no_vat_email_match_reuses_vat_less_account(self, sf_mock):
        """Counterpart to the hijack guard — email match WITH no existing VAT
        is safe to reuse (matches Facturatie's own earlier sync without VAT).
        """
        no_vat_xml = VALID_FACTURATIE_COMPANY_CREATED_XML.replace(
            b"<vatNumber>BE0123456789</vatNumber>\n    ",
            b"",
        )
        parsed_xml = etree.fromstring(no_vat_xml)
        existing_vat_less = {
            **FACTURATIE_ACCOUNT_RETURN,
            "VAT_Number__c": None,
            "Email__c": "billing@acme.example",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.facturatie_company_created.get_account_match_by_email",
                return_value=("unique", existing_vat_less),
            ),
            patch("src.handlers.facturatie_company_created.create_account") as mock_create,
            patch("src.sender.publish_company_confirmed") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(no_vat_xml)
            await handle_facturatie_company_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_company_created

            msg = _make_message(INVALID_XML)
            await handle_facturatie_company_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)


class TestHandleFacturatieCompanyUpdated:
    @pytest.fixture
    def sf_mock(self):
        sf = AsyncMock()
        sf.Account = AsyncMock()
        return sf

    @pytest.mark.asyncio
    async def test_unique_crm_id_updates_and_publishes_c19(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_UPDATED_XML)
        existing = {**FACTURATIE_ACCOUNT_RETURN}
        updated = {**existing, "Name": "Acme Updated NV"}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_updated.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.handlers.facturatie_company_updated.update_facturatie_account", return_value=updated) as mock_update,
            patch("src.receiver.apply_is_active", return_value={}),
            patch("src.sender.publish_company_updated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_updated

            msg = _make_message(VALID_FACTURATIE_COMPANY_UPDATED_XML)
            await handle_facturatie_company_updated(msg, sf_mock)

            mock_update.assert_called_once()
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == FACTURATIE_ACCOUNT_RETURN["CRM_ID__c"]
            assert payload["name"] == "Acme Updated NV"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_inactive_deactivates_and_publishes_c23(self, sf_mock):
        inactive_xml = VALID_FACTURATIE_COMPANY_UPDATED_XML.replace(
            b"<isActive>true</isActive>",
            b"<isActive>false</isActive>",
        )
        parsed_xml = etree.fromstring(inactive_xml)
        existing = {**FACTURATIE_ACCOUNT_RETURN}
        deactivated = {**existing, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_updated.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.handlers.facturatie_company_updated.deactivate_account_record", return_value=deactivated) as mock_deact,
            patch("src.sender.publish_company_deactivated") as mock_publish,
            patch("src.sender.publish_company_updated") as mock_updated_publish,
        ):
            from src.receiver import handle_facturatie_company_updated

            msg = _make_message(inactive_xml)
            await handle_facturatie_company_updated(msg, sf_mock)

            mock_deact.assert_called_once()
            mock_publish.assert_called_once()
            mock_updated_publish.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_updated.get_account_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_company_updated") as mock_publish,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_facturatie_company_updated

            msg = _make_message(VALID_FACTURATIE_COMPANY_UPDATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_facturatie_company_updated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_company_updated

            msg = _make_message(INVALID_XML)
            await handle_facturatie_company_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)


class TestHandleFacturatieCompanyDeactivated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_unique_crm_id_deactivates_and_publishes(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        existing = {**FACTURATIE_ACCOUNT_RETURN}
        deactivated = {**existing, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_deactivated.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.handlers.facturatie_company_deactivated.deactivate_account_record", return_value=deactivated) as mock_deact,
            patch("src.sender.publish_company_deactivated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
            await handle_facturatie_company_deactivated(msg, sf_mock)

            mock_deact.assert_called_once()
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == FACTURATIE_ACCOUNT_RETURN["CRM_ID__c"]
            assert payload["vatNumber"] == "BE0123456789"
            assert payload["deactivatedAt"] == "2026-04-22T11:00:00Z"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_deactivated.get_account_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_company_deactivated") as mock_publish,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_facturatie_company_deactivated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_deactivated.get_account_match_by_crm_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_company_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
            await handle_facturatie_company_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_email_mismatch_logs_but_proceeds(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        existing = {**FACTURATIE_ACCOUNT_RETURN, "Email__c": "renamed@acme.example"}
        deactivated = {**existing, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.facturatie_company_deactivated.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.handlers.facturatie_company_deactivated.deactivate_account_record", return_value=deactivated),
            patch("src.sender.publish_company_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
            await handle_facturatie_company_deactivated(msg, sf_mock)

            mock_publish.assert_called_once()
            msg.ack.assert_called_once()
            assert "email mismatch" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(INVALID_XML)
            await handle_facturatie_company_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)


# ==========================================================================
# Contract 27 + 13 + 15: mailing.user.created
# ==========================================================================


class TestHandleMailingUserCreated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_new_mailing_user_creates_contact_and_publishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.create_contact", return_value=MAILING_CONTACT_RETURN) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.sender.publish_mail_requested") as mock_mail,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_called_once()
            create_payload = mock_create.call_args.args[1]
            assert create_payload["Mailing_ID__c"] == "323e4567-e89b-42d3-a456-426614174027"
            assert create_payload["Email"] == "mia.mail@example.com"
            assert create_payload["FirstName"] == "Mia"
            assert create_payload["LastName"] == "Mail"
            assert create_payload["Company_ID__c"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            assert create_payload["Role__c"] == "COMPANY_CONTACT"
            mock_publish.assert_called_once()
            mock_conflict.assert_not_called()
            mock_mail.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_new_mailing_user_without_last_name_uses_email_fallback(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_MINIMAL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch(
                "src.handlers.mailing_user_created.create_contact",
                return_value=MAILING_MINIMAL_CONTACT_RETURN,
            ) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_MINIMAL_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_called_once()
            create_payload = mock_create.call_args.args[1]
            assert create_payload["Email"] == "mia.mail@example.com"
            assert create_payload["LastName"] == "mia.mail@example.com"
            assert create_payload["Role__c"] == "VISITOR"
            assert "FirstName" not in create_payload
            assert "Company_ID__c" not in create_payload
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            confirmed_payload = mock_publish.call_args.args[0]
            assert confirmed_payload["lastName"] == "mia.mail@example.com"
            assert confirmed_payload["role"] == "VISITOR"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_unique_email_without_mailing_id_attaches_identifier(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000028",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": "COMPANY_CONTACT",
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
        }
        normalized_contact = {
            **MAILING_CONTACT_RETURN,
            "Id": "003000000000028",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=normalized_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.handlers.mailing_user_created.create_contact") as mock_create,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                mailing_id="323e4567-e89b-42d3-a456-426614174027",
            )
            mock_backfill.assert_called_once_with(
                sf_mock,
                normalized_contact,
                first_name="Mia",
                last_name="Mail",
                company_id="c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
                role="COMPANY_CONTACT",
                gdpr_consent=True,
            )
            mock_create.assert_not_called()
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_exact_replay_republishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=existing_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once()
            mock_backfill.assert_called_once_with(
                sf_mock,
                existing_contact,
                first_name="Mia",
                last_name="Mail",
                company_id="c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
                role="COMPANY_CONTACT",
                gdpr_consent=True,
            )
            mock_publish.assert_called_once()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_exact_replay_is_case_insensitive_for_email(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Email": "Mia.Mail@Example.com",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=existing_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once()
            mock_backfill.assert_called_once()
            mock_publish.assert_called_once()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_email_is_disambiguated_by_unique_mailing_id(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Id": "003000000000031",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=existing_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.handlers.mailing_user_created.create_contact") as mock_create,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                mailing_id="323e4567-e89b-42d3-a456-426614174027",
            )
            mock_backfill.assert_called_once()
            mock_publish.assert_called_once()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_conflicting_existing_email_publishes_user_conflict(self, sf_mock):
        conflicting_xml = VALID_MAILING_USER_CREATED_XML.replace(
            b"<firstName>Mia</firstName>",
            b"<firstName>Different</firstName>",
        )
        parsed_xml = etree.fromstring(conflicting_xml)
        existing_contact = {
            "Id": "003000000000029",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": "COMPANY_CONTACT",
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.mailing_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(conflicting_xml)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_ensure.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["existingValue"]["company"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            assert conflict_payload["incomingValue"]["company"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_mailing_id_already_linked_to_other_email_publishes_user_conflict(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000030",
            "Email": "other@example.com",
            "FirstName": "Other",
            "LastName": "Person",
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
            "Company_ID__c": "existing-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["existingValue"]["company"] == "existing-company-id"
            assert conflict_payload["incomingValue"]["company"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()
            assert "Mailing ID 323e4567-e89b-42d3-a456-426614174027 already linked" in caplog.text

    @pytest.mark.asyncio
    async def test_cross_contact_conflict_uses_mailing_id_owner_in_payload(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        email_contact = {
            "Id": "003000000000032",
            "Email": "mia.mail@example.com",
            "FirstName": "Email",
            "LastName": "Owner",
            "Company_ID__c": "email-company-id",
        }
        mailing_contact = {
            "Id": "003000000000033",
            "Email": None,
            "FirstName": "Mailing",
            "LastName": "Owner",
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
            "Company_ID__c": "mailing-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", email_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("unique", mailing_contact)),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["existingValue"]["firstName"] == "Mailing"
            assert conflict_payload["existingValue"]["lastName"] == "Owner"
            assert conflict_payload["existingValue"]["company"] == "mailing-company-id"
            msg.ack.assert_called_once()
            assert "email mia.mail@example.com and Mailing ID" in caplog.text

    @pytest.mark.asyncio
    async def test_compatible_existing_contact_is_backfilled_before_confirmation(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000034",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": None,
        }
        normalized_contact = {
            **existing_contact,
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
        }
        backfilled_contact = {
            **MAILING_CONTACT_RETURN,
            "Id": "003000000000034",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=backfilled_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                mailing_id="323e4567-e89b-42d3-a456-426614174027",
            )
            mock_backfill.assert_called_once_with(
                sf_mock,
                normalized_contact,
                first_name="Mia",
                last_name="Mail",
                company_id="c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
                role="COMPANY_CONTACT",
                gdpr_consent=True,
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            confirmed_payload = mock_publish.call_args.args[0]
            assert confirmed_payload["id"] == "323e4567-e89b-42d3-a456-426614174127"
            assert confirmed_payload["email"] == "mia.mail@example.com"
            assert confirmed_payload["firstName"] == "Mia"
            assert confirmed_payload["lastName"] == "Mail"
            assert confirmed_payload["role"] == "COMPANY_CONTACT"
            assert confirmed_payload["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_visitor_is_enriched_without_conflict(self, sf_mock):
        no_last_name_xml = VALID_MAILING_USER_CREATED_XML.replace(
            b"    <lastName>Mail</lastName>\n",
            b"",
        )
        parsed_xml = etree.fromstring(no_last_name_xml)
        existing_contact = {
            "Id": "003000000000036",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": None,
            "Role__c": "VISITOR",
            "Mailing_ID__c": None,
            "Company_ID__c": None,
        }
        normalized_contact = {
            **existing_contact,
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
        }
        backfilled_contact = {
            **MAILING_CONTACT_RETURN,
            "Id": "003000000000036",
            "LastName": "mia.mail@example.com",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=backfilled_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(no_last_name_xml)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                mailing_id="323e4567-e89b-42d3-a456-426614174027",
            )
            mock_backfill.assert_called_once_with(
                sf_mock,
                normalized_contact,
                first_name="Mia",
                last_name="mia.mail@example.com",
                company_id="c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
                role="COMPANY_CONTACT",
                gdpr_consent=True,
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            confirmed_payload = mock_publish.call_args.args[0]
            assert confirmed_payload["lastName"] == "mia.mail@example.com"
            assert confirmed_payload["role"] == "COMPANY_CONTACT"
            assert confirmed_payload["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_specialized_role_with_company_id_publishes_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        existing_contact = {
            "Id": "003000000000038",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": "ADMIN",
            "Mailing_ID__c": None,
            "Company_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.mailing_user_created.backfill_mailing_contact_fields") as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_backfill.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["incomingValue"]["company"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_specialized_role_with_stored_company_linkage_publishes_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_MINIMAL_XML)
        existing_contact = {
            "Id": "003000000000040",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": "ADMIN",
            "Mailing_ID__c": None,
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.mailing_user_created.backfill_mailing_contact_fields") as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_MINIMAL_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_backfill.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_company_linkage_sets_company_contact_role_on_reuse(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_MINIMAL_XML)
        existing_contact = {
            "Id": "003000000000037",
            "Email": "mia.mail@example.com",
            "FirstName": None,
            "LastName": None,
            "Role__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
            "GDPR_Consent__c": None,
        }
        normalized_contact = {
            **existing_contact,
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
        }
        backfilled_contact = {
            "Id": "003000000000037",
            "CRM_ID__c": "323e4567-e89b-42d3-a456-426614174129",
            "Mailing_ID__c": "323e4567-e89b-42d3-a456-426614174027",
            "Email": "mia.mail@example.com",
            "FirstName": None,
            "LastName": "mia.mail@example.com",
            "Role__c": "COMPANY_CONTACT",
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
            "GDPR_Consent__c": True,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.handlers.mailing_user_created.backfill_mailing_contact_fields",
                return_value=backfilled_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_MINIMAL_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                mailing_id="323e4567-e89b-42d3-a456-426614174027",
            )
            mock_backfill.assert_called_once_with(
                sf_mock,
                normalized_contact,
                last_name="mia.mail@example.com",
                company_id="c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
                role="COMPANY_CONTACT",
                gdpr_consent=True,
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            confirmed_payload = mock_publish.call_args.args[0]
            assert confirmed_payload["role"] == "COMPANY_CONTACT"
            assert confirmed_payload["gdprConsent"] is True
            assert confirmed_payload["lastName"] == "mia.mail@example.com"
            assert confirmed_payload["companyId"] == "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_existing_contact_with_explicit_false_gdpr_publishes_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_MINIMAL_XML)
        existing_contact = {
            "Id": "003000000000039",
            "Email": "mia.mail@example.com",
            "FirstName": None,
            "LastName": "mia.mail@example.com",
            "Role__c": None,
            "Mailing_ID__c": None,
            "GDPR_Consent__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.mailing_user_created.backfill_mailing_contact_fields") as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_MINIMAL_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_backfill.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_mailing_id_field_rejects_without_requeue(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Mailing_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_mailing_user_created_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(INVALID_XML)
            await handle_mailing_user_created(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_mailing_user_created_inactive_creates_inactive_contact(self, sf_mock, caplog):
        inactive_xml = VALID_MAILING_USER_CREATED_XML.replace(
            b"<isActive>true</isActive>",
            b"<isActive>false</isActive>",
        )
        parsed_xml = etree.fromstring(inactive_xml)
        inactive_contact = {**MAILING_CONTACT_RETURN, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.apply_is_active", side_effect=lambda _sf, data, flag: {**data, "IsActive__c": flag}),
            patch("src.handlers.mailing_user_created.create_contact", return_value=inactive_contact) as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_confirmed,
            caplog.at_level(logging.INFO),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(inactive_xml)
            await handle_mailing_user_created(msg, sf_mock)

            create_payload = mock_create.call_args.args[1]
            assert create_payload["IsActive__c"] is False
            mock_confirmed.assert_called_once()
            confirmed_payload = mock_confirmed.call_args.args[0]
            assert confirmed_payload["isActive"] is False
            msg.ack.assert_called_once()
            assert "isActive=False" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_email_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.handlers.mailing_user_created.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(VALID_MAILING_USER_CREATED_XML)
            await handle_mailing_user_created(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous email mia.mail@example.com" in caplog.text

    @pytest.mark.asyncio
    async def test_conflict_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        conflicting_xml = VALID_MAILING_USER_CREATED_XML.replace(
            b"<firstName>Mia</firstName>",
            b"<firstName>Different</firstName>",
        )
        parsed_xml = etree.fromstring(conflicting_xml)
        existing_contact = {
            "Id": "003000000000029",
            "Email": "mia.mail@example.com",
            "FirstName": "Mia",
            "LastName": "Mail",
            "Role__c": "COMPANY_CONTACT",
            "Company_ID__c": "c3d4e5f6-a7b8-4901-8d23-ef4567ab8901",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_created.has_contact_mailing_id_field", return_value=True),
            patch("src.handlers.mailing_user_created.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_created.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.sender.publish_user_conflict", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(conflicting_xml)
            with pytest.raises(Exception, match="publish failed"):
                await handle_mailing_user_created(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 28 + 18 + 15: mailing.user.updated
# ==========================================================================


class TestHandleMailingUserUpdated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_mailing_user_updates_contact_and_publishes_user_updated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == existing_contact["CRM_ID__c"]
            assert published_user["email"] == "mia.mail@example.com"
            assert published_user["firstName"] == "Mia"
            assert published_user["lastName"] == "Mail"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_optional_fields_clear_company_and_fallback_last_name(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["lastName"] == "Mail"
            assert published_user["role"] == "COMPANY_CONTACT"
            assert "companyId" in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_specialized_role_preserves_existing_company_link_in_published_update(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Role__c": "ADMIN",
            "Company_ID__c": "old-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["role"] == "ADMIN"
            assert published_user["companyId"] == "old-company-id"
            assert published_user["email"] == "mia.mail@example.com"
            assert published_user["firstName"] == "Mia"
            assert published_user["lastName"] == "Mail"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_mailing_user_updated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_conflicting_existing_email_publishes_user_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        conflicting_contact = {
            "Id": "003000000000099",
            "Email": "mia.updated@example.com",
            "FirstName": "Other",
            "LastName": "Owner",
            "Company_ID__c": "other-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            conflict_payload = mock_conflict.call_args.args[0]
            assert conflict_payload["existingValue"]["firstName"] == "Other"
            assert conflict_payload["existingValue"]["company"] == "other-company-id"
            assert conflict_payload["incomingValue"]["firstName"] == "Mila"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_email_publishes_user_conflict(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_update.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()
            assert "email mia.updated@example.com is ambiguous" in caplog.text

    @pytest.mark.asyncio
    async def test_mailing_user_updated_inactive_deactivates_contact(self, sf_mock, caplog):
        inactive_xml = VALID_MAILING_USER_UPDATED_XML.replace(
            b"<isActive>true</isActive>",
            b"<isActive>false</isActive>",
        )
        parsed_xml = etree.fromstring(inactive_xml)
        existing_contact = {**MAILING_CONTACT_RETURN}
        deactivated_contact = {**existing_contact, "IsActive__c": False}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.mailing_user_updated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.sender.publish_user_deactivated") as mock_deactivated_publish,
            patch("src.sender.publish_user_updated") as mock_updated_publish,
            caplog.at_level(logging.INFO),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(inactive_xml)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_deactivate.assert_called_once()
            mock_deactivated_publish.assert_called_once()
            mock_updated_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "isActive=false on update" in caplog.text

    @pytest.mark.asyncio
    async def test_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_mailing_user_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 31 + 18 + 15: planning.user.updated
# ==========================================================================


class TestHandlePlanningUserUpdated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_planning_user_updates_contact_and_publishes_user_updated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        normalized_contact = {
            **existing_contact,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.planning_user_updated.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.handlers.planning_user_updated.update_planning_contact", return_value={**PLANNING_UPDATED_CONTACT_RETURN, "Email": "sofie.declercq@example.com", "LastName": "Declercq"}) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                planning_id="423e4567-e89b-42d3-a456-426614174030",
            )
            mock_update.assert_called_once_with(
                sf_mock,
                normalized_contact,
                email="sofie.declercq@example.com",
                first_name="Sofie",
                last_name="Declercq",
                role="SPEAKER",
                phone_number="+32470999999",
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == PLANNING_UPDATED_CONTACT_RETURN["CRM_ID__c"]
            assert published_user["email"] == "sofie.declercq@example.com"
            assert published_user["firstName"] == "Sofie"
            assert published_user["lastName"] == "Declercq"
            assert published_user["role"] == "SPEAKER"
            assert published_user["phone"] == "+32470999999"
            assert "updatedAt" in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_planning_id_field_rejects_without_requeue(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Planning_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_unknown_planning_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.handlers.planning_user_updated.get_contact_match_by_email") as mock_email_lookup,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_planning_user_updated(msg, sf_mock)

            assert excinfo.value.identifier_label == "Planning_ID__c"
            mock_email_lookup.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_planning_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous Planning_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_conflicting_existing_email_publishes_user_conflict(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        conflicting_contact = {
            "Id": "003000000000199",
            "Email": "sofie.updated@example.com",
            "FirstName": "Other",
            "LastName": "Owner",
            "Company_ID__c": "other-company-id",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_updated.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.handlers.planning_user_updated.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.planning_user_updated.update_planning_contact") as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_update.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_ambiguous_email_publishes_user_conflict(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_updated.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.handlers.planning_user_updated.ensure_contact_identifiers") as mock_ensure,
            patch("src.handlers.planning_user_updated.update_planning_contact") as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            mock_ensure.assert_not_called()
            mock_update.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_called_once()
            msg.ack.assert_called_once()
            assert "email sofie.updated@example.com is ambiguous" in caplog.text

    @pytest.mark.asyncio
    async def test_planning_user_updated_without_gdpr_consent_rejected(self, sf_mock, caplog):
        invalid_gdpr_xml = VALID_PLANNING_USER_UPDATED_XML.replace(
            b"<gdprConsent>true</gdprConsent>",
            b"<gdprConsent>false</gdprConsent>",
        )
        parsed_xml = etree.fromstring(invalid_gdpr_xml)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(invalid_gdpr_xml)
            await handle_planning_user_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "PlanningUserUpdated refused — gdprConsent=false" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected_without_requeue(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(INVALID_XML)
            await handle_planning_user_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_updated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_updated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_updated.get_contact_match_by_email", return_value=("none", None)),
            patch("src.handlers.planning_user_updated.ensure_contact_identifiers", return_value=existing_contact),
            patch("src.handlers.planning_user_updated.update_planning_contact", return_value=PLANNING_UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_planning_user_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 32 + 22: planning.user.deactivated
# ==========================================================================


class TestHandlePlanningUserDeactivated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_planning_user_deactivates_contact_and_publishes_user_deactivated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        normalized_contact = {
            **existing_contact,
        }
        deactivated_contact = {
            **normalized_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_deactivated.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.handlers.planning_user_deactivated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                existing_contact,
                planning_id="423e4567-e89b-42d3-a456-426614174030",
            )
            mock_deactivate.assert_called_once_with(
                sf_mock,
                normalized_contact,
                log_value="Planning_ID__c 423e4567-e89b-42d3-a456-426614174030",
            )
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == PLANNING_CONTACT_RETURN["CRM_ID__c"]
            assert payload["email"] == "sofie.declercq@example.com"
            assert payload["deactivatedAt"] == "2026-04-15T16:00:00Z"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_planning_id_field_rejects_without_requeue(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Planning_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_unknown_planning_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_planning_user_deactivated(msg, sf_mock)

            assert excinfo.value.identifier_label == "Planning_ID__c"
            mock_publish.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_planning_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous Planning_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_email_mismatch_logs_warning_but_deactivates(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
            "Email": "other@example.com",
        }
        normalized_contact = {
            **existing_contact,
        }
        deactivated_contact = {
            **normalized_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_deactivated.ensure_contact_identifiers", return_value=normalized_contact),
            patch("src.handlers.planning_user_deactivated.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            payload = mock_publish.call_args.args[0]
            assert payload["email"] == "other@example.com"
            msg.ack.assert_called_once()
            assert "email mismatch" in caplog.text

    @pytest.mark.asyncio
    async def test_missing_crm_id_is_backfilled_before_deactivation(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        legacy_contact = {
            **PLANNING_CONTACT_RETURN,
            "CRM_ID__c": None,
        }
        normalized_contact = {
            **legacy_contact,
            "CRM_ID__c": "523e4567-e89b-42d3-a456-426614174132",
        }
        deactivated_contact = {
            **normalized_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("unique", legacy_contact)),
            patch("src.handlers.planning_user_deactivated.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.handlers.planning_user_deactivated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            mock_ensure.assert_called_once_with(
                sf_mock,
                legacy_contact,
                planning_id="423e4567-e89b-42d3-a456-426614174030",
            )
            mock_deactivate.assert_called_once_with(
                sf_mock,
                normalized_contact,
                log_value="Planning_ID__c 423e4567-e89b-42d3-a456-426614174030",
            )
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == "523e4567-e89b-42d3-a456-426614174132"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected_without_requeue(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(INVALID_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_salesforce_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_deactivated.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_planning_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        deactivated_contact = {
            **existing_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_user_deactivated.has_contact_planning_id_field", return_value=True),
            patch("src.handlers.planning_user_deactivated.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.handlers.planning_user_deactivated.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_planning_user_deactivated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 29 + 22: mailing.user.deactivated
# ==========================================================================


class TestHandleMailingUserDeactivated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_existing_mailing_user_deactivates_contact_and_publishes_user_deactivated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Email": "mia.mail@example.com",
        }
        deactivated_contact = {
            **existing_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_deactivated.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            mock_deactivate.assert_called_once_with(
                sf_mock,
                existing_contact,
                log_value="CRM_ID__c 323e4567-e89b-42d3-a456-426614174027",
            )
            mock_publish.assert_called_once()
            payload = mock_publish.call_args.args[0]
            assert payload["id"] == MAILING_CONTACT_RETURN["CRM_ID__c"]
            assert payload["email"] == "mia.mail@example.com"
            assert payload["deactivatedAt"] == "2026-04-15T16:00:00Z"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_raises_missing_dependency(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.handlers._exceptions import MissingDependencyError
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            with pytest.raises(MissingDependencyError) as excinfo:
                await handle_mailing_user_deactivated(msg, sf_mock)

            assert excinfo.value.identifier_label == "CRM_ID__c"
            mock_publish.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "ambiguous CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_email_mismatch_logs_warning_but_deactivates(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Email": "other@example.com",
        }
        deactivated_contact = {
            **existing_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_deactivated.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            payload = mock_publish.call_args.args[0]
            assert payload["email"] == "other@example.com"
            msg.ack.assert_called_once()
            assert "email mismatch" in caplog.text

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected_without_requeue(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(INVALID_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_salesforce_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_deactivated.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_mailing_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        deactivated_contact = {
            **existing_contact,
            "IsActive__c": False,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.mailing_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.handlers.mailing_user_deactivated.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            with pytest.raises(Exception, match="publish failed"):
                await handle_mailing_user_deactivated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 2 + 18 + 22: frontend.registration.updated
# ==========================================================================

VALID_UPDATE_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>REG-12345</registrationId>
    <email>john.doe@example.com</email>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Jane</firstName>
        <lastName>Smith</lastName>
        <phone>+32400000000</phone>
    </updatedFields>
</RegistrationChange>"""

VALID_CANCEL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>cancel@example.com</email>
    <changeType>cancelled</changeType>
</RegistrationChange>"""

VALID_SESSION_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<SessionUpdate>
    <sessionId>SESS-001</sessionId>
    <sessionName>Workshop AI</sessionName>
    <newTime>2026-04-15T15:00:00Z</newTime>
    <newLocation>Zaal B</newLocation>
    <changeType>rescheduled</changeType>
</SessionUpdate>"""

VALID_SESSION_CANCELLED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<SessionUpdate>
    <sessionId>SESS-001</sessionId>
    <sessionName>Workshop AI</sessionName>
    <changeType>cancelled</changeType>
</SessionUpdate>"""

VALID_PAYMENT_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<PaymentConfirmed>
    <userId>550e8400-e29b-41d4-a716-446655440099</userId>
    <email>john.doe@example.com</email>
    <registrationId>REG-12345</registrationId>
    <amount>99.95</amount>
    <currency>EUR</currency>
    <paidAt>2026-04-02T09:30:00Z</paidAt>
</PaymentConfirmed>"""

VALID_UNPAID_REQUEST_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UnpaidRequest>
    <requestId>UNPAID-001</requestId>
</UnpaidRequest>"""

VALID_PERSON_LOOKUP_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<PersonLookupRequest>
    <requestId>LOOKUP-001</requestId>
    <email>john.doe@example.com</email>
</PersonLookupRequest>"""

PERSON_LOOKUP_CONTACT_WITH_ACCOUNT = {
    "Id": "003000000000055",
    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440099",
    "AccountId": "001000000000055",
    "Account": {
        "Name": "Acme NV",
        "CRM_ID__c": "660e8400-e29b-41d4-a716-446655440055",
    },
}

PERSON_LOOKUP_CONTACT_WITHOUT_ACCOUNT = {
    "Id": "003000000000066",
    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440066",
    "AccountId": None,
    "Account": None,
}

UPDATED_CONTACT_RETURN = {
    "Id": "003000000000099",
    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440099",
    "Email": "john.doe@example.com",
    "FirstName": "Jane",
    "LastName": "Smith",
    "Role__c": "VISITOR",
    "GDPR_Consent__c": True,
    "Phone": "+32400000000",
}

DEACTIVATED_CONTACT_RETURN = {
    "Id": "003000000000088",
    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440088",
    "Email": "cancel@example.com",
    "FirstName": "Cancelled",
    "LastName": "User",
    "IsActive__c": False,
}

DEACTIVATED_ACCOUNT_RETURN = {
    "Id": "001000000000042",
    "CRM_ID__c": "acc-crm-001",
    "VAT_Number__c": "BE0123456789",
    "IsActive__c": False,
}

PAID_CONTACT_RETURN = {
    "Id": "003000000000077",
    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440099",
    "Email": "john.doe@example.com",
    "Registration_ID__c": "REG-12345",
    "Paid_At__c": "2026-04-02T09:30:00Z",
}

UNPAID_CONTACTS_RETURN = [
    {
        "id": "550e8400-e29b-41d4-a716-446655440010",
        "firstName": "Anna",
        "lastName": "Peeters",
        "email": "anna.peeters@example.com",
        "linkedToCompany": False,
    },
    {
        "id": "550e8400-e29b-41d4-a716-446655440011",
        "firstName": "Bert",
        "lastName": "Smeets",
        "email": "bert.smeets@example.com",
        "linkedToCompany": True,
        "companyName": "Acme NV",
    },
]


class TestBuildUpdatedUserData:
    """Contract 18 country normalization: MailingCountryCode preferred,
    MailingCountry resolved via pycountry, unresolvable omits country."""

    def _base(self, **extra):
        return {**UPDATED_CONTACT_RETURN, **extra}

    def test_prefers_mailing_country_code(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_user_data

        to_iso_alpha2.cache_clear()
        payload = _build_updated_user_data(
            self._base(MailingCountryCode="BE", MailingCountry="United States"),
        )
        assert payload["country"] == "BE"

    def test_falls_back_to_mailing_country(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_user_data

        to_iso_alpha2.cache_clear()
        payload = _build_updated_user_data(
            self._base(MailingCountryCode=None, MailingCountry="Belgium"),
        )
        assert payload["country"] == "BE"

    def test_omits_country_when_unresolvable(self):
        from src.country_code import to_iso_alpha2
        from src.receiver import _build_updated_user_data

        to_iso_alpha2.cache_clear()
        payload = _build_updated_user_data(
            self._base(MailingCountryCode="", MailingCountry="Atlantis"),
        )
        assert "country" not in payload


class TestHandleRegistrationUpdated:
    """Contract 2 — Frontend RegistrationChange (updated|cancelled).

    Handler is Contact-only sinds 2026-04-29: geen ``Session_Registration__c``
    junction-writes meer. Sessie-deelname is Planning's verantwoordelijkheid.
    """

    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    # ------------------------------------------------------------------
    # Updated path
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_updated_calls_upsert_contact(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_upsert.assert_called_once()
            call_args = mock_upsert.call_args
            assert call_args[0][1] == "john.doe@example.com"
            update_data = call_args[0][2]
            assert update_data["FirstName"] == "Jane"
            assert update_data["LastName"] == "Smith"
            assert update_data["Phone"] == "+32400000000"

    @pytest.mark.asyncio
    async def test_updated_publishes_user_updated(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_UPDATE_XML), sf_mock)

            mock_publish.assert_called_once()
            user_data = mock_publish.call_args[0][0]
            assert user_data["id"] == "550e8400-e29b-41d4-a716-446655440099"
            assert user_data["email"] == "john.doe@example.com"
            assert "updatedAt" in user_data

    @pytest.mark.asyncio
    async def test_updated_publishes_full_profile_fields_when_available(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        contact_with_optional_fields = {
            **UPDATED_CONTACT_RETURN,
            "IsActive__c": False,
            "Company_ID__c": "acc-001",
            "Badge_Code__c": "B-42",
            "MailingStreet": "Main Street",
            "House_Number__c": "10A",
            "MailingPostalCode": "2000",
            "MailingCity": "Antwerp",
            "MailingCountry": "Belgium",
        }

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=contact_with_optional_fields),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_UPDATE_XML), sf_mock)

            user_data = mock_publish.call_args[0][0]
            assert user_data["isActive"] is False
            assert user_data["companyId"] == "acc-001"
            assert user_data["badgeCode"] == "B-42"
            assert user_data["street"] == "Main Street"
            assert user_data["houseNumber"] == "10A"
            assert user_data["postalCode"] == "2000"
            assert user_data["city"] == "Antwerp"
            # Country is normalized to ISO 3166-1 alpha-2 for the XSD pattern.
            assert user_data["country"] == "BE"

    @pytest.mark.asyncio
    async def test_updated_uses_active_field_fallbacks_for_is_active(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        contact_with_fallback_active_field = {
            **UPDATED_CONTACT_RETURN,
            "Active__c": False,
        }

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=contact_with_fallback_active_field),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_UPDATE_XML), sf_mock)

            user_data = mock_publish.call_args[0][0]
            assert user_data["isActive"] is False

    @pytest.mark.asyncio
    async def test_updated_acks_message(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_updated_without_updated_fields_calls_upsert_with_empty_data(self, sf_mock):
        xml_no_fields = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>john.doe@example.com</email>
    <changeType>updated</changeType>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_no_fields)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(xml_no_fields)
            await handle_registration_updated(msg, sf_mock)

            mock_upsert.assert_called_once()
            update_data = mock_upsert.call_args[0][2]
            assert update_data == {}
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_updated_without_registration_id_still_processes(self, sf_mock):
        """registrationId is optional in canonical C2 spec — handler must not require it."""
        xml_no_reg_id = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>john.doe@example.com</email>
    <changeType>updated</changeType>
    <updatedFields>
        <firstName>Jane</firstName>
    </updatedFields>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_no_reg_id)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(xml_no_reg_id)
            await handle_registration_updated(msg, sf_mock)

            mock_upsert.assert_called_once()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_updated_maps_role_to_salesforce_field(self, sf_mock):
        xml_with_role = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>john.doe@example.com</email>
    <changeType>updated</changeType>
    <updatedFields>
        <role>COMPANY_CONTACT</role>
    </updatedFields>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_with_role)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(xml_with_role), sf_mock)
            update_data = mock_upsert.call_args[0][2]
            assert update_data["Role__c"] == "COMPANY_CONTACT"


    # ------------------------------------------------------------------
    # Cancelled path
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_cancelled_deactivates_contact_directly(self, sf_mock):
        """Sinds 2026-04-29 raakt cancel altijd de Contact zelf (geen junction)."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN) as mock_deact_contact,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_CANCEL_XML), sf_mock)
            mock_deact_contact.assert_called_once()
            mock_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_publishes_user_deactivated_when_last_registration_is_removed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_CANCEL_XML), sf_mock)

            mock_publish.assert_called_once()
            deact_data = mock_publish.call_args[0][0]
            assert deact_data["id"] == "550e8400-e29b-41d4-a716-446655440088"
            assert deact_data["email"] == "cancel@example.com"
            assert "deactivatedAt" in deact_data

    @pytest.mark.asyncio
    async def test_cancelled_with_linked_company_deactivates_account_and_publishes_company_deactivated(self, sf_mock):
        """Contract 23: a linked company must be deactivated and published on cancellation."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": "acc-crm-001",
        }
        deactivated_contact_with_company = {
            **DEACTIVATED_CONTACT_RETURN,
            "Company_ID__c": "acc-crm-001",
        }
        account_before_deactivation = {
            **DEACTIVATED_ACCOUNT_RETURN,
            "IsActive__c": True,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.handlers.frontend_registration_updated.count_active_contacts_for_company", return_value=0),
            patch("src.handlers.frontend_registration_updated.get_account_by_crm_id", return_value=account_before_deactivation),
            patch("src.handlers.frontend_registration_updated.deactivate_account_by_crm_id", return_value=DEACTIVATED_ACCOUNT_RETURN) as mock_deactivate_account,
            patch("src.sender.publish_user_deactivated") as mock_publish_user,
            patch("src.sender.publish_company_deactivated") as mock_publish_company,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_deactivate_account.assert_called_once_with(sf_mock, "acc-crm-001")
            mock_publish_user.assert_called_once()
            mock_publish_company.assert_called_once()

            company_deact_data = mock_publish_company.call_args.args[0]
            assert company_deact_data["id"] == "acc-crm-001"
            assert company_deact_data["vatNumber"] == "BE0123456789"
            assert "deactivatedAt" in company_deact_data
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_without_company_link_does_not_deactivate_or_publish_company(self, sf_mock):
        """Contract 23 is skipped when the cancelled contact is not linked to a company."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.handlers.frontend_registration_updated.count_active_contacts_for_company") as mock_sibling_count,
            patch("src.handlers.frontend_registration_updated.deactivate_account_by_crm_id") as mock_deactivate_account,
            patch("src.sender.publish_user_deactivated"),
            patch("src.sender.publish_company_deactivated") as mock_publish_company,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_sibling_count.assert_not_called()
            mock_deactivate_account.assert_not_called()
            mock_publish_company.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_with_sibling_contacts_skips_account_deactivation(self, sf_mock):
        """C1 fix: when other active contacts share the Company_ID__c, the Account must NOT be deactivated."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": "acc-crm-001",
        }
        deactivated_contact_with_company = {
            **DEACTIVATED_CONTACT_RETURN,
            "Company_ID__c": "acc-crm-001",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.handlers.frontend_registration_updated.count_active_contacts_for_company", return_value=2) as mock_sibling_count,
            patch("src.handlers.frontend_registration_updated.get_account_by_crm_id") as mock_get_account,
            patch("src.handlers.frontend_registration_updated.deactivate_account_by_crm_id") as mock_deactivate_account,
            patch("src.sender.publish_user_deactivated") as mock_publish_user,
            patch("src.sender.publish_company_deactivated") as mock_publish_company,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_sibling_count.assert_called_once_with(sf_mock, "acc-crm-001")
            mock_get_account.assert_not_called()
            mock_deactivate_account.assert_not_called()
            mock_publish_user.assert_called_once()
            mock_publish_company.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_with_no_vat_skips_account_mutation(self, sf_mock):
        """C2 fix: when Account lacks VAT_Number__c, SF must NOT be mutated to avoid split-brain."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": "acc-crm-001",
        }
        deactivated_contact_with_company = {
            **DEACTIVATED_CONTACT_RETURN,
            "Company_ID__c": "acc-crm-001",
        }
        account_without_vat = {
            "Id": "001000000000042",
            "CRM_ID__c": "acc-crm-001",
            "VAT_Number__c": None,
            "IsActive__c": True,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.handlers.frontend_registration_updated.count_active_contacts_for_company", return_value=0),
            patch("src.handlers.frontend_registration_updated.get_account_by_crm_id", return_value=account_without_vat),
            patch("src.handlers.frontend_registration_updated.deactivate_account_by_crm_id") as mock_deactivate_account,
            patch("src.sender.publish_user_deactivated") as mock_publish_user,
            patch("src.sender.publish_company_deactivated") as mock_publish_company,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            # C2: Account must NOT be mutated in SF when VAT is missing
            mock_deactivate_account.assert_not_called()
            mock_publish_user.assert_called_once()
            mock_publish_company.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_company_block_failure_still_acks_message(self, sf_mock):
        """H2 fix: if company deactivation raises after Contract 22, the message must still be acked."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
            "Company_ID__c": "acc-crm-001",
        }
        deactivated_contact_with_company = {
            **DEACTIVATED_CONTACT_RETURN,
            "Company_ID__c": "acc-crm-001",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.handlers.frontend_registration_updated.count_active_contacts_for_company", side_effect=Exception("SF API down")),
            patch("src.sender.publish_user_deactivated") as mock_publish_user,
            patch("src.sender.publish_company_deactivated") as mock_publish_company,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            # H2: Contract 22 already fired, message must be acked despite company failure
            mock_publish_user.assert_called_once()
            mock_publish_company.assert_not_called()
            msg.ack.assert_called_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancelled_keeps_native_identity_contact_active(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": "plan-123",
            "Mailing_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record") as mock_deact_contact,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)
            mock_deact_contact.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_contact_not_found_acks_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=None),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "acking without action" in caplog.text

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_invalid_xml_rejected(self, sf_mock, caplog):
        """Invalid XML must be rejected without requeue."""
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")), caplog.at_level(logging.ERROR):
            from src.receiver import handle_registration_updated

            msg = _make_message(INVALID_XML)
            await handle_registration_updated(msg, sf_mock)
            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_salesforce_error_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_registration_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_on_update_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            with pytest.raises(Exception, match="RabbitMQ down"):
                await handle_registration_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_on_cancel_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        existing_contact = {
            **DEACTIVATED_CONTACT_RETURN,
            "Id": "003000000000088",
            "Email": "cancel@example.com",
            "Planning_ID__c": None,
            "Mailing_ID__c": None,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.frontend_registration_updated.get_contact_by_email", return_value=existing_contact),
            patch("src.handlers.frontend_registration_updated.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            with pytest.raises(Exception, match="RabbitMQ down"):
                await handle_registration_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


SESSION_PARTICIPANTS = [
    {
        "Id": "003000000000201",
        "Email": "anna@example.com",
        "FirstName": "Anna",
        "LastName": "Alpha",
    },
    {
        "Id": "003000000000202",
        "Email": "bert@example.com",
        "FirstName": "Bert",
        "LastName": "Beta",
    },
]


class TestHandleSessionUpdated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_rescheduled_publishes_mail_per_participant(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_session_updated.has_session_registration_object", return_value=True),
            patch("src.handlers.planning_session_updated.get_active_session_participants", return_value=SESSION_PARTICIPANTS),
            patch("src.sender.publish_mail_requested") as mock_publish,
        ):
            from src.receiver import handle_session_updated

            msg = _make_message(VALID_SESSION_UPDATED_XML)
            await handle_session_updated(msg, sf_mock)

            assert mock_publish.call_count == 2
            first_call = mock_publish.call_args_list[0].args
            assert first_call[0] == "session_change"
            assert first_call[1]["email"] == "anna@example.com"
            assert first_call[2]["session_name"] == "Workshop AI"
            assert first_call[2]["session_time"] == "2026-04-15T15:00:00Z"
            assert first_call[2]["session_location"] == "Zaal B"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_omits_session_time_when_not_provided(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_CANCELLED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_session_updated.has_session_registration_object", return_value=True),
            patch("src.handlers.planning_session_updated.get_active_session_participants", return_value=[SESSION_PARTICIPANTS[0]]),
            patch("src.sender.publish_mail_requested") as mock_publish,
        ):
            from src.receiver import handle_session_updated

            await handle_session_updated(_make_message(VALID_SESSION_CANCELLED_XML), sf_mock)

            dynamic_data = mock_publish.call_args.args[2]
            assert "session_time" not in dynamic_data
            assert dynamic_data["session_name"] == "Workshop AI"

    @pytest.mark.asyncio
    async def test_session_update_acks_without_publish_when_no_participants(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_session_updated.has_session_registration_object", return_value=True),
            patch("src.handlers.planning_session_updated.get_active_session_participants", return_value=[]),
            patch("src.sender.publish_mail_requested") as mock_publish,
        ):
            from src.receiver import handle_session_updated

            msg = _make_message(VALID_SESSION_UPDATED_XML)
            await handle_session_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_session_update_missing_object_rejects_without_requeue(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_session_updated.has_session_registration_object", return_value=False),
        ):
            from src.receiver import handle_session_updated

            msg = _make_message(VALID_SESSION_UPDATED_XML)
            await handle_session_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_session_update_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_session_updated

            msg = _make_message(INVALID_XML)
            await handle_session_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_session_update_publish_error_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.planning_session_updated.has_session_registration_object", return_value=True),
            patch("src.handlers.planning_session_updated.get_active_session_participants", return_value=[SESSION_PARTICIPANTS[0]]),
            patch("src.sender.publish_mail_requested", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_session_updated

            msg = _make_message(VALID_SESSION_UPDATED_XML)
            with pytest.raises(Exception, match="RabbitMQ down"):
                await handle_session_updated(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 16: kassa.payment.confirmed
# ==========================================================================


class TestHandlePaymentConfirmed:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_payment_confirmed_calls_update_payment_status(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PAYMENT_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_payment_confirmed.update_payment_status", return_value=PAID_CONTACT_RETURN) as mock_update,
        ):
            from src.receiver import handle_payment_confirmed

            await handle_payment_confirmed(_make_message(VALID_PAYMENT_XML), sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                user_id="550e8400-e29b-41d4-a716-446655440099",
                email="john.doe@example.com",
                registration_id="REG-12345",
                paid_at="2026-04-02T09:30:00Z",
            )

    @pytest.mark.asyncio
    async def test_payment_confirmed_acks_on_success(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PAYMENT_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_payment_confirmed.update_payment_status", return_value=PAID_CONTACT_RETURN),
        ):
            from src.receiver import handle_payment_confirmed

            msg = _make_message(VALID_PAYMENT_XML)
            await handle_payment_confirmed(msg, sf_mock)

            msg.ack.assert_called_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_payment_confirmed_acks_when_contact_not_found_or_ambiguous(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PAYMENT_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_payment_confirmed.update_payment_status", return_value=None),
        ):
            from src.receiver import handle_payment_confirmed

            msg = _make_message(VALID_PAYMENT_XML)
            await handle_payment_confirmed(msg, sf_mock)

            msg.ack.assert_called_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_payment_confirmed_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_payment_confirmed

            msg = _make_message(INVALID_XML)
            await handle_payment_confirmed(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_payment_confirmed_salesforce_error_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PAYMENT_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_payment_confirmed.update_payment_status", side_effect=Exception("SF Down")),
        ):
            from src.receiver import handle_payment_confirmed

            msg = _make_message(VALID_PAYMENT_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_payment_confirmed(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


class TestHandleUnpaidRequested:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_unpaid_requested_calls_salesforce_and_publishes_response(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_unpaid_requested.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN) as mock_get,
            patch("src.sender.publish_unpaid_responded") as mock_publish,
        ):
            from src.receiver import handle_unpaid_requested

            await handle_unpaid_requested(_make_message(VALID_UNPAID_REQUEST_XML), sf_mock)

            mock_get.assert_called_once_with(sf_mock)
            mock_publish.assert_called_once_with("UNPAID-001", UNPAID_CONTACTS_RETURN)

    @pytest.mark.asyncio
    async def test_unpaid_requested_acks_on_success(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_unpaid_requested.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN),
            patch("src.sender.publish_unpaid_responded"),
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            await handle_unpaid_requested(msg, sf_mock)

            msg.ack.assert_called_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unpaid_requested_publishes_empty_list(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_unpaid_requested.get_unpaid_contacts", return_value=[]),
            patch("src.sender.publish_unpaid_responded") as mock_publish,
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            await handle_unpaid_requested(msg, sf_mock)

            mock_publish.assert_called_once_with("UNPAID-001", [])
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unpaid_requested_invalid_xml_rejected(self, sf_mock):
        with patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(INVALID_XML)
            await handle_unpaid_requested(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_unpaid_requested_salesforce_error_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_unpaid_requested.get_unpaid_contacts", side_effect=Exception("SF Down")),
            patch("src.sender.publish_unpaid_responded"),
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_unpaid_requested(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unpaid_requested_publish_error_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_unpaid_requested.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN),
            patch("src.sender.publish_unpaid_responded", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            with pytest.raises(Exception, match="RabbitMQ down"):
                await handle_unpaid_requested(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 10a: kassa.person.lookup.requested  →  Contract 10b response
# ==========================================================================


class TestHandlePersonLookup:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_publishes_found_true_with_company_when_contact_linked(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PERSON_LOOKUP_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.kassa_person_lookup_requested.get_contact_for_person_lookup",
                return_value=PERSON_LOOKUP_CONTACT_WITH_ACCOUNT,
            ) as mock_lookup,
            patch("src.sender.publish_person_lookup_responded") as mock_publish,
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(VALID_PERSON_LOOKUP_XML)
            await handle_person_lookup(msg, sf_mock)

            mock_lookup.assert_called_once_with(sf_mock, "john.doe@example.com")
            mock_publish.assert_called_once_with(
                "LOOKUP-001",
                {
                    "found": True,
                    "linkedToCompany": True,
                    "id": "550e8400-e29b-41d4-a716-446655440099",
                    "companyName": "Acme NV",
                    "companyId": "660e8400-e29b-41d4-a716-446655440055",
                },
            )
            msg.ack.assert_called_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publishes_found_true_without_company_when_contact_not_linked(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PERSON_LOOKUP_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.kassa_person_lookup_requested.get_contact_for_person_lookup",
                return_value=PERSON_LOOKUP_CONTACT_WITHOUT_ACCOUNT,
            ),
            patch("src.sender.publish_person_lookup_responded") as mock_publish,
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(VALID_PERSON_LOOKUP_XML)
            await handle_person_lookup(msg, sf_mock)

            mock_publish.assert_called_once_with(
                "LOOKUP-001",
                {
                    "found": True,
                    "linkedToCompany": False,
                    "id": "550e8400-e29b-41d4-a716-446655440066",
                },
            )
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_publishes_found_false_when_contact_missing(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PERSON_LOOKUP_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.handlers.kassa_person_lookup_requested.get_contact_for_person_lookup", return_value=None),
            patch("src.sender.publish_person_lookup_responded") as mock_publish,
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(VALID_PERSON_LOOKUP_XML)
            await handle_person_lookup(msg, sf_mock)

            mock_publish.assert_called_once_with(
                "LOOKUP-001",
                {"found": False, "linkedToCompany": False},
            )
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_rejects_invalid_xml_without_requeue(self, sf_mock):
        with (
            patch("src.xml_validator.validate", side_effect=ValueError("Bad XML")),
            patch("src.handlers.kassa_person_lookup_requested.get_contact_for_person_lookup") as mock_lookup,
            patch("src.sender.publish_person_lookup_responded") as mock_publish,
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(INVALID_XML)
            await handle_person_lookup(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            mock_lookup.assert_not_called()
            mock_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_salesforce_error_bubbles_to_wrap_handler(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PERSON_LOOKUP_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.kassa_person_lookup_requested.get_contact_for_person_lookup",
                side_effect=Exception("SF Down"),
            ),
            patch("src.sender.publish_person_lookup_responded"),
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(VALID_PERSON_LOOKUP_XML)
            with pytest.raises(Exception, match="SF Down"):
                await handle_person_lookup(msg, sf_mock)

            msg.ack.assert_not_called()
            msg.reject.assert_not_called()


class TestRunReceiver:
    async def _run_receiver(self):
        queues = {}
        sf_client = MagicMock()

        async def _declare_queue(_channel, queue_name, durable, *, routing_key=None, exchange_name=None):  # noqa: ARG001
            queue = queues.get(queue_name)
            if queue is None:
                queue = AsyncMock(name=f"{queue_name}_queue")
                queue._routing_key = routing_key or queue_name
                queues[queue_name] = queue
            return queue

        mock_declare = AsyncMock(side_effect=_declare_queue)

        with (
            patch("src.receiver.get_salesforce_client", return_value=sf_client),
            patch("src.receiver._declare_and_bind", mock_declare),
            patch("src.receiver.asyncio.Future", side_effect=RuntimeError("stop receiver loop")),
        ):
            from src.receiver import run_receiver

            with pytest.raises(RuntimeError, match="stop receiver loop"):
                await run_receiver(AsyncMock(), MagicMock())

        return queues, mock_declare, sf_client

    @staticmethod
    def _assert_declared_queue(mock_declare, queue_name: str, durable: bool):
        queue_call = next(
            call for call in mock_declare.call_args_list
            if call.args[1] == queue_name
        )
        assert queue_call.kwargs["durable"] is durable

    @staticmethod
    def _assert_direct_callback(queue, handler):
        # Consumers are wrapped in functools.partial(_wrap_handler, inner, queue_name).
        # For non-sf handlers, `inner` is the handler itself.
        callback = queue.consume.call_args.args[0]
        from src.receiver import _wrap_handler

        assert callback.func is _wrap_handler
        assert callback.args[0] is handler

    @staticmethod
    def _assert_partial_callback(queue, handler, sf_client):
        # Consumer is partial(_wrap_handler, partial(handler, sf=sf_client), queue_name).
        callback = queue.consume.call_args.args[0]
        from src.receiver import _wrap_handler

        assert callback.func is _wrap_handler
        inner = callback.args[0]
        assert inner.func is handler
        assert inner.keywords["sf"] is sf_client

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_9_queue(self):
        from src.receiver import handle_warning

        queues, mock_declare, _sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "controlroom.warning.issued", durable=True)
        self._assert_direct_callback(queues["controlroom.warning.issued"], handle_warning)

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_1_queue(self):
        from src.receiver import handle_registration

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.frontend.registration.created", durable=True)
        self._assert_partial_callback(
            queues["crm.frontend.registration.created"], handle_registration, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_2_queue(self):
        from src.receiver import handle_registration_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.frontend.registration.updated", durable=True)
        self._assert_partial_callback(
            queues["crm.frontend.registration.updated"], handle_registration_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_24_queue(self):
        from src.receiver import handle_facturatie_user_created

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.facturatie.user.created", durable=True)
        self._assert_partial_callback(
            queues["crm.facturatie.user.created"], handle_facturatie_user_created, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_25_queue(self):
        from src.receiver import handle_facturatie_user_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.facturatie.user.updated", durable=True)
        self._assert_partial_callback(
            queues["crm.facturatie.user.updated"], handle_facturatie_user_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_26_queue(self):
        from src.receiver import handle_facturatie_user_deactivated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.facturatie.user.deactivated", durable=True)
        self._assert_partial_callback(
            queues["crm.facturatie.user.deactivated"], handle_facturatie_user_deactivated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_16_queue(self):
        from src.receiver import handle_payment_confirmed

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "kassa.payment.confirmed", durable=True)
        self._assert_partial_callback(
            queues["kassa.payment.confirmed"], handle_payment_confirmed, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_17_queue(self):
        from src.receiver import handle_unpaid_requested

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "kassa.unpaid.requested", durable=True)
        self._assert_partial_callback(
            queues["kassa.unpaid.requested"], handle_unpaid_requested, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_11_queue(self):
        from src.receiver import handle_session_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "planning.session.updated", durable=True)
        self._assert_partial_callback(
            queues["planning.session.updated"], handle_session_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_27_queue(self):
        from src.receiver import handle_mailing_user_created

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.mailing.user.created", durable=True)
        self._assert_partial_callback(
            queues["crm.mailing.user.created"], handle_mailing_user_created, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_28_queue(self):
        from src.receiver import handle_mailing_user_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.mailing.user.updated", durable=True)
        self._assert_partial_callback(
            queues["crm.mailing.user.updated"], handle_mailing_user_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_29_queue(self):
        from src.receiver import handle_mailing_user_deactivated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.mailing.user.deactivated", durable=True)
        self._assert_partial_callback(
            queues["crm.mailing.user.deactivated"], handle_mailing_user_deactivated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_30_queue(self):
        from src.receiver import handle_planning_user_created

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.planning.user.created", durable=True)
        self._assert_partial_callback(
            queues["crm.planning.user.created"], handle_planning_user_created, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_32_queue(self):
        from src.receiver import handle_planning_user_deactivated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "crm.planning.user.deactivated", durable=True)
        self._assert_partial_callback(
            queues["crm.planning.user.deactivated"], handle_planning_user_deactivated, sf_client
        )

    @pytest.mark.asyncio
    async def test_contract_28_queue_uses_consumer_prefix_and_producer_routing_key(self):
        """Consumer-prefixed queue `crm.mailing.user.updated` binds to `mailing.user.updated` routing key."""
        queues, mock_declare, _sf_client = await self._run_receiver()

        call = next(
            call for call in mock_declare.call_args_list
            if call.args[1] == "crm.mailing.user.updated"
        )
        assert call.kwargs["routing_key"] == "mailing.user.updated"
        assert "crm.mailing.user.updated" in queues
        assert "mailing.user.updated" not in queues

    @pytest.mark.asyncio
    async def test_contract_29_queue_uses_consumer_prefix_and_producer_routing_key(self):
        """Consumer-prefixed queue `crm.mailing.user.deactivated` binds to `mailing.user.deactivated` routing key."""
        queues, mock_declare, _sf_client = await self._run_receiver()

        call = next(
            call for call in mock_declare.call_args_list
            if call.args[1] == "crm.mailing.user.deactivated"
        )
        assert call.kwargs["routing_key"] == "mailing.user.deactivated"
        assert "crm.mailing.user.deactivated" in queues
        assert "mailing.user.deactivated" not in queues


class TestWrapHandler:
    """Safety guarantees of the receiver's centralized failure-routing wrapper."""

    @pytest.fixture
    def message(self):
        msg = MagicMock()
        msg.body = b"<Probe/>"
        msg.headers = {}
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()
        return msg

    @pytest.mark.asyncio
    async def test_no_retry_queue_acks_and_drops_without_calling_handle_failure(self, message):
        """C9 contract: log-and-ack on any exception; never route to retry/DLQ."""
        from src.receiver import _wrap_handler

        async def failing_handler(_msg):
            raise RuntimeError("controlroom warning processing failed")

        with patch("src.receiver._handle_failure", new_callable=AsyncMock) as mock_handle_failure:
            await _wrap_handler(
                failing_handler, "controlroom.warning.issued", message,
            )

        mock_handle_failure.assert_not_called()
        message.ack.assert_awaited_once()
        message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_retry_queue_swallows_ack_failure(self, message):
        """If ack itself raises (channel closed), we must not propagate the exception."""
        from src.receiver import _wrap_handler

        message.ack = AsyncMock(side_effect=Exception("channel closed"))

        async def failing_handler(_msg):
            raise RuntimeError("boom")

        with patch("src.receiver._handle_failure", new_callable=AsyncMock) as mock_handle_failure:
            # Must not raise — _wrap_handler is the outermost layer.
            await _wrap_handler(
                failing_handler, "controlroom.warning.issued", message,
            )

        mock_handle_failure.assert_not_called()
        message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_normal_queue_routes_exception_through_handle_failure(self, message):
        """Sanity: queues NOT in _NO_RETRY_QUEUES still route via _handle_failure."""
        from src.receiver import _wrap_handler

        async def failing_handler(_msg):
            raise RuntimeError("boom")

        with patch("src.receiver._handle_failure", new_callable=AsyncMock) as mock_handle_failure:
            await _wrap_handler(
                failing_handler, "crm.facturatie.user.updated", message,
            )

        mock_handle_failure.assert_awaited_once()
        call = mock_handle_failure.await_args
        assert call.args[0] == "crm.facturatie.user.updated"
        assert call.args[1] is message
        assert isinstance(call.args[2], RuntimeError)
        assert call.kwargs["work_queue"] == "crm.facturatie.user.updated"
        # _wrap_handler does not ack/reject directly when delegating to _handle_failure;
        # the helper itself handles message lifecycle.
        message.ack.assert_not_called()
        message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_meta_failure_force_rejects_when_handle_failure_raises(self, message, caplog):
        """If _handle_failure itself raises, last-resort reject(requeue=False) prevents poison loops."""
        from src.receiver import _wrap_handler

        async def failing_handler(_msg):
            raise RuntimeError("primary failure")

        with (
            patch(
                "src.receiver._handle_failure",
                new_callable=AsyncMock,
                side_effect=Exception("channel closed during retry publish"),
            ),
            caplog.at_level(logging.ERROR),
        ):
            await _wrap_handler(
                failing_handler, "crm.facturatie.user.updated", message,
            )

        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_called()
        assert "meta-failure" in caplog.text

    @pytest.mark.asyncio
    async def test_meta_failure_swallows_reject_failure(self, message):
        """If both _handle_failure AND reject raise, we must not propagate either."""
        from src.receiver import _wrap_handler

        message.reject = AsyncMock(side_effect=Exception("reject also failed"))

        async def failing_handler(_msg):
            raise RuntimeError("primary failure")

        with patch(
            "src.receiver._handle_failure",
            new_callable=AsyncMock,
            side_effect=Exception("meta failure"),
        ):
            # Must not raise — last-resort fallback is best-effort.
            await _wrap_handler(
                failing_handler, "crm.facturatie.user.updated", message,
            )

        message.reject.assert_awaited_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_successful_handler_does_not_invoke_failure_path(self, message):
        """Handlers that succeed must not trigger _handle_failure or reject."""
        from src.receiver import _wrap_handler

        async def ok_handler(_msg):
            return None

        with patch("src.receiver._handle_failure", new_callable=AsyncMock) as mock_handle_failure:
            await _wrap_handler(
                ok_handler, "crm.facturatie.user.updated", message,
            )

        mock_handle_failure.assert_not_called()
        message.reject.assert_not_called()
        # Handler is responsible for its own ack on success path.
        message.ack.assert_not_called()


class TestExponentialBackoff:
    @pytest.mark.parametrize(
        "attempt,expected",
        [(0, 1.0), (1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0), (5, 30.0), (6, 30.0), (10, 30.0)],
    )
    def test_backoff_progression(self, attempt: int, expected: float):
        from src.receiver import _exponential_backoff_seconds

        assert _exponential_backoff_seconds(attempt) == expected

    def test_backoff_negative_attempt_clamped_to_zero(self):
        from src.receiver import _exponential_backoff_seconds

        assert _exponential_backoff_seconds(-5) == 1.0

    def test_backoff_respects_custom_cap(self):
        from src.receiver import _exponential_backoff_seconds

        assert _exponential_backoff_seconds(10, cap=5) == 5.0


class TestRepublishWithRetryCount:
    @pytest.fixture
    def message(self):
        msg = MagicMock()
        msg.body = b"<Placeholder/>"
        msg.ack = AsyncMock()
        msg.headers = {}
        msg.content_type = "application/xml"
        msg.content_encoding = None
        msg.delivery_mode = 2
        msg.exchange = "user.topic"
        msg.routing_key = "mailing.user.updated"

        # aio-pika exposes the raw aiormq.Channel through IncomingMessage.channel;
        # that's the surface that basic_publish is called on directly.
        mock_channel = MagicMock()
        mock_channel.basic_publish = AsyncMock()
        msg.channel = mock_channel
        return msg

    @pytest.mark.asyncio
    async def test_publishes_with_incremented_header(self, message):
        from src.receiver import _republish_with_retry_count

        await _republish_with_retry_count(message, 3)

        message.channel.basic_publish.assert_awaited_once()
        call = message.channel.basic_publish.await_args
        assert call.kwargs["exchange"] == "user.topic"
        assert call.kwargs["routing_key"] == "mailing.user.updated"
        assert call.kwargs["body"] == b"<Placeholder/>"
        props = call.kwargs["properties"]
        assert props.headers["x-retry-count"] == 3
        assert props.content_type == "application/xml"
        assert props.delivery_mode == 2

    @pytest.mark.asyncio
    async def test_acks_original_message(self, message):
        from src.receiver import _republish_with_retry_count

        await _republish_with_retry_count(message, 1)

        message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_preserves_existing_headers(self, message):
        from src.receiver import _republish_with_retry_count

        message.headers = {"custom-header": "preserved-value", "x-retry-count": 1}

        await _republish_with_retry_count(message, 2)

        props = message.channel.basic_publish.await_args.kwargs["properties"]
        assert props.headers["custom-header"] == "preserved-value"
        assert props.headers["x-retry-count"] == 2

    @pytest.mark.asyncio
    async def test_uses_empty_string_exchange_when_source_empty(self, message):
        from src.receiver import _republish_with_retry_count

        message.exchange = ""

        await _republish_with_retry_count(message, 1)

        call = message.channel.basic_publish.await_args
        assert call.kwargs["exchange"] == ""


class TestDeclareAndBind:
    """Direct tests for `_declare_and_bind` topology behaviour."""

    @staticmethod
    def _make_channel():
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(side_effect=lambda *a, **kw: AsyncMock())
        channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        return channel

    @pytest.mark.asyncio
    async def test_no_retry_queue_skips_retry_sibling(self):
        """C9 is in _NO_RETRY_QUEUES — declare work-queue only, no `.retry`."""
        from src.receiver import _declare_and_bind

        channel = self._make_channel()
        await _declare_and_bind(
            channel,
            "controlroom.warning.issued",
            durable=True,
            exchange_name="planning.topic",
        )

        declared = [call.args[0] for call in channel.declare_queue.await_args_list]
        assert declared == ["controlroom.warning.issued"], (
            f"expected only the work-queue, got {declared}"
        )

    @pytest.mark.asyncio
    async def test_normal_queue_declares_retry_sibling(self):
        """Sanity: queues NOT in _NO_RETRY_QUEUES still declare `<queue>.retry`."""
        from src.receiver import _declare_and_bind

        channel = self._make_channel()
        await _declare_and_bind(
            channel,
            "crm.facturatie.user.updated",
            durable=True,
            exchange_name="user.topic",
        )

        declared = [call.args[0] for call in channel.declare_queue.await_args_list]
        assert declared == [
            "crm.facturatie.user.updated",
            "crm.facturatie.user.updated.retry",
        ]
