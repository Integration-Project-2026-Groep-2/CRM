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
    <sessionId>SESS-001</sessionId>
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
        patch("src.receiver.has_session_registration_object", return_value=True),
        patch("src.receiver.get_contact_by_email", return_value=existing_contact),
        patch("src.receiver.get_session_registration_by_registration_id", return_value=None),
        patch("src.receiver.create_contact", return_value=created_contact),
        patch("src.receiver.upsert_session_registration"),
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
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            mock_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_registration_publishes_mail_requested_with_correct_args(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail as mock_mail_publish:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)

            mock_mail_publish.assert_called_once_with(
                "registration_confirmation",
                {"email": "john.doe@example.com", "name": "John Doe"},
                {"guest_name": "John Doe"},
            )

    @pytest.mark.asyncio
    async def test_registration_user_data_id(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["id"] == "123e4567-e89b-12d3-a456-426614174000"

    @pytest.mark.asyncio
    async def test_registration_user_data_email(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["email"] == "john.doe@example.com"

    @pytest.mark.asyncio
    async def test_registration_user_data_names(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["firstName"] == "John"
            assert user_data["lastName"] == "Doe"

    @pytest.mark.asyncio
    async def test_registration_user_data_role(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["role"] == "VISITOR"

    @pytest.mark.asyncio
    async def test_registration_user_data_gdpr_consent(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["gdprConsent"] is True

    @pytest.mark.asyncio
    async def test_registration_user_data_confirmed_at(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert "confirmedAt" in user_data

    @pytest.mark.asyncio
    async def test_registration_user_data_is_active(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish as mock_publish, p_mail:
            from src.receiver import handle_registration

            await handle_registration(_make_message(VALID_REG_XML), sf_mock)
            user_data = mock_publish.call_args[0][0]
            assert user_data["isActive"] is True

    @pytest.mark.asyncio
    async def test_registration_acks_message(self, sf_mock):
        p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail = _registration_patches()
        with p_val, p_obj, p_get, p_get_reg, p_create, p_upsert, p_publish, p_mail:
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.receiver.upsert_session_registration"),
            patch("src.receiver.create_contact", return_value=CONTACT_RETURN) as mock_create,
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
    async def test_duplicate_email_logged_and_ignored(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch(
                "src.receiver.get_contact_by_email",
                return_value={
                    "Id": "003xxx",
                    "Registration_ID__c": "OTHER",
                    "FirstName": "Other",
                    "LastName": "Person",
                    "Role__c": "VISITOR",
                },
            ),
            patch("src.receiver.get_session_registration_by_registration_id", return_value=None),
            patch("src.receiver.create_contact") as mock_create,
            patch("src.receiver.upsert_session_registration") as mock_upsert,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            mock_upsert.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "incompatible person fields" in caplog.text

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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.get_session_registration_by_registration_id", return_value=None),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.receiver.upsert_session_registration") as mock_upsert,
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
            mock_upsert.assert_called_once_with(
                sf_mock,
                registration_id="REG-12345",
                session_id="SESS-001",
                contact_id="003000000000001",
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch(
                "src.receiver.get_session_registration_by_registration_id",
                return_value={"Id": "a01", "Registration_ID__c": "REG-12345", "Is_Active__c": False},
            ),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.receiver.upsert_session_registration") as mock_upsert,
            patch("src.receiver.create_contact") as mock_create,
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
            mock_upsert.assert_called_once_with(
                sf_mock,
                registration_id="REG-12345",
                session_id="SESS-001",
                contact_id="003000000000001",
            )
            mock_publish.assert_called_once()
            mock_mail.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_salesforce_create_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.receiver.upsert_session_registration"),
            patch("src.receiver.create_contact", side_effect=Exception("SF Create Down")),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_publish.assert_not_called()
            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
            patch("src.receiver.get_session_registration_by_registration_id", return_value={"Id": "a01"}),
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.receiver.upsert_session_registration"),
            patch("src.receiver.create_contact", return_value=CONTACT_RETURN),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.receiver.upsert_session_registration"),
            patch("src.receiver.create_contact", return_value=contact_no_phone),
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
    async def test_retry_path_publish_failure_requeues(self, sf_mock):
        """If publish fails during retry (same registrationId), message must be requeued."""
        parsed_xml = etree.fromstring(VALID_REG_XML)

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
            patch("src.receiver.get_session_registration_by_registration_id", return_value={"Id": "a01"}),
            patch("src.receiver.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed", side_effect=Exception("Publish failed")),
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.create_contact", return_value=FACTURATIE_CONTACT_RETURN) as mock_create,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.create_contact", return_value=PLANNING_CONTACT_RETURN) as mock_create,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email") as mock_email_lookup,
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch("src.receiver.backfill_planning_contact_fields", return_value=existing_contact) as mock_backfill,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.receiver.backfill_planning_contact_fields", return_value=normalized_contact) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.backfill_planning_contact_fields") as mock_backfill,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.has_contact_planning_id_field", return_value=False),
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
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
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
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact),
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
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.apply_is_active", side_effect=lambda _sf, data, flag: {**data, "IsActive__c": flag}),
            patch("src.receiver.create_contact", return_value=inactive_contact) as mock_create,
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
    async def test_facturatie_user_created_publish_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.create_contact", return_value=FACTURATIE_CONTACT_RETURN),
            patch("src.sender.publish_user_confirmed", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_facturatie_user_created

            msg = _make_message(VALID_FACTURATIE_USER_CREATED_XML)
            await handle_facturatie_user_created(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=FACTURATIE_CONTACT_RETURN),
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_facturatie_contact", return_value=FACTURATIE_UPDATED_CONTACT_RETURN) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="els.updated@example.com",
                first_name="Els",
                last_name="Updated",
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
            assert published_user["email"] == "els.updated@example.com"
            assert published_user["firstName"] == "Els"
            assert published_user["lastName"] == "Updated"
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
            "Email": "els.updated@example.com",
            "FirstName": "Els",
            "LastName": "Updated",
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_facturatie_contact", return_value=minimal_updated_contact) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_MINIMAL_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="els.updated@example.com",
                first_name="Els",
                last_name="Updated",
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_facturatie_contact", return_value=guarded_contact) as mock_update,
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
    async def test_unknown_crm_id_is_requeued_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "FacturatieUserUpdated deferred" in caplog.text
            assert "CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.receiver.update_facturatie_contact") as mock_update,
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.update_facturatie_contact") as mock_update,
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.receiver.update_facturatie_contact") as mock_update,
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
    async def test_publish_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_UPDATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_facturatie_contact", return_value=FACTURATIE_UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_facturatie_user_updated

            msg = _make_message(VALID_FACTURATIE_USER_UPDATED_XML)
            await handle_facturatie_user_updated(msg, sf_mock)

            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
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
    async def test_unknown_crm_id_is_requeued_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "FacturatieUserDeactivated deferred" in caplog.text
            assert "CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact),
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
    async def test_salesforce_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_USER_DEACTIVATED_XML)
        existing_contact = {**FACTURATIE_CONTACT_RETURN}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_user_deactivated

            msg = _make_message(VALID_FACTURATIE_USER_DEACTIVATED_XML)
            await handle_facturatie_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_awaited_once()
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
    "IsActive__c": True,
}


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
            patch("src.receiver._resolve_account_email_field", new_callable=AsyncMock, return_value="Email__c"),
            patch("src.receiver._resolve_account_country_field", new_callable=AsyncMock, return_value="BillingCountryCode"),
        ):
            yield

    @pytest.mark.asyncio
    async def test_upserts_by_vat_and_publishes_confirmed(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_CREATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_account_by_vat", return_value=FACTURATIE_ACCOUNT_RETURN) as mock_upsert,
            patch("src.receiver.get_account_match_by_email") as mock_email_match,
            patch("src.receiver.create_account") as mock_create,
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
    async def test_no_vat_falls_back_to_email_match_create(self, sf_mock):
        no_vat_xml = VALID_FACTURATIE_COMPANY_CREATED_XML.replace(
            b"<vatNumber>BE0123456789</vatNumber>\n    ",
            b"",
        )
        parsed_xml = etree.fromstring(no_vat_xml)
        created_account = {**FACTURATIE_ACCOUNT_RETURN, "VAT_Number__c": None}
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_account_by_vat") as mock_upsert,
            patch("src.receiver.get_account_match_by_email", return_value=("none", None)),
            patch("src.receiver.create_account", return_value=created_account) as mock_create,
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
            patch("src.receiver.get_account_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.create_account") as mock_create,
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
                "src.receiver.get_account_match_by_email",
                return_value=("unique", existing_vat_linked),
            ),
            patch("src.receiver.create_account") as mock_create,
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
                "src.receiver.get_account_match_by_email",
                return_value=("unique", existing_vat_less),
            ),
            patch("src.receiver.create_account") as mock_create,
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
            patch("src.receiver.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.receiver.update_facturatie_account", return_value=updated) as mock_update,
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
            patch("src.receiver.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.receiver.deactivate_account_record", return_value=deactivated) as mock_deact,
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
    async def test_unknown_crm_id_is_requeued(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_account_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_company_updated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_updated

            msg = _make_message(VALID_FACTURATIE_COMPANY_UPDATED_XML)
            await handle_facturatie_company_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)

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
            patch("src.receiver.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.receiver.deactivate_account_record", return_value=deactivated) as mock_deact,
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
    async def test_unknown_crm_id_is_requeued(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_account_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_company_deactivated") as mock_publish,
        ):
            from src.receiver import handle_facturatie_company_deactivated

            msg = _make_message(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
            await handle_facturatie_company_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_FACTURATIE_COMPANY_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_account_match_by_crm_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.get_account_match_by_crm_id", return_value=("unique", existing)),
            patch("src.receiver.deactivate_account_record", return_value=deactivated),
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.create_contact", return_value=MAILING_CONTACT_RETURN) as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch(
                "src.receiver.create_contact",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
                return_value=normalized_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
                return_value=existing_contact,
            ) as mock_backfill,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("unique", existing_contact)),
            patch("src.receiver.create_contact") as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", email_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("unique", mailing_contact)),
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.backfill_mailing_contact_fields") as mock_backfill,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.backfill_mailing_contact_fields") as mock_backfill,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch(
                "src.receiver.backfill_mailing_contact_fields",
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.backfill_mailing_contact_fields") as mock_backfill,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=False),
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.apply_is_active", side_effect=lambda _sf, data, flag: {**data, "IsActive__c": flag}),
            patch("src.receiver.create_contact", return_value=inactive_contact) as mock_create,
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.receiver.create_contact") as mock_create,
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
    async def test_conflict_publish_failure_requeues(self, sf_mock):
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
            patch("src.receiver.has_contact_mailing_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_mailing_id", return_value=("none", None)),
            patch("src.sender.publish_user_conflict", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_created

            msg = _make_message(conflicting_xml)
            await handle_mailing_user_created(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_mailing_contact", return_value=MAILING_UPDATED_CONTACT_RETURN) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="mia.updated@example.com",
                first_name="Mila",
                last_name="Updated",
                company_id="f4e5d6c7-b8a9-4012-8f34-ab5678cd9012",
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == MAILING_UPDATED_CONTACT_RETURN["CRM_ID__c"]
            assert published_user["email"] == "mia.updated@example.com"
            assert published_user["firstName"] == "Mila"
            assert published_user["lastName"] == "Updated"
            assert published_user["role"] == "COMPANY_CONTACT"
            assert published_user["companyId"] == "f4e5d6c7-b8a9-4012-8f34-ab5678cd9012"
            assert "updatedAt" in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_optional_fields_clear_company_and_fallback_last_name(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch(
                "src.receiver.update_mailing_contact",
                return_value=MAILING_UPDATED_MINIMAL_CONTACT_RETURN,
            ) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="mia.updated@example.com",
                first_name=None,
                last_name="mia.updated@example.com",
                company_id=None,
            )
            published_user = mock_publish.call_args.args[0]
            assert published_user["lastName"] == "mia.updated@example.com"
            assert published_user["role"] == "VISITOR"
            assert "companyId" not in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_specialized_role_preserves_existing_company_link_in_published_update(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
            "Role__c": "ADMIN",
            "Company_ID__c": "old-company-id",
        }
        updated_contact = {
            **existing_contact,
            "FirstName": "Mia",
            "LastName": "mia.updated@example.com",
            "Email": "mia.updated@example.com",
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_mailing_contact", return_value=updated_contact) as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_MINIMAL_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_update.assert_called_once_with(
                sf_mock,
                existing_contact,
                email="mia.updated@example.com",
                first_name=None,
                last_name="mia.updated@example.com",
                company_id=None,
            )
            published_user = mock_publish.call_args.args[0]
            assert published_user["role"] == "ADMIN"
            assert published_user["companyId"] == "old-company-id"
            assert published_user["lastName"] == "mia.updated@example.com"
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_crm_id_is_requeued_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "MailingUserUpdated deferred" in caplog.text
            assert "CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.receiver.update_mailing_contact") as mock_update,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_update.assert_not_called()
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.update_mailing_contact") as mock_update,
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
            patch("src.receiver.update_mailing_contact") as mock_update,
            patch("src.sender.publish_user_deactivated") as mock_deactivated_publish,
            patch("src.sender.publish_user_updated") as mock_updated_publish,
            caplog.at_level(logging.INFO),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(inactive_xml)
            await handle_mailing_user_updated(msg, sf_mock)

            mock_deactivate.assert_called_once()
            mock_update.assert_not_called()
            mock_deactivated_publish.assert_called_once()
            mock_updated_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "isActive=false on update" in caplog.text

    @pytest.mark.asyncio
    async def test_publish_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_UPDATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.update_mailing_contact", return_value=MAILING_UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_updated

            msg = _make_message(VALID_MAILING_USER_UPDATED_XML)
            await handle_mailing_user_updated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.receiver.update_planning_contact", return_value=PLANNING_UPDATED_CONTACT_RETURN) as mock_update,
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
                email="sofie.updated@example.com",
                first_name="Sofie",
                last_name="Updated",
                role="SPEAKER",
                phone_number="+32470999999",
            )
            mock_conflict.assert_not_called()
            mock_publish.assert_called_once()
            published_user = mock_publish.call_args.args[0]
            assert published_user["id"] == PLANNING_UPDATED_CONTACT_RETURN["CRM_ID__c"]
            assert published_user["email"] == "sofie.updated@example.com"
            assert published_user["firstName"] == "Sofie"
            assert published_user["lastName"] == "Updated"
            assert published_user["role"] == "SPEAKER"
            assert published_user["phone"] == "+32470999999"
            assert "updatedAt" in published_user
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_missing_planning_id_field_rejects_without_requeue(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Planning_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_unknown_planning_id_is_requeued_without_email_lookup(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.receiver.get_contact_match_by_email") as mock_email_lookup,
            patch("src.sender.publish_user_updated") as mock_publish,
            patch("src.sender.publish_user_conflict") as mock_conflict,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            mock_email_lookup.assert_not_called()
            mock_publish.assert_not_called()
            mock_conflict.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "PlanningUserUpdated deferred" in caplog.text
            assert "Planning_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_planning_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("unique", conflicting_contact)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.update_planning_contact") as mock_update,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("ambiguous", None)),
            patch("src.receiver.ensure_contact_identifiers") as mock_ensure,
            patch("src.receiver.update_planning_contact") as mock_update,
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
    async def test_publish_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_UPDATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.get_contact_match_by_email", return_value=("none", None)),
            patch("src.receiver.ensure_contact_identifiers", return_value=existing_contact),
            patch("src.receiver.update_planning_contact", return_value=PLANNING_UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_planning_user_updated

            msg = _make_message(VALID_PLANNING_USER_UPDATED_XML)
            await handle_planning_user_updated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
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
            patch("src.receiver.has_contact_planning_id_field", return_value=False),
            caplog.at_level(logging.ERROR),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            assert "Planning_ID__c is missing" in caplog.text

    @pytest.mark.asyncio
    async def test_unknown_planning_id_is_requeued_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "PlanningUserDeactivated deferred" in caplog.text
            assert "Planning_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_planning_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact),
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", legacy_contact)),
            patch("src.receiver.ensure_contact_identifiers", return_value=normalized_contact) as mock_ensure,
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
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
    async def test_salesforce_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PLANNING_USER_DEACTIVATED_XML)
        existing_contact = {
            **PLANNING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_requeues(self, sf_mock):
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
            patch("src.receiver.has_contact_planning_id_field", return_value=True),
            patch("src.receiver.get_contact_match_by_planning_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_planning_user_deactivated

            msg = _make_message(VALID_PLANNING_USER_DEACTIVATED_XML)
            await handle_planning_user_deactivated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact) as mock_deactivate,
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
    async def test_unknown_crm_id_is_requeued_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("none", None)),
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock),
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            mock_republish.assert_awaited_once_with(msg, 1)
            msg.reject.assert_not_called()
            assert "MailingUserDeactivated deferred" in caplog.text
            assert "CRM_ID__c" in caplog.text

    @pytest.mark.asyncio
    async def test_ambiguous_crm_id_is_acked_without_publish(self, sf_mock, caplog):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact),
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
    async def test_salesforce_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_MAILING_USER_DEACTIVATED_XML)
        existing_contact = {
            **MAILING_CONTACT_RETURN,
        }
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            mock_publish.assert_not_called()
            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_requeues(self, sf_mock):
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
            patch("src.receiver.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("publish failed")),
        ):
            from src.receiver import handle_mailing_user_deactivated

            msg = _make_message(VALID_MAILING_USER_DEACTIVATED_XML)
            await handle_mailing_user_deactivated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()


# ==========================================================================
# Contract 2 + 18 + 22: frontend.registration.updated
# ==========================================================================

VALID_UPDATE_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <registrationId>REG-12345</registrationId>
    <email>john.doe@example.com</email>
    <sessionId>SESS-001</sessionId>
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
    <sessionId>SESS-002</sessionId>
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


class TestHandleRegistrationUpdated:
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.receiver.ensure_session_registration_active"),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.receiver.ensure_session_registration_active"),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=contact_with_optional_fields),
            patch("src.receiver.ensure_session_registration_active"),
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
            assert user_data["country"] == "Belgium"

    @pytest.mark.asyncio
    async def test_updated_uses_active_field_fallbacks_for_is_active(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        contact_with_fallback_active_field = {
            **UPDATED_CONTACT_RETURN,
            "Active__c": False,
        }

        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=contact_with_fallback_active_field),
            patch("src.receiver.ensure_session_registration_active"),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.receiver.ensure_session_registration_active"),
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
    <sessionId>SESS-001</sessionId>
    <changeType>updated</changeType>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_no_fields)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.receiver.ensure_session_registration_active") as mock_ensure_session,
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(xml_no_fields)
            await handle_registration_updated(msg, sf_mock)

            mock_upsert.assert_called_once()
            update_data = mock_upsert.call_args[0][2]
            assert update_data == {}
            mock_ensure_session.assert_called_once()
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_updated_maps_role_to_salesforce_field(self, sf_mock):
        xml_with_role = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>john.doe@example.com</email>
    <sessionId>SESS-001</sessionId>
    <changeType>updated</changeType>
    <updatedFields>
        <role>COMPANY_CONTACT</role>
    </updatedFields>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_with_role)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
            patch("src.receiver.ensure_session_registration_active"),
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(xml_with_role), sf_mock)
            update_data = mock_upsert.call_args[0][2]
            assert update_data["Role__c"] == "COMPANY_CONTACT"

    @pytest.mark.asyncio
    async def test_updated_missing_session_registration_object_rejects_without_requeue(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=False),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

    # ------------------------------------------------------------------
    # Cancelled path
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_cancelled_deactivates_session_registration(self, sf_mock):
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}) as mock_deact_reg,
            patch("src.receiver.count_active_session_registrations", return_value=1),
            patch("src.receiver.deactivate_contact_record") as mock_deact_contact,
            patch("src.sender.publish_user_deactivated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_CANCEL_XML), sf_mock)
            mock_deact_reg.assert_called_once_with(
                sf_mock,
                registration_id=None,
                contact_id="003000000000088",
                session_id="SESS-002",
            )
            mock_deact_contact.assert_not_called()
            mock_publish.assert_not_called()

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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.receiver.count_active_contacts_for_company", return_value=0),
            patch("src.receiver.get_account_by_crm_id", return_value=account_before_deactivation),
            patch("src.receiver.deactivate_account_by_crm_id", return_value=DEACTIVATED_ACCOUNT_RETURN) as mock_deactivate_account,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.receiver.count_active_contacts_for_company") as mock_sibling_count,
            patch("src.receiver.deactivate_account_by_crm_id") as mock_deactivate_account,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.receiver.count_active_contacts_for_company", return_value=2) as mock_sibling_count,
            patch("src.receiver.get_account_by_crm_id") as mock_get_account,
            patch("src.receiver.deactivate_account_by_crm_id") as mock_deactivate_account,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.receiver.count_active_contacts_for_company", return_value=0),
            patch("src.receiver.get_account_by_crm_id", return_value=account_without_vat),
            patch("src.receiver.deactivate_account_by_crm_id") as mock_deactivate_account,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=deactivated_contact_with_company),
            patch("src.receiver.count_active_contacts_for_company", side_effect=Exception("SF API down")),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record") as mock_deact_contact,
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "acking without action" in caplog.text

    @pytest.mark.asyncio
    async def test_cancelled_without_session_row_uses_legacy_contact_fallback(self, sf_mock, caplog):
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value=None),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN) as mock_deact_contact,
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_deact_contact.assert_called_once()
            mock_publish.assert_called_once()
            msg.ack.assert_called_once()
            assert "using legacy Contact fallback" in caplog.text

    @pytest.mark.asyncio
    async def test_cancelled_without_session_row_keeps_native_identity_contact_active(self, sf_mock, caplog):
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value=None),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record") as mock_deact_contact,
            patch("src.sender.publish_user_deactivated") as mock_publish,
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_deact_contact.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "skipping legacy Contact fallback" in caplog.text

    @pytest.mark.asyncio
    async def test_cancelled_missing_session_registration_object_rejects_without_requeue(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=False),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)

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
    async def test_salesforce_error_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_on_update_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.receiver.ensure_session_registration_active"),
            patch("src.sender.publish_user_updated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_failure_on_cancel_requeues(self, sf_mock):
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_contact_by_email", return_value=existing_contact),
            patch("src.receiver.deactivate_session_registration", return_value={"Id": "a01", "Is_Active__c": False}),
            patch("src.receiver.count_active_session_registrations", return_value=0),
            patch("src.receiver.deactivate_contact_record", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_active_session_participants", return_value=SESSION_PARTICIPANTS),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_active_session_participants", return_value=[SESSION_PARTICIPANTS[0]]),
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
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_active_session_participants", return_value=[]),
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
            patch("src.receiver.has_session_registration_object", return_value=False),
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
    async def test_session_update_publish_error_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_SESSION_UPDATED_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.has_session_registration_object", return_value=True),
            patch("src.receiver.get_active_session_participants", return_value=[SESSION_PARTICIPANTS[0]]),
            patch("src.sender.publish_mail_requested", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_session_updated

            msg = _make_message(VALID_SESSION_UPDATED_XML)
            await handle_session_updated(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.update_payment_status", return_value=PAID_CONTACT_RETURN) as mock_update,
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
            patch("src.receiver.update_payment_status", return_value=PAID_CONTACT_RETURN),
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
            patch("src.receiver.update_payment_status", return_value=None),
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
            patch("src.receiver.update_payment_status", side_effect=Exception("SF Down")),
        ):
            from src.receiver import handle_payment_confirmed

            msg = _make_message(VALID_PAYMENT_XML)
            await handle_payment_confirmed(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
            patch("src.receiver.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN) as mock_get,
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
            patch("src.receiver.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN),
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
            patch("src.receiver.get_unpaid_contacts", return_value=[]),
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
    async def test_unpaid_requested_salesforce_error_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_unpaid_contacts", side_effect=Exception("SF Down")),
            patch("src.sender.publish_unpaid_responded"),
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            await handle_unpaid_requested(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unpaid_requested_publish_error_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UNPAID_REQUEST_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_unpaid_contacts", return_value=UNPAID_CONTACTS_RETURN),
            patch("src.sender.publish_unpaid_responded", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_unpaid_requested

            msg = _make_message(VALID_UNPAID_REQUEST_XML)
            await handle_unpaid_requested(msg, sf_mock)

            # Transient error → republish acks original + publishes new copy
            # with incremented x-retry-count (instead of raw reject(requeue=True)).
            msg.ack.assert_awaited_once()
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
                "src.receiver.get_contact_for_person_lookup",
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
                "src.receiver.get_contact_for_person_lookup",
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
            patch("src.receiver.get_contact_for_person_lookup", return_value=None),
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
            patch("src.receiver.get_contact_for_person_lookup") as mock_lookup,
            patch("src.sender.publish_person_lookup_responded") as mock_publish,
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(INVALID_XML)
            await handle_person_lookup(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=False)
            mock_lookup.assert_not_called()
            mock_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_requeues_on_salesforce_error(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_PERSON_LOOKUP_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.receiver.get_contact_for_person_lookup",
                side_effect=Exception("SF Down"),
            ),
            patch("src.sender.publish_person_lookup_responded"),
        ):
            from src.receiver import handle_person_lookup

            msg = _make_message(VALID_PERSON_LOOKUP_XML)
            await handle_person_lookup(msg, sf_mock)

            # Transient error → _handle_processing_error acks original and
            # republishes with incremented x-retry-count (no raw reject).
            msg.ack.assert_awaited_once()
            msg.reject.assert_not_called()


class TestRunReceiver:
    async def _run_receiver(self):
        queues = {}
        sf_client = MagicMock()

        async def _declare_queue(_channel, queue_name, durable, *, routing_key=None):  # noqa: ARG001
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
        callback = queue.consume.call_args.args[0]
        assert callback is handler

    @staticmethod
    def _assert_partial_callback(queue, handler, sf_client):
        callback = queue.consume.call_args.args[0]
        assert callback.func is handler
        assert callback.keywords["sf"] is sf_client

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_9_queue(self):
        from src.receiver import handle_warning

        queues, mock_declare, _sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "controlroom.warning.issued", durable=False)
        self._assert_direct_callback(queues["controlroom.warning.issued"], handle_warning)

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_1_queue(self):
        from src.receiver import handle_registration

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "frontend.registration.created", durable=True)
        self._assert_partial_callback(
            queues["frontend.registration.created"], handle_registration, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_2_queue(self):
        from src.receiver import handle_registration_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "frontend.registration.updated", durable=True)
        self._assert_partial_callback(
            queues["frontend.registration.updated"], handle_registration_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_24_queue(self):
        from src.receiver import handle_facturatie_user_created

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "facturatie.user.created", durable=True)
        self._assert_partial_callback(
            queues["facturatie.user.created"], handle_facturatie_user_created, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_25_queue(self):
        from src.receiver import handle_facturatie_user_updated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "facturatie.user.updated", durable=True)
        self._assert_partial_callback(
            queues["facturatie.user.updated"], handle_facturatie_user_updated, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_26_queue(self):
        from src.receiver import handle_facturatie_user_deactivated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "facturatie.user.deactivated", durable=True)
        self._assert_partial_callback(
            queues["facturatie.user.deactivated"], handle_facturatie_user_deactivated, sf_client
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

        self._assert_declared_queue(mock_declare, "mailing.user.created", durable=True)
        self._assert_partial_callback(
            queues["mailing.user.created"], handle_mailing_user_created, sf_client
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

        self._assert_declared_queue(mock_declare, "planning.user.created", durable=True)
        self._assert_partial_callback(
            queues["planning.user.created"], handle_planning_user_created, sf_client
        )

    @pytest.mark.asyncio
    async def test_run_receiver_registers_contract_32_queue(self):
        from src.receiver import handle_planning_user_deactivated

        queues, mock_declare, sf_client = await self._run_receiver()

        self._assert_declared_queue(mock_declare, "planning.user.deactivated", durable=True)
        self._assert_partial_callback(
            queues["planning.user.deactivated"], handle_planning_user_deactivated, sf_client
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


class TestHandleProcessingError:
    """Centralised error handling used by every handler's generic except-block."""

    @pytest.fixture
    def message(self):
        msg = MagicMock()
        msg.body = b"<Placeholder/>"
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()
        msg.headers = {}
        return msg

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_and_drops_without_requeue(self, message):
        from src.receiver import _handle_processing_error

        exc = Exception("Request refused. Response content: [{'errorCode': 'REQUEST_LIMIT_EXCEEDED'}]")
        exc.content = [{"errorCode": "REQUEST_LIMIT_EXCEEDED", "message": "TotalRequests Limit exceeded."}]

        with patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await _handle_processing_error("MailingUserCreated", message, exc)

        mock_sleep.assert_awaited_once_with(60)
        message.reject.assert_awaited_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_rate_limit_detected_by_string_fallback(self, message):
        from src.receiver import _handle_processing_error

        exc = RuntimeError(
            "Salesforce query failed: REQUEST_LIMIT_EXCEEDED TotalRequests Limit exceeded."
        )

        with patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await _handle_processing_error("FacturatieUserCreated", message, exc)

        mock_sleep.assert_awaited_once_with(60)
        message.reject.assert_awaited_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_transient_error_requeues_with_backoff(self, message, caplog):
        from src.receiver import _handle_processing_error

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            caplog.at_level(logging.ERROR),
        ):
            await _handle_processing_error("MailingUserUpdated", message, RuntimeError("boom"))

        mock_sleep.assert_awaited_once_with(1.0)
        mock_republish.assert_awaited_once_with(message, 1)
        message.reject.assert_not_awaited()
        assert "attempt 1/5" in caplog.text
        assert "sleeping 1.0s" in caplog.text

    @pytest.mark.asyncio
    async def test_transient_error_progression_reads_retry_count(self, message, caplog):
        """After 3 previous retries, next attempt uses 2**3 = 8s backoff."""
        from src.receiver import _handle_processing_error

        message.headers = {"x-retry-count": 3}

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            caplog.at_level(logging.ERROR),
        ):
            await _handle_processing_error("MailingUserUpdated", message, RuntimeError("still broken"))

        mock_sleep.assert_awaited_once_with(8.0)
        mock_republish.assert_awaited_once_with(message, 4)
        assert "attempt 4/5" in caplog.text

    @pytest.mark.asyncio
    async def test_max_retry_drops_without_requeue(self, message, caplog):
        from src.receiver import _handle_processing_error

        message.headers = {"x-retry-count": 5}

        with caplog.at_level(logging.ERROR):
            await _handle_processing_error("MailingUserUpdated", message, RuntimeError("persistent failure"))

        message.reject.assert_awaited_once_with(requeue=False)
        assert "max retries (5) exceeded" in caplog.text


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


class TestHandleOutOfOrderDeferral:
    @pytest.fixture
    def message(self):
        msg = MagicMock()
        msg.body = b"<Placeholder/>"
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()
        msg.headers = {}
        return msg

    @pytest.mark.asyncio
    async def test_first_attempt_sleeps_1s_and_republishes(self, message, caplog):
        from src.receiver import _handle_out_of_order_deferral

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            caplog.at_level(logging.WARNING),
        ):
            await _handle_out_of_order_deferral(
                "MailingUserUpdated",
                message,
                identifier_label="Mailing_ID__c",
                identifier_value="abc-123",
            )

        mock_sleep.assert_awaited_once_with(1.0)
        mock_republish.assert_awaited_once_with(message, 1)
        message.reject.assert_not_awaited()
        assert "attempt 1/10" in caplog.text
        assert "Mailing_ID__c=abc-123" in caplog.text

    @pytest.mark.asyncio
    async def test_sixth_attempt_caps_at_30s(self, message):
        from src.receiver import _handle_out_of_order_deferral

        message.headers = {"x-retry-count": 5}

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
        ):
            await _handle_out_of_order_deferral(
                "PlanningUserUpdated",
                message,
                identifier_label="Planning_ID__c",
                identifier_value="xyz-789",
            )

        mock_sleep.assert_awaited_once_with(30.0)
        mock_republish.assert_awaited_once_with(message, 6)
        message.reject.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_attempt_counter_read_from_x_retry_count_header(self, message):
        """Verify the counter comes from the new header, not x-death."""
        from src.receiver import _handle_out_of_order_deferral

        # x-death present but should be ignored; only x-retry-count matters.
        message.headers = {"x-retry-count": 3, "x-death": [{"count": 99}]}

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
        ):
            await _handle_out_of_order_deferral(
                "MailingUserDeactivated",
                message,
                identifier_label="Mailing_ID__c",
                identifier_value="tick-tock",
            )

        mock_sleep.assert_awaited_once_with(8.0)
        mock_republish.assert_awaited_once_with(message, 4)

    @pytest.mark.asyncio
    async def test_max_attempts_drops_without_requeue(self, message, caplog):
        from src.receiver import _handle_out_of_order_deferral

        message.headers = {"x-retry-count": 10}

        with (
            patch("src.receiver.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("src.receiver._republish_with_retry_count", new_callable=AsyncMock) as mock_republish,
            caplog.at_level(logging.WARNING),
        ):
            await _handle_out_of_order_deferral(
                "MailingUserDeactivated",
                message,
                identifier_label="Mailing_ID__c",
                identifier_value="dead-beef",
            )

        mock_sleep.assert_not_awaited()
        mock_republish.assert_not_awaited()
        message.reject.assert_awaited_once_with(requeue=False)
        assert "deferred 10 times" in caplog.text
        assert "dropping" in caplog.text


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
