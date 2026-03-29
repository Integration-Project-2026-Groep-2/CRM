"""
Unit tests — receiver.py
Contract 9: controlroom.warning.issued
Contract 1 + 13: frontend.registration.created → crm.user.confirmed
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
    "CRM_ID__c": "123e4567-e89b-12d3-a456-426614174000",
    "Email": "john.doe@example.com",
    "FirstName": "John",
    "LastName": "Doe",
    "Role__c": "VISITOR",
    "GDPR_Consent__c": True,
    "Phone": "+32412345678",
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
        patch("src.receiver.get_contact_by_email", return_value=existing_contact),
        patch("src.receiver.create_contact", return_value=created_contact),
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
            patch("src.receiver.get_contact_by_email", return_value=None),
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
            patch("src.receiver.get_contact_by_email", return_value={"Id": "003xxx", "Registration_ID__c": "OTHER"}),
            patch("src.receiver.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
            caplog.at_level(logging.WARNING),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            mock_publish.assert_not_called()
            msg.ack.assert_called_once()
            assert "Conflict: email john.doe@example.com exists with different registrationId" in caplog.text

    @pytest.mark.asyncio
    async def test_salesforce_create_failure_requeues(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_REG_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.get_contact_by_email", return_value=None),
            patch("src.receiver.create_contact", side_effect=Exception("SF Create Down")),
            patch("src.sender.publish_user_confirmed") as mock_publish,
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.reject.assert_called_once_with(requeue=True)

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
            patch("src.receiver.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
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
            patch("src.receiver.get_contact_by_email", return_value=None),
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
            patch("src.receiver.get_contact_by_email", return_value=None),
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
            patch("src.receiver.get_contact_by_email", return_value={
                "CRM_ID__c": "123e4567-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "Email": "john.doe@example.com",
                "Registration_ID__c": "REG-12345",
            }),
            patch("src.receiver.create_contact") as mock_create,
            patch("src.sender.publish_user_confirmed", side_effect=Exception("Publish failed")),
            patch("src.sender.publish_mail_requested"),
        ):
            from src.receiver import handle_registration

            msg = _make_message(VALID_REG_XML)
            await handle_registration(msg, sf_mock)

            mock_create.assert_not_called()
            msg.reject.assert_called_once_with(requeue=True)


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


class TestHandleRegistrationUpdated:
    @pytest.fixture
    def sf_mock(self):
        return AsyncMock()

    # ------------------------------------------------------------------
    # Updated path
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_updated_calls_upsert_contact(self, sf_mock):
        """changeType=updated must call upsert_contact_by_email with mapped fields."""
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
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
        """After upsert, crm.user.updated (C18) must be published."""
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
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
        """Contract 18: publish full profile fields when Salesforce record contains them."""
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
            patch("src.receiver.upsert_contact_by_email", return_value=contact_with_optional_fields),
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
    async def test_updated_acks_message(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_updated_without_updated_fields_calls_upsert_with_empty_data(self, sf_mock):
        """changeType=updated without <updatedFields> should still upsert (no field changes)."""
        xml_no_fields = b"""<?xml version='1.0' encoding='utf-8'?>
<RegistrationChange>
    <email>john.doe@example.com</email>
    <sessionId>SESS-001</sessionId>
    <changeType>updated</changeType>
</RegistrationChange>"""
        parsed_xml = etree.fromstring(xml_no_fields)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
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
    async def test_updated_maps_role_to_salesforce_field(self, sf_mock):
        """role in updatedFields must map to Role__c in Salesforce."""
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
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN) as mock_upsert,
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
    async def test_cancelled_deactivates_contact(self, sf_mock):
        """changeType=cancelled must call deactivate_contact."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.deactivate_contact", return_value=DEACTIVATED_CONTACT_RETURN) as mock_deact,
            patch("src.sender.publish_user_deactivated"),
        ):
            from src.receiver import handle_registration_updated

            await handle_registration_updated(_make_message(VALID_CANCEL_XML), sf_mock)
            mock_deact.assert_called_once_with(sf_mock, "cancel@example.com")

    @pytest.mark.asyncio
    async def test_cancelled_publishes_user_deactivated(self, sf_mock):
        """After deactivation, crm.user.deactivated (C22) must be published."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.deactivate_contact", return_value=DEACTIVATED_CONTACT_RETURN),
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
    async def test_cancelled_acks_message(self, sf_mock):
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.deactivate_contact", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.sender.publish_user_deactivated"),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)
            msg.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancelled_contact_not_found_acks_without_publish(self, sf_mock, caplog):
        """Cancelling a non-existent contact: log warning, ack, no publish."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.deactivate_contact", return_value=None),
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
    async def test_salesforce_error_requeues(self, sf_mock):
        """Salesforce failure must requeue the message for retry."""
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", side_effect=Exception("SF Down")),
            patch("src.sender.publish_user_updated") as mock_publish,
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            mock_publish.assert_not_called()
            msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_publish_failure_on_update_requeues(self, sf_mock):
        """If publish_user_updated fails after successful upsert, message must requeue."""
        parsed_xml = etree.fromstring(VALID_UPDATE_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.upsert_contact_by_email", return_value=UPDATED_CONTACT_RETURN),
            patch("src.sender.publish_user_updated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_UPDATE_XML)
            await handle_registration_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_publish_failure_on_cancel_requeues(self, sf_mock):
        """If publish_user_deactivated fails after deactivation, message must requeue."""
        parsed_xml = etree.fromstring(VALID_CANCEL_XML)
        with (
            patch("src.xml_validator.validate", return_value=parsed_xml),
            patch("src.receiver.deactivate_contact", return_value=DEACTIVATED_CONTACT_RETURN),
            patch("src.sender.publish_user_deactivated", side_effect=Exception("RabbitMQ down")),
        ):
            from src.receiver import handle_registration_updated

            msg = _make_message(VALID_CANCEL_XML)
            await handle_registration_updated(msg, sf_mock)

            msg.reject.assert_called_once_with(requeue=True)

