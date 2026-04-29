from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from lxml import etree

VALID_KASSA_USER_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserCreated>
    <userId>523e4567-e89b-42d3-a456-426614174036</userId>
    <firstName>Karel</firstName>
    <lastName>Kassa</lastName>
    <email>karel.kassa@example.com</email>
    <companyId>c5d4e5f6-a7b8-4901-8d23-ef4567ab8936</companyId>
    <badgeCode>BADGE-036</badgeCode>
    <role>VISITOR</role>
    <createdAt>2026-04-25T09:30:00Z</createdAt>
</KassaUserCreated>"""

VALID_KASSA_USER_UPDATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserUpdated>
    <userId>523e4567-e89b-42d3-a456-426614174036</userId>
    <firstName>Karel</firstName>
    <lastName>Update</lastName>
    <email>karel.update@example.com</email>
    <companyId>c5d4e5f6-a7b8-4901-8d23-ef4567ab8936</companyId>
    <badgeCode>BADGE-036A</badgeCode>
    <role>COMPANY_CONTACT</role>
    <updatedAt>2026-04-25T10:00:00Z</updatedAt>
</KassaUserUpdated>"""

VALID_KASSA_USER_DEACTIVATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>523e4567-e89b-42d3-a456-426614174036</id>
    <email>karel.kassa@example.com</email>
    <deactivatedAt>2026-04-25T16:00:00Z</deactivatedAt>
</UserDeactivated>"""

KASSA_CONTACT_RETURN = {
    "Id": "003000000000036",
    "CRM_ID__c": "523e4567-e89b-42d3-a456-426614174136",
    "Kassa_ID__c": "523e4567-e89b-42d3-a456-426614174036",
    "Email": "karel.kassa@example.com",
    "FirstName": "Karel",
    "LastName": "Kassa",
    "Role__c": "VISITOR",
    "Badge_Code__c": "BADGE-036",
    "Company_ID__c": "c5d4e5f6-a7b8-4901-8d23-ef4567ab8936",
}

KASSA_UPDATED_CONTACT_RETURN = {
    "Id": "003000000000036",
    "CRM_ID__c": "523e4567-e89b-42d3-a456-426614174136",
    "Kassa_ID__c": "523e4567-e89b-42d3-a456-426614174036",
    "Email": "karel.update@example.com",
    "FirstName": "Karel",
    "LastName": "Update",
    "Role__c": "COMPANY_CONTACT",
    "Badge_Code__c": "BADGE-036A",
    "Company_ID__c": "c5d4e5f6-a7b8-4901-8d23-ef4567ab8936",
}


@pytest.fixture
def sf_mock():
    return AsyncMock()


@pytest.mark.asyncio
async def test_kassa_user_created_publishes_confirmed(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_CREATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_created.has_contact_kassa_id_field", return_value=True),
        patch("src.handlers.kassa_user_created.get_contact_match_by_kassa_id", return_value=("none", None)),
        patch("src.handlers.kassa_user_created.get_contact_match_by_email", return_value=("none", None)),
        patch("src.handlers.kassa_user_created.create_contact", return_value=KASSA_CONTACT_RETURN) as mock_create,
        patch("src.sender.publish_user_confirmed") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_created

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_CREATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_created(msg, sf_mock)

        create_payload = mock_create.call_args.args[1]
        assert create_payload["Kassa_ID__c"] == "523e4567-e89b-42d3-a456-426614174036"
        assert create_payload["Badge_Code__c"] == "BADGE-036"
        mock_publish.assert_called_once()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_publishes_updated(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.has_contact_kassa_id_field", return_value=True),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_kassa_id", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_email", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.handlers.kassa_user_updated.update_kassa_contact", return_value=KASSA_UPDATED_CONTACT_RETURN) as mock_update,
        patch("src.sender.publish_user_updated") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        update_kwargs = mock_update.call_args.kwargs
        assert update_kwargs["badge_code"] == "BADGE-036A"
        assert update_kwargs["role"] == "COMPANY_CONTACT"
        mock_publish.assert_called_once()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_conflicts_when_registration_owner_email_changes(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    existing_contact = dict(KASSA_CONTACT_RETURN)
    existing_contact["Registration_ID__c"] = "reg-123"
    existing_contact["Email"] = "karel.old@example.com"

    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.has_contact_kassa_id_field", return_value=True),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_kassa_id", return_value=("unique", existing_contact)),
        patch("src.sender.publish_user_conflict") as mock_conflict,
        patch("src.handlers.kassa_user_updated.get_contact_match_by_email") as mock_email_match,
        patch("src.handlers.kassa_user_updated.update_kassa_contact") as mock_update,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_conflict.assert_called_once()
        mock_email_match.assert_not_called()
        mock_update.assert_not_called()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_acks_when_kassa_id_is_ambiguous(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.has_contact_kassa_id_field", return_value=True),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_kassa_id", return_value=("ambiguous", None)),
        patch("src.sender.publish_user_conflict") as mock_conflict,
        patch("src.handlers.kassa_user_updated.get_contact_match_by_email") as mock_email_match,
        patch("src.handlers.kassa_user_updated.update_kassa_contact") as mock_update,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_conflict.assert_not_called()
        mock_email_match.assert_not_called()
        mock_update.assert_not_called()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_deactivated_publishes_deactivated(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_DEACTIVATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_deactivated.has_contact_kassa_id_field", return_value=True),
        patch("src.handlers.kassa_user_deactivated.get_contact_match_by_kassa_id", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.handlers.kassa_user_deactivated.deactivate_contact_record", return_value=KASSA_CONTACT_RETURN),
        patch("src.sender.publish_user_deactivated") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_deactivated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_DEACTIVATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_deactivated(msg, sf_mock)

        mock_publish.assert_called_once()
        msg.ack.assert_called_once()
