from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from lxml import etree

VALID_KASSA_USER_CREATED_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserCreated>
    <userId>1036</userId>
    <firstName>Karel</firstName>
    <lastName>Kassa</lastName>
    <email>karel.kassa@example.com</email>
    <companyId>c5d4e5f6-a7b8-4901-8d23-ef4567ab8936</companyId>
    <badgeCode>BADGE-036</badgeCode>
    <role>VISITOR</role>
    <createdAt>2026-04-25T09:30:00Z</createdAt>
</KassaUserCreated>"""

# C37/C38 since 2026-04-30: <userId>/<id> carry the CRM master UUID
# (Kassa learned it via crm.user.confirmed). Lookup is by CRM_ID__c.
_CRM_ID = "523e4567-e89b-42d3-a456-426614174136"

VALID_KASSA_USER_UPDATED_XML = f"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserUpdated>
    <userId>{_CRM_ID}</userId>
    <firstName>Karel</firstName>
    <lastName>Update</lastName>
    <email>karel.update@example.com</email>
    <companyId>c5d4e5f6-a7b8-4901-8d23-ef4567ab8936</companyId>
    <badgeCode>BADGE-036A</badgeCode>
    <role>COMPANY_CONTACT</role>
    <updatedAt>2026-04-25T10:00:00Z</updatedAt>
</KassaUserUpdated>""".encode()

VALID_KASSA_USER_DEACTIVATED_XML = f"""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>{_CRM_ID}</id>
    <email>karel.kassa@example.com</email>
    <deactivatedAt>2026-04-25T16:00:00Z</deactivatedAt>
</UserDeactivated>""".encode()

KASSA_CONTACT_RETURN = {
    "Id": "003000000000036",
    "CRM_ID__c": _CRM_ID,
    "Kassa_ID__c": "1036",
    "Email": "karel.kassa@example.com",
    "FirstName": "Karel",
    "LastName": "Kassa",
    "Role__c": "VISITOR",
    "Badge_Code__c": "BADGE-036",
    "Company_ID__c": "c5d4e5f6-a7b8-4901-8d23-ef4567ab8936",
}

KASSA_UPDATED_CONTACT_RETURN = {
    "Id": "003000000000036",
    "CRM_ID__c": _CRM_ID,
    "Kassa_ID__c": "1036",
    "Email": "karel.update@example.com",
    "FirstName": "Karel",
    "LastName": "Update",
    "Role__c": "COMPANY_CONTACT",
    "Badge_Code__c": "BADGE-036A",
    "Company_ID__c": "c5d4e5f6-a7b8-4901-8d23-ef4567ab8936",
}

# C36 create_contact return — handler immediately compares Kassa_ID__c
# against the incoming integer userId to decide whether to call
# ensure_contact_identifiers, so the mock must echo the same value.
KASSA_CREATED_CONTACT_RETURN = dict(KASSA_CONTACT_RETURN)


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
        patch("src.handlers.kassa_user_created.create_contact", return_value=KASSA_CREATED_CONTACT_RETURN) as mock_create,
        patch("src.sender.publish_user_confirmed") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_created

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_CREATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_created(msg, sf_mock)

        create_payload = mock_create.call_args.args[1]
        assert create_payload["Kassa_ID__c"] == "1036"
        assert create_payload["Badge_Code__c"] == "BADGE-036"
        mock_publish.assert_called_once()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_publishes_canonical_update_when_payload_drifts(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.sender.publish_user_updated") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_publish.assert_called_once()
        user_data = mock_publish.call_args.args[0]
        assert user_data["email"] == "karel.kassa@example.com"
        assert user_data["lastName"] == "Kassa"
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_publishes_canonical_update_when_registration_owner_email_changes(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    existing_contact = dict(KASSA_CONTACT_RETURN)
    existing_contact["Registration_ID__c"] = "reg-123"
    existing_contact["Email"] = "karel.old@example.com"

    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("unique", existing_contact)),
        patch("src.sender.publish_user_updated") as mock_publish,
        patch("src.sender.publish_user_conflict") as mock_conflict,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_publish.assert_called_once()
        assert mock_publish.call_args.args[0]["email"] == "karel.old@example.com"
        mock_conflict.assert_not_called()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_acks_when_crm_id_is_ambiguous(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
        patch("src.sender.publish_user_conflict") as mock_conflict,
        patch("src.sender.publish_user_updated") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_conflict.assert_not_called()
        mock_publish.assert_not_called()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_raises_missing_dependency_when_crm_id_unknown(sf_mock):
    from src.handlers._exceptions import MissingDependencyError

    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("none", None)),
        patch("src.sender.publish_user_updated") as mock_publish_updated,
        patch("src.sender.publish_user_conflict") as mock_publish_conflict,
    ):
        from src.handlers.kassa_user_updated import handle as handle_direct

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(MissingDependencyError) as exc_info:
            await handle_direct(msg, sf_mock)

        assert exc_info.value.identifier_label == "CRM_ID__c"
        assert exc_info.value.identifier_value == _CRM_ID

        mock_publish_updated.assert_not_called()
        mock_publish_conflict.assert_not_called()
        msg.ack.assert_not_called()


@pytest.mark.asyncio
async def test_kassa_user_updated_ignores_email_ownership_and_resyncs_by_crm_id(sf_mock):
    """Kassa cannot move ownership via email; CRM_ID__c remains the authority."""
    parsed_xml = etree.fromstring(VALID_KASSA_USER_UPDATED_XML)

    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.sender.publish_user_conflict") as mock_publish_conflict,
        patch("src.sender.publish_user_updated") as mock_publish_updated,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_publish_conflict.assert_not_called()
        mock_publish_updated.assert_called_once()
        assert mock_publish_updated.call_args.args[0]["email"] == "karel.kassa@example.com"
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_updated_acks_without_publish_when_already_in_sync(sf_mock):
    in_sync_xml = f"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserUpdated>
    <userId>{_CRM_ID}</userId>
    <firstName>Karel</firstName>
    <lastName>Kassa</lastName>
    <email>karel.kassa@example.com</email>
    <companyId>c5d4e5f6-a7b8-4901-8d23-ef4567ab8936</companyId>
    <badgeCode>BADGE-036</badgeCode>
    <role>VISITOR</role>
    <updatedAt>2026-04-25T10:00:00Z</updatedAt>
</KassaUserUpdated>""".encode()
    parsed_xml = etree.fromstring(in_sync_xml)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_updated.get_contact_match_by_crm_id", return_value=("unique", KASSA_CONTACT_RETURN)),
        patch("src.sender.publish_user_conflict") as mock_publish_conflict,
        patch("src.sender.publish_user_updated") as mock_publish_updated,
    ):
        from src.receiver import handle_kassa_user_updated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_UPDATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_updated(msg, sf_mock)

        mock_publish_conflict.assert_not_called()
        mock_publish_updated.assert_not_called()
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_kassa_user_deactivated_publishes_deactivated(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_DEACTIVATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_deactivated.get_contact_match_by_crm_id", return_value=("unique", KASSA_CONTACT_RETURN)),
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


@pytest.mark.asyncio
async def test_kassa_user_deactivated_raises_missing_dependency_when_crm_id_unknown(sf_mock):
    from src.handlers._exceptions import MissingDependencyError

    parsed_xml = etree.fromstring(VALID_KASSA_USER_DEACTIVATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_deactivated.get_contact_match_by_crm_id", return_value=("none", None)),
        patch("src.handlers.kassa_user_deactivated.deactivate_contact_record") as mock_deactivate,
        patch("src.sender.publish_user_deactivated") as mock_publish,
    ):
        from src.handlers.kassa_user_deactivated import handle as handle_direct

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_DEACTIVATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(MissingDependencyError) as exc_info:
            await handle_direct(msg, sf_mock)

        assert exc_info.value.identifier_label == "CRM_ID__c"
        assert exc_info.value.identifier_value == _CRM_ID

        mock_deactivate.assert_not_called()
        mock_publish.assert_not_called()
        msg.ack.assert_not_called()


@pytest.mark.asyncio
async def test_kassa_user_deactivated_acks_when_crm_id_is_ambiguous(sf_mock):
    parsed_xml = etree.fromstring(VALID_KASSA_USER_DEACTIVATED_XML)
    with (
        patch("src.xml_validator.validate_kassa", return_value=parsed_xml),
        patch("src.handlers.kassa_user_deactivated.get_contact_match_by_crm_id", return_value=("ambiguous", None)),
        patch("src.handlers.kassa_user_deactivated.deactivate_contact_record") as mock_deactivate,
        patch("src.sender.publish_user_deactivated") as mock_publish,
    ):
        from src.receiver import handle_kassa_user_deactivated

        msg = AsyncMock()
        msg.body = VALID_KASSA_USER_DEACTIVATED_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle_kassa_user_deactivated(msg, sf_mock)

        mock_deactivate.assert_not_called()
        mock_publish.assert_not_called()
        msg.ack.assert_called_once()
