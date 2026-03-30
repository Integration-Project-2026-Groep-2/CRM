"""
Unit tests — sender.py
Contracten 13, 14, 5b, 10b, 17b, 6
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

from src import sender

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def setup_sender():
    mock_channel = MagicMock()
    mock_channel.default_exchange = MagicMock()
    mock_channel.default_exchange.publish = AsyncMock()
    sender._channel = mock_channel
    yield mock_channel


def _get_published_xml(mock_channel) -> etree._Element:
    call_args = mock_channel.default_exchange.publish.call_args
    message = call_args[0][0]
    return etree.fromstring(message.body)


def _get_routing_key(mock_channel) -> str:
    return mock_channel.default_exchange.publish.call_args[1]["routing_key"]


def _get_delivery_mode(mock_channel):
    message = mock_channel.default_exchange.publish.call_args[0][0]
    return message.delivery_mode


# ---------------------------------------------------------------------------
# Contract 13 — publish_user_confirmed
# ---------------------------------------------------------------------------

class TestPublishUserConfirmed:

    # role must be a valid UserRoleType enum value per the XSD
    BASE_DATA = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "email": "jan@example.com",
        "firstName": "Jan",
        "lastName": "Janssen",
        "role": "VISITOR",  # valid enum value — not ATTENDEE
        "isActive": True,
        "gdprConsent": True,
        "confirmedAt": "2025-01-01T10:00:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_confirmed(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.user.confirmed"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        """durable queue — message must survive broker restart."""
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_confirmed(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_user_confirmed(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "UserConfirmed"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["id", "email", "firstName", "lastName", "role",
                      "isActive", "gdprConsent", "confirmedAt"]:
            assert xml.find(field) is not None, f"Required field '{field}' missing"

    @pytest.mark.asyncio
    async def test_optional_fields_present_when_provided(self, setup_sender):
        data = {**self.BASE_DATA, "phone": "0499123456",
                "companyId": "123e4567-e89b-42d3-a456-556642440001", "badgeCode": "B001"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("phone") == "0499123456"
        assert xml.findtext("companyId") == "123e4567-e89b-42d3-a456-556642440001"
        assert xml.findtext("badgeCode") == "B001"

    @pytest.mark.asyncio
    async def test_optional_fields_absent_when_not_provided(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.find("phone") is None
        assert xml.find("companyId") is None
        assert xml.find("badgeCode") is None

    @pytest.mark.asyncio
    async def test_booleans_serialized_as_lowercase(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("isActive") == "true"
        assert xml.findtext("gdprConsent") == "true"

    @pytest.mark.asyncio
    async def test_xsd_field_order_optional_fields_after_required(self, setup_sender):
        """XSD xs:sequence is strict — phone before role, confirmedAt last."""
        data = {**self.BASE_DATA, "phone": "0499123456", "badgeCode": "B001"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_confirmed(data)
        xml = _get_published_xml(setup_sender)
        tags = [child.tag for child in xml]
        assert tags.index("phone") < tags.index("role")
        assert tags.index("confirmedAt") == len(tags) - 1


# ---------------------------------------------------------------------------
# Contract 14 — publish_company_confirmed
# ---------------------------------------------------------------------------

class TestPublishCompanyConfirmed:

    BASE_DATA = {
        "id": "660e8400-e29b-41d4-a716-446655440001",
        "vatNumber": "BE0123456789",
        "name": "Acme NV",
        "email": "info@acme.be",
        "isActive": True,
        "confirmedAt": "2025-01-01T10:00:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_confirmed(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.company.confirmed"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_confirmed(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_company_confirmed(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "CompanyConfirmed"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["id", "vatNumber", "name", "email", "isActive", "confirmedAt"]:
            assert xml.find(field) is not None, f"Required field '{field}' missing"


# ---------------------------------------------------------------------------
# Contract 5b — publish_company_responded
# ---------------------------------------------------------------------------

class TestPublishCompanyResponded:

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_responded("req-001", {"found": False})
        assert _get_routing_key(setup_sender) == "crm.company.responded"

    @pytest.mark.asyncio
    async def test_message_is_not_persistent(self, setup_sender):
        """durable: false — no need for persistence."""
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_responded("req-001", {"found": False})
        assert _get_delivery_mode(setup_sender) == DeliveryMode.NOT_PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_company_response(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_responded("req-001", {"found": False})
        assert _get_published_xml(setup_sender).tag == "CompanyResponse"

    @pytest.mark.asyncio
    async def test_found_false_sends_no_company_fields(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_responded("req-001", {"found": False})
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("found") == "false"
        assert xml.find("vatNumber") is None
        assert xml.find("name") is None

    @pytest.mark.asyncio
    async def test_found_true_includes_company_fields(self, setup_sender):
        data = {"found": True, "id": "uuid-1", "name": "Acme NV",
                "vatNumber": "BE0123456789", "email": "info@acme.be"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_responded("req-001", data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("found") == "true"
        assert xml.findtext("name") == "Acme NV"

    @pytest.mark.asyncio
    async def test_request_id_in_response(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_responded("req-xyz", {"found": False})
        assert _get_published_xml(setup_sender).findtext("requestId") == "req-xyz"


# ---------------------------------------------------------------------------
# Contract 10b — publish_person_lookup_responded
# ---------------------------------------------------------------------------

class TestPublishPersonLookupResponded:

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_person_lookup_responded(
                "req-001", {"found": False, "linkedToCompany": False})
        assert _get_routing_key(setup_sender) == "crm.person.lookup.responded"

    @pytest.mark.asyncio
    async def test_message_is_not_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_person_lookup_responded(
                "req-001", {"found": False, "linkedToCompany": False})
        assert _get_delivery_mode(setup_sender) == DeliveryMode.NOT_PERSISTENT

    @pytest.mark.asyncio
    async def test_not_found_excludes_optional_fields(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_person_lookup_responded(
                "req-001", {"found": False, "linkedToCompany": False})
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("found") == "false"
        assert xml.findtext("linkedToCompany") == "false"
        assert xml.find("id") is None
        assert xml.find("companyName") is None

    @pytest.mark.asyncio
    async def test_found_with_company_includes_company_id(self, setup_sender):
        data = {"found": True, "linkedToCompany": True,
                "id": "uuid-p1", "companyName": "Acme NV", "companyId": "uuid-c1"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_person_lookup_responded("req-001", data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("companyId") == "uuid-c1"
        assert xml.findtext("companyName") == "Acme NV"


# ---------------------------------------------------------------------------
# Contract 17b — publish_unpaid_responded
# ---------------------------------------------------------------------------

class TestPublishUnpaidResponded:

    PERSONS = [
        {"id": "uuid-1", "firstName": "Anna", "lastName": "Peeters",
         "email": "anna@example.com", "linkedToCompany": False},
        {"id": "uuid-2", "firstName": "Bob", "lastName": "Smeets",
         "email": "bob@example.com", "linkedToCompany": True, "companyName": "Acme NV"},
    ]

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_unpaid_responded("req-001", self.PERSONS)
        assert _get_routing_key(setup_sender) == "crm.unpaid.responded"

    @pytest.mark.asyncio
    async def test_message_is_not_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_unpaid_responded("req-001", self.PERSONS)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.NOT_PERSISTENT

    @pytest.mark.asyncio
    async def test_persons_count_matches(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_unpaid_responded("req-001", self.PERSONS)
        xml = _get_published_xml(setup_sender)
        assert len(xml.find("persons")) == 2

    @pytest.mark.asyncio
    async def test_company_name_present_when_linked(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_unpaid_responded("req-001", self.PERSONS)
        bob = _get_published_xml(setup_sender).findall("persons/person")[1]
        assert bob.findtext("companyName") == "Acme NV"

    @pytest.mark.asyncio
    async def test_company_name_absent_when_not_linked(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_unpaid_responded("req-001", self.PERSONS)
        anna = _get_published_xml(setup_sender).findall("persons/person")[0]
        assert anna.find("companyName") is None

    @pytest.mark.asyncio
    async def test_empty_persons_list(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_unpaid_responded("req-001", [])
        assert len(_get_published_xml(setup_sender).find("persons")) == 0


# ---------------------------------------------------------------------------
# Contract 6 — publish_mail_requested
# ---------------------------------------------------------------------------

class TestPublishMailRequested:

    RECIPIENT = {"email": "jan@example.com", "name": "Jan Janssen"}

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        dynamic = {"guest_name": "Jan", "session_name": "Keynote",
                   "session_time": "2025-06-01T09:00:00Z"}
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        assert _get_routing_key(setup_sender) == "crm.mail.requested"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        dynamic = {"guest_name": "Jan", "session_name": "K",
                   "session_time": "2025-06-01T09:00:00Z"}
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_header_source_is_crm(self, setup_sender):
        dynamic = {"guest_name": "Jan", "session_name": "K",
                   "session_time": "2025-06-01T09:00:00Z"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        assert _get_published_xml(setup_sender).findtext("header/source") == "CRM"

    @pytest.mark.asyncio
    async def test_bulk_event_has_no_session_fields(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested(
                "bulk_event", self.RECIPIENT, {"guest_name": "Jan"})
        xml = _get_published_xml(setup_sender)
        assert xml.find("dynamic_data/session_name") is None
        assert xml.find("dynamic_data/session_time") is None

    @pytest.mark.asyncio
    async def test_session_change_has_session_fields(self, setup_sender):
        dynamic = {"guest_name": "Jan", "session_name": "Keynote",
                   "session_time": "2025-06-01T09:00:00Z"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested("session_change", self.RECIPIENT, dynamic)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("dynamic_data/session_name") == "Keynote"
        assert xml.findtext("dynamic_data/session_time") is not None

    @pytest.mark.asyncio
    async def test_recipient_fields_correct(self, setup_sender):
        dynamic = {"guest_name": "Jan", "session_name": "K",
                   "session_time": "2025-06-01T09:00:00Z"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("recipient/email") == "jan@example.com"
        assert xml.findtext("recipient/name") == "Jan Janssen"


# ---------------------------------------------------------------------------
# Contract 18 — publish_user_updated
# ---------------------------------------------------------------------------

class TestPublishUserUpdated:

    BASE_DATA = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "email": "jan@example.com",
        "firstName": "Jan",
        "lastName": "Janssen",
        "role": "VISITOR",
        "isActive": True,
        "gdprConsent": True,
        "updatedAt": "2026-04-15T12:00:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_updated(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.user.updated"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        """durable queue — message must survive broker restart."""
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_updated(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_user_updated(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "UserUpdated"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["id", "email", "firstName", "lastName", "role",
                      "isActive", "gdprConsent", "updatedAt"]:
            assert xml.find(field) is not None, f"Required field '{field}' missing"

    @pytest.mark.asyncio
    async def test_optional_fields_present_when_provided(self, setup_sender):
        data = {
            **self.BASE_DATA,
            "phone": "+32 471 12 34 56",
            "companyId": "123e4567-e89b-42d3-a456-556642440001",
            "badgeCode": "BADGE-042",
            "street": "Kerkstraat",
            "houseNumber": "42",
            "postalCode": "1000",
            "city": "Brussel",
            "country": "BE",
        }
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("phone") == "+32 471 12 34 56"
        assert xml.findtext("companyId") == "123e4567-e89b-42d3-a456-556642440001"
        assert xml.findtext("badgeCode") == "BADGE-042"
        assert xml.findtext("street") == "Kerkstraat"
        assert xml.findtext("houseNumber") == "42"
        assert xml.findtext("postalCode") == "1000"
        assert xml.findtext("city") == "Brussel"
        assert xml.findtext("country") == "BE"

    @pytest.mark.asyncio
    async def test_optional_fields_absent_when_not_provided(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["phone", "companyId", "badgeCode",
                      "street", "houseNumber", "postalCode", "city", "country"]:
            assert xml.find(field) is None, f"Optional field '{field}' should be absent"

    @pytest.mark.asyncio
    async def test_booleans_true_serialized_as_lowercase(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("isActive") == "true"
        assert xml.findtext("gdprConsent") == "true"

    @pytest.mark.asyncio
    async def test_booleans_false_serialized_as_lowercase(self, setup_sender):
        data = {**self.BASE_DATA, "isActive": False, "gdprConsent": False}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("isActive") == "false"
        assert xml.findtext("gdprConsent") == "false"

    @pytest.mark.asyncio
    async def test_xsd_field_order_required_only(self, setup_sender):
        """Required-only fields must still respect xs:sequence order."""
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(self.BASE_DATA)
        tags = [child.tag for child in _get_published_xml(setup_sender)]
        assert tags == ["id", "email", "firstName", "lastName",
                        "role", "isActive", "gdprConsent", "updatedAt"]

    @pytest.mark.asyncio
    async def test_full_xsd_field_order_all_optionals(self, setup_sender):
        """XSD xs:sequence is strict — all 16 fields in exact order."""
        data = {
            **self.BASE_DATA,
            "phone": "+32 471 12 34 56",
            "companyId": "123e4567-e89b-42d3-a456-556642440001",
            "badgeCode": "BADGE-042",
            "street": "Kerkstraat",
            "houseNumber": "42",
            "postalCode": "1000",
            "city": "Brussel",
            "country": "BE",
        }
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_updated(data)
        xml = _get_published_xml(setup_sender)
        tags = [child.tag for child in xml]
        expected = [
            "id", "email", "firstName", "lastName", "phone",
            "role", "companyId", "badgeCode",
            "street", "houseNumber", "postalCode", "city", "country",
            "isActive", "gdprConsent", "updatedAt",
        ]
        assert tags == expected


# ---------------------------------------------------------------------------
# Contract 22 — publish_user_deactivated
# ---------------------------------------------------------------------------

class TestPublishUserDeactivated:

    BASE_DATA = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "email": "jan@example.com",
        "deactivatedAt": "2026-04-15T14:00:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_deactivated(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.user.deactivated"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        """durable queue — message must survive broker restart."""
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_deactivated(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_user_deactivated(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_deactivated(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "UserDeactivated"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_deactivated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["id", "email", "deactivatedAt"]:
            assert xml.find(field) is not None, f"Required field '{field}' missing"

    @pytest.mark.asyncio
    async def test_xsd_field_order(self, setup_sender):
        """XSD xs:sequence is strict — id, email, deactivatedAt."""
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_deactivated(self.BASE_DATA)
        tags = [child.tag for child in _get_published_xml(setup_sender)]
        assert tags == ["id", "email", "deactivatedAt"]

    @pytest.mark.asyncio
    async def test_field_values_match_input(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_deactivated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("id") == "550e8400-e29b-41d4-a716-446655440000"
        assert xml.findtext("email") == "jan@example.com"
        assert xml.findtext("deactivatedAt") == "2026-04-15T14:00:00Z"

