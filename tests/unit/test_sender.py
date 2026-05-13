"""
Unit tests — sender.py
Contracten 15, 13, 14, 23, 5b, 10b, 17b, 6, 18, 22
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
    mock_channel.is_closed = False
    mock_exchange = MagicMock()
    mock_exchange.publish = AsyncMock()
    mock_conflict_exchange = MagicMock()
    mock_conflict_exchange.publish = AsyncMock()
    mock_logs_exchange = MagicMock()
    mock_logs_exchange.publish = AsyncMock()
    sender._channel = mock_channel
    sender._connection = None
    sender._exchange = mock_exchange
    sender._conflict_exchange = mock_conflict_exchange
    sender._logs_exchange = mock_logs_exchange
    mock_exchange.conflict_exchange = mock_conflict_exchange
    mock_exchange.logs_exchange = mock_logs_exchange
    yield mock_exchange


def _get_published_xml(mock_exchange) -> etree._Element:
    call_args = mock_exchange.publish.call_args
    message = call_args[0][0]
    return etree.fromstring(message.body)


def _get_routing_key(mock_exchange) -> str:
    return mock_exchange.publish.call_args[1]["routing_key"]


def _get_delivery_mode(mock_exchange):
    message = mock_exchange.publish.call_args[0][0]
    return message.delivery_mode


def _get_conflict_published_xml(mock_exchange) -> etree._Element:
    call_args = mock_exchange.conflict_exchange.publish.call_args
    message = call_args[0][0]
    return etree.fromstring(message.body)


def _get_conflict_routing_key(mock_exchange) -> str:
    return mock_exchange.conflict_exchange.publish.call_args[1]["routing_key"]


def _get_conflict_delivery_mode(mock_exchange):
    message = mock_exchange.conflict_exchange.publish.call_args[0][0]
    return message.delivery_mode


# ---------------------------------------------------------------------------
# Contract 15 — publish_user_conflict
# ---------------------------------------------------------------------------

class TestPublishUserConflict:

    BASE_DATA = {
        "email": "jan@example.com",
        "existingValue": {
            "firstName": "Jan",
            "lastName": "Janssen",
        },
        "incomingValue": {
            "firstName": "Johan",
            "lastName": "Janssen",
        },
        "detectedAt": "2026-04-15T10:01:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_fanout_exchange(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_conflict(self.BASE_DATA)
        assert _get_conflict_routing_key(setup_sender) == ""

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_user_conflict(self.BASE_DATA)
        assert _get_conflict_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_user_conflict(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_conflict(self.BASE_DATA)
        assert _get_conflict_published_xml(setup_sender).tag == "UserConflict"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_conflict(self.BASE_DATA)
        xml = _get_conflict_published_xml(setup_sender)
        assert xml.findtext("email") == "jan@example.com"
        assert xml.findtext("existingValue/firstName") == "Jan"
        assert xml.findtext("existingValue/lastName") == "Janssen"
        assert xml.findtext("incomingValue/firstName") == "Johan"
        assert xml.findtext("incomingValue/lastName") == "Janssen"
        assert xml.findtext("detectedAt") == "2026-04-15T10:01:00Z"

    @pytest.mark.asyncio
    async def test_optional_company_present_when_provided(self, setup_sender):
        data = {
            **self.BASE_DATA,
            "existingValue": {
                **self.BASE_DATA["existingValue"],
                "company": "Acme NV",
            },
            "incomingValue": {
                **self.BASE_DATA["incomingValue"],
                "company": "Beta BV",
            },
        }
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_conflict(data)
        xml = _get_conflict_published_xml(setup_sender)
        assert xml.findtext("existingValue/company") == "Acme NV"
        assert xml.findtext("incomingValue/company") == "Beta BV"

    @pytest.mark.asyncio
    async def test_xsd_field_order(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_user_conflict(self.BASE_DATA)
        tags = [child.tag for child in _get_conflict_published_xml(setup_sender)]
        assert tags == ["email", "existingValue", "incomingValue", "detectedAt"]


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

    REQUIRED_DATA = {
        "id": "660e8400-e29b-41d4-a716-446655440001",
        "vatNumber": "BE0123456789",
        "name": "Acme NV",
        "email": "info@acme.be",
        "street": "Main Street",
        "houseNumber": "42",
        "postalCode": "1000",
        "city": "Brussels",
        "country": "BE",
        "isActive": True,
        "confirmedAt": "2025-01-01T10:00:00Z",
    }

    FULL_DATA = {
        **REQUIRED_DATA,
        "phone": "+32 2 123 45 67",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_confirmed(self.REQUIRED_DATA)
        assert _get_routing_key(setup_sender) == "crm.company.confirmed"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_confirmed(self.REQUIRED_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_company_confirmed(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.REQUIRED_DATA)
        assert _get_published_xml(setup_sender).tag == "CompanyConfirmed"

    @pytest.mark.asyncio
    async def test_all_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.REQUIRED_DATA)
        xml = _get_published_xml(setup_sender)
        for field in (
            "id", "vatNumber", "name", "email",
            "street", "houseNumber", "postalCode", "city", "country",
            "isActive", "confirmedAt",
        ):
            assert xml.find(field) is not None, f"Required field '{field}' missing"

    @pytest.mark.asyncio
    async def test_phone_absent_when_not_provided(self, setup_sender):
        """phone is the only optional element on C14 after the v1.X.0 contract update."""
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.REQUIRED_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.find("phone") is None

    @pytest.mark.asyncio
    async def test_phone_included_when_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_confirmed(self.FULL_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("phone") == "+32 2 123 45 67"
        tags = [child.tag for child in xml]
        assert tags.index("email") < tags.index("phone") < tags.index("street") < tags.index("houseNumber") < tags.index("postalCode") < tags.index("city") < tags.index("country") < tags.index("isActive") < tags.index("confirmedAt")

    @pytest.mark.asyncio
    async def test_raises_key_error_when_required_address_field_missing(self, setup_sender):
        incomplete = {k: v for k, v in self.REQUIRED_DATA.items() if k != "street"}
        with pytest.raises(KeyError, match="street"):
            await sender.publish_company_confirmed(incomplete)


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
        dynamic = {"guest_name": "Jan"}
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        assert _get_routing_key(setup_sender) == "crm.mail.requested"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        dynamic = {"guest_name": "Jan"}
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_mail_requested(
                "registration_confirmation", self.RECIPIENT, dynamic)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_header_source_is_crm(self, setup_sender):
        dynamic = {"guest_name": "Jan"}
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
                   "session_time": "2025-06-01T09:00:00Z", "session_location": "Room 1"}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested("session_change", self.RECIPIENT, dynamic)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("dynamic_data/session_name") == "Keynote"
        assert xml.findtext("dynamic_data/session_time") == "2025-06-01T09:00:00Z"
        assert xml.findtext("dynamic_data/session_location") == "Room 1"

    @pytest.mark.asyncio
    async def test_optional_session_fields_include_empty_string_but_skip_none(self, setup_sender):
        dynamic = {
            "guest_name": "Jan",
            "session_name": "",
            "session_time": None,
            "session_location": "Room 1",
        }
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_mail_requested("session_change", self.RECIPIENT, dynamic)
        xml = _get_published_xml(setup_sender)
        assert xml.find("dynamic_data/session_name") is not None
        assert xml.findtext("dynamic_data/session_name") == ""
        assert xml.find("dynamic_data/session_time") is None
        assert xml.findtext("dynamic_data/session_location") == "Room 1"

    @pytest.mark.asyncio
    async def test_recipient_fields_correct(self, setup_sender):
        dynamic = {"guest_name": "Jan"}
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


# ---------------------------------------------------------------------------
# Contract 19 — publish_company_updated
# ---------------------------------------------------------------------------

class TestPublishCompanyUpdated:

    BASE_DATA = {
        "id": "660e8400-e29b-41d4-a716-446655440002",
        "vatNumber": "BE0123456789",
        "name": "Acme NV",
        "isActive": True,
        "updatedAt": "2026-04-21T12:00:00Z",
    }

    FULL_DATA = {
        **BASE_DATA,
        "email": "info@acme.be",
        "phone": "+32 2 123 45 67",
        "street": "Main Street",
        "houseNumber": "42",
        "postalCode": "1000",
        "city": "Brussels",
        "country": "BE",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_updated(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.company.updated"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_updated(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_company_updated(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "CompanyUpdated"

    @pytest.mark.asyncio
    async def test_required_fields_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["id", "vatNumber", "name", "isActive", "updatedAt"]:
            assert xml.find(field) is not None, f"Required field '{field}' missing"

    @pytest.mark.asyncio
    async def test_optional_fields_omitted_when_absent(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        for field in ["email", "phone", "street", "houseNumber", "postalCode", "city", "country"]:
            assert xml.find(field) is None, f"Optional field '{field}' should be absent"

    @pytest.mark.asyncio
    async def test_optional_fields_included_when_present(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(self.FULL_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("email") == "info@acme.be"
        assert xml.findtext("phone") == "+32 2 123 45 67"
        assert xml.findtext("street") == "Main Street"
        assert xml.findtext("houseNumber") == "42"
        assert xml.findtext("postalCode") == "1000"
        assert xml.findtext("city") == "Brussels"
        assert xml.findtext("country") == "BE"

    @pytest.mark.asyncio
    async def test_xsd_field_order(self, setup_sender):
        """XSD xs:sequence: id, vatNumber, name, [email, phone, street, houseNumber, postalCode, city, country], isActive, updatedAt."""
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(self.FULL_DATA)
        tags = [child.tag for child in _get_published_xml(setup_sender)]
        expected = [
            "id", "vatNumber", "name",
            "email", "phone", "street", "houseNumber", "postalCode", "city", "country",
            "isActive", "updatedAt",
        ]
        assert tags == expected

    @pytest.mark.asyncio
    async def test_is_active_serialized_as_lowercase(self, setup_sender):
        data = {**self.BASE_DATA, "isActive": False}
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_updated(data)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("isActive") == "false"

    @pytest.mark.asyncio
    async def test_xml_is_xsd_valid(self, setup_sender):
        """Real XSD validation via the project schema — end-to-end contract test."""
        from src import xml_validator
        await sender.publish_company_updated(self.FULL_DATA)
        xml = _get_published_xml(setup_sender)
        xml_validator.validate(etree.tostring(xml))


# ---------------------------------------------------------------------------
# Contract 23 — publish_company_deactivated
# ---------------------------------------------------------------------------

class TestPublishCompanyDeactivated:

    BASE_DATA = {
        "id": "660e8400-e29b-41d4-a716-446655440003",
        "vatNumber": "BE0123456789",
        "deactivatedAt": "2026-04-21T12:00:00Z",
    }

    @pytest.mark.asyncio
    async def test_publishes_to_correct_queue(self, setup_sender):
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_deactivated(self.BASE_DATA)
        assert _get_routing_key(setup_sender) == "crm.company.deactivated"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        with patch("src.xml_validator.validate", return_value=MagicMock()):
            await sender.publish_company_deactivated(self.BASE_DATA)
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_root_element_is_company_deactivated(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_deactivated(self.BASE_DATA)
        assert _get_published_xml(setup_sender).tag == "CompanyDeactivated"

    @pytest.mark.asyncio
    async def test_xsd_field_order(self, setup_sender):
        """XSD xs:sequence: id, vatNumber, deactivatedAt."""
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_deactivated(self.BASE_DATA)
        tags = [child.tag for child in _get_published_xml(setup_sender)]
        assert tags == ["id", "vatNumber", "deactivatedAt"]

    @pytest.mark.asyncio
    async def test_field_values_match_input(self, setup_sender):
        with patch("src.xml_validator.validate") as v:
            v.side_effect = lambda b: etree.fromstring(b)
            await sender.publish_company_deactivated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        assert xml.findtext("id") == "660e8400-e29b-41d4-a716-446655440003"
        assert xml.findtext("vatNumber") == "BE0123456789"
        assert xml.findtext("deactivatedAt") == "2026-04-21T12:00:00Z"

    @pytest.mark.asyncio
    async def test_xml_is_xsd_valid(self, setup_sender):
        """Real XSD validation via the project schema — end-to-end contract test."""
        from src import xml_validator
        await sender.publish_company_deactivated(self.BASE_DATA)
        xml = _get_published_xml(setup_sender)
        xml_validator.validate(etree.tostring(xml))

    @pytest.mark.asyncio
    async def test_integration_contract_23_full_roundtrip(self, setup_sender):
        """H4 — Contract 23 integration: no mocked validator, verify XML content and schema."""
        from src import xml_validator

        data = {
            "id": "770e8400-e29b-41d4-a716-446655440099",
            "vatNumber": "BE9999999999",
            "deactivatedAt": "2026-04-22T10:00:00Z",
        }
        await sender.publish_company_deactivated(data)

        xml = _get_published_xml(setup_sender)

        # Schema validation (real XSD, no mock)
        xml_validator.validate(etree.tostring(xml))

        # Content validation
        assert xml.tag == "CompanyDeactivated"
        assert xml.findtext("id") == "770e8400-e29b-41d4-a716-446655440099"
        assert xml.findtext("vatNumber") == "BE9999999999"
        assert xml.findtext("deactivatedAt") == "2026-04-22T10:00:00Z"

        # Field order per XSD xs:sequence
        tags = [child.tag for child in xml]
        assert tags == ["id", "vatNumber", "deactivatedAt"]

        # Routing and persistence
        assert _get_routing_key(setup_sender) == "crm.company.deactivated"
        from aio_pika import DeliveryMode
        assert _get_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT


# ---------------------------------------------------------------------------
# LogEvent — publish_log_event_raw
# Exchange: logs.direct (direct, durable) | rk: routing.log → controlroom.logs.queue
# ---------------------------------------------------------------------------

def _get_logs_published_xml(mock_exchange) -> etree._Element:
    call_args = mock_exchange.logs_exchange.publish.call_args
    message = call_args[0][0]
    return etree.fromstring(message.body)


def _get_logs_routing_key(mock_exchange) -> str:
    return mock_exchange.logs_exchange.publish.call_args[1]["routing_key"]


def _get_logs_delivery_mode(mock_exchange):
    message = mock_exchange.logs_exchange.publish.call_args[0][0]
    return message.delivery_mode


class TestPublishLogEventRaw:
    """Tests for sender.publish_log_event_raw — LogEvent → logs.direct."""

    SAMPLE_XML = (
        b"<?xml version='1.0' encoding='UTF-8'?>"
        b"<LogEvent>"
        b"<level>INFO</level>"
        b"<timestamp>2026-05-04T10:31:42Z</timestamp>"
        b"<service>crm</service>"
        b"<data>integration test marker</data>"
        b"</LogEvent>"
    )

    @pytest.mark.asyncio
    async def test_publishes_to_logs_direct_exchange(self, setup_sender):
        await sender.publish_log_event_raw(self.SAMPLE_XML)
        setup_sender.logs_exchange.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_routing_key_is_routing_log(self, setup_sender):
        """Live broker bindt logs.direct -> controlroom.logs.queue met rk 'routing.log'.

        ClickUp-spec table heeft labels omgedraaid (rk en queue-naam zijn gewisseld).
        Geverifieerd 2026-05-04: rk 'controlroom.logs.queue' dropt 25/26 berichten.
        """
        await sender.publish_log_event_raw(self.SAMPLE_XML)
        assert _get_logs_routing_key(setup_sender) == "routing.log"

    @pytest.mark.asyncio
    async def test_message_is_persistent(self, setup_sender):
        from aio_pika import DeliveryMode
        await sender.publish_log_event_raw(self.SAMPLE_XML)
        assert _get_logs_delivery_mode(setup_sender) == DeliveryMode.PERSISTENT

    @pytest.mark.asyncio
    async def test_payload_is_passed_through_unchanged(self, setup_sender):
        await sender.publish_log_event_raw(self.SAMPLE_XML)
        message = setup_sender.logs_exchange.publish.call_args[0][0]
        assert message.body == self.SAMPLE_XML

    @pytest.mark.asyncio
    async def test_root_element_is_log_event(self, setup_sender):
        await sender.publish_log_event_raw(self.SAMPLE_XML)
        assert _get_logs_published_xml(setup_sender).tag == "LogEvent"

    @pytest.mark.asyncio
    async def test_no_op_when_exchange_not_initialised(self):
        """Defensive: publishing before init() must not raise."""
        sender._logs_exchange = None
        # Must not raise
        await sender.publish_log_event_raw(self.SAMPLE_XML)

    @pytest.mark.asyncio
    async def test_xml_is_xsd_valid(self, setup_sender):
        """End-to-end: payload must validate against the real XSD manifest."""
        from src import xml_validator

        await sender.publish_log_event_raw(self.SAMPLE_XML)
        xml = _get_logs_published_xml(setup_sender)
        xml_validator.validate(etree.tostring(xml))


class TestChannelRecovery:
    """Recovery when the sender's channel is closed by the broker."""

    @staticmethod
    def _wire_connection_returning_new_channel():
        new_exchange = MagicMock()
        new_exchange.publish = AsyncMock()
        new_conflict = MagicMock()
        new_conflict.publish = AsyncMock()
        new_logs = MagicMock()
        new_logs.publish = AsyncMock()

        new_channel = MagicMock()
        new_channel.is_closed = False

        async def declare_exchange(name, **_kw):
            return {
                "contact.topic": new_exchange,
                "crm.user.conflict": new_conflict,
                "logs.direct": new_logs,
            }[name]

        new_channel.declare_exchange = declare_exchange

        connection = MagicMock()
        connection.channel = AsyncMock(return_value=new_channel)
        return connection, new_channel, new_exchange, new_conflict, new_logs

    @pytest.mark.asyncio
    async def test_publish_recreates_channel_when_closed(self, setup_sender):
        conn, new_channel, new_exchange, *_ = self._wire_connection_returning_new_channel()
        sender._channel.is_closed = True
        sender._connection = conn

        with patch("src.xml_validator.validate"):
            await sender._publish("crm.user.confirmed", b"<UserConfirmed/>", persistent=True)

        conn.channel.assert_awaited_once()
        new_exchange.publish.assert_awaited_once()
        assert sender._channel is new_channel
        assert sender._exchange is new_exchange

    @pytest.mark.asyncio
    async def test_publish_skips_recovery_when_no_connection(self, setup_sender):
        sender._channel.is_closed = True
        sender._connection = None

        # Existing _exchange is the fixture mock; publish must still go through it
        # because recovery is disabled when init() did not receive a connection.
        with patch("src.xml_validator.validate"):
            await sender._publish("crm.user.confirmed", b"<UserConfirmed/>", persistent=True)

        setup_sender.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_concurrent_publishes_share_single_channel_recreation(self, setup_sender):
        import asyncio as _asyncio

        conn, _new_channel, new_exchange, *_ = self._wire_connection_returning_new_channel()
        sender._channel.is_closed = True
        sender._connection = conn

        with patch("src.xml_validator.validate"):
            await _asyncio.gather(
                sender._publish("a", b"<X/>"),
                sender._publish("b", b"<X/>"),
                sender._publish("c", b"<X/>"),
            )

        conn.channel.assert_awaited_once()
        assert new_exchange.publish.await_count == 3

    @pytest.mark.asyncio
    async def test_conflict_publish_also_recovers(self, setup_sender):
        conn, _new_channel, _new_exchange, new_conflict, *_ = (
            self._wire_connection_returning_new_channel()
        )
        sender._channel.is_closed = True
        sender._connection = conn

        with patch("src.xml_validator.validate"):
            await sender._publish_conflict(b"<UserConflict/>", persistent=True)

        conn.channel.assert_awaited_once()
        new_conflict.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_log_event_publish_also_recovers(self, setup_sender):
        conn, _new_channel, _new_exchange, _new_conflict, new_logs = (
            self._wire_connection_returning_new_channel()
        )
        sender._channel.is_closed = True
        sender._connection = conn

        await sender.publish_log_event_raw(b"<LogEvent/>")

        conn.channel.assert_awaited_once()
        new_logs.publish.assert_awaited_once()


class TestInitSignature:
    """Back-compat: init still accepts a bare channel; connection is optional."""

    @pytest.mark.asyncio
    async def test_init_without_connection_disables_recovery(self):
        channel = MagicMock()
        channel.is_closed = False
        channel.declare_exchange = AsyncMock(return_value=MagicMock())

        await sender.init(channel)

        assert sender._channel is channel
        assert sender._connection is None

    @pytest.mark.asyncio
    async def test_init_with_connection_stores_it_for_recovery(self):
        channel = MagicMock()
        channel.is_closed = False
        channel.declare_exchange = AsyncMock(return_value=MagicMock())
        connection = MagicMock()

        await sender.init(channel, connection=connection)

        assert sender._channel is channel
        assert sender._connection is connection
