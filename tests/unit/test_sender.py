"""Tests for src.sender publisher utilities."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from lxml import etree

from src import sender


class TestSenderInit:
    """Tests for sender.init()."""

    @pytest.mark.asyncio
    async def test_init_sets_global_channel_and_logs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()

        await sender.init(mock_channel)

        # Global _channel should now point to our mock instance
        assert sender._channel is mock_channel  # type: ignore[attr-defined]


class TestPublishUserConfirmed:
    """Tests for publish_user_confirmed()."""

    @pytest.mark.asyncio
    async def test_builds_valid_xml_and_publishes_correct_queue(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Arrange: fake channel and xml_validator
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        validate_calls: list[bytes] = []

        def fake_validate(xml_bytes: bytes):  # type: ignore[override]
            validate_calls.append(xml_bytes)

        monkeypatch.setattr("src.sender.xml_validator.validate", fake_validate)

        user_data = {
            "id": 123,
            "email": "user@example.com",
            "firstName": "Alice",
            "lastName": "Doe",
            "role": "ATTENDEE",
            "isActive": True,
            "gdprConsent": False,
            "confirmedAt": "2026-03-25T10:00:00Z",
            "phone": "+3212345678",
            "companyId": 42,
            "badgeCode": "ABC123",
        }

        # Act
        await sender.publish_user_confirmed(user_data)

        # Assert: xml_validator.validate called once with the built XML
        assert len(validate_calls) == 1
        xml_bytes = validate_calls[0]

        # XML structure checks
        root = etree.fromstring(xml_bytes)
        assert root.tag == "UserConfirmed"
        assert root.findtext("id") == "123"
        assert root.findtext("email") == "user@example.com"
        assert root.findtext("firstName") == "Alice"
        assert root.findtext("lastName") == "Doe"
        assert root.findtext("role") == "ATTENDEE"
        assert root.findtext("isActive") == "true"
        assert root.findtext("gdprConsent") == "false"
        assert root.findtext("confirmedAt") == "2026-03-25T10:00:00Z"
        assert root.findtext("phone") == "+3212345678"
        assert root.findtext("companyId") == "42"
        assert root.findtext("badgeCode") == "ABC123"

        # Publish called with correct routing key and body
        mock_exchange.publish.assert_awaited_once()
        args, kwargs = mock_exchange.publish.call_args
        message = args[0]
        assert kwargs["routing_key"] == "crm.user.confirmed"
        assert message.body == xml_bytes


class TestPublishCompanyConfirmed:
    """Tests for publish_company_confirmed()."""

    @pytest.mark.asyncio
    async def test_company_confirmed_uses_expected_structure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        with patch("src.sender.xml_validator.validate") as mock_validate:
            company = {
                "id": 7,
                "vatNumber": "BE0123456789",
                "name": "Example Corp",
                "email": "info@example.com",
                "isActive": True,
                "confirmedAt": "2026-03-25T11:00:00Z",
            }

            await sender.publish_company_confirmed(company)

            # Validation called once
            mock_validate.assert_called_once()
            xml_bytes = mock_validate.call_args.args[0]

        root = etree.fromstring(xml_bytes)
        assert root.tag == "CompanyConfirmed"
        assert root.findtext("id") == "7"
        assert root.findtext("vatNumber") == "BE0123456789"
        assert root.findtext("name") == "Example Corp"
        assert root.findtext("email") == "info@example.com"
        assert root.findtext("isActive") == "true"
        assert root.findtext("confirmedAt") == "2026-03-25T11:00:00Z"

        mock_exchange.publish.assert_awaited_once()
        _, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.company.confirmed"


class TestPublishCompanyResponded:
    """Tests for publish_company_responded()."""

    @pytest.mark.asyncio
    async def test_includes_optional_fields_when_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        with patch("src.sender.xml_validator.validate") as mock_validate:
            data = {
                "found": True,
                "id": 10,
                "name": "Found Co",
                "vatNumber": "VAT123",
                "email": "contact@found.co",
                "phone": "+3200000000",
                "street": "Main St",
                "houseNumber": "1A",
                "postalCode": "1000",
                "city": "Brussels",
                "country": "BE",
            }

            await sender.publish_company_responded("REQ-1", data)

            mock_validate.assert_called_once()
            xml_bytes = mock_validate.call_args.args[0]

        root = etree.fromstring(xml_bytes)
        assert root.tag == "CompanyResponse"
        assert root.findtext("requestId") == "REQ-1"
        assert root.findtext("found") == "true"
        assert root.findtext("name") == "Found Co"
        assert root.findtext("city") == "Brussels"

        mock_exchange.publish.assert_awaited_once()
        _, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.company.responded"


class TestPublishPersonLookupResponded:
    """Tests for publish_person_lookup_responded()."""

    @pytest.mark.asyncio
    async def test_linked_person_includes_company_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        with patch("src.sender.xml_validator.validate") as mock_validate:
            person = {
                "found": True,
                "linkedToCompany": True,
                "id": 5,
                "companyName": "Partner BV",
                "companyId": 99,
            }

            await sender.publish_person_lookup_responded("REQ-2", person)

            mock_validate.assert_called_once()
            xml_bytes = mock_validate.call_args.args[0]

        root = etree.fromstring(xml_bytes)
        assert root.tag == "PersonResponse"
        assert root.findtext("requestId") == "REQ-2"
        assert root.findtext("found") == "true"
        assert root.findtext("linkedToCompany") == "true"
        assert root.findtext("companyName") == "Partner BV"
        assert root.findtext("companyId") == "99"

        mock_exchange.publish.assert_awaited_once()
        _, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.person.lookup.responded"


class TestPublishUnpaidResponded:
    """Tests for publish_unpaid_responded()."""

    @pytest.mark.asyncio
    async def test_builds_person_list_payload(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        with patch("src.sender.xml_validator.validate") as mock_validate:
            persons = [
                {
                    "id": 1,
                    "firstName": "John",
                    "lastName": "Doe",
                    "email": "john@example.com",
                    "linkedToCompany": False,
                },
                {
                    "id": 2,
                    "firstName": "Jane",
                    "lastName": "Roe",
                    "email": "jane@example.com",
                    "linkedToCompany": True,
                    "companyName": "Acme NV",
                },
            ]

            await sender.publish_unpaid_responded("REQ-3", persons)

            mock_validate.assert_called_once()
            xml_bytes = mock_validate.call_args.args[0]

        root = etree.fromstring(xml_bytes)
        assert root.tag == "UnpaidResponse"
        assert root.findtext("requestId") == "REQ-3"

        persons_el = root.find("persons")
        assert persons_el is not None
        person_elems = persons_el.findall("person")
        assert len(person_elems) == 2

        first = person_elems[0]
        assert first.findtext("id") == "1"
        assert first.findtext("linkedToCompany") == "false"
        assert first.find("companyName") is None

        second = person_elems[1]
        assert second.findtext("id") == "2"
        assert second.findtext("linkedToCompany") == "true"
        assert second.findtext("companyName") == "Acme NV"

        mock_exchange.publish.assert_awaited_once()
        _, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.unpaid.responded"


class TestPublishMailRequested:
    """Tests for publish_mail_requested()."""

    @pytest.mark.asyncio
    async def test_mail_request_includes_header_and_dynamic_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.default_exchange = mock_exchange
        monkeypatch.setattr(sender, "_channel", mock_channel)

        # Freeze time so timestamp is predictable
        fixed_dt = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
        with patch("src.sender.datetime") as mock_datetime, patch(
            "src.sender.xml_validator.validate"
        ) as mock_validate:
            mock_datetime.now.return_value = fixed_dt
            mock_datetime.timezone = timezone  # for attribute access

            recipient = {"email": "user@example.com", "name": "User"}
            dynamic = {
                "guest_name": "Guest",
                "session_name": "Session A",
                "session_time": "2026-04-01T09:00:00Z",
            }

            await sender.publish_mail_requested(
                "session_reminder", recipient, dynamic
            )

            mock_validate.assert_called_once()
            xml_bytes = mock_validate.call_args.args[0]

        root = etree.fromstring(xml_bytes)
        assert root.tag == "MailRequest"
        assert root.findtext("mailType") == "session_reminder"

        header = root.find("header")
        assert header is not None
        assert header.findtext("source") == "CRM"
        assert header.findtext("timestamp") == "2026-03-25T12:00:00Z"

        recipient_el = root.find("recipient")
        assert recipient_el is not None
        assert recipient_el.findtext("email") == "user@example.com"
        assert recipient_el.findtext("name") == "User"

        dynamic_el = root.find("dynamic_data")
        assert dynamic_el is not None
        assert dynamic_el.findtext("guest_name") == "Guest"
        assert dynamic_el.findtext("session_name") == "Session A"
        assert dynamic_el.findtext("session_time") == "2026-04-01T09:00:00Z"

        mock_exchange.publish.assert_awaited_once()
        _, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.mail.requested"
