from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from lxml import etree

from src.handlers.frontend_company_created import _build_frontend_account_data

VALID_FULL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <phone>+32 2 123 45 67</phone>
    <street>Hoofdstraat</street>
    <houseNumber>42</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussel</city>
    <country>BE</country>
</CompanyCreated>"""

VALID_MINIMAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>Minimal NV</name>
    <vatNumber>BE0987654321</vatNumber>
</CompanyCreated>"""

INVALID_XML = b"not xml at all"

ACCOUNT_RETURN = {
    "Id": "001000000000001",
    "CRM_ID__c": "aaaabbbb-cccc-dddd-eeee-ffffffffffff",
    "VAT_Number__c": "BE0123456789",
    "Name": "Acme NV",
    "Email__c": "info@acme.be",
    "Phone": "+32 2 123 45 67",
    "BillingStreet": "Hoofdstraat",
    "House_Number__c": "42",
    "BillingPostalCode": "1000",
    "BillingCity": "Brussel",
    "BillingCountry": "BE",
}


@pytest.fixture
def sf_mock():
    return AsyncMock()


# ---------------------------------------------------------------------------
# Integration-style handler tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_full_xml_upserts_and_publishes(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    company_data_return = {
        "id": "aaaabbbb-cccc-dddd-eeee-ffffffffffff",
        "vatNumber": "BE0123456789",
        "name": "Acme NV",
        "email": "info@acme.be",
        "phone": "+32 2 123 45 67",
        "street": "Hoofdstraat",
        "houseNumber": "42",
        "postalCode": "1000",
        "city": "Brussel",
        "country": "BE",
        "isActive": True,
        "confirmedAt": "2026-04-29T00:00:00Z",
    }
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            return_value=ACCOUNT_RETURN,
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created._build_company_data",
            return_value=company_data_return,
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed"
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        mock_upsert.assert_called_once_with(sf_mock, "BE0123456789", mock_upsert.call_args.args[2])
        mock_publish.assert_called_once()
        msg.ack.assert_called_once()
        msg.reject.assert_not_called()


@pytest.mark.asyncio
async def test_valid_full_xml_upsert_called_with_correct_vat(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    company_data_return = {
        "id": "aaaabbbb-cccc-dddd-eeee-ffffffffffff",
        "vatNumber": "BE0123456789",
        "name": "Acme NV",
        "email": "info@acme.be",
        "street": "Hoofdstraat",
        "houseNumber": "42",
        "postalCode": "1000",
        "city": "Brussel",
        "country": "BE",
        "isActive": True,
        "confirmedAt": "2026-04-29T00:00:00Z",
    }
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            return_value=ACCOUNT_RETURN,
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created._build_company_data",
            return_value=company_data_return,
        ),
        patch("src.handlers.frontend_company_created.sender.publish_company_confirmed"),
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        vat_arg = mock_upsert.call_args.args[1]
        assert vat_arg == "BE0123456789"


@pytest.mark.asyncio
async def test_valid_minimal_xml_only_required_fields_in_account_data(sf_mock):
    parsed_xml = etree.fromstring(VALID_MINIMAL_XML)
    minimal_account = {
        "Id": "001000000000002",
        "CRM_ID__c": "bbbbcccc-dddd-eeee-ffff-000000000000",
        "VAT_Number__c": "BE0987654321",
        "Name": "Minimal NV",
        "Email__c": "noreply@example.com",
        "BillingStreet": "Teststraat",
        "House_Number__c": "1",
        "BillingPostalCode": "2000",
        "BillingCity": "Antwerpen",
        "BillingCountry": "BE",
    }
    company_data_return = {
        "id": "bbbbcccc-dddd-eeee-ffff-000000000000",
        "vatNumber": "BE0987654321",
        "name": "Minimal NV",
        "email": "noreply@example.com",
        "street": "Teststraat",
        "houseNumber": "1",
        "postalCode": "2000",
        "city": "Antwerpen",
        "country": "BE",
        "isActive": True,
        "confirmedAt": "2026-04-29T00:00:00Z",
    }
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            return_value=minimal_account,
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created._build_company_data",
            return_value=company_data_return,
        ),
        patch("src.handlers.frontend_company_created.sender.publish_company_confirmed"),
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_MINIMAL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        account_data_arg = mock_upsert.call_args.args[2]
        assert set(account_data_arg.keys()) == {"Name", "VAT_Number__c"}
        assert account_data_arg["Name"] == "Minimal NV"
        assert account_data_arg["VAT_Number__c"] == "BE0987654321"
        msg.ack.assert_called_once()


@pytest.mark.asyncio
async def test_invalid_xml_rejects_without_upsert_or_publish(sf_mock):
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            side_effect=ValueError("bad xml"),
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat"
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed"
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = INVALID_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        msg.reject.assert_called_once_with(requeue=False)
        msg.ack.assert_not_called()
        mock_upsert.assert_not_called()
        mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_raises_bubbles_no_ack_no_reject(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            side_effect=RuntimeError("Salesforce down"),
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed"
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(RuntimeError, match="Salesforce down"):
            await handle(msg, sf_mock)

        msg.ack.assert_not_called()
        msg.reject.assert_not_called()
        mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_publish_failure_bubbles_no_ack(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            return_value=ACCOUNT_RETURN,
        ),
        patch(
            "src.handlers.frontend_company_created._build_company_data",
            side_effect=ValueError("missing address fields"),
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed"
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(ValueError, match="missing address fields"):
            await handle(msg, sf_mock)

        msg.ack.assert_not_called()
        mock_publish.assert_not_called()


# ---------------------------------------------------------------------------
# Unit tests for _build_frontend_account_data
# ---------------------------------------------------------------------------


def test_build_frontend_account_data_full_xml_maps_all_fields():
    xml = etree.fromstring(VALID_FULL_XML)
    data = _build_frontend_account_data(xml)
    assert data["Name"] == "Acme NV"
    assert data["VAT_Number__c"] == "BE0123456789"
    assert data["Email__c"] == "info@acme.be"
    assert data["Phone"] == "+32 2 123 45 67"
    assert data["BillingStreet"] == "Hoofdstraat"
    assert data["House_Number__c"] == "42"
    assert data["BillingPostalCode"] == "1000"
    assert data["BillingCity"] == "Brussel"
    assert data["BillingCountry"] == "BE"


def test_build_frontend_account_data_minimal_xml_omits_optional_fields():
    xml = etree.fromstring(VALID_MINIMAL_XML)
    data = _build_frontend_account_data(xml)
    assert data["Name"] == "Minimal NV"
    assert data["VAT_Number__c"] == "BE0987654321"
    assert "Email__c" not in data
    assert "Phone" not in data
    assert "BillingStreet" not in data
    assert "House_Number__c" not in data
    assert "BillingPostalCode" not in data
    assert "BillingCity" not in data
    assert "BillingCountry" not in data
