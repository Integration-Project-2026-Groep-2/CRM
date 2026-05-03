from __future__ import annotations

from contextlib import contextmanager
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


def _full_account(crm_id: str = "aaaabbbb-cccc-dddd-eeee-ffffffffffff") -> dict:
    """Salesforce Account dict that satisfies _build_company_data's requirements."""
    return {
        "Id": "001000000000001",
        "CRM_ID__c": crm_id,
        "VAT_Number__c": "BE0123456789",
        "Name": "Acme NV",
        "Email__c": "info@acme.be",
        "Phone": "+32 2 123 45 67",
        "BillingStreet": "Hoofdstraat",
        "House_Number__c": "42",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussel",
        "BillingCountryCode": "BE",
        "BillingCountry": "Belgium",
        "IsActive__c": True,
    }


def _addressless_account() -> dict:
    """Account record after a minimal C3 upsert — no address, no email."""
    return {
        "Id": "001000000000002",
        "CRM_ID__c": "bbbbcccc-dddd-eeee-ffff-000000000000",
        "VAT_Number__c": "BE0987654321",
        "Name": "Minimal NV",
        "IsActive__c": True,
    }


@pytest.fixture
def sf_mock():
    return AsyncMock()


@contextmanager
def _patch_field_resolvers(
    *,
    email_field: str | None = "Email__c",
    has_house_number: bool = True,
    country_field: str = "BillingCountryCode",
):
    """Convenience patch-stack for the org-introspection helpers."""
    with (
        patch(
            "src.handlers.frontend_company_created._resolve_account_email_field",
            new=AsyncMock(return_value=email_field),
        ),
        patch(
            "src.handlers.frontend_company_created.has_account_house_number_field",
            new=AsyncMock(return_value=has_house_number),
        ),
        patch(
            "src.handlers.frontend_company_created._resolve_account_country_field",
            new=AsyncMock(return_value=country_field),
        ),
    ):
        yield


# ---------------------------------------------------------------------------
# handle() — integration-style tests against real builders (no mock of
# _build_company_data — we want regressions in that contract to surface here).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_payload_upserts_and_publishes_c14(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(return_value=_full_account()),
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(),
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        mock_upsert.assert_awaited_once()
        assert mock_upsert.call_args.args[1] == "BE0123456789"
        mock_publish.assert_awaited_once()
        msg.ack.assert_awaited_once()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_payload_c14_carries_full_address(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(return_value=_full_account()),
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(),
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        await handle(msg, sf_mock)

        published = mock_publish.call_args.args[0]
        assert published["street"] == "Hoofdstraat"
        assert published["houseNumber"] == "42"
        assert published["postalCode"] == "1000"
        assert published["city"] == "Brussel"
        assert published["country"] == "BE"
        assert published["email"] == "info@acme.be"


@pytest.mark.asyncio
async def test_minimal_payload_acks_without_publishing(sf_mock):
    """C3-XSD-valid minimal payload (no address) is consumed cleanly: SF
    Account is upserted, but C14 publish is deferred to the polling task —
    no DLQ, no retry storm."""
    parsed_xml = etree.fromstring(VALID_MINIMAL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(return_value=_addressless_account()),
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(),
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_MINIMAL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        mock_upsert.assert_awaited_once()
        mock_publish.assert_not_awaited()
        msg.ack.assert_awaited_once()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_xml_rejects_without_upsert_or_publish(sf_mock):
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            side_effect=ValueError("bad xml"),
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(),
        ) as mock_upsert,
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(),
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = INVALID_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.ack.assert_not_awaited()
        mock_upsert.assert_not_awaited()
        mock_publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_blank_required_fields_reject_without_upsert(sf_mock):
    """Defense-in-depth: even if validator slips through whitespace-only required
    fields, the handler must not call SF with an empty external-ID."""
    blank_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyCreated>
    <name>   </name>
    <vatNumber>BE0123456789</vatNumber>
</CompanyCreated>"""
    parsed_xml = etree.fromstring(blank_xml)
    with (
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(),
        ) as mock_upsert,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = blank_xml
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        await handle(msg, sf_mock)

        msg.reject.assert_awaited_once_with(requeue=False)
        mock_upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_upsert_raises_bubbles_no_ack_no_reject(sf_mock):
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(side_effect=RuntimeError("Salesforce down")),
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(),
        ) as mock_publish,
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(RuntimeError, match="Salesforce down"):
            await handle(msg, sf_mock)

        msg.ack.assert_not_awaited()
        msg.reject.assert_not_awaited()
        mock_publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_failure_bubbles_no_ack(sf_mock):
    """Generic publish failure (e.g. broker hiccup) must bubble for retry."""
    parsed_xml = etree.fromstring(VALID_FULL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_created.xml_validator.validate",
            return_value=parsed_xml,
        ),
        patch(
            "src.handlers.frontend_company_created.upsert_account_by_vat",
            new=AsyncMock(return_value=_full_account()),
        ),
        patch(
            "src.handlers.frontend_company_created.sender.publish_company_confirmed",
            new=AsyncMock(side_effect=RuntimeError("broker down")),
        ),
    ):
        from src.handlers.frontend_company_created import handle

        msg = AsyncMock()
        msg.body = VALID_FULL_XML
        msg.ack = AsyncMock()
        msg.reject = AsyncMock()

        with pytest.raises(RuntimeError, match="broker down"):
            await handle(msg, sf_mock)

        msg.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# _build_frontend_account_data — verifies dynamic SF field resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_account_data_full_xml_picklist_org(sf_mock):
    """Picklist-enabled org: country lands on BillingCountryCode (ISO-2)."""
    xml = etree.fromstring(VALID_FULL_XML)
    with _patch_field_resolvers(
        email_field="Email__c",
        has_house_number=True,
        country_field="BillingCountryCode",
    ):
        data = await _build_frontend_account_data(xml, sf_mock)
    assert data["Name"] == "Acme NV"
    assert data["VAT_Number__c"] == "BE0123456789"
    assert data["Email__c"] == "info@acme.be"
    assert data["Phone"] == "+32 2 123 45 67"
    assert data["BillingStreet"] == "Hoofdstraat"
    assert data["House_Number__c"] == "42"
    assert data["BillingPostalCode"] == "1000"
    assert data["BillingCity"] == "Brussel"
    assert data["BillingCountryCode"] == "BE"
    assert "BillingCountry" not in data


@pytest.mark.asyncio
async def test_build_account_data_full_xml_picklist_disabled_org(sf_mock):
    """Picklist-disabled org: country lands on free-text BillingCountry."""
    xml = etree.fromstring(VALID_FULL_XML)
    with _patch_field_resolvers(
        email_field="Email__c",
        has_house_number=True,
        country_field="BillingCountry",
    ):
        data = await _build_frontend_account_data(xml, sf_mock)
    assert data["BillingCountry"] == "BE"
    assert "BillingCountryCode" not in data


@pytest.mark.asyncio
async def test_build_account_data_falls_back_to_standard_email(sf_mock):
    """Org without Email__c: handler must use standard Email field instead."""
    xml = etree.fromstring(VALID_FULL_XML)
    with _patch_field_resolvers(email_field="Email"):
        data = await _build_frontend_account_data(xml, sf_mock)
    assert data["Email"] == "info@acme.be"
    assert "Email__c" not in data


@pytest.mark.asyncio
async def test_build_account_data_omits_email_when_org_has_no_email_field(sf_mock):
    xml = etree.fromstring(VALID_FULL_XML)
    with _patch_field_resolvers(email_field=None):
        data = await _build_frontend_account_data(xml, sf_mock)
    assert "Email" not in data
    assert "Email__c" not in data


@pytest.mark.asyncio
async def test_build_account_data_skips_house_number_when_field_absent(sf_mock):
    """Org without House_Number__c: handler skips that field rather than
    submitting an INVALID_FIELD payload."""
    xml = etree.fromstring(VALID_FULL_XML)
    with _patch_field_resolvers(has_house_number=False):
        data = await _build_frontend_account_data(xml, sf_mock)
    assert "House_Number__c" not in data
    assert data["BillingStreet"] == "Hoofdstraat"


@pytest.mark.asyncio
async def test_build_account_data_minimal_xml_omits_all_optional_fields(sf_mock):
    xml = etree.fromstring(VALID_MINIMAL_XML)
    with _patch_field_resolvers():
        data = await _build_frontend_account_data(xml, sf_mock)
    assert data == {"Name": "Minimal NV", "VAT_Number__c": "BE0987654321"}
