from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

from src.handlers._exceptions import MissingDependencyError

_CRM_ID = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"

VALID_FULL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <phone>+32 2 123 45 67</phone>
    <street>Hoofdstraat</street>
    <houseNumber>42</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussel</city>
    <country>BE</country>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

# Only the required fields — every optional field omitted (the natural
# "edit just the name" case). Must NOT clobber the existing address/phone.
PARTIAL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme Renamed NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

# <city> present but empty → explicit clear.
PRESENT_EMPTY_CITY_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <city></city>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

# Whitespace-padded boolean — XSD-valid (xs:boolean collapse facet), must
# be read as active, not deactivated.
WHITESPACE_ISACTIVE_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <isActive> true </isActive>
</FrontendCompanyUpdated>"""

DEACTIVATE_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <isActive>false</isActive>
</FrontendCompanyUpdated>"""

BLANK_NAME_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>   </name>
    <vatNumber>BE0123456789</vatNumber>
    <email>info@acme.be</email>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

MISSING_EMAIL_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

# vatNumber before name — out of canonical order; the lenient reorder shim
# must still accept it (tolerance for out-of-spec producers).
OUT_OF_ORDER_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<FrontendCompanyUpdated>
    <id>a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme NV</name>
    <email>info@acme.be</email>
    <isActive>true</isActive>
</FrontendCompanyUpdated>"""

INVALID_XML = b"not xml at all"


def _active_account() -> dict:
    return {
        "Id": "001000000000001",
        "CRM_ID__c": _CRM_ID,
        "VAT_Number__c": "BE0123456789",
        "Name": "Acme NV",
        "Email__c": "info@acme.be",
        "Phone": "+32 2 123 45 67",
        "BillingStreet": "Hoofdstraat",
        "House_Number__c": "42",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussel",
        "BillingCountryCode": "BE",
        "IsActive__c": True,
    }


def _inactive_account() -> dict:
    account = _active_account()
    account["IsActive__c"] = False
    return account


def _deactivated_account() -> dict:
    return {
        "Id": "001000000000001",
        "CRM_ID__c": _CRM_ID,
        "VAT_Number__c": "BE0123456789",
        "Name": "Acme NV",
        "IsActive__c": False,
    }


@pytest.fixture
def sf_mock():
    return AsyncMock()


def _msg(body: bytes) -> AsyncMock:
    msg = AsyncMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


@contextmanager
def _patch_field_resolvers(
    *,
    email_field: str | None = "Email__c",
    has_house_number: bool = True,
    country_field: str = "BillingCountryCode",
):
    with (
        patch(
            "src.handlers.frontend_company_updated._resolve_account_email_field",
            new=AsyncMock(return_value=email_field),
        ),
        patch(
            "src.handlers.frontend_company_updated.has_account_house_number_field",
            new=AsyncMock(return_value=has_house_number),
        ),
        patch(
            "src.handlers.frontend_company_updated._resolve_account_country_field",
            new=AsyncMock(return_value=country_field),
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_full_update_resolves_unique_and_publishes_c19(sf_mock):
    parsed = etree.fromstring(VALID_FULL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(return_value=_active_account()),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(VALID_FULL_XML)
        await handle(msg, sf_mock)

        mock_patch.assert_awaited_once()
        data = mock_patch.call_args.args[2]
        assert data["Name"] == "Acme NV"
        assert data["VAT_Number__c"] == "BE0123456789"
        assert data["Email__c"] == "info@acme.be"
        assert data["BillingStreet"] == "Hoofdstraat"
        assert data["House_Number__c"] == "42"
        assert data["BillingPostalCode"] == "1000"
        assert data["BillingCity"] == "Brussel"
        assert data["BillingCountryCode"] == "BE"
        assert data["Phone"] == "+32 2 123 45 67"

        mock_pub.assert_awaited_once()
        payload = mock_pub.call_args.args[0]
        assert payload["id"] == _CRM_ID
        assert payload["isActive"] is True

        msg.ack.assert_awaited_once()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_partial_update_omits_absent_fields(sf_mock):
    """Critical regression: a valid edit that omits optional fields must NOT
    clear them — they simply must not appear in the patch dict."""
    parsed = etree.fromstring(PARTIAL_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(return_value=_active_account()),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ),
    ):
        from src.handlers.frontend_company_updated import handle

        await handle(_msg(PARTIAL_XML), sf_mock)

        data = mock_patch.call_args.args[2]
        assert data["Name"] == "Acme Renamed NV"
        assert data["VAT_Number__c"] == "BE0123456789"
        assert data["Email__c"] == "info@acme.be"
        for absent in (
            "Phone",
            "BillingStreet",
            "House_Number__c",
            "BillingPostalCode",
            "BillingCity",
            "BillingCountryCode",
            "BillingCountry",
        ):
            assert absent not in data


@pytest.mark.asyncio
async def test_present_empty_field_clears(sf_mock):
    """An explicitly empty element is a clear (value None), not a skip."""
    parsed = etree.fromstring(PRESENT_EMPTY_CITY_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(return_value=_active_account()),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ),
    ):
        from src.handlers.frontend_company_updated import handle

        await handle(_msg(PRESENT_EMPTY_CITY_XML), sf_mock)

        data = mock_patch.call_args.args[2]
        assert "BillingCity" in data
        assert data["BillingCity"] is None
        assert "BillingStreet" not in data


@pytest.mark.asyncio
async def test_isactive_whitespace_treated_as_active(sf_mock):
    """Regression: a whitespace-padded but XSD-valid boolean must take the
    update path, not a wrongful deactivation."""
    parsed = etree.fromstring(WHITESPACE_ISACTIVE_XML)
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.deactivate_account_record",
            new=AsyncMock(),
        ) as mock_deactivate,
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(return_value=_active_account()),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub_upd,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_deactivated",
            new=AsyncMock(),
        ) as mock_pub_deact,
    ):
        from src.handlers.frontend_company_updated import handle

        await handle(_msg(WHITESPACE_ISACTIVE_XML), sf_mock)

        mock_deactivate.assert_not_awaited()
        mock_pub_deact.assert_not_awaited()
        mock_patch.assert_awaited_once()
        mock_pub_upd.assert_awaited_once()


@pytest.mark.asyncio
async def test_isactive_false_deactivates_and_publishes_c23(sf_mock):
    parsed = etree.fromstring(DEACTIVATE_XML)
    with (
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.deactivate_account_record",
            new=AsyncMock(return_value=_deactivated_account()),
        ) as mock_deactivate,
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_deactivated",
            new=AsyncMock(),
        ) as mock_pub_deact,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub_upd,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(DEACTIVATE_XML)
        await handle(msg, sf_mock)

        mock_deactivate.assert_awaited_once()
        mock_pub_deact.assert_awaited_once()
        assert mock_pub_deact.call_args.args[0]["id"] == _CRM_ID
        mock_patch.assert_not_awaited()
        mock_pub_upd.assert_not_awaited()
        msg.ack.assert_awaited_once()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_crm_id_raises_missing_dependency(sf_mock):
    parsed = etree.fromstring(VALID_FULL_XML)
    with (
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("none", None)),
        ),
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(VALID_FULL_XML)
        with pytest.raises(MissingDependencyError):
            await handle(msg, sf_mock)

        mock_pub.assert_not_awaited()
        msg.ack.assert_not_awaited()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_crm_id_acks_without_update(sf_mock):
    parsed = etree.fromstring(VALID_FULL_XML)
    with (
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("ambiguous", None)),
        ),
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(),
        ) as mock_patch,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(VALID_FULL_XML)
        await handle(msg, sf_mock)

        mock_patch.assert_not_awaited()
        mock_pub.assert_not_awaited()
        msg.ack.assert_awaited_once()
        msg.reject.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_xml_rejects_without_lookup(sf_mock):
    with (
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            side_effect=ValueError("bad xml"),
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(),
        ) as mock_lookup,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(INVALID_XML)
        await handle(msg, sf_mock)

        msg.reject.assert_awaited_once_with(requeue=False)
        msg.ack.assert_not_awaited()
        mock_lookup.assert_not_awaited()
        mock_pub.assert_not_awaited()


@pytest.mark.asyncio
async def test_blank_name_rejects_without_lookup(sf_mock):
    parsed = etree.fromstring(BLANK_NAME_XML)
    with (
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(),
        ) as mock_lookup,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(BLANK_NAME_XML)
        await handle(msg, sf_mock)

        msg.reject.assert_awaited_once_with(requeue=False)
        mock_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_reactivation_when_patch_returns_inactive(sf_mock):
    parsed = etree.fromstring(VALID_FULL_XML)
    sf_mock.Account.update = MagicMock(return_value=None)
    sf_mock.Account.get = MagicMock(return_value=_active_account())
    with (
        _patch_field_resolvers(),
        patch(
            "src.handlers.frontend_company_updated.xml_validator.validate",
            return_value=parsed,
        ),
        patch(
            "src.handlers.frontend_company_updated.get_account_match_by_crm_id",
            new=AsyncMock(return_value=("unique", _active_account())),
        ),
        patch(
            "src.handlers.frontend_company_updated.patch_account_fields",
            new=AsyncMock(return_value=_inactive_account()),
        ),
        patch(
            "src.handlers.frontend_company_updated.apply_account_is_active",
            new=AsyncMock(return_value={"IsActive__c": True}),
        ) as mock_apply,
        patch(
            "src.handlers.frontend_company_updated.sender.publish_company_updated",
            new=AsyncMock(),
        ) as mock_pub,
    ):
        from src.handlers.frontend_company_updated import handle

        msg = _msg(VALID_FULL_XML)
        await handle(msg, sf_mock)

        mock_apply.assert_awaited_once()
        sf_mock.Account.update.assert_called_once()
        mock_pub.assert_awaited_once()
        assert mock_pub.call_args.args[0]["isActive"] is True
        msg.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# XSD — the new inbound contract must validate against the real schema
# ---------------------------------------------------------------------------


def test_schema_accepts_valid_frontend_company_updated():
    from src.xml_validator import validate

    doc = validate(VALID_FULL_XML)
    assert doc.tag == "FrontendCompanyUpdated"
    assert doc.findtext("isActive") == "true"


def test_schema_rejects_missing_email():
    from src.xml_validator import validate

    with pytest.raises(ValueError):
        validate(MISSING_EMAIL_XML)


def test_canonical_order_needs_no_reorder():
    """The documented order (name before vatNumber) is the canonical XSD order,
    so a conforming payload must validate without tripping the out-of-spec
    reorder shim."""
    from src.xml_validator import reorder_was_applied, validate

    validate(VALID_FULL_XML)
    assert reorder_was_applied() is False


def test_out_of_order_payload_still_validates_via_reorder():
    from src.xml_validator import reorder_was_applied, validate

    doc = validate(OUT_OF_ORDER_XML)
    assert doc.tag == "FrontendCompanyUpdated"
    assert reorder_was_applied() is True
