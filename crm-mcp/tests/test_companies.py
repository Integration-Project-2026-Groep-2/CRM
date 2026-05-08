"""Unit tests for company tools."""

from __future__ import annotations

import pytest

from crm_mcp.tools import companies as company_tools


@pytest.mark.asyncio
async def test_search_company_rejects_short_query(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="at least 2"):
        await company_tools.search_company(fake_sf_client, query="a")


@pytest.mark.asyncio
async def test_search_company_rejects_overlong_query(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="at most 200"):
        await company_tools.search_company(fake_sf_client, query="x" * 201)


@pytest.mark.asyncio
async def test_search_company_rejects_zero_or_negative_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await company_tools.search_company(fake_sf_client, query="Acme", limit=0)


@pytest.mark.asyncio
async def test_get_company_requires_one_input(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="vat_number or company_id"):
        await company_tools.get_company(fake_sf_client)


@pytest.mark.asyncio
async def test_get_company_validates_vat_format(fake_sf_client) -> None:
    """Regression: vat_number must be shape-validated, not only escaped."""
    with pytest.raises(ValueError, match="vat_number must match"):
        await company_tools.get_company(fake_sf_client, vat_number="not-a-vat")
    with pytest.raises(ValueError, match="vat_number must match"):
        await company_tools.get_company(fake_sf_client, vat_number="X" * 100)


@pytest.mark.asyncio
async def test_get_company_accepts_valid_vat(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    # Should not raise — just returns None when no record found.
    result = await company_tools.get_company(fake_sf_client, vat_number="BE0123456789")
    assert result is None


@pytest.mark.asyncio
async def test_get_company_by_vat(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000PqRStUAA",
                "Name": "Acme NV",
                "VAT_Number__c": "BE0123456789",
                "BillingStreet": "Stationsstraat 12",
                "BillingCity": "Brussel",
                "BillingPostalCode": "1000",
                "BillingCountryCode": "BE",
                "IsActive__c": True,
                "CreatedDate": "2026-01-15T08:00:00.000+0000",
                "LastModifiedDate": "2026-04-20T11:30:00.000+0000",
            },
        ]
    )

    result = await company_tools.get_company(fake_sf_client, vat_number="BE0123456789")

    assert result is not None
    assert result.id == "001gK00000PqRStUAA"
    assert result.name == "Acme NV"
    assert result.vat_number == "BE0123456789"
    assert result.country == "BE"
    assert result.is_active is True


@pytest.mark.asyncio
async def test_get_company_rejects_bad_company_id(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="001"):
        await company_tools.get_company(fake_sf_client, company_id="bogus")


@pytest.mark.asyncio
async def test_get_company_rejects_contact_id_prefix(fake_sf_client) -> None:
    """Regression: company_id with Contact prefix '003' must be rejected."""
    with pytest.raises(ValueError, match="001"):
        await company_tools.get_company(fake_sf_client, company_id="003gK00000XyZAbCAA")


@pytest.mark.asyncio
async def test_get_company_without_account_active_field(
    fake_sf_client, make_query_response
) -> None:
    """Regression: when no Account active field exists, is_active defaults to True."""
    fake_sf_client.get_account_active_field.return_value = None
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000PqRStUAA",
                "Name": "Acme NV",
                "VAT_Number__c": "BE0123456789",
                "BillingStreet": None,
                "BillingCity": None,
                "BillingPostalCode": None,
                "BillingCountryCode": "BE",
                "CreatedDate": "2026-01-15T08:00:00.000+0000",
                "LastModifiedDate": "2026-04-20T11:30:00.000+0000",
            },
        ]
    )

    result = await company_tools.get_company(fake_sf_client, vat_number="BE0123456789")

    assert result is not None
    assert result.is_active is True
    soql = fake_sf_client.query.await_args.args[0]
    assert "IsActive__c" not in soql
    assert "Active__c" not in soql


@pytest.mark.asyncio
async def test_search_company_returns_summaries(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000PqRStUAA",
                "Name": "Acme NV",
                "VAT_Number__c": "BE0123456789",
                "BillingCountryCode": "BE",
            },
        ]
    )

    results = await company_tools.search_company(fake_sf_client, query="Acme")

    assert len(results) == 1
    assert results[0].name == "Acme NV"
    assert results[0].vat_number == "BE0123456789"
    assert results[0].country == "BE"


# ---- Write-tools (R2) ----


_VALID_CREATE_COMPANY_KWARGS = dict(
    name="TestNV",
    vat_number="BE0123456789",
    email="info@testnv.be",
    street="Stationsstraat",
    house_number="12",
    postal_code="1000",
    city="Brussel",
    country="BE",
)


@pytest.mark.asyncio
async def test_create_company_happy_path(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    result = await company_tools.create_company(
        fake_sf_client, fake_publisher, **_VALID_CREATE_COMPANY_KWARGS
    )

    assert result.success is True
    assert result.routing_key == "crm.company.confirmed"
    assert result.salesforce_id == "001gK00000NewAcc1AAA"
    assert len(result.id) == 36  # UUID v4
    fake_sf_client.create_account.assert_awaited_once()
    payload = fake_sf_client.create_account.await_args.args[0]
    assert payload["VAT_Number__c"] == "BE0123456789"
    assert payload["CRM_ID__c"] == result.id
    assert payload["BillingStreet"] == "Stationsstraat"  # no concat
    assert payload["House_Number__c"] == "12"  # separate custom field
    assert payload["BillingCountryCode"] == "BE"  # via dynamic probe
    assert payload["Email__c"] == "info@testnv.be"  # dynamic email field
    assert payload["IsActive__c"] is True
    fake_publisher.publish_company_confirmed.assert_called_once()
    broadcast = fake_publisher.publish_company_confirmed.call_args.args[0]
    assert broadcast["id"] == result.id
    assert broadcast["vatNumber"] == "BE0123456789"
    assert broadcast["isActive"] is True
    assert "confirmedAt" in broadcast


@pytest.mark.asyncio
async def test_create_company_rejects_duplicate_vat(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([{"Id": "001gK00000ExistAA"}])

    with pytest.raises(ValueError, match="already exists"):
        await company_tools.create_company(
            fake_sf_client, fake_publisher, **_VALID_CREATE_COMPANY_KWARGS
        )
    fake_sf_client.create_account.assert_not_awaited()
    fake_publisher.publish_company_confirmed.assert_not_called()


@pytest.mark.asyncio
async def test_create_company_rejects_invalid_vat(fake_sf_client, fake_publisher) -> None:
    bad_kwargs = {**_VALID_CREATE_COMPANY_KWARGS, "vat_number": "NOT-A-VAT"}
    with pytest.raises(ValueError):
        await company_tools.create_company(fake_sf_client, fake_publisher, **bad_kwargs)
    fake_publisher.publish_company_confirmed.assert_not_called()


@pytest.mark.asyncio
async def test_create_company_rejects_empty_name(fake_sf_client, fake_publisher) -> None:
    bad_kwargs = {**_VALID_CREATE_COMPANY_KWARGS, "name": "  "}
    with pytest.raises(ValueError, match="name must not be empty"):
        await company_tools.create_company(fake_sf_client, fake_publisher, **bad_kwargs)


@pytest.mark.asyncio
async def test_update_company_happy_path(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {"Id": "001gK00000ExistAA", "Name": "Old Name", "VAT_Number__c": "BE0123456789"},
        ]
    )

    result = await company_tools.update_company(
        fake_sf_client,
        fake_publisher,
        crm_id="11111111-2222-4333-8444-555555555555",
        name="New Name",
        city="Antwerpen",
    )

    assert result.routing_key == "crm.company.updated"
    fake_sf_client.update_account.assert_awaited_once_with(
        "001gK00000ExistAA",
        {"Name": "New Name", "BillingCity": "Antwerpen"},
    )
    broadcast = fake_publisher.publish_company_updated.call_args.args[0]
    assert broadcast["name"] == "New Name"
    assert broadcast["vatNumber"] == "BE0123456789"
    assert broadcast["isActive"] is True
    assert broadcast["city"] == "Antwerpen"
    assert "updatedAt" in broadcast


@pytest.mark.asyncio
async def test_update_company_unknown_crm_id_raises(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    with pytest.raises(ValueError, match="no company found"):
        await company_tools.update_company(
            fake_sf_client, fake_publisher, crm_id="missing-uuid", name="X"
        )
    fake_sf_client.update_account.assert_not_awaited()
    fake_publisher.publish_company_updated.assert_not_called()


@pytest.mark.asyncio
async def test_create_company_omits_email_when_no_account_email_field(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    fake_sf_client.get_account_email_field.return_value = None  # no Email__c, no Email

    await company_tools.create_company(
        fake_sf_client, fake_publisher, **_VALID_CREATE_COMPANY_KWARGS
    )

    payload = fake_sf_client.create_account.await_args.args[0]
    assert "Email__c" not in payload
    assert "Email" not in payload
    # broadcast still carries email per XSD requirement
    broadcast = fake_publisher.publish_company_confirmed.call_args.args[0]
    assert broadcast["email"] == "info@testnv.be"


@pytest.mark.asyncio
async def test_create_company_uses_billing_country_when_no_picklist(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    fake_sf_client.get_account_country_field.return_value = "BillingCountry"

    await company_tools.create_company(
        fake_sf_client, fake_publisher, **_VALID_CREATE_COMPANY_KWARGS
    )

    payload = fake_sf_client.create_account.await_args.args[0]
    assert payload["BillingCountry"] == "BE"
    assert "BillingCountryCode" not in payload


@pytest.mark.asyncio
async def test_create_company_skips_house_number_when_field_absent(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    fake_sf_client.has_account_house_number_field.return_value = False

    await company_tools.create_company(
        fake_sf_client, fake_publisher, **_VALID_CREATE_COMPANY_KWARGS
    )

    payload = fake_sf_client.create_account.await_args.args[0]
    assert "House_Number__c" not in payload
    assert payload["BillingStreet"] == "Stationsstraat"


@pytest.mark.asyncio
async def test_delete_company_soft_default(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {"Id": "001gK00000ExistAA", "Name": "X", "VAT_Number__c": "BE0123456789"},
        ]
    )

    result = await company_tools.delete_company(
        fake_sf_client, fake_publisher, crm_id="11111111-2222-4333-8444-555555555555"
    )

    assert result.routing_key == "crm.company.deactivated"
    fake_sf_client.update_account.assert_awaited_once_with(
        "001gK00000ExistAA", {"IsActive__c": False}
    )
    broadcast = fake_publisher.publish_company_deactivated.call_args.args[0]
    assert broadcast["vatNumber"] == "BE0123456789"
    assert "deactivatedAt" in broadcast


@pytest.mark.asyncio
async def test_delete_company_rejects_hard(fake_sf_client, fake_publisher) -> None:
    with pytest.raises(ValueError, match="hard-delete not supported"):
        await company_tools.delete_company(fake_sf_client, fake_publisher, crm_id="abc", hard=True)
    fake_sf_client.update_account.assert_not_awaited()
    fake_publisher.publish_company_deactivated.assert_not_called()
