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
    fake_sf_client.query.return_value = make_query_response([
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
    ])

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
        await company_tools.get_company(
            fake_sf_client, company_id="003gK00000XyZAbCAA"
        )


@pytest.mark.asyncio
async def test_get_company_without_account_active_field(
    fake_sf_client, make_query_response
) -> None:
    """Regression: when no Account active field exists, is_active defaults to True."""
    fake_sf_client.get_account_active_field.return_value = None
    fake_sf_client.query.return_value = make_query_response([
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
    ])

    result = await company_tools.get_company(fake_sf_client, vat_number="BE0123456789")

    assert result is not None
    assert result.is_active is True
    soql = fake_sf_client.query.await_args.args[0]
    assert "IsActive__c" not in soql
    assert "Active__c" not in soql


@pytest.mark.asyncio
async def test_search_company_returns_summaries(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response([
        {
            "Id": "001gK00000PqRStUAA",
            "Name": "Acme NV",
            "VAT_Number__c": "BE0123456789",
            "BillingCountryCode": "BE",
        },
    ])

    results = await company_tools.search_company(fake_sf_client, query="Acme")

    assert len(results) == 1
    assert results[0].name == "Acme NV"
    assert results[0].vat_number == "BE0123456789"
    assert results[0].country == "BE"
