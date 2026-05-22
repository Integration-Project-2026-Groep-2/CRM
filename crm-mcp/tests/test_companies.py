"""Unit tests for company tools."""

from __future__ import annotations

from unittest.mock import AsyncMock

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


# ---- get_company_contacts ----


@pytest.mark.asyncio
async def test_get_company_contacts_requires_one_input(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="vat_number or company_id"):
        await company_tools.get_company_contacts(fake_sf_client)


@pytest.mark.asyncio
async def test_get_company_contacts_rejects_bad_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await company_tools.get_company_contacts(
            fake_sf_client, company_id="001gK00000ExistAA", limit=0
        )


@pytest.mark.asyncio
async def test_get_company_contacts_rejects_invalid_role(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="role must be one of"):
        await company_tools.get_company_contacts(
            fake_sf_client, company_id="001gK00000ExistAA", role="JANITOR"
        )


@pytest.mark.asyncio
async def test_get_company_contacts_company_not_found_by_vat_raises(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    with pytest.raises(ValueError, match="no company found"):
        await company_tools.get_company_contacts(fake_sf_client, vat_number="BE0123456789")


@pytest.mark.asyncio
async def test_get_company_contacts_company_not_found_by_id_raises(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    with pytest.raises(ValueError, match="no company found"):
        await company_tools.get_company_contacts(
            fake_sf_client, company_id="001gK00000ExistAA"
        )


@pytest.mark.asyncio
async def test_get_company_contacts_returns_summaries(
    fake_sf_client, make_query_response
) -> None:
    # First call: resolve account by SF Id; second call: fetch contacts.
    fake_sf_client.query.side_effect = [
        make_query_response(
            [{"Id": "001gK00000ExistAA", "CRM_ID__c": "uuid-1", "Name": "Acme", "VAT_Number__c": "BE01"}]
        ),
        make_query_response(
            [
                {
                    "Id": "003gK00000Con1AAA",
                    "Name": "Alice",
                    "Email": "alice@acme.be",
                    "Role__c": "COMPANY_CONTACT",
                    "IsActive__c": True,
                    "Paid_At__c": None,
                },
                {
                    "Id": "003gK00000Con2AAA",
                    "Name": "Bob",
                    "Email": "bob@acme.be",
                    "Role__c": "VISITOR",
                    "IsActive__c": True,
                    "Paid_At__c": "2026-05-01T10:00:00.000+0000",
                },
            ]
        ),
    ]

    results = await company_tools.get_company_contacts(
        fake_sf_client, company_id="001gK00000ExistAA"
    )

    assert len(results) == 2
    assert results[0].name == "Alice"
    assert results[0].role == "COMPANY_CONTACT"
    assert results[0].is_active is True
    assert results[0].paid_at is None
    assert results[1].name == "Bob"
    assert results[1].paid_at is not None


@pytest.mark.asyncio
async def test_get_company_contacts_by_vat(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([{"Id": "001gK00000ExistAA"}]),  # VAT lookup
        make_query_response([]),  # contacts (empty)
    ]

    results = await company_tools.get_company_contacts(
        fake_sf_client, vat_number="BE0123456789"
    )

    assert results == []
    # Verify first query used VAT_Number__c
    first_soql = fake_sf_client.query.call_args_list[0].args[0]
    assert "VAT_Number__c" in first_soql
    # Verify second query used the resolved AccountId
    second_soql = fake_sf_client.query.call_args_list[1].args[0]
    assert "AccountId = '001gK00000ExistAA'" in second_soql


@pytest.mark.asyncio
async def test_get_company_contacts_active_filter_in_soql(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([{"Id": "001gK00000ExistAA", "CRM_ID__c": "u", "Name": "X", "VAT_Number__c": "BE01"}]),
        make_query_response([]),
    ]

    await company_tools.get_company_contacts(
        fake_sf_client, company_id="001gK00000ExistAA", is_active=True
    )

    contact_soql = fake_sf_client.query.call_args_list[1].args[0]
    assert "IsActive__c = true" in contact_soql


@pytest.mark.asyncio
async def test_get_company_contacts_role_filter_in_soql(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([{"Id": "001gK00000ExistAA", "CRM_ID__c": "u", "Name": "X", "VAT_Number__c": "BE01"}]),
        make_query_response([]),
    ]

    await company_tools.get_company_contacts(
        fake_sf_client, company_id="001gK00000ExistAA", role="VISITOR"
    )

    contact_soql = fake_sf_client.query.call_args_list[1].args[0]
    assert "Role__c = 'VISITOR'" in contact_soql


@pytest.mark.asyncio
async def test_get_company_contacts_caps_limit(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([{"Id": "001gK00000ExistAA", "CRM_ID__c": "u", "Name": "X", "VAT_Number__c": "BE01"}]),
        make_query_response([]),
    ]

    await company_tools.get_company_contacts(
        fake_sf_client, company_id="001gK00000ExistAA", limit=500
    )

    contact_soql = fake_sf_client.query.call_args_list[1].args[0]
    assert "LIMIT 100" in contact_soql


# ---- list_companies ----


@pytest.mark.asyncio
async def test_list_companies_rejects_bad_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await company_tools.list_companies(fake_sf_client, limit=0)


@pytest.mark.asyncio
async def test_list_companies_rejects_negative_offset(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="offset must be non-negative"):
        await company_tools.list_companies(fake_sf_client, offset=-1)


@pytest.mark.asyncio
async def test_list_companies_rejects_invalid_country(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="ISO 3166-1 alpha-2"):
        await company_tools.list_companies(fake_sf_client, country="BEL")
    with pytest.raises(ValueError, match="ISO 3166-1 alpha-2"):
        await company_tools.list_companies(fake_sf_client, country="B1")


@pytest.mark.asyncio
async def test_list_companies_returns_summaries(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {"Id": "001gK00000AAA", "Name": "Acme NV", "VAT_Number__c": "BE01", "BillingCountryCode": "BE"},
            {"Id": "001gK00000BBB", "Name": "Beta BV", "VAT_Number__c": "NL01", "BillingCountryCode": "NL"},
        ]
    )

    results = await company_tools.list_companies(fake_sf_client)

    assert len(results) == 2
    assert results[0].name == "Acme NV"
    assert results[1].country == "NL"


@pytest.mark.asyncio
async def test_list_companies_country_filter_in_soql(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.list_companies(fake_sf_client, country="be")  # lowercase

    soql = fake_sf_client.query.await_args.args[0]
    assert "BillingCountryCode = 'BE'" in soql  # normalised to uppercase


@pytest.mark.asyncio
async def test_list_companies_active_filter_in_soql(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.list_companies(fake_sf_client, is_active=False)

    soql = fake_sf_client.query.await_args.args[0]
    assert "IsActive__c = false" in soql


@pytest.mark.asyncio
async def test_list_companies_no_active_field_skips_filter(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.get_account_active_field.return_value = None
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.list_companies(fake_sf_client, is_active=True)

    soql = fake_sf_client.query.await_args.args[0]
    assert "IsActive__c" not in soql
    assert "Active__c" not in soql


@pytest.mark.asyncio
async def test_list_companies_pagination_offset(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.list_companies(fake_sf_client, limit=10, offset=20)

    soql = fake_sf_client.query.await_args.args[0]
    assert "LIMIT 10" in soql
    assert "OFFSET 20" in soql


@pytest.mark.asyncio
async def test_list_companies_caps_limit(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.list_companies(fake_sf_client, limit=999)

    soql = fake_sf_client.query.await_args.args[0]
    assert "LIMIT 100" in soql


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
            {
                "Id": "001gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Name": "Old Name",
                "VAT_Number__c": "BE0123456789",
            },
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
            {
                "Id": "001gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Name": "X",
                "VAT_Number__c": "BE0123456789",
            },
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


@pytest.mark.asyncio
async def test_update_company_null_crm_id_raises(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Resolved by SF Id but CRM_ID__c empty → refuse, never broadcast "None"."""
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000ExistAA",
                "CRM_ID__c": None,
                "Name": "Acme",
                "VAT_Number__c": "BE0123456789",
            },
        ]
    )

    with pytest.raises(ValueError, match="CRM_ID__c"):
        await company_tools.update_company(
            fake_sf_client, fake_publisher, crm_id="001gK00000ExistAA", name="New"
        )

    fake_sf_client.update_account.assert_not_awaited()
    fake_publisher.publish_company_updated.assert_not_called()


@pytest.mark.asyncio
async def test_delete_company_null_crm_id_raises(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000ExistAA",
                "CRM_ID__c": None,
                "Name": "Acme",
                "VAT_Number__c": "BE0123456789",
            },
        ]
    )

    with pytest.raises(ValueError, match="CRM_ID__c"):
        await company_tools.delete_company(
            fake_sf_client, fake_publisher, crm_id="001gK00000ExistAA"
        )

    fake_sf_client.update_account.assert_not_awaited()
    fake_publisher.publish_company_deactivated.assert_not_called()


@pytest.mark.asyncio
async def test_update_company_preserves_existing_is_active(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Partial update of a soft-deleted company must not broadcast isActive=True."""
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Name": "Acme",
                "VAT_Number__c": "BE0123456789",
                "IsActive__c": False,
            },
        ]
    )

    await company_tools.update_company(
        fake_sf_client,
        fake_publisher,
        crm_id="11111111-2222-4333-8444-555555555555",
        name="New",
    )

    # The resolve must SELECT the active flag — the mock returns the record
    # regardless of SOQL, so without this assertion the missing column hides.
    assert "IsActive__c" in fake_sf_client.query.await_args.args[0]
    broadcast = fake_publisher.publish_company_updated.call_args.args[0]
    assert broadcast["isActive"] is False
    payload = fake_sf_client.update_account.await_args.args[1]
    assert "IsActive__c" not in payload


@pytest.mark.asyncio
async def test_update_company_accepts_sf_id(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Plan B — SF Id (001-prefix, 18 chars) input resolves to canonical UUID."""
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001dM00003kxOrvQAE",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Name": "Old Name",
                "VAT_Number__c": "BE0123456789",
            },
        ]
    )

    result = await company_tools.update_company(
        fake_sf_client,
        fake_publisher,
        crm_id="001dM00003kxOrvQAE",  # SF Id (18 char), not UUID
        city="Antwerpen",
    )

    soql_arg = fake_sf_client.query.call_args.args[0]
    assert "Id = '001dM00003kxOrvQAE'" in soql_arg
    assert "CRM_ID__c =" not in soql_arg
    assert result.id == "11111111-2222-4333-8444-555555555555"
    broadcast = fake_publisher.publish_company_updated.call_args.args[0]
    assert broadcast["id"] == "11111111-2222-4333-8444-555555555555"


@pytest.mark.asyncio
async def test_delete_company_accepts_sf_id(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001dM00003kxOrvQAE",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Name": "X",
                "VAT_Number__c": "BE0123456789",
            },
        ]
    )

    result = await company_tools.delete_company(
        fake_sf_client, fake_publisher, crm_id="001dM00003kxOrvQAE"
    )

    soql_arg = fake_sf_client.query.call_args.args[0]
    assert "Id = '001dM00003kxOrvQAE'" in soql_arg
    assert result.id == "11111111-2222-4333-8444-555555555555"
    broadcast = fake_publisher.publish_company_deactivated.call_args.args[0]
    assert broadcast["id"] == "11111111-2222-4333-8444-555555555555"


# ---- count_companies ----


@pytest.mark.asyncio
async def test_count_companies_rejects_invalid_country(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="ISO 3166-1 alpha-2"):
        await company_tools.count_companies(fake_sf_client, country="BEL")


@pytest.mark.asyncio
async def test_count_companies_happy_path(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(side_effect=[50, 40])

    result = await company_tools.count_companies(fake_sf_client)

    assert result.total == 50
    assert result.active == 40
    assert result.inactive == 10


@pytest.mark.asyncio
async def test_count_companies_no_active_field_returns_zeros(fake_sf_client) -> None:
    fake_sf_client.get_account_active_field.return_value = None
    fake_sf_client.query_count = AsyncMock(return_value=25)

    result = await company_tools.count_companies(fake_sf_client)

    assert result.total == 25
    assert result.active == 0
    assert result.inactive == 0
    fake_sf_client.query_count.assert_awaited_once()  # only total query fired


@pytest.mark.asyncio
async def test_count_companies_country_filter_in_soql(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(side_effect=[10, 8])

    result = await company_tools.count_companies(fake_sf_client, country="be")

    # Both queries must contain the country filter normalised to uppercase
    calls = fake_sf_client.query_count.await_args_list
    assert all("BillingCountryCode = 'BE'" in call.args[0] for call in calls)
    assert result.total == 10


@pytest.mark.asyncio
async def test_count_companies_fires_two_queries_with_active_field(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(return_value=0)

    await company_tools.count_companies(fake_sf_client)

    assert fake_sf_client.query_count.await_count == 2


# ---- get_recent_companies ----


@pytest.mark.asyncio
async def test_get_recent_companies_rejects_bad_hours(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="since_hours"):
        await company_tools.get_recent_companies(fake_sf_client, since_hours=0)


@pytest.mark.asyncio
async def test_get_recent_companies_rejects_bad_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit"):
        await company_tools.get_recent_companies(fake_sf_client, limit=0)


@pytest.mark.asyncio
async def test_get_recent_companies_mode_created_uses_created_date(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.get_recent_companies(fake_sf_client, mode="created")

    soql = fake_sf_client.query.await_args.args[0]
    assert "CreatedDate" in soql
    assert "LastModifiedDate" not in soql


@pytest.mark.asyncio
async def test_get_recent_companies_mode_modified_uses_last_modified(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.get_recent_companies(fake_sf_client, mode="modified")

    soql = fake_sf_client.query.await_args.args[0]
    assert "LastModifiedDate" in soql


@pytest.mark.asyncio
async def test_get_recent_companies_returns_summaries(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000AAA",
                "Name": "Acme NV",
                "VAT_Number__c": "BE0123456789",
                "BillingCountryCode": "BE",
                "LastModifiedDate": "2026-05-10T09:00:00.000+0000",
            }
        ]
    )

    results = await company_tools.get_recent_companies(fake_sf_client)

    assert len(results) == 1
    assert results[0].name == "Acme NV"
    assert results[0].country == "BE"
    assert results[0].last_modified_at is not None


@pytest.mark.asyncio
async def test_get_recent_companies_caps_hours_and_limit(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    await company_tools.get_recent_companies(fake_sf_client, since_hours=999, limit=999)

    soql = fake_sf_client.query.await_args.args[0]
    assert "LIMIT 100" in soql
    # threshold must be at most 168 h back — verify '>=' appears (not 999h back)
    assert ">=" in soql


# ---- get_company_profile ----


@pytest.mark.asyncio
async def test_get_company_profile_requires_one_input(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="vat_number or company_id"):
        await company_tools.get_company_profile(fake_sf_client)


@pytest.mark.asyncio
async def test_get_company_profile_returns_none_when_not_found(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    result = await company_tools.get_company_profile(fake_sf_client, vat_number="BE0123456789")

    assert result is None


@pytest.mark.asyncio
async def test_get_company_profile_happy_path(
    fake_sf_client, make_query_response
) -> None:
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
            }
        ]
    )
    fake_sf_client.query_count = AsyncMock(return_value=5)

    result = await company_tools.get_company_profile(fake_sf_client, vat_number="BE0123456789")

    assert result is not None
    assert result.id == "001gK00000PqRStUAA"
    assert result.name == "Acme NV"
    assert result.city == "Brussel"
    assert result.is_active is True
    assert result.contact_count == 5


@pytest.mark.asyncio
async def test_get_company_profile_fires_two_queries(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000PqRStUAA",
                "Name": "X",
                "VAT_Number__c": "BE0123456789",
                "BillingStreet": None,
                "BillingCity": None,
                "BillingPostalCode": None,
                "BillingCountryCode": "BE",
                "IsActive__c": True,
                "CreatedDate": "2026-01-15T08:00:00.000+0000",
                "LastModifiedDate": "2026-04-20T11:30:00.000+0000",
            }
        ]
    )
    fake_sf_client.query_count = AsyncMock(return_value=0)

    await company_tools.get_company_profile(fake_sf_client, vat_number="BE0123456789")

    fake_sf_client.query.assert_awaited_once()
    fake_sf_client.query_count.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_company_profile_contact_count_uses_account_id(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "001gK00000PqRStUAA",
                "Name": "X",
                "VAT_Number__c": "BE0123456789",
                "BillingStreet": None,
                "BillingCity": None,
                "BillingPostalCode": None,
                "BillingCountryCode": "BE",
                "IsActive__c": True,
                "CreatedDate": "2026-01-15T08:00:00.000+0000",
                "LastModifiedDate": "2026-04-20T11:30:00.000+0000",
            }
        ]
    )
    fake_sf_client.query_count = AsyncMock(return_value=3)

    await company_tools.get_company_profile(fake_sf_client, vat_number="BE0123456789")

    count_soql = fake_sf_client.query_count.await_args.args[0]
    assert "AccountId = '001gK00000PqRStUAA'" in count_soql
