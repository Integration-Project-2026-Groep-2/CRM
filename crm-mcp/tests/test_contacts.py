"""Unit tests for contact tools."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from crm_mcp.tools import contacts as contact_tools


@pytest.mark.asyncio
async def test_search_contact_rejects_short_query(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="at least 2"):
        await contact_tools.search_contact(fake_sf_client, query="a")


@pytest.mark.asyncio
async def test_search_contact_rejects_overlong_query(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="at most 200"):
        await contact_tools.search_contact(fake_sf_client, query="x" * 201)


@pytest.mark.asyncio
async def test_search_contact_rejects_zero_or_negative_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await contact_tools.search_contact(fake_sf_client, query="Jan", limit=0)
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await contact_tools.search_contact(fake_sf_client, query="Jan", limit=-5)


@pytest.mark.asyncio
async def test_search_contact_returns_summaries(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response([
        {
            "Id": "003gK00000XyZAbCAA",
            "Name": "Jan Janssens",
            "Email": "jan@acme.be",
            "IsActive__c": True,
            "LastModifiedDate": "2026-05-04T10:00:00.000+0000",
        },
    ])

    results = await contact_tools.search_contact(fake_sf_client, query="Jan")

    assert len(results) == 1
    assert results[0].id == "003gK00000XyZAbCAA"
    assert results[0].name == "Jan Janssens"
    assert results[0].email == "jan@acme.be"
    assert results[0].is_active is True


@pytest.mark.asyncio
async def test_search_contact_escapes_quotes(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    await contact_tools.search_contact(fake_sf_client, query="O'Brien")
    soql = fake_sf_client.query.await_args.args[0]
    assert r"O\'Brien" in soql


@pytest.mark.asyncio
async def test_search_contact_escapes_like_wildcards(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    await contact_tools.search_contact(fake_sf_client, query="50%")
    soql = fake_sf_client.query.await_args.args[0]
    assert r"50\%" in soql


@pytest.mark.asyncio
async def test_get_contact_rejects_invalid_id(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="003"):
        await contact_tools.get_contact(fake_sf_client, contact_id="bogus")


@pytest.mark.asyncio
async def test_get_contact_rejects_non_alphanumeric_id(fake_sf_client) -> None:
    # Length is right (15), prefix is right ('003'), but contains injection chars
    with pytest.raises(ValueError, match="003"):
        await contact_tools.get_contact(fake_sf_client, contact_id="003' OR 1=1--")


@pytest.mark.asyncio
async def test_get_contact_returns_none_when_not_found(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    result = await contact_tools.get_contact(
        fake_sf_client, contact_id="003gK00000XyZAbCAA"
    )
    assert result is None


@pytest.mark.asyncio
async def test_get_contact_maps_account_relation(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([
        {
            "Id": "003gK00000XyZAbCAA",
            "Name": "Jan Janssens",
            "FirstName": "Jan",
            "LastName": "Janssens",
            "Email": "jan@acme.be",
            "Phone": "+32470123456",
            "IsActive__c": True,
            "Role__c": "COMPANY_CONTACT",
            "GDPR_Consent__c": True,
            "Paid_At__c": None,
            "AccountId": "001gK00000PqRStUAA",
            "Account": {"Name": "Acme NV"},
            "CreatedDate": "2026-04-15T09:23:11.000+0000",
            "LastModifiedDate": "2026-04-30T14:12:08.000+0000",
        },
    ])

    result = await contact_tools.get_contact(
        fake_sf_client, contact_id="003gK00000XyZAbCAA"
    )

    assert result is not None
    assert result.account_id == "001gK00000PqRStUAA"
    assert result.account_name == "Acme NV"
    assert result.role == "COMPANY_CONTACT"
    assert result.gdpr_consent is True


@pytest.mark.asyncio
async def test_get_contact_coerces_unknown_role_to_none(
    fake_sf_client, make_query_response
) -> None:
    """Regression: unknown Role__c picklist values must not crash Pydantic Literal."""
    fake_sf_client.query.return_value = make_query_response([
        {
            "Id": "003gK00000XyZAbCAA",
            "Name": "Jan Janssens",
            "FirstName": "Jan",
            "LastName": "Janssens",
            "Email": "jan@acme.be",
            "Phone": None,
            "IsActive__c": True,
            "Role__c": "SPEAKER",  # ← unknown picklist value
            "GDPR_Consent__c": False,
            "Paid_At__c": None,
            "AccountId": None,
            "Account": None,
            "CreatedDate": "2026-04-15T09:23:11.000+0000",
            "LastModifiedDate": "2026-04-30T14:12:08.000+0000",
        },
    ])

    result = await contact_tools.get_contact(
        fake_sf_client, contact_id="003gK00000XyZAbCAA"
    )

    assert result is not None
    assert result.role is None  # unknown coerced to None


@pytest.mark.asyncio
async def test_count_contacts_breakdown_no_role_filter(fake_sf_client) -> None:
    # Sequential calls: total, VISITOR, COMPANY_CONTACT.
    # is_active=True → no extra active query needed.
    fake_sf_client.query_count = AsyncMock(side_effect=[150, 120, 25])

    result = await contact_tools.count_contacts(fake_sf_client, is_active=True)

    assert result.total == 150
    assert result.by_role["VISITOR"] == 120
    assert result.by_role["COMPANY_CONTACT"] == 25
    assert result.by_role["UNKNOWN"] == 5
    assert result.by_active == {"active": 150, "inactive": 0}


@pytest.mark.asyncio
async def test_count_contacts_with_role_filter_skips_breakdown_query(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(side_effect=[42])

    result = await contact_tools.count_contacts(
        fake_sf_client, role="VISITOR", is_active=True
    )

    assert result.total == 42
    assert result.by_role["VISITOR"] == 42
    assert result.by_role["COMPANY_CONTACT"] == 0
    assert result.by_role["UNKNOWN"] == 0


@pytest.mark.asyncio
async def test_count_contacts_active_breakdown_includes_filters(fake_sf_client) -> None:
    """Regression: when is_active=None, the active subquery must include base filters
    (gdpr/has_paid/role), otherwise 'active' counts the entire org."""
    # Sequential: total, VISITOR (filtered), COMPANY_CONTACT (filtered), active (filtered).
    fake_sf_client.query_count = AsyncMock(side_effect=[100, 70, 25, 80])

    result = await contact_tools.count_contacts(
        fake_sf_client, is_active=None, gdpr_consent=True
    )

    # Inspect every query: each must include GDPR_Consent__c filter.
    soqls = [call.args[0] for call in fake_sf_client.query_count.await_args_list]
    assert all("GDPR_Consent__c = true" in s for s in soqls), soqls

    assert result.total == 100
    assert result.by_active == {"active": 80, "inactive": 20}


@pytest.mark.asyncio
async def test_recent_contacts_caps_hours_and_limit(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    results = await contact_tools.recent_contacts(
        fake_sf_client, mode="modified", since_hours=10_000, limit=10_000
    )

    assert results == []
    soql = fake_sf_client.query.await_args.args[0]
    assert "LIMIT 100" in soql


@pytest.mark.asyncio
async def test_recent_contacts_rejects_negative_hours(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="since_hours must be at least 1"):
        await contact_tools.recent_contacts(fake_sf_client, since_hours=-5)


@pytest.mark.asyncio
async def test_recent_contacts_rejects_zero_or_negative_limit(fake_sf_client) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        await contact_tools.recent_contacts(fake_sf_client, limit=0)


@pytest.mark.asyncio
async def test_recent_contacts_mode_created_uses_created_date(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    await contact_tools.recent_contacts(fake_sf_client, mode="created", since_hours=24)
    soql = fake_sf_client.query.await_args.args[0]
    assert "WHERE CreatedDate >=" in soql
    assert "WHERE LastModifiedDate >=" not in soql


@pytest.mark.asyncio
async def test_recent_contacts_mode_modified_uses_last_modified(
    fake_sf_client, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    await contact_tools.recent_contacts(fake_sf_client, mode="modified", since_hours=24)
    soql = fake_sf_client.query.await_args.args[0]
    assert "WHERE LastModifiedDate >=" in soql
