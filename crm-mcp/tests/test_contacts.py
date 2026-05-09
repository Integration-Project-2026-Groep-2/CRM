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
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003gK00000XyZAbCAA",
                "Name": "Jan Janssens",
                "Email": "jan@acme.be",
                "IsActive__c": True,
                "LastModifiedDate": "2026-05-04T10:00:00.000+0000",
            },
        ]
    )

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
async def test_search_contact_escapes_like_wildcards(fake_sf_client, make_query_response) -> None:
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
async def test_get_contact_returns_none_when_not_found(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    result = await contact_tools.get_contact(fake_sf_client, contact_id="003gK00000XyZAbCAA")
    assert result is None


@pytest.mark.asyncio
async def test_get_contact_maps_account_relation(fake_sf_client, make_query_response) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
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
        ]
    )

    result = await contact_tools.get_contact(fake_sf_client, contact_id="003gK00000XyZAbCAA")

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
    fake_sf_client.query.return_value = make_query_response(
        [
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
        ]
    )

    result = await contact_tools.get_contact(fake_sf_client, contact_id="003gK00000XyZAbCAA")

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

    result = await contact_tools.count_contacts(fake_sf_client, role="VISITOR", is_active=True)

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

    result = await contact_tools.count_contacts(fake_sf_client, is_active=None, gdpr_consent=True)

    # Inspect every query: each must include GDPR_Consent__c filter.
    soqls = [call.args[0] for call in fake_sf_client.query_count.await_args_list]
    assert all("GDPR_Consent__c = true" in s for s in soqls), soqls

    assert result.total == 100
    assert result.by_active == {"active": 80, "inactive": 20}


@pytest.mark.asyncio
async def test_recent_contacts_caps_hours_and_limit(fake_sf_client, make_query_response) -> None:
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


# ---- Write-tools (R2) ----


_VALID_CREATE_CONTACT_KWARGS = dict(
    email="jan@example.com",
    first_name="Jan",
    last_name="Janssens",
    role="VISITOR",
    gdpr_consent=True,
)


@pytest.mark.asyncio
async def test_create_contact_happy_path(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])  # email not yet taken

    result = await contact_tools.create_contact(
        fake_sf_client, fake_publisher, **_VALID_CREATE_CONTACT_KWARGS
    )

    assert result.success is True
    assert result.routing_key == "crm.user.confirmed"
    assert result.salesforce_id == "003gK00000NewCon1AAA"
    assert len(result.id) == 36  # UUID v4
    fake_sf_client.create_contact.assert_awaited_once()
    payload = fake_sf_client.create_contact.await_args.args[0]
    assert payload["Email"] == "jan@example.com"
    assert payload["FirstName"] == "Jan"
    assert payload["LastName"] == "Janssens"
    assert payload["Role__c"] == "VISITOR"
    assert payload["GDPR_Consent__c"] is True
    assert payload["CRM_ID__c"] == result.id
    assert payload["IsActive__c"] is True
    fake_publisher.publish_user_confirmed.assert_called_once()
    broadcast = fake_publisher.publish_user_confirmed.call_args.args[0]
    assert broadcast["email"] == "jan@example.com"
    assert broadcast["role"] == "VISITOR"
    assert broadcast["isActive"] is True
    assert "confirmedAt" in broadcast


@pytest.mark.asyncio
async def test_create_contact_with_company_id_uses_uuid_in_custom_field(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([]),  # email-uniqueness probe
        make_query_response(
            [
                {
                    "Id": "001gK00000LinkedAA",
                    "CRM_ID__c": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                }
            ]
        ),  # company exists
    ]

    result = await contact_tools.create_contact(
        fake_sf_client,
        fake_publisher,
        company_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        **_VALID_CREATE_CONTACT_KWARGS,
    )

    payload = fake_sf_client.create_contact.await_args.args[0]
    # Custom-field stores the UUID directly, NOT the resolved Salesforce Id —
    # matches Frontend/Facturatie/Mailing handler convention; AccountId stays unset.
    assert payload["Company_ID__c"] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    assert "AccountId" not in payload
    broadcast = fake_publisher.publish_user_confirmed.call_args.args[0]
    assert broadcast["companyId"] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    assert result.routing_key == "crm.user.confirmed"


@pytest.mark.asyncio
async def test_create_contact_unknown_company_id_raises(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.side_effect = [
        make_query_response([]),  # email-probe
        make_query_response([]),  # company lookup empty
    ]

    with pytest.raises(ValueError, match="no company found"):
        await contact_tools.create_contact(
            fake_sf_client,
            fake_publisher,
            company_id="bogus",
            **_VALID_CREATE_CONTACT_KWARGS,
        )
    fake_sf_client.create_contact.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_contact_rejects_duplicate_email(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([{"Id": "003gK00000ExistAA"}])

    with pytest.raises(ValueError, match="already exists"):
        await contact_tools.create_contact(
            fake_sf_client, fake_publisher, **_VALID_CREATE_CONTACT_KWARGS
        )
    fake_publisher.publish_user_confirmed.assert_not_called()


@pytest.mark.asyncio
async def test_create_contact_rejects_invalid_role(fake_sf_client, fake_publisher) -> None:
    bad = {**_VALID_CREATE_CONTACT_KWARGS, "role": "OPERATOR"}
    with pytest.raises(ValueError, match="role must be one of"):
        await contact_tools.create_contact(fake_sf_client, fake_publisher, **bad)


@pytest.mark.asyncio
async def test_update_contact_happy_path(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Email": "jan@example.com",
                "FirstName": "Jan",
                "LastName": "Janssens",
                "Role__c": "VISITOR",
            },
        ]
    )

    result = await contact_tools.update_contact(
        fake_sf_client,
        fake_publisher,
        crm_id="11111111-2222-4333-8444-555555555555",
        phone="+32499000111",
    )

    assert result.routing_key == "crm.user.updated"
    fake_sf_client.update_contact.assert_awaited_once_with(
        "003gK00000ExistAA", {"Phone": "+32499000111"}
    )
    broadcast = fake_publisher.publish_user_updated.call_args.args[0]
    assert broadcast["email"] == "jan@example.com"  # carried from existing
    assert broadcast["firstName"] == "Jan"
    assert broadcast["role"] == "VISITOR"
    assert broadcast["phone"] == "+32499000111"
    assert broadcast["isActive"] is True
    assert "updatedAt" in broadcast


@pytest.mark.asyncio
async def test_update_contact_unknown_crm_id_raises(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])

    with pytest.raises(ValueError, match="no contact found"):
        await contact_tools.update_contact(fake_sf_client, fake_publisher, crm_id="missing")
    fake_sf_client.update_contact.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_contact_falls_back_to_existing_gdpr_and_active(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """H1 — XSD C18 requires gdprConsent+isActive; missing kwargs must use existing record."""
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Email": "jan@example.com",
                "FirstName": "Jan",
                "LastName": "Janssens",
                "Role__c": "VISITOR",
                "GDPR_Consent__c": True,  # existing record has consent
                "IsActive__c": False,  # existing record is deactivated
            },
        ]
    )

    await contact_tools.update_contact(
        fake_sf_client,
        fake_publisher,
        crm_id="11111111-2222-4333-8444-555555555555",
        phone="+32499000111",
        # gdpr_consent + is_active deliberately omitted
    )

    broadcast = fake_publisher.publish_user_updated.call_args.args[0]
    # Without fallback, `isActive` would silently flip to True and reactivate the contact.
    assert broadcast["isActive"] is False
    # Without fallback, `gdprConsent` would be missing and XSD validation would fail.
    assert broadcast["gdprConsent"] is True


@pytest.mark.asyncio
async def test_delete_contact_soft_default(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003gK00000ExistAA",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Email": "jan@example.com",
                "FirstName": "Jan",
                "LastName": "Janssens",
                "Role__c": "VISITOR",
            },
        ]
    )

    result = await contact_tools.delete_contact(
        fake_sf_client, fake_publisher, crm_id="11111111-2222-4333-8444-555555555555"
    )

    assert result.routing_key == "crm.user.deactivated"
    fake_sf_client.update_contact.assert_awaited_once_with(
        "003gK00000ExistAA", {"IsActive__c": False}
    )
    broadcast = fake_publisher.publish_user_deactivated.call_args.args[0]
    assert broadcast["email"] == "jan@example.com"
    assert "deactivatedAt" in broadcast


@pytest.mark.asyncio
async def test_delete_contact_rejects_hard(fake_sf_client, fake_publisher) -> None:
    with pytest.raises(ValueError, match="hard-delete not supported"):
        await contact_tools.delete_contact(fake_sf_client, fake_publisher, crm_id="abc", hard=True)


@pytest.mark.asyncio
async def test_update_contact_accepts_sf_id(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Plan B — SF Id (003-prefix, 18 chars) input resolves to canonical UUID."""
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003dM00001wmNGXQA2",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Email": "jan@example.com",
                "FirstName": "Jan",
                "LastName": "Janssens",
                "Role__c": "VISITOR",
            },
        ]
    )

    result = await contact_tools.update_contact(
        fake_sf_client,
        fake_publisher,
        crm_id="003dM00001wmNGXQA2",  # SF Id (18 char), not UUID
        phone="+32499000111",
    )

    soql_arg = fake_sf_client.query.call_args.args[0]
    assert "Id = '003dM00001wmNGXQA2'" in soql_arg
    assert "CRM_ID__c =" not in soql_arg  # took the SF-Id path
    assert result.id == "11111111-2222-4333-8444-555555555555"  # canonical UUID
    broadcast = fake_publisher.publish_user_updated.call_args.args[0]
    assert broadcast["id"] == "11111111-2222-4333-8444-555555555555"


@pytest.mark.asyncio
async def test_delete_contact_accepts_sf_id(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response(
        [
            {
                "Id": "003dM00001wmNGXQA2",
                "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                "Email": "jan@example.com",
                "FirstName": "Jan",
                "LastName": "Janssens",
                "Role__c": "VISITOR",
            },
        ]
    )

    result = await contact_tools.delete_contact(
        fake_sf_client, fake_publisher, crm_id="003dM00001wmNGXQA2"
    )

    soql_arg = fake_sf_client.query.call_args.args[0]
    assert "Id = '003dM00001wmNGXQA2'" in soql_arg
    assert result.id == "11111111-2222-4333-8444-555555555555"
    broadcast = fake_publisher.publish_user_deactivated.call_args.args[0]
    assert broadcast["id"] == "11111111-2222-4333-8444-555555555555"


@pytest.mark.asyncio
async def test_update_contact_company_id_accepts_sf_id(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Plan B — secondary company_id lookup also accepts both formats."""
    fake_sf_client.query.side_effect = [
        make_query_response(  # contact lookup
            [
                {
                    "Id": "003dM00001ContaQA1",
                    "CRM_ID__c": "11111111-2222-4333-8444-555555555555",
                    "Email": "jan@example.com",
                    "FirstName": "Jan",
                    "LastName": "Janssens",
                    "Role__c": "VISITOR",
                },
            ]
        ),
        make_query_response(  # company lookup via SF Id (001-prefix, 18 char)
            [
                {
                    "Id": "001dM00003kxOrvQAE",
                    "CRM_ID__c": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                }
            ]
        ),
    ]

    await contact_tools.update_contact(
        fake_sf_client,
        fake_publisher,
        crm_id="11111111-2222-4333-8444-555555555555",
        company_id="001dM00003kxOrvQAE",  # SF Id for company
    )

    company_soql = fake_sf_client.query.call_args_list[1].args[0]
    assert "Id = '001dM00003kxOrvQAE'" in company_soql
    payload = fake_sf_client.update_contact.await_args.args[1]
    # Stored value is canonical CRM_ID__c UUID, not the input SF Id.
    assert payload["Company_ID__c"] == "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
