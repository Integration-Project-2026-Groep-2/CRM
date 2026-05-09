import asyncio
import logging
import re
from unittest.mock import AsyncMock, MagicMock

import pytest
from simple_salesforce import SalesforceError
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

import src.salesforce.client as salesforce_client_module
from src.salesforce_client import (
    apply_account_is_active,
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    count_active_contacts_for_company,
    count_active_session_registrations,
    create_account,
    create_contact,
    deactivate_account_by_crm_id,
    deactivate_account_record,
    deactivate_contact,
    deactivate_session_registration,
    ensure_contact_identifiers,
    ensure_session_registration_active,
    find_unique_contact_by_email,
    get_account_by_crm_id,
    get_account_by_vat,
    get_account_match_by_crm_id,
    get_account_match_by_email,
    get_active_session_participants,
    get_contact_by_crm_id,
    get_contact_by_email,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    get_contact_match_by_planning_id,
    get_salesforce_client,
    get_session_registration_by_registration_id,
    get_unique_active_session_registration_for_contact,
    get_unpaid_contacts,
    has_contact_kassa_id_field,
    has_contact_mailing_id_field,
    has_contact_planning_id_field,
    has_session_registration_object,
    update_facturatie_account,
    update_kassa_contact,
    update_mailing_contact,
    update_payment_status,
    update_planning_contact,
    upsert_account_by_vat,
    upsert_contact_by_email,
    upsert_session_registration,
)


@pytest.fixture
def sf(monkeypatch):
    salesforce_client_module._active_field_cache = None
    salesforce_client_module._mailing_id_field_supported_cache = None
    salesforce_client_module._kassa_id_field_supported_cache = None
    salesforce_client_module._account_active_field_cache = None
    salesforce_client_module._planning_id_field_supported_cache = None
    salesforce_client_module._session_registration_object_supported_cache = None
    salesforce_client_module._account_email_field_cache = None
    salesforce_client_module._account_country_field_cache = None
    salesforce_client_module._account_house_number_field_supported_cache = None

    async def immediate_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(
        salesforce_client_module.asyncio,
        "to_thread",
        immediate_to_thread,
    )

    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.Account = MagicMock()
    sf.Session_Registration__c = MagicMock()
    sf.query = MagicMock()
    sf.query_all = MagicMock()
    sf.describe = MagicMock(return_value={"sobjects": [{"name": "Session_Registration__c"}]})
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}]
    }
    return sf


@pytest.mark.asyncio
async def test_create_contact_success(sf):
    sf.Contact.create.return_value = {"id": "003000000000001"}
    sf.Contact.get.return_value = {"Id": "003000000000001", "FirstName": "Alice", "Email": "a@a.com"}

    payload = {"FirstName": "Alice", "LastName": "Test", "Email": "a@a.com"}
    result = await create_contact(sf, payload)

    assert result == {"Id": "003000000000001", "FirstName": "Alice", "Email": "a@a.com"}
    sf.Contact.create.assert_called_once()
    sf.Contact.get.assert_called_once_with("003000000000001")
    # CRM_ID__c wordt niet meer toegevoegd aan input dict (kopie gebruikt)


@pytest.mark.asyncio
async def test_create_contact_sets_active_field_true(sf):
    sf.Contact.create.return_value = {"id": "003000000000012"}
    sf.Contact.get.return_value = {"Id": "003000000000012", "Email": "active@example.com"}

    await create_contact(sf, {"FirstName": "Active", "LastName": "User", "Email": "active@example.com"})

    create_payload = sf.Contact.create.call_args.args[0]
    assert create_payload["IsActive__c"] is True


@pytest.mark.asyncio
async def test_create_contact_does_not_require_active_field_migration(sf):
    sf.Contact.describe.return_value = {"fields": []}
    sf.Contact.create.return_value = {"id": "003000000000014"}
    sf.Contact.get.return_value = {"Id": "003000000000014", "Email": "no-active@example.com"}

    await create_contact(sf, {"FirstName": "No", "LastName": "ActiveField", "Email": "no-active@example.com"})

    create_payload = sf.Contact.create.call_args.args[0]
    assert "IsActive__c" not in create_payload
    assert "Active__c" not in create_payload
    assert "Is_Active__c" not in create_payload


@pytest.mark.asyncio
async def test_create_contact_raises_salesforce_error(sf):
    # Simple SalesforceError constructie heeft 4 parameters in deze version
    sf.Contact.create.side_effect = SalesforceError(400, "Contact", "ERROR", {"message": "boom"})
    with pytest.raises(SalesforceError):
        await create_contact(sf, {"FirstName": "A", "LastName": "B", "Email": "a@a.com"})


@pytest.mark.asyncio
async def test_upsert_contact_by_email(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}
    sf.Contact.upsert.return_value = {"id": "003000000000002"}
    sf.Contact.get.return_value = {"Id": "003000000000002", "Email": "a@a.com"}

    payload = {"FirstName": "Bob"}
    result = await upsert_contact_by_email(sf, "a@a.com", payload)

    sf.Contact.upsert.assert_called_once()
    assert result == {"Id": "003000000000002", "Email": "a@a.com"}


@pytest.mark.asyncio
async def test_upsert_contact_by_email_handles_int_response(sf):
    """Salesforce may return only an HTTP status code for update upserts."""
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000011"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000011",
        "Email": "exists@example.com",
        "CRM_ID__c": "existing-crm-id",
    }
    sf.Contact.upsert.return_value = 204

    result = await upsert_contact_by_email(sf, "exists@example.com", {"FirstName": "New"})

    sf.Contact.upsert.assert_called_once()
    sf.Contact.get.assert_called_with("003000000000011")
    assert result["Id"] == "003000000000011"


@pytest.mark.asyncio
async def test_upsert_contact_by_email_sets_active_field_true_when_missing(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}
    sf.Contact.upsert.return_value = {"id": "003000000000013"}
    sf.Contact.get.return_value = {"Id": "003000000000013", "Email": "upsert@example.com"}

    await upsert_contact_by_email(sf, "upsert@example.com", {"FirstName": "Upsert"})

    upsert_payload = sf.Contact.upsert.call_args.args[1]
    assert upsert_payload["IsActive__c"] is True


@pytest.mark.asyncio
async def test_upsert_contact_by_email_preserves_inactive_existing_contact(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000015"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000015",
        "Email": "inactive@example.com",
        "CRM_ID__c": "inactive-crm-id",
        "IsActive__c": False,
    }
    sf.Contact.upsert.return_value = 204

    await upsert_contact_by_email(sf, "inactive@example.com", {"FirstName": "StillInactive"})

    upsert_payload = sf.Contact.upsert.call_args.args[1]
    assert "IsActive__c" not in upsert_payload
    assert "Active__c" not in upsert_payload
    assert "Is_Active__c" not in upsert_payload


@pytest.mark.asyncio
async def test_get_contact_by_email_found(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000004"}]}
    sf.Contact.get.return_value = {"Id": "003000000000004", "Email": "x@y.com"}

    result = await get_contact_by_email(sf, "x@y.com")

    assert result == {"Id": "003000000000004", "Email": "x@y.com"}


@pytest.mark.asyncio
async def test_get_contact_by_email_not_found(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}
    result = await get_contact_by_email(sf, "x@y.com")

    assert result is None


@pytest.mark.asyncio
async def test_get_contact_by_crm_id_found(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000005"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000005",
        "CRM_ID__c": "crm-123",
        "Email": "x@y.com",
    }

    result = await get_contact_by_crm_id(sf, "crm-123")

    assert result["CRM_ID__c"] == "crm-123"
    sf.Contact.get.assert_called_once_with("003000000000005")


@pytest.mark.asyncio
async def test_find_unique_contact_by_email_found(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000006"}]}
    sf.Contact.get.return_value = {"Id": "003000000000006", "Email": "unique@example.com"}

    result = await find_unique_contact_by_email(sf, "unique@example.com")

    assert result == {"Id": "003000000000006", "Email": "unique@example.com"}


@pytest.mark.asyncio
async def test_find_unique_contact_by_email_not_found(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    result = await find_unique_contact_by_email(sf, "missing@example.com")

    assert result is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_find_unique_contact_by_email_returns_none_for_ambiguous_match(sf):
    sf.query.return_value = {
        "totalSize": 2,
        "records": [{"Id": "003000000000006"}, {"Id": "003000000000007"}],
    }

    result = await find_unique_contact_by_email(sf, "duplicate@example.com")

    assert result is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_contact_match_by_email_returns_none_for_no_match(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    match_status, contact = await get_contact_match_by_email(sf, "missing@example.com")

    assert match_status == "none"
    assert contact is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_contact_match_by_email_returns_unique_contact(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000010"}]}
    sf.Contact.get.return_value = {"Id": "003000000000010", "Email": "unique@example.com"}

    match_status, contact = await get_contact_match_by_email(sf, "unique@example.com")

    assert match_status == "unique"
    assert contact == {"Id": "003000000000010", "Email": "unique@example.com"}
    sf.Contact.get.assert_called_once_with("003000000000010")


@pytest.mark.asyncio
async def test_get_contact_match_by_email_returns_ambiguous_without_fetching_contact(sf):
    sf.query.return_value = {
        "totalSize": 2,
        "records": [{"Id": "003000000000010"}, {"Id": "003000000000011"}],
    }

    match_status, contact = await get_contact_match_by_email(sf, "duplicate@example.com")

    assert match_status == "ambiguous"
    assert contact is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_has_contact_mailing_id_field_returns_true_when_present(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}, {"name": "Mailing_ID__c"}]
    }

    result = await has_contact_mailing_id_field(sf)

    assert result is True


@pytest.mark.asyncio
async def test_has_contact_mailing_id_field_returns_false_when_absent(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}]
    }

    result = await has_contact_mailing_id_field(sf)

    assert result is False


@pytest.mark.asyncio
async def test_has_contact_planning_id_field_returns_true_when_present(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}, {"name": "Planning_ID__c"}]
    }

    result = await has_contact_planning_id_field(sf)

    assert result is True


@pytest.mark.asyncio
async def test_has_contact_planning_id_field_returns_false_when_absent(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}]
    }

    result = await has_contact_planning_id_field(sf)

    assert result is False


@pytest.mark.asyncio
async def test_has_contact_kassa_id_field_returns_true_when_present(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}, {"name": "Kassa_ID__c"}]
    }

    result = await has_contact_kassa_id_field(sf)

    assert result is True


@pytest.mark.asyncio
async def test_has_contact_kassa_id_field_returns_false_when_absent(sf):
    sf.Contact.describe.return_value = {
        "fields": [{"name": "IsActive__c"}]
    }

    result = await has_contact_kassa_id_field(sf)

    assert result is False


@pytest.mark.asyncio
async def test_has_session_registration_object_returns_true_when_present(sf):
    sf.describe.return_value = {
        "sobjects": [{"name": "Session_Registration__c"}]
    }

    result = await has_session_registration_object(sf)

    assert result is True


@pytest.mark.asyncio
async def test_has_session_registration_object_returns_false_when_absent(sf):
    sf.describe.return_value = {
        "sobjects": [{"name": "Contact"}]
    }

    result = await has_session_registration_object(sf)

    assert result is False


@pytest.mark.asyncio
async def test_get_session_registration_by_registration_id_returns_registration(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "a01000000000001"}]}
    sf.Session_Registration__c.get.return_value = {
        "Id": "a01000000000001",
        "Registration_ID__c": "REG-12345",
        "Session_ID__c": "SESS-001",
        "Contact__c": "003000000000001",
        "Is_Active__c": True,
    }

    result = await get_session_registration_by_registration_id(sf, "REG-12345")

    assert result["Registration_ID__c"] == "REG-12345"
    sf.Session_Registration__c.get.assert_called_once_with("a01000000000001")


@pytest.mark.asyncio
async def test_upsert_session_registration_retrieves_row_after_upsert(sf):
    sf.Session_Registration__c.upsert.return_value = {"id": "a01000000000001"}
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "a01000000000001"}]}
    sf.Session_Registration__c.get.return_value = {
        "Id": "a01000000000001",
        "Registration_ID__c": "REG-12345",
        "Session_ID__c": "SESS-001",
        "Contact__c": "003000000000001",
        "Is_Active__c": True,
    }

    result = await upsert_session_registration(
        sf,
        registration_id="REG-12345",
        session_id="SESS-001",
        contact_id="003000000000001",
    )

    sf.Session_Registration__c.upsert.assert_called_once_with(
        "Registration_ID__c/REG-12345",
        {
            "Registration_ID__c": "REG-12345",
            "Session_ID__c": "SESS-001",
            "Contact__c": "003000000000001",
            "Is_Active__c": True,
        },
    )
    assert result["Session_ID__c"] == "SESS-001"


@pytest.mark.asyncio
async def test_ensure_session_registration_active_reactivates_contact_session_match(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "a01000000000002"}]}
    sf.Session_Registration__c.get.side_effect = [
        {
            "Id": "a01000000000002",
            "Session_ID__c": "SESS-001",
            "Contact__c": "003000000000001",
            "Is_Active__c": False,
        },
        {
            "Id": "a01000000000002",
            "Session_ID__c": "SESS-001",
            "Contact__c": "003000000000001",
            "Is_Active__c": True,
        },
    ]

    result = await ensure_session_registration_active(
        sf,
        contact_id="003000000000001",
        session_id="SESS-001",
        registration_id=None,
    )

    sf.Session_Registration__c.update.assert_called_once_with(
        "a01000000000002",
        {"Is_Active__c": True},
    )
    assert result["Is_Active__c"] is True


@pytest.mark.asyncio
async def test_get_active_session_participants_returns_sorted_contacts(sf):
    sf.query_all.return_value = {
        "records": [
            {
                "Id": "a01-2",
                "Contact__c": "003000000000002",
                "Contact__r": {
                    "Id": "003000000000002",
                    "Email": "bert@example.com",
                    "FirstName": "Bert",
                    "LastName": "Beta",
                    "IsActive__c": True,
                },
            },
            {
                "Id": "a01-1",
                "Contact__c": "003000000000001",
                "Contact__r": {
                    "Id": "003000000000001",
                    "Email": "anna@example.com",
                    "FirstName": "Anna",
                    "LastName": "Alpha",
                    "IsActive__c": True,
                },
            },
        ]
    }

    result = await get_active_session_participants(sf, "SESS-001")

    assert [contact["Email"] for contact in result] == [
        "anna@example.com",
        "bert@example.com",
    ]


@pytest.mark.asyncio
async def test_deactivate_session_registration_uses_registration_id_first(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "a01000000000001"}]}
    sf.Session_Registration__c.get.side_effect = [
        {
            "Id": "a01000000000001",
            "Registration_ID__c": "REG-12345",
            "Is_Active__c": True,
        },
        {
            "Id": "a01000000000001",
            "Registration_ID__c": "REG-12345",
            "Is_Active__c": False,
        },
    ]

    result = await deactivate_session_registration(sf, registration_id="REG-12345")

    sf.Session_Registration__c.update.assert_called_once_with(
        "a01000000000001",
        {"Is_Active__c": False},
    )
    assert result["Is_Active__c"] is False


@pytest.mark.asyncio
async def test_deactivate_session_registration_falls_back_to_contact_and_session(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "a01000000000002"}]}
    sf.Session_Registration__c.get.side_effect = [
        {
            "Id": "a01000000000002",
            "Session_ID__c": "SESS-001",
            "Contact__c": "003000000000001",
            "Is_Active__c": True,
        },
        {
            "Id": "a01000000000002",
            "Session_ID__c": "SESS-001",
            "Contact__c": "003000000000001",
            "Is_Active__c": False,
        },
    ]

    result = await deactivate_session_registration(
        sf,
        registration_id=None,
        contact_id="003000000000001",
        session_id="SESS-001",
    )

    sf.Session_Registration__c.update.assert_called_once_with(
        "a01000000000002",
        {"Is_Active__c": False},
    )
    assert result["Is_Active__c"] is False


@pytest.mark.asyncio
async def test_count_active_session_registrations_returns_total_size(sf):
    sf.query.return_value = {"totalSize": 2, "records": [{"Id": "a01-1"}, {"Id": "a01-2"}]}

    result = await count_active_session_registrations(sf, "003000000000001")

    assert result == 2


@pytest.mark.asyncio
async def test_count_active_contacts_for_company_returns_count(sf):
    sf.query.return_value = {"totalSize": 3}

    result = await count_active_contacts_for_company(sf, "comp-uuid-001")

    assert result == 3
    query_arg = sf.query.call_args.args[0]
    assert "Company_ID__c" in query_arg
    assert "comp-uuid-001" in query_arg
    assert "IsActive__c = true" in query_arg


@pytest.mark.asyncio
async def test_count_active_contacts_for_company_returns_zero_when_none_match(sf):
    sf.query.return_value = {"totalSize": 0}

    result = await count_active_contacts_for_company(sf, "comp-uuid-002")

    assert result == 0


@pytest.mark.asyncio
async def test_count_active_contacts_for_company_omits_active_filter_when_no_field(sf):
    sf.Contact.describe.return_value = {"fields": []}  # No active field

    sf.query.return_value = {"totalSize": 1}

    result = await count_active_contacts_for_company(sf, "comp-uuid-003")

    assert result == 1
    query_arg = sf.query.call_args.args[0]
    assert "IsActive__c" not in query_arg
    assert "Company_ID__c" in query_arg


@pytest.mark.asyncio
async def test_get_unique_active_session_registration_for_contact_returns_none_when_ambiguous(sf):
    sf.query.return_value = {"totalSize": 2, "records": [{"Id": "a01-1"}, {"Id": "a01-2"}]}

    result = await get_unique_active_session_registration_for_contact(sf, "003000000000001")

    assert result is None
    sf.Session_Registration__c.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_contact_match_by_planning_id_returns_unique_contact(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000021"}]}
    sf.Contact.get.return_value = {"Id": "003000000000021", "Planning_ID__c": "planning-id-1"}

    match_status, contact = await get_contact_match_by_planning_id(sf, "planning-id-1")

    assert match_status == "unique"
    assert contact == {"Id": "003000000000021", "Planning_ID__c": "planning-id-1"}
    sf.Contact.get.assert_called_once_with("003000000000021")


@pytest.mark.asyncio
async def test_get_contact_match_by_planning_id_returns_none_for_no_match(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    match_status, contact = await get_contact_match_by_planning_id(sf, "missing-planning-id")

    assert match_status == "none"
    assert contact is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_contact_identifiers_adds_missing_crm_id_and_registration_id(sf, monkeypatch):
    monkeypatch.setattr(salesforce_client_module.uuid, "uuid4", lambda: "generated-crm-id")
    sf.Contact.get.return_value = {
        "Id": "003000000000008",
        "Email": "ensure@example.com",
        "CRM_ID__c": "generated-crm-id",
        "Registration_ID__c": "REG-NEW",
    }

    result = await ensure_contact_identifiers(
        sf,
        {"Id": "003000000000008", "Email": "ensure@example.com"},
        registration_id="REG-NEW",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000008",
        {"CRM_ID__c": "generated-crm-id", "Registration_ID__c": "REG-NEW"},
    )
    assert result["CRM_ID__c"] == "generated-crm-id"
    assert result["Registration_ID__c"] == "REG-NEW"


@pytest.mark.asyncio
async def test_get_contact_match_by_kassa_id_returns_unique_contact(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000040"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000040",
        "Kassa_ID__c": "kassa-id-1",
    }

    match_status, contact = await get_contact_match_by_kassa_id(sf, "kassa-id-1")

    assert match_status == "unique"
    assert contact == {"Id": "003000000000040", "Kassa_ID__c": "kassa-id-1"}
    sf.Contact.get.assert_called_once_with("003000000000040")


@pytest.mark.asyncio
async def test_get_contact_match_by_kassa_id_returns_none_for_no_match(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    match_status, contact = await get_contact_match_by_kassa_id(sf, "missing-kassa-id")

    assert match_status == "none"
    assert contact is None
    sf.Contact.get.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_contact_identifiers_preserves_existing_registration_id(sf):
    existing_contact = {
        "Id": "003000000000009",
        "Email": "keep@example.com",
        "CRM_ID__c": "existing-crm-id",
        "Registration_ID__c": "REG-OLD",
    }

    result = await ensure_contact_identifiers(
        sf,
        existing_contact,
        registration_id="REG-NEW",
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_ensure_contact_identifiers_adds_missing_planning_id(sf):
    sf.Contact.get.return_value = {
        "Id": "003000000000022",
        "Email": "planning@example.com",
        "CRM_ID__c": "existing-crm-id",
        "Planning_ID__c": "planning-id-30",
    }

    result = await ensure_contact_identifiers(
        sf,
        {
            "Id": "003000000000022",
            "Email": "planning@example.com",
            "CRM_ID__c": "existing-crm-id",
            "Planning_ID__c": None,
        },
        planning_id="planning-id-30",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000022",
        {"Planning_ID__c": "planning-id-30"},
    )
    assert result["Planning_ID__c"] == "planning-id-30"


@pytest.mark.asyncio
async def test_backfill_planning_contact_fields_updates_only_missing_fields(sf):
    existing_contact = {
        "Id": "003000000000023",
        "Email": "planning@example.com",
        "FirstName": None,
        "LastName": None,
        "Role__c": None,
        "Phone": None,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000023",
        "Email": "planning@example.com",
        "FirstName": "Sofie",
        "LastName": "Declercq",
        "Role__c": "SPEAKER",
        "Phone": "+32470123456",
    }

    result = await backfill_planning_contact_fields(
        sf,
        existing_contact,
        first_name="Sofie",
        last_name="Declercq",
        role="SPEAKER",
        phone_number="+32470123456",
        gdpr_consent=True,
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000023",
        {
            "FirstName": "Sofie",
            "LastName": "Declercq",
            "Role__c": "SPEAKER",
            "Phone": "+32470123456",
            "GDPR_Consent__c": True,
        },
    )
    assert result["Role__c"] == "SPEAKER"


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_updates_only_missing_fields(sf):
    existing_contact = {
        "Id": "003000000000016",
        "Email": "mia.mail@example.com",
        "FirstName": None,
        "LastName": "",
        "Company_ID__c": None,
        "Role__c": None,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000016",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": "company-id-123",
        "Role__c": "COMPANY_CONTACT",
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        first_name="Mia",
        last_name="Mail",
        company_id="company-id-123",
        role="COMPANY_CONTACT",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000016",
        {
            "FirstName": "Mia",
            "LastName": "Mail",
            "Company_ID__c": "company-id-123",
            "Role__c": "COMPANY_CONTACT",
        },
    )
    assert result["FirstName"] == "Mia"
    assert result["Company_ID__c"] == "company-id-123"


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_sets_missing_visitor_role(sf):
    existing_contact = {
        "Id": "003000000000017",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Role__c": None,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000017",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Role__c": "VISITOR",
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        role="VISITOR",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000017",
        {"Role__c": "VISITOR"},
    )
    assert result["Role__c"] == "VISITOR"


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_preserves_existing_values(sf):
    existing_contact = {
        "Id": "003000000000019",
        "Email": "mia.mail@example.com",
        "FirstName": "Existing",
        "LastName": "Name",
        "Company_ID__c": "existing-company",
        "Role__c": "ADMIN",
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        first_name="Mia",
        last_name="Mail",
        company_id="company-id-123",
        role="COMPANY_CONTACT",
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_does_not_add_company_to_specialized_role(sf):
    existing_contact = {
        "Id": "003000000000024",
        "Email": "mia.mail@example.com",
        "Company_ID__c": None,
        "Role__c": "ADMIN",
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        company_id="company-id-123",
        role="COMPANY_CONTACT",
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_promotes_visitor_to_company_contact(sf):
    existing_contact = {
        "Id": "003000000000020",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": None,
        "Role__c": "VISITOR",
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000020",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": "company-id-123",
        "Role__c": "COMPANY_CONTACT",
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        company_id="company-id-123",
        role="COMPANY_CONTACT",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000020",
        {
            "Company_ID__c": "company-id-123",
            "Role__c": "COMPANY_CONTACT",
        },
    )
    assert result["Company_ID__c"] == "company-id-123"
    assert result["Role__c"] == "COMPANY_CONTACT"


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_sets_gdpr_consent_true_when_missing(sf):
    existing_contact = {
        "Id": "003000000000021",
        "Email": "mia.mail@example.com",
        "GDPR_Consent__c": None,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000021",
        "Email": "mia.mail@example.com",
        "GDPR_Consent__c": True,
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        gdpr_consent=True,
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000021",
        {"GDPR_Consent__c": True},
    )
    assert result["GDPR_Consent__c"] is True


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_preserves_explicit_false_gdpr_consent(sf):
    existing_contact = {
        "Id": "003000000000022",
        "Email": "mia.mail@example.com",
        "GDPR_Consent__c": False,
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        gdpr_consent=True,
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_backfill_mailing_contact_fields_keeps_existing_true_gdpr_consent(sf):
    existing_contact = {
        "Id": "003000000000023",
        "Email": "mia.mail@example.com",
        "GDPR_Consent__c": True,
    }

    result = await backfill_mailing_contact_fields(
        sf,
        existing_contact,
        gdpr_consent=True,
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_update_mailing_contact_authoritatively_overwrites_owned_fields(sf):
    existing_contact = {
        "Id": "003000000000041",
        "Email": "old@example.com",
        "FirstName": "Old",
        "LastName": "Name",
        "Company_ID__c": "old-company-id",
        "Role__c": "COMPANY_CONTACT",
        "GDPR_Consent__c": False,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000041",
        "Email": "new@example.com",
        "FirstName": None,
        "LastName": "new@example.com",
        "Company_ID__c": None,
        "Role__c": "VISITOR",
        "GDPR_Consent__c": True,
    }

    result = await update_mailing_contact(
        sf,
        existing_contact,
        email="new@example.com",
        first_name=None,
        last_name="new@example.com",
        company_id=None,
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000041",
        {
            "Email": "new@example.com",
            "FirstName": None,
            "LastName": "new@example.com",
            "Company_ID__c": None,
            "GDPR_Consent__c": True,
            "Role__c": "VISITOR",
        },
    )
    assert result["Email"] == "new@example.com"
    assert result["Role__c"] == "VISITOR"
    assert result["GDPR_Consent__c"] is True


@pytest.mark.asyncio
async def test_update_mailing_contact_preserves_specialized_role(sf):
    existing_contact = {
        "Id": "003000000000042",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": "old-company-id",
        "Role__c": "ADMIN",
        "GDPR_Consent__c": True,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000042",
        "Email": "mia.mail@example.com",
        "FirstName": "Updated",
        "LastName": "User",
        "Company_ID__c": "old-company-id",
        "Role__c": "ADMIN",
        "GDPR_Consent__c": True,
    }

    result = await update_mailing_contact(
        sf,
        existing_contact,
        email="mia.mail@example.com",
        first_name="Updated",
        last_name="User",
        company_id="new-company-id",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000042",
        {
            "FirstName": "Updated",
            "LastName": "User",
        },
    )
    assert result["Role__c"] == "ADMIN"
    assert result["Company_ID__c"] == "old-company-id"


@pytest.mark.asyncio
async def test_update_mailing_contact_does_not_clear_company_link_for_specialized_role(sf):
    existing_contact = {
        "Id": "003000000000044",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": "old-company-id",
        "Role__c": "SPEAKER",
        "GDPR_Consent__c": True,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000044",
        "Email": "mia.mail@example.com",
        "FirstName": "Updated",
        "LastName": "User",
        "Company_ID__c": "old-company-id",
        "Role__c": "SPEAKER",
        "GDPR_Consent__c": True,
    }

    result = await update_mailing_contact(
        sf,
        existing_contact,
        email="mia.mail@example.com",
        first_name="Updated",
        last_name="User",
        company_id=None,
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000044",
        {
            "FirstName": "Updated",
            "LastName": "User",
        },
    )
    assert result["Role__c"] == "SPEAKER"
    assert result["Company_ID__c"] == "old-company-id"


@pytest.mark.asyncio
async def test_update_planning_contact_authoritatively_overwrites_owned_fields(sf):
    existing_contact = {
        "Id": "003000000000051",
        "Email": "old@example.com",
        "FirstName": "Old",
        "LastName": "Name",
        "Role__c": "VISITOR",
        "Phone": "+32000000000",
        "GDPR_Consent__c": False,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000051",
        "Email": "new@example.com",
        "FirstName": "Sofie",
        "LastName": "Updated",
        "Role__c": "SPEAKER",
        "Phone": "+32470999999",
        "GDPR_Consent__c": True,
    }

    result = await update_planning_contact(
        sf,
        existing_contact,
        email="new@example.com",
        first_name="Sofie",
        last_name="Updated",
        role="SPEAKER",
        phone_number="+32470999999",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000051",
        {
            "Email": "new@example.com",
            "FirstName": "Sofie",
            "LastName": "Updated",
            "Role__c": "SPEAKER",
            "Phone": "+32470999999",
            "GDPR_Consent__c": True,
        },
    )
    assert result["Email"] == "new@example.com"
    assert result["Role__c"] == "SPEAKER"


@pytest.mark.asyncio
async def test_update_planning_contact_clears_phone_when_payload_omits_it(sf):
    existing_contact = {
        "Id": "003000000000052",
        "Email": "sofie@example.com",
        "FirstName": "Sofie",
        "LastName": "Declercq",
        "Role__c": "SPEAKER",
        "Phone": "+32470123456",
        "GDPR_Consent__c": True,
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000052",
        "Email": "sofie@example.com",
        "FirstName": "Sofie",
        "LastName": "Declercq",
        "Role__c": "SPEAKER",
        "Phone": None,
        "GDPR_Consent__c": True,
    }

    result = await update_planning_contact(
        sf,
        existing_contact,
        email="sofie@example.com",
        first_name="Sofie",
        last_name="Declercq",
        role="SPEAKER",
        phone_number=None,
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000052",
        {"Phone": None},
    )
    assert result["Phone"] is None


@pytest.mark.asyncio
async def test_update_planning_contact_skips_update_when_no_changes(sf):
    existing_contact = {
        "Id": "003000000000053",
        "Email": "sofie@example.com",
        "FirstName": "Sofie",
        "LastName": "Declercq",
        "Role__c": "SPEAKER",
        "Phone": "+32470123456",
        "GDPR_Consent__c": True,
    }

    result = await update_planning_contact(
        sf,
        existing_contact,
        email="sofie@example.com",
        first_name="Sofie",
        last_name="Declercq",
        role="SPEAKER",
        phone_number="+32470123456",
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_update_mailing_contact_returns_existing_contact_when_no_changes(sf):
    existing_contact = {
        "Id": "003000000000043",
        "Email": "mia.mail@example.com",
        "FirstName": "Mia",
        "LastName": "Mail",
        "Company_ID__c": None,
        "Role__c": "VISITOR",
        "GDPR_Consent__c": True,
    }

    result = await update_mailing_contact(
        sf,
        existing_contact,
        email="mia.mail@example.com",
        first_name="Mia",
        last_name="Mail",
        company_id=None,
    )

    sf.Contact.update.assert_not_called()
    assert result == existing_contact


@pytest.mark.asyncio
async def test_update_kassa_contact_preserves_specialized_role_and_skips_empty_badge(sf):
    existing_contact = {
        "Id": "003000000000090",
        "Email": "admin.old@example.com",
        "FirstName": "Admin",
        "LastName": "User",
        "Badge_Code__c": "BADGE-OLD",
        "Role__c": "ADMIN",
        "Company_ID__c": "company-old",
    }
    sf.Contact.get.return_value = {
        "Id": "003000000000090",
        "Email": "admin.new@example.com",
        "FirstName": "Admin",
        "LastName": "User",
        "Badge_Code__c": "BADGE-OLD",
        "Role__c": "ADMIN",
        "Company_ID__c": "company-old",
    }

    result = await update_kassa_contact(
        sf,
        existing_contact,
        email="admin.new@example.com",
        first_name="Admin",
        last_name="User",
        badge_code=None,
        role="VISITOR",
        company_id="company-new",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000090",
        {"Email": "admin.new@example.com"},
    )
    assert result["Role__c"] == "ADMIN"
    assert result["Company_ID__c"] == "company-old"
    assert result["Badge_Code__c"] == "BADGE-OLD"


@pytest.mark.asyncio
async def test_create_account_success(sf):
    sf.Account.create.return_value = {"id": "001000000000001"}
    sf.Account.get.return_value = {"Id": "001000000000001", "Name": "Company", "VAT_Number__c": "BE0123456789"}

    payload = {"Name": "Company", "VAT_Number__c": "BE0123456789"}
    result = await create_account(sf, payload)

    sf.Account.create.assert_called_once()
    assert result == {"Id": "001000000000001", "Name": "Company", "VAT_Number__c": "BE0123456789"}
    # CRM_ID__c wordt niet meer toegevoegd aan input dict (kopie gebruikt)


@pytest.mark.asyncio
async def test_upsert_account_by_vat(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}
    sf.Account.upsert.return_value = {"id": "001000000000002"}
    sf.Account.get.return_value = {"Id": "001000000000002", "Name": "Acme", "VAT_Number__c": "BE0123456789"}

    payload = {"Name": "Acme"}
    result = await upsert_account_by_vat(sf, "BE0123456789", payload)

    sf.Account.upsert.assert_called_once()
    assert result == {"Id": "001000000000002", "Name": "Acme", "VAT_Number__c": "BE0123456789"}


@pytest.mark.asyncio
async def test_get_account_by_vat_found(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000004"}]}
    sf.Account.get.return_value = {"Id": "001000000000004", "Name": "Corp", "VAT_Number__c": "BE0123456789"}

    result = await get_account_by_vat(sf, "BE0123456789")

    assert result == {"Id": "001000000000004", "Name": "Corp", "VAT_Number__c": "BE0123456789"}


@pytest.mark.asyncio
async def test_get_account_by_vat_not_found(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}
    result = await get_account_by_vat(sf, "BE0123456789")

    assert result is None


@pytest.mark.asyncio
async def test_upsert_contact_preserves_existing_crm_id(sf):
    sf.Contact.upsert.return_value = {"id": "003000000000010"}
    sf.Contact.get.return_value = {"Id": "003000000000010", "Email": "a@a.com", "CRM_ID__c": "FIXED-UUID"}
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000010"}]}

    result = await upsert_contact_by_email(sf, "a@a.com", {"FirstName": "Bob"})
    assert result["CRM_ID__c"] == "FIXED-UUID"
    sf.Contact.upsert.assert_called_once()
    assert sf.Contact.upsert.call_args.args[1]["CRM_ID__c"] == "FIXED-UUID"


@pytest.mark.asyncio
async def test_upsert_account_preserves_existing_crm_id(sf):
    sf.Account.upsert.return_value = {"id": "001000000000010"}
    sf.Account.get.return_value = {"Id": "001000000000010", "VAT_Number__c": "BE123", "CRM_ID__c": "FIXED-UUID"}
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000010"}]}

    result = await upsert_account_by_vat(sf, "BE123", {"Name": "Acme"})
    assert result["CRM_ID__c"] == "FIXED-UUID"
    sf.Account.upsert.assert_called_once()
    assert sf.Account.upsert.call_args.args[1]["CRM_ID__c"] == "FIXED-UUID"


@pytest.mark.asyncio
async def test_upsert_account_by_vat_handles_int_status_on_update(sf):
    """Regression — 2026-04-22 production. simple-salesforce's `upsert()`
    returns a dict for CREATE (201 with body) and a raw int for UPDATE
    (204 No Content). When Facturatie re-sends the same company, the
    Account already exists → SF update → int returned → crashed on
    `result["id"]` with `'int' object is not subscriptable`.

    The helper must accept the int, recover the ID from the pre-upsert
    lookup (`existing`), and return the fetched record.
    """
    existing_account = {
        "Id": "001000000000700",
        "Name": "Acme",
        "VAT_Number__c": "BE0412341112",
        "CRM_ID__c": "existing-crm-uuid",
    }
    # First query (get_account_by_vat before upsert) finds the existing record.
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000700"}]}
    sf.Account.get.return_value = existing_account
    # SF upsert for an update returns HTTP 204 as a bare int.
    sf.Account.upsert.return_value = 204

    result = await upsert_account_by_vat(sf, "BE0412341112", {"Name": "Acme Updated"})

    sf.Account.upsert.assert_called_once()
    # Must have fetched the final record via the id recovered from existing.
    assert sf.Account.get.called
    fetched_id = sf.Account.get.call_args_list[-1].args[0]
    assert fetched_id == "001000000000700"
    # Must NOT have allocated a new CRM UUID (not a create path).
    update_calls = [
        call for call in sf.Account.update.call_args_list
        if len(call.args) >= 2 and "CRM_ID__c" in call.args[1]
    ]
    assert update_calls == [], "no CRM_ID__c mint should happen on update path"
    assert result == existing_account


@pytest.mark.asyncio
async def test_upsert_account_by_vat_mints_crm_id_for_new_record(sf):
    """Regression — 2026-04-22 production. simple-salesforce's upsert()
    never returns a dict, so the post-upsert mint branch was dead code
    and new Accounts landed in Salesforce with CRM_ID__c = null. That
    null propagated to the C14 outbound payload (<id>None</id>) and
    failed XSD UUID-pattern validation.

    Fix: stamp CRM_ID__c in the request body atomically on create.
    """
    freshly_created = {
        "Id": "001000000000800",
        "Name": "NewCo",
        "VAT_Number__c": "BE0412341500",
    }
    sf.query.side_effect = [
        {"totalSize": 0, "records": []},                                  # pre-upsert: not found
        {"totalSize": 1, "records": [{"Id": "001000000000800"}]},         # post-upsert: refresh
    ]

    def fetch_with_crm_id(_id: str):
        # The refreshed record reflects the CRM_ID__c that was stamped in
        # the upsert body — capture whatever the caller actually sent.
        stamped = sf.Account.upsert.call_args.args[1].get("CRM_ID__c")
        return {**freshly_created, "CRM_ID__c": stamped}

    sf.Account.get.side_effect = fetch_with_crm_id
    sf.Account.upsert.return_value = 201

    result = await upsert_account_by_vat(sf, "BE0412341500", {"Name": "NewCo"})

    sf.Account.upsert.assert_called_once()
    body = sf.Account.upsert.call_args.args[1]
    crm_id = body.get("CRM_ID__c")
    assert crm_id, "CRM_ID__c must be present in upsert body"
    # Shape check — UUID v4 format.
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(crm_id), f"CRM_ID__c '{crm_id}' is not a UUID v4"
    # No secondary sf.Account.update for CRM_ID__c is needed anymore.
    crm_id_update_calls = [
        call for call in sf.Account.update.call_args_list
        if len(call.args) >= 2 and "CRM_ID__c" in call.args[1]
    ]
    assert crm_id_update_calls == []
    assert result["CRM_ID__c"] == crm_id


@pytest.mark.asyncio
async def test_upsert_account_by_vat_backfills_crm_id_when_existing_has_none(sf):
    """Backfill path — an Account already exists for this VAT (created by
    an earlier buggy rollout) but its CRM_ID__c is null. Next upsert must
    stamp a new UUID so subsequent C14 publishes succeed.
    """
    ghost_account = {
        "Id": "001000000000801",
        "Name": "GhostCo",
        "VAT_Number__c": "BE0412341600",
        "CRM_ID__c": None,
    }
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000801"}]}

    def fetch(_id: str):
        # Pre-upsert: ghost state. Post-upsert: reflect the stamped UUID.
        if sf.Account.upsert.call_args is None:
            return ghost_account
        stamped = sf.Account.upsert.call_args.args[1].get("CRM_ID__c")
        return {**ghost_account, "CRM_ID__c": stamped}

    sf.Account.get.side_effect = fetch
    sf.Account.upsert.return_value = 204

    result = await upsert_account_by_vat(sf, "BE0412341600", {"Name": "GhostCo"})

    body = sf.Account.upsert.call_args.args[1]
    crm_id = body.get("CRM_ID__c")
    assert crm_id, "ghost account must get a freshly-minted CRM_ID__c"
    assert result["CRM_ID__c"] == crm_id


@pytest.mark.asyncio
async def test_upsert_account_by_vat_accepts_preset_crm_id_in_data(sf):
    """Future-proof: if a caller passes CRM_ID__c explicitly (e.g. a data
    import tool that knows the canonical UUID), don't overwrite it.
    """
    sf.query.return_value = {"totalSize": 0, "records": []}
    sf.Account.upsert.return_value = 201
    sf.query.side_effect = [
        {"totalSize": 0, "records": []},
        {"totalSize": 1, "records": [{"Id": "001000000000802"}]},
    ]
    sf.Account.get.return_value = {
        "Id": "001000000000802",
        "Name": "Imported",
        "VAT_Number__c": "BE0412341700",
        "CRM_ID__c": "caller-supplied-uuid",
    }

    payload = {"Name": "Imported", "CRM_ID__c": "caller-supplied-uuid"}
    await upsert_account_by_vat(sf, "BE0412341700", payload)

    body = sf.Account.upsert.call_args.args[1]
    assert body["CRM_ID__c"] == "caller-supplied-uuid"


@pytest.mark.asyncio
async def test_upsert_contact_by_email_mints_crm_id_for_new_record(sf):
    """Symmetric regression for upsert_contact_by_email — same latent bug
    as upsert_account_by_vat: simple-salesforce returns int, dead-code
    post-upsert mint never ran. Not currently triggered by any production
    flow (facturatie user creates go through create_contact), but fixed
    proactively to avoid future whack-a-mole.
    """
    sf.Contact.describe.return_value = {"fields": [{"name": "IsActive__c"}]}
    # Pre-upsert lookup: no existing contact. Post-upsert refresh: found.
    sf.query.side_effect = [
        {"totalSize": 0, "records": []},
        {"totalSize": 1, "records": [{"Id": "003000000000900"}]},
    ]
    sf.Contact.upsert.return_value = 201
    sf.Contact.get.return_value = {
        "Id": "003000000000900",
        "Email": "new@example.com",
        "CRM_ID__c": "mocked-uuid",
    }

    await upsert_contact_by_email(
        sf, "new@example.com", {"FirstName": "New", "LastName": "User", "Email": "new@example.com"}
    )

    body = sf.Contact.upsert.call_args.args[1]
    crm_id = body.get("CRM_ID__c")
    assert crm_id, "CRM_ID__c must be stamped in upsert body for new contacts"
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(crm_id), f"CRM_ID__c '{crm_id}' is not a UUID v4"


@pytest.mark.asyncio
async def test_upsert_account_by_vat_refetches_when_int_and_no_existing(sf):
    """Edge case — upsert returned int (so we didn't get an id back) and the
    pre-upsert `get_account_by_vat` returned None (account didn't exist at the
    time of our lookup). Can happen under a race with another producer. Helper
    must re-query by VAT and return the freshly-created record.
    """
    # First lookup: nothing there (account doesn't exist yet from our POV).
    # Second lookup (after upsert): account exists, return it.
    freshly_created = {
        "Id": "001000000000701",
        "Name": "RaceWinner",
        "VAT_Number__c": "BE0412341999",
        "CRM_ID__c": "new-crm-uuid",
    }
    sf.query.side_effect = [
        {"totalSize": 0, "records": []},                                     # pre-upsert
        {"totalSize": 1, "records": [{"Id": "001000000000701"}]},            # re-query
    ]
    sf.Account.get.return_value = freshly_created
    sf.Account.upsert.return_value = 204  # update-ish response even though we didn't see it

    result = await upsert_account_by_vat(sf, "BE0412341999", {"Name": "RaceWinner"})

    sf.Account.upsert.assert_called_once()
    assert result == freshly_created


@pytest.mark.asyncio
async def test_upsert_account_by_vat_strips_external_id_from_body(sf):
    """Regression — 2026-04-22 production. Salesforce v59+ rejects the
    external-ID field in the body when it's in the URL path. Helper must
    remove VAT_Number__c from the data dict before calling upsert.

    Real-world error caught on dev:
        INVALID_FIELD: The VAT_Number__c field should not be specified in
        the sobject data.
    """
    sf.query.return_value = {"totalSize": 0, "records": []}
    sf.Account.upsert.return_value = {"id": "001000000000999", "created": True}
    sf.Account.get.return_value = {
        "Id": "001000000000999",
        "Name": "Acme",
        "VAT_Number__c": "BE0412341178",
    }

    # Caller may include VAT_Number__c in the dict (matches how
    # _build_facturatie_account_data builds it). Helper must strip it.
    payload = {"Name": "Acme", "VAT_Number__c": "BE0412341178", "Email__c": "a@b.c"}
    await upsert_account_by_vat(sf, "BE0412341178", payload)

    sf.Account.upsert.assert_called_once()
    url_arg = sf.Account.upsert.call_args.args[0]
    body_arg = sf.Account.upsert.call_args.args[1]
    assert url_arg == "VAT_Number__c/BE0412341178"
    assert "VAT_Number__c" not in body_arg, (
        "VAT_Number__c must not appear in the request body when used as external ID"
    )
    assert body_arg["Name"] == "Acme"
    assert body_arg["Email__c"] == "a@b.c"


# ---------------------------------------------------------------------------
# deactivate_contact (Contract 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deactivate_contact_success(sf):
    """Happy path: Contact found → IsActive__c set to False → record returned."""
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000020"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000020",
        "Email": "cancel@example.com",
        "CRM_ID__c": "uuid-deact",
        "IsActive__c": False,
    }
    sf.Contact.update.return_value = None

    result = await deactivate_contact(sf, "cancel@example.com")

    sf.Contact.update.assert_called_once_with("003000000000020", {"IsActive__c": False})
    assert result["IsActive__c"] is False
    assert result["CRM_ID__c"] == "uuid-deact"


@pytest.mark.asyncio
async def test_deactivate_contact_fallback_active_field(sf):
    """If IsActive__c does not exist, a supported fallback field should be used."""
    sf.Contact.describe.return_value = {
        "fields": [{"name": "Active__c"}]
    }
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000021"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000021",
        "Email": "cancel2@example.com",
        "CRM_ID__c": "uuid-deact-2",
        "Active__c": False,
    }

    result = await deactivate_contact(sf, "cancel2@example.com")

    sf.Contact.update.assert_called_once_with("003000000000021", {"Active__c": False})
    assert result["IsActive__c"] is False


@pytest.mark.asyncio
async def test_deactivate_contact_caches_active_field(sf):
    """describe() should run once; subsequent deactivations use cached field."""
    salesforce_client_module._active_field_cache = None
    sf.Contact.describe.return_value = {
        "fields": [{"name": "Active__c"}]
    }

    sf.query.side_effect = [
        {"totalSize": 1, "records": [{"Id": "003000000000021"}]},
        {"totalSize": 1, "records": [{"Id": "003000000000022"}]},
    ]
    sf.Contact.get.side_effect = [
        {
            "Id": "003000000000021",
            "Email": "cancel2@example.com",
            "CRM_ID__c": "uuid-deact-2",
            "Active__c": False,
        },
        {
            "Id": "003000000000021",
            "Email": "cancel2@example.com",
            "CRM_ID__c": "uuid-deact-2",
            "Active__c": False,
        },
        {
            "Id": "003000000000022",
            "Email": "cancel3@example.com",
            "CRM_ID__c": "uuid-deact-3",
            "Active__c": False,
        },
        {
            "Id": "003000000000022",
            "Email": "cancel3@example.com",
            "CRM_ID__c": "uuid-deact-3",
            "Active__c": False,
        },
    ]

    await deactivate_contact(sf, "cancel2@example.com")
    await deactivate_contact(sf, "cancel3@example.com")

    sf.Contact.describe.assert_called_once()


@pytest.mark.asyncio
async def test_deactivate_contact_not_found(sf):
    """Email not in Salesforce → returns None, no update call."""
    sf.query.return_value = {"totalSize": 0, "records": []}

    result = await deactivate_contact(sf, "unknown@example.com")

    assert result is None
    sf.Contact.update.assert_not_called()


@pytest.mark.asyncio
async def test_deactivate_contact_raises_salesforce_error(sf):
    """SalesforceError during update must propagate — caller decides retry."""
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000030"}]}
    sf.Contact.get.return_value = {"Id": "003000000000030", "Email": "err@example.com"}
    sf.Contact.update.side_effect = SalesforceError(400, "Contact", "ERROR", {"message": "update failed"})

    with pytest.raises(SalesforceError):
        await deactivate_contact(sf, "err@example.com")


# ---------------------------------------------------------------------------
# deactivate_account_by_crm_id (Contract 23)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_account_by_crm_id_returns_unique_match(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000010"}]}
    sf.Account.get.return_value = {
        "Id": "001000000000010",
        "CRM_ID__c": "crm-company-1",
        "VAT_Number__c": "BE0123456789",
    }

    result = await get_account_by_crm_id(sf, "crm-company-1")

    assert result["Id"] == "001000000000010"


@pytest.mark.asyncio
async def test_deactivate_account_by_crm_id_success(sf):
    sf.Account.describe.return_value = {
        "fields": [{"name": "IsActive__c"}]
    }
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000011"}]}
    sf.Account.get.side_effect = [
        {
            "Id": "001000000000011",
            "CRM_ID__c": "crm-company-2",
            "VAT_Number__c": "BE9876543210",
            "IsActive__c": True,
        },
        {
            "Id": "001000000000011",
            "CRM_ID__c": "crm-company-2",
            "VAT_Number__c": "BE9876543210",
            "IsActive__c": False,
        },
    ]

    result = await deactivate_account_by_crm_id(sf, "crm-company-2")

    sf.Account.update.assert_called_once_with("001000000000011", {"IsActive__c": False})
    assert result["IsActive__c"] is False


@pytest.mark.asyncio
async def test_deactivate_account_by_crm_id_returns_none_when_missing(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    result = await deactivate_account_by_crm_id(sf, "missing-company")

    assert result is None
    sf.Account.update.assert_not_called()


# ---------------------------------------------------------------------------
# Contracts 33/34/35 — Facturatie company sync (v1.9.0)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_account_match_by_crm_id_returns_none(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    match_status, account = await get_account_match_by_crm_id(sf, "missing-crm-id")

    assert match_status == "none"
    assert account is None
    sf.Account.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_account_match_by_crm_id_returns_unique(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000100"}]}
    sf.Account.get.return_value = {
        "Id": "001000000000100",
        "CRM_ID__c": "crm-company-xyz",
        "Name": "Acme NV",
        "VAT_Number__c": "BE0123456789",
    }

    match_status, account = await get_account_match_by_crm_id(sf, "crm-company-xyz")

    assert match_status == "unique"
    assert account["Id"] == "001000000000100"
    sf.Account.get.assert_called_once_with("001000000000100")


@pytest.mark.asyncio
async def test_get_account_match_by_crm_id_returns_ambiguous(sf):
    sf.query.return_value = {
        "totalSize": 2,
        "records": [{"Id": "001000000000100"}, {"Id": "001000000000101"}],
    }

    match_status, account = await get_account_match_by_crm_id(sf, "duplicate-crm-id")

    assert match_status == "ambiguous"
    assert account is None
    sf.Account.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_account_match_by_email_prefers_email_custom_field(sf):
    sf.Account.describe.return_value = {
        "fields": [{"name": "Email__c"}, {"name": "Name"}]
    }
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000102"}]}
    sf.Account.get.return_value = {"Id": "001000000000102", "Email__c": "hello@acme.example"}

    match_status, account = await get_account_match_by_email(sf, "hello@acme.example")

    assert match_status == "unique"
    assert account["Id"] == "001000000000102"
    # Verify SOQL used Email__c (not Email)
    query_call = sf.query.call_args[0][0]
    assert "Email__c" in query_call


@pytest.mark.asyncio
async def test_get_account_match_by_email_falls_back_to_email_standard(sf):
    sf.Account.describe.return_value = {
        "fields": [{"name": "Email"}, {"name": "Name"}]
    }
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "001000000000103"}]}
    sf.Account.get.return_value = {"Id": "001000000000103", "Email": "hello@acme.example"}

    match_status, account = await get_account_match_by_email(sf, "hello@acme.example")

    assert match_status == "unique"
    query_call = sf.query.call_args[0][0]
    assert "Email" in query_call


@pytest.mark.asyncio
async def test_get_account_match_by_email_returns_none_when_no_email_field(sf):
    sf.Account.describe.return_value = {
        "fields": [{"name": "Name"}, {"name": "VAT_Number__c"}]
    }

    match_status, account = await get_account_match_by_email(sf, "no-field@example.com")

    assert match_status == "none"
    assert account is None
    sf.query.assert_not_called()


@pytest.mark.asyncio
async def test_get_account_match_by_email_returns_ambiguous(sf):
    sf.Account.describe.return_value = {"fields": [{"name": "Email__c"}]}
    sf.query.return_value = {
        "totalSize": 2,
        "records": [{"Id": "001000000000104"}, {"Id": "001000000000105"}],
    }

    match_status, account = await get_account_match_by_email(sf, "duplicate@example.com")

    assert match_status == "ambiguous"
    assert account is None
    sf.Account.get.assert_not_called()


@pytest.mark.asyncio
async def test_update_facturatie_account_overwrites_changed_fields(sf):
    sf.Account.describe.return_value = {"fields": [{"name": "Email__c"}, {"name": "Name"}]}
    account = {
        "Id": "001000000000200",
        "Name": "Old Name",
        "VAT_Number__c": "BE0123456789",
        "Email__c": "old@acme.example",
        "Phone": "+32 2 000 00 00",
        "BillingStreet": "Oldstreet",
        "BillingPostalCode": "1000",
        "BillingCity": "Brussels",
        "BillingCountry": "BE",
    }
    sf.Account.get.return_value = {**account, "Name": "New Name", "Email__c": "new@acme.example"}

    result = await update_facturatie_account(
        sf,
        account,
        vat_number="BE0123456789",
        name="New Name",
        email="new@acme.example",
        phone="+32 2 000 00 00",
        street="Oldstreet",
        house_number=None,
        postal_code="1000",
        city="Brussels",
        country="BE",
    )

    sf.Account.update.assert_called_once()
    update_call = sf.Account.update.call_args
    assert update_call[0][0] == "001000000000200"
    updates = update_call[0][1]
    assert updates["Name"] == "New Name"
    assert updates["Email__c"] == "new@acme.example"
    assert result["Name"] == "New Name"


@pytest.mark.asyncio
async def test_update_facturatie_account_no_changes_skips_update(sf):
    sf.Account.describe.return_value = {"fields": [{"name": "Email__c"}, {"name": "Name"}]}
    account = {
        "Id": "001000000000201",
        "Name": "Acme NV",
        "VAT_Number__c": "BE0123456789",
        "Email__c": "same@acme.example",
        "Phone": None,
        "BillingStreet": None,
        "BillingPostalCode": None,
        "BillingCity": None,
        "BillingCountry": None,
    }

    result = await update_facturatie_account(
        sf,
        account,
        vat_number="BE0123456789",
        name="Acme NV",
        email="same@acme.example",
        phone=None,
        street=None,
        house_number=None,
        postal_code=None,
        city=None,
        country=None,
    )

    sf.Account.update.assert_not_called()
    assert result == account


@pytest.mark.asyncio
async def test_update_facturatie_account_preserves_vat_when_omitted(sf):
    """Regression — 2026-04-22 review Blocker #2. Omitted vatNumber must NOT
    clear an existing VAT_Number__c (it doubles as external ID / dedup key).
    """
    sf.Account.describe.return_value = {"fields": [{"name": "Email__c"}, {"name": "Name"}]}
    account = {
        "Id": "001000000000202",
        "Name": "Acme NV",
        "VAT_Number__c": "BE0123456789",
        "Email__c": "same@acme.example",
        "Phone": None,
        "BillingStreet": None,
        "BillingPostalCode": None,
        "BillingCity": None,
        "BillingCountry": None,
    }

    result = await update_facturatie_account(
        sf,
        account,
        vat_number=None,  # Facturatie omitted vatNumber in payload
        name="Acme Updated NV",
        email="same@acme.example",
        phone=None,
        street=None,
        house_number=None,
        postal_code=None,
        city=None,
        country=None,
    )

    assert sf.Account.update.called
    updates = sf.Account.update.call_args[0][1]
    assert "VAT_Number__c" not in updates
    assert updates == {"Name": "Acme Updated NV"}
    del result  # We only care about the update payload, not the post-fetch record.


@pytest.mark.asyncio
async def test_apply_account_is_active_uses_account_resolver(sf):
    """Regression — 2026-04-22 review Blocker #1. apply_account_is_active must
    resolve the Account active field, not Contact's.
    """
    sf.Account.describe.return_value = {
        "fields": [{"name": "Active__c", "type": "boolean"}]
    }

    result = await apply_account_is_active(sf, {}, True)

    assert result == {"Active__c": True}


@pytest.mark.asyncio
async def test_apply_account_is_active_returns_data_when_no_field(sf):
    sf.Account.describe.return_value = {"fields": [{"name": "Name"}]}

    result = await apply_account_is_active(sf, {"existing": "key"}, True)

    # No active field on Account → leave data untouched.
    assert result == {"existing": "key"}


@pytest.mark.asyncio
async def test_deactivate_account_record_sets_active_false(sf):
    sf.Account.describe.return_value = {
        "fields": [{"name": "IsActive__c", "type": "boolean"}]
    }
    account = {
        "Id": "001000000000300",
        "CRM_ID__c": "crm-deact-1",
        "VAT_Number__c": "BE0111111111",
        "IsActive__c": True,
    }
    sf.Account.get.return_value = {**account, "IsActive__c": False}

    result = await deactivate_account_record(sf, account, log_value="crm-deact-1")

    sf.Account.update.assert_called_once_with("001000000000300", {"IsActive__c": False})
    assert result["IsActive__c"] is False


@pytest.mark.asyncio
async def test_resolve_account_country_field_prefers_billing_country_code(sf):
    """Regression — 2026-04-22 production FIELD_INTEGRITY_EXCEPTION.

    Salesforce orgs with State & Country Picklists enabled have a read-only
    BillingCountry and require writes to go to BillingCountryCode (ISO-2).
    Resolver must prefer BillingCountryCode when it exists.
    """
    from src.salesforce_client import _resolve_account_country_field

    sf.Account.describe.return_value = {
        "fields": [
            {"name": "BillingCountry"},
            {"name": "BillingCountryCode"},
            {"name": "BillingCity"},
        ]
    }

    result = await _resolve_account_country_field(sf)

    assert result == "BillingCountryCode"


@pytest.mark.asyncio
async def test_resolve_account_country_field_falls_back_to_billing_country(sf):
    """Orgs without picklists only have BillingCountry (free text)."""
    from src.salesforce_client import _resolve_account_country_field

    sf.Account.describe.return_value = {
        "fields": [{"name": "BillingCountry"}, {"name": "BillingCity"}]
    }

    result = await _resolve_account_country_field(sf)

    assert result == "BillingCountry"


@pytest.mark.asyncio
async def test_update_facturatie_account_writes_to_resolved_country_field(sf):
    """update_facturatie_account must honour the picklist-enabled org layout."""
    sf.Account.describe.return_value = {
        "fields": [
            {"name": "Email__c"},
            {"name": "Name"},
            {"name": "BillingCountry"},
            {"name": "BillingCountryCode"},
        ]
    }
    account = {
        "Id": "001000000000400",
        "Name": "Acme",
        "VAT_Number__c": "BE0123456789",
        "Email__c": "a@b.c",
        "Phone": None,
        "BillingStreet": None,
        "BillingPostalCode": None,
        "BillingCity": None,
        "BillingCountry": "Belgium",
        "BillingCountryCode": "BE",
    }
    sf.Account.get.return_value = {**account, "BillingCountryCode": "NL"}

    await update_facturatie_account(
        sf,
        account,
        vat_number="BE0123456789",
        name="Acme",
        email="a@b.c",
        phone=None,
        street=None,
        house_number=None,
        postal_code=None,
        city=None,
        country="NL",
    )

    # Should update BillingCountryCode (picklist-enabled path), not BillingCountry.
    sf.Account.update.assert_called_once()
    updates = sf.Account.update.call_args[0][1]
    assert updates == {"BillingCountryCode": "NL"}


@pytest.mark.asyncio
async def test_update_facturatie_account_writes_house_number_when_field_exists(sf):
    sf.Account.describe.return_value = {
        "fields": [
            {"name": "Email__c"},
            {"name": "Name"},
            {"name": "BillingCountry"},
            {"name": "House_Number__c"},
        ]
    }
    account = {
        "Id": "001000000000401",
        "Name": "Acme",
        "VAT_Number__c": "BE0123456789",
        "Email__c": "a@b.c",
        "Phone": None,
        "BillingStreet": None,
        "House_Number__c": None,
        "BillingPostalCode": None,
        "BillingCity": None,
        "BillingCountry": "BE",
    }
    sf.Account.get.return_value = {**account, "House_Number__c": "12"}

    await update_facturatie_account(
        sf,
        account,
        vat_number="BE0123456789",
        name="Acme",
        email="a@b.c",
        phone=None,
        street=None,
        house_number="12",
        postal_code=None,
        city=None,
        country="BE",
    )

    sf.Account.update.assert_called_once()
    updates = sf.Account.update.call_args[0][1]
    assert updates == {"House_Number__c": "12"}


@pytest.mark.asyncio
async def test_deactivate_account_record_normalizes_legacy_active_field(sf):
    """When the org uses Active__c or Is_Active__c, the returned record should
    expose IsActive__c for stable downstream payload building."""
    sf.Account.describe.return_value = {
        "fields": [{"name": "Is_Active__c", "type": "boolean"}]
    }
    account = {
        "Id": "001000000000301",
        "CRM_ID__c": "crm-deact-2",
        "VAT_Number__c": "BE0222222222",
        "Is_Active__c": True,
    }
    sf.Account.get.return_value = {**account, "Is_Active__c": False}

    result = await deactivate_account_record(sf, account)

    sf.Account.update.assert_called_once_with("001000000000301", {"Is_Active__c": False})
    assert result["IsActive__c"] is False


# ---------------------------------------------------------------------------
# get_unpaid_contacts (Contract 17)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_unpaid_contacts_returns_sorted_mapped_persons(sf):
    sf.query_all.return_value = {
        "records": [
            {
                "Contact__r": {
                    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440102",
                    "FirstName": "Zara",
                    "LastName": "Alpha",
                    "Email": "zara@example.com",
                    "AccountId": "001-company",
                    "Account": {"Name": "Acme NV"},
                    "IsActive__c": True,
                },
            },
            {
                "Contact__r": {
                    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440101",
                    "FirstName": "Anna",
                    "LastName": "Alpha",
                    "Email": "anna@example.com",
                    "AccountId": None,
                    "IsActive__c": True,
                },
            },
        ]
    }

    result = await get_unpaid_contacts(sf)

    assert result == [
        {
            "id": "550e8400-e29b-41d4-a716-446655440101",
            "firstName": "Anna",
            "lastName": "Alpha",
            "email": "anna@example.com",
            "linkedToCompany": False,
        },
        {
            "id": "550e8400-e29b-41d4-a716-446655440102",
            "firstName": "Zara",
            "lastName": "Alpha",
            "email": "zara@example.com",
            "linkedToCompany": True,
            "companyName": "Acme NV",
        },
    ]
    assert "FROM Session_Registration__c" in sf.query_all.call_args.args[0]
    assert "Paid_At__c = NULL" in sf.query_all.call_args.args[0]
    assert "Contact__r.Account.Name" in sf.query_all.call_args.args[0]


@pytest.mark.asyncio
async def test_get_unpaid_contacts_skips_inactive_records(sf):
    sf.query_all.return_value = {
        "records": [
            {
                "Contact__r": {
                    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440103",
                    "FirstName": "Inactive",
                    "LastName": "User",
                    "Email": "inactive@example.com",
                    "IsActive__c": False,
                },
            },
            {
                "Contact__r": {
                    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440104",
                    "FirstName": "Active",
                    "LastName": "User",
                    "Email": "active@example.com",
                    "IsActive__c": None,
                },
            },
        ]
    }

    result = await get_unpaid_contacts(sf)

    assert result == [
        {
            "id": "550e8400-e29b-41d4-a716-446655440104",
            "firstName": "Active",
            "lastName": "User",
            "email": "active@example.com",
            "linkedToCompany": False,
        }
    ]


@pytest.mark.asyncio
async def test_get_unpaid_contacts_skips_records_missing_required_fields(sf, caplog):
    sf.query_all.return_value = {
        "records": [
            {"Contact__r": {"CRM_ID__c": None, "Email": "missing-id@example.com", "IsActive__c": True}},
            {"Contact__r": {"CRM_ID__c": "crm-missing-email", "Email": None, "IsActive__c": True}},
        ]
    }

    result = await get_unpaid_contacts(sf)

    assert result == []
    assert "Skipping unpaid contact without required fields" in caplog.text


@pytest.mark.asyncio
async def test_get_unpaid_contacts_returns_empty_list_when_no_records(sf):
    sf.query_all.return_value = {"records": []}

    result = await get_unpaid_contacts(sf)

    assert result == []


@pytest.mark.asyncio
async def test_get_unpaid_contacts_does_not_require_active_field_migration(sf):
    sf.Contact.describe.return_value = {"fields": []}
    sf.query_all.return_value = {
        "records": [
            {
                "Contact__r": {
                    "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440105",
                    "FirstName": "Legacy",
                    "LastName": "Contact",
                    "Email": "legacy@example.com",
                    "AccountId": None,
                },
            }
        ]
    }

    result = await get_unpaid_contacts(sf)

    assert result == [
        {
            "id": "550e8400-e29b-41d4-a716-446655440105",
            "firstName": "Legacy",
            "lastName": "Contact",
            "email": "legacy@example.com",
            "linkedToCompany": False,
        }
    ]
    query = sf.query_all.call_args.args[0]
    assert "Contact__r.IsActive__c" not in query
    assert "Contact__r.Active__c" not in query
    assert "Contact__r.Is_Active__c" not in query


@pytest.mark.asyncio
async def test_get_unpaid_contacts_skips_invalid_crm_ids(sf, caplog):
    sf.query_all.return_value = {
        "records": [
            {
                "Contact__r": {
                    "CRM_ID__c": "legacy-text-id",
                    "FirstName": "Legacy",
                    "LastName": "Broken",
                    "Email": "legacy.broken@example.com",
                    "AccountId": None,
                    "IsActive__c": True,
                },
            },
            {
                "Contact__r": {
                    "CRM_ID__c": "550E8400-E29B-41D4-A716-446655440099",
                    "FirstName": "Upper",
                    "LastName": "Case",
                    "Email": "upper.case@example.com",
                    "AccountId": None,
                    "IsActive__c": True,
                },
            },
        ]
    }

    result = await get_unpaid_contacts(sf)

    assert result == [
        {
            "id": "550e8400-e29b-41d4-a716-446655440099",
            "firstName": "Upper",
            "lastName": "Case",
            "email": "upper.case@example.com",
            "linkedToCompany": False,
        }
    ]
    assert "Skipping unpaid contact with invalid CRM_ID__c: legacy-text-id" in caplog.text


# ---------------------------------------------------------------------------
# update_payment_status (Contract 16)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_payment_status_updates_via_crm_id(sf):
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000040"}]}
    sf.Contact.get.side_effect = [
        {
            "Id": "003000000000040",
            "CRM_ID__c": "crm-user-1",
            "Email": "john@example.com",
        },
        {
            "Id": "003000000000040",
            "CRM_ID__c": "crm-user-1",
            "Email": "john@example.com",
            "Paid_At__c": "2026-04-02T10:00:00Z",
        },
    ]

    result = await update_payment_status(
        sf,
        user_id="crm-user-1",
        email="john@example.com",
        registration_id="REG-123",
        paid_at="2026-04-02T10:00:00Z",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000040", {"Paid_At__c": "2026-04-02T10:00:00Z"}
    )
    sf.Session_Registration__c.update.assert_not_called()
    assert result["Paid_At__c"] == "2026-04-02T10:00:00Z"


@pytest.mark.asyncio
async def test_update_payment_status_advances_contact_timestamp_when_newer(sf):
    sf.query.return_value = {
        "totalSize": 1,
        "records": [{"Id": "003000000000041"}],
    }
    sf.Contact.get.side_effect = [
        {
            "Id": "003000000000041",
            "CRM_ID__c": "crm-user-2",
            "Email": "unique@example.com",
        },
        {
            "Id": "003000000000041",
            "CRM_ID__c": "crm-user-2",
            "Email": "unique@example.com",
            "Paid_At__c": "2026-04-02T11:00:00Z",
        },
    ]

    result = await update_payment_status(
        sf,
        user_id=None,
        email="unique@example.com",
        registration_id=None,
        paid_at="2026-04-02T11:00:00Z",
    )

    sf.Contact.update.assert_called_once_with(
        "003000000000041", {"Paid_At__c": "2026-04-02T11:00:00Z"}
    )
    sf.Session_Registration__c.update.assert_not_called()
    assert result["Email"] == "unique@example.com"


@pytest.mark.asyncio
async def test_update_payment_status_returns_none_for_ambiguous_email(sf):
    sf.query.return_value = {
        "totalSize": 2,
        "records": [{"Id": "003000000000041"}, {"Id": "003000000000042"}],
    }

    result = await update_payment_status(
        sf,
        user_id=None,
        email="duplicate@example.com",
        registration_id=None,
        paid_at="2026-04-02T11:00:00Z",
    )

    assert result is None
    sf.Contact.update.assert_not_called()


@pytest.mark.asyncio
async def test_update_payment_status_returns_none_for_registration_id_mismatch(sf):
    """Met Contact-only flow geldt: als CRM_ID__c lookup faalt, return None."""
    sf.query.return_value = {"totalSize": 0, "records": []}

    result = await update_payment_status(
        sf,
        user_id="crm-user-3",
        email="mismatch@example.com",
        registration_id="REG-NEW",
        paid_at="2026-04-02T12:00:00Z",
    )

    assert result is None
    sf.Contact.update.assert_not_called()


@pytest.mark.asyncio
async def test_update_payment_status_does_not_move_contact_timestamp_backwards(sf):
    sf.query.return_value = {
        "totalSize": 1,
        "records": [{"Id": "003000000000044"}],
    }
    sf.Contact.get.side_effect = [
        {
            "Id": "003000000000044",
            "CRM_ID__c": "crm-user-4",
            "Email": "backwards@example.com",
            "Paid_At__c": "2026-04-02T12:00:00Z",
        },
        {
            "Id": "003000000000044",
            "CRM_ID__c": "crm-user-4",
            "Email": "backwards@example.com",
            "Paid_At__c": "2026-04-02T12:00:00Z",
        },
    ]

    result = await update_payment_status(
        sf,
        user_id=None,
        email="backwards@example.com",
        registration_id=None,
        paid_at="2026-04-02T11:00:00Z",
    )

    sf.Contact.update.assert_not_called()
    sf.Session_Registration__c.update.assert_not_called()
    assert result["Paid_At__c"] == "2026-04-02T12:00:00Z"


@pytest.mark.asyncio
async def test_update_payment_status_overwrites_invalid_existing_contact_timestamp(sf, caplog):
    sf.query.return_value = {
        "totalSize": 1,
        "records": [{"Id": "003000000000045"}],
    }
    sf.Contact.get.side_effect = [
        {
            "Id": "003000000000045",
            "CRM_ID__c": "crm-user-5",
            "Email": "badts@example.com",
            "Paid_At__c": "not-a-date",
        },
        {
            "Id": "003000000000045",
            "CRM_ID__c": "crm-user-5",
            "Email": "badts@example.com",
            "Paid_At__c": "2026-04-02T13:00:00Z",
        },
    ]

    with caplog.at_level(logging.WARNING):
        result = await update_payment_status(
            sf,
            user_id=None,
            email="badts@example.com",
            registration_id="REG-BAD-TS",
            paid_at="2026-04-02T13:00:00Z",
        )

    sf.Contact.update.assert_called_once_with(
        "003000000000045", {"Paid_At__c": "2026-04-02T13:00:00Z"}
    )
    sf.Session_Registration__c.update.assert_not_called()
    assert "invalid Paid_At__c value" in caplog.text
    assert result["Paid_At__c"] == "2026-04-02T13:00:00Z"


@pytest.mark.asyncio
async def test_update_payment_status_does_not_fallback_to_email_when_user_id_missing_in_salesforce(sf):
    sf.query.return_value = {"totalSize": 0, "records": []}

    result = await update_payment_status(
        sf,
        user_id="missing-user-id",
        email="john@example.com",
        registration_id=None,
        paid_at="2026-04-02T13:00:00Z",
    )

    assert result is None
    sf.Contact.update.assert_not_called()


# ===========================================================================
# get_salesforce_client — startup retry
# ===========================================================================


@pytest.fixture
def sleep_mock(monkeypatch):
    """Replace asyncio.sleep with a no-op so retry tests do not actually wait."""
    sleep = AsyncMock()
    monkeypatch.setattr(salesforce_client_module.asyncio, "sleep", sleep)
    return sleep


@pytest.mark.asyncio
async def test_get_salesforce_client_returns_authenticated_client(config, monkeypatch):
    client_stub = MagicMock(name="SalesforceClient")
    sf_constructor = MagicMock(return_value=client_stub)
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    result = await get_salesforce_client(config)

    assert result is client_stub
    sf_constructor.assert_called_once()
    call_kwargs = sf_constructor.call_args.kwargs
    assert call_kwargs["username"] == config.salesforce_username
    assert call_kwargs["password"] == config.salesforce_password
    assert call_kwargs["security_token"] == config.salesforce_security_token
    assert call_kwargs["domain"] == config.salesforce_domain
    # Session must enforce a default timeout to prevent silent hangs.
    assert isinstance(call_kwargs["session"], salesforce_client_module._TimeoutSession)


@pytest.mark.asyncio
async def test_get_salesforce_client_retries_on_transient_server_unavailable(
    config, monkeypatch, sleep_mock,
):
    client_stub = MagicMock(name="SalesforceClient")
    sf_constructor = MagicMock(
        side_effect=[
            SalesforceAuthenticationFailed("SERVER_UNAVAILABLE", "transient 1"),
            SalesforceAuthenticationFailed("SERVER_UNAVAILABLE", "transient 2"),
            client_stub,
        ],
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    result = await get_salesforce_client(config)

    assert result is client_stub
    assert sf_constructor.call_count == 3
    # Backoff sequence on 2 failures before success: 1.0s, then 2.0s.
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1.0, 2.0]


@pytest.mark.asyncio
async def test_get_salesforce_client_retries_on_service_unavailable(
    config, monkeypatch, sleep_mock,
):
    client_stub = MagicMock(name="SalesforceClient")
    sf_constructor = MagicMock(
        side_effect=[
            SalesforceAuthenticationFailed("SERVICE_UNAVAILABLE", "transient"),
            client_stub,
        ],
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    result = await get_salesforce_client(config)

    assert result is client_stub
    assert sf_constructor.call_count == 2
    sleep_mock.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_get_salesforce_client_retries_on_generic_network_error(
    config, monkeypatch, sleep_mock,
):
    client_stub = MagicMock(name="SalesforceClient")
    sf_constructor = MagicMock(
        side_effect=[
            ConnectionError("DNS failure"),
            client_stub,
        ],
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    result = await get_salesforce_client(config)

    assert result is client_stub
    assert sf_constructor.call_count == 2
    sleep_mock.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_get_salesforce_client_fails_fast_on_invalid_login(
    config, monkeypatch, sleep_mock,
):
    sf_constructor = MagicMock(
        side_effect=SalesforceAuthenticationFailed(
            "INVALID_LOGIN", "invalid username or password",
        ),
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    with pytest.raises(SalesforceAuthenticationFailed) as exc_info:
        await get_salesforce_client(config)

    assert exc_info.value.code == "INVALID_LOGIN"
    assert sf_constructor.call_count == 1
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_salesforce_client_fails_fast_on_password_lockout(
    config, monkeypatch, sleep_mock,
):
    sf_constructor = MagicMock(
        side_effect=SalesforceAuthenticationFailed(
            "PASSWORD_LOCKOUT", "account locked",
        ),
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    with pytest.raises(SalesforceAuthenticationFailed) as exc_info:
        await get_salesforce_client(config)

    assert exc_info.value.code == "PASSWORD_LOCKOUT"
    assert sf_constructor.call_count == 1
    sleep_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_salesforce_client_aborts_when_shutdown_set_before_first_attempt(
    config, monkeypatch,
):
    sf_constructor = MagicMock()
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    shutdown_event = asyncio.Event()
    shutdown_event.set()

    with pytest.raises(RuntimeError, match="cancelled by shutdown"):
        await get_salesforce_client(config, shutdown_event=shutdown_event)

    sf_constructor.assert_not_called()


@pytest.mark.asyncio
async def test_get_salesforce_client_aborts_when_shutdown_set_during_backoff(
    config, monkeypatch,
):
    shutdown_event = asyncio.Event()

    def fail_and_signal_shutdown(**_kwargs):
        shutdown_event.set()
        raise SalesforceAuthenticationFailed("SERVER_UNAVAILABLE", "transient")

    sf_constructor = MagicMock(side_effect=fail_and_signal_shutdown)
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    with pytest.raises(RuntimeError, match="cancelled by shutdown"):
        await get_salesforce_client(config, shutdown_event=shutdown_event)

    assert sf_constructor.call_count == 1


@pytest.mark.asyncio
async def test_get_salesforce_client_backoff_caps_at_max_delay(
    config, monkeypatch, sleep_mock,
):
    # 7 consecutive transient failures, then success. Expected backoff:
    # 1, 2, 4, 8, 16, 32, 60 (capped) — 7 sleeps before the 8th attempt succeeds.
    client_stub = MagicMock(name="SalesforceClient")
    sf_constructor = MagicMock(
        side_effect=[
            SalesforceAuthenticationFailed("SERVER_UNAVAILABLE", f"fail {i}")
            for i in range(7)
        ] + [client_stub],
    )
    monkeypatch.setattr(salesforce_client_module, "Salesforce", sf_constructor)

    result = await get_salesforce_client(config)

    assert result is client_stub
    assert sf_constructor.call_count == 8
    assert [call.args[0] for call in sleep_mock.await_args_list] == [
        1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0,
    ]


# ---------------------------------------------------------------------------
# escape_soql — SOQL string-literal escaping
# ---------------------------------------------------------------------------

class TestEscapeSoql:
    """Verify SOQL injection protection. Regression for Facturatie sync review."""

    def test_escapes_single_quote_with_backslash(self):
        from src.salesforce_client import escape_soql

        assert escape_soql("O'Brien") == "O\\'Brien"

    def test_escapes_backslash_before_quote(self):
        """Known SOQL injection vector: backslash-quote breaks naive escaping.

        Without backslash escaping, `foo\\'` becomes `foo\\''` after quote doubling;
        SOQL parses `\\'` as escaped quote so the trailing `'` closes the literal.
        """
        from src.salesforce_client import escape_soql

        # Input contains a single backslash followed by a quote.
        escaped = escape_soql("foo\\' OR CreatedDate > 2000-01-01 --")
        # Expect: backslash doubled, then quote escaped → `foo\\\\\\' OR ...`
        assert escaped == "foo\\\\\\' OR CreatedDate > 2000-01-01 --"
        # After escaping, the backslash is a literal `\\\\` (no escape meaning to
        # SOQL) and the `\\'` is a proper escaped quote, so the literal remains closed.

    def test_no_metachars_passthrough(self):
        from src.salesforce_client import escape_soql

        assert escape_soql("plain@example.com") == "plain@example.com"


# ---------------------------------------------------------------------------
# is_rate_limit_error — shared detector for REQUEST_LIMIT_EXCEEDED
# ---------------------------------------------------------------------------

class TestIsRateLimitError:
    """Shared between receiver and polling task — detects SF daily API cap."""

    def test_detects_errorcode_in_content_list(self):
        from src.salesforce_client import is_rate_limit_error

        exc = Exception("refused")
        exc.content = [{"errorCode": "REQUEST_LIMIT_EXCEEDED", "message": "limit"}]
        assert is_rate_limit_error(exc) is True

    def test_detects_in_message_string(self):
        from src.salesforce_client import is_rate_limit_error

        exc = RuntimeError("Salesforce query failed: REQUEST_LIMIT_EXCEEDED TotalRequests Limit exceeded.")
        assert is_rate_limit_error(exc) is True

    def test_returns_false_for_unrelated_error(self):
        from src.salesforce_client import is_rate_limit_error

        assert is_rate_limit_error(ValueError("bad value")) is False

    def test_returns_false_for_content_without_errorcode(self):
        from src.salesforce_client import is_rate_limit_error

        exc = Exception("other")
        exc.content = [{"errorCode": "INVALID_FIELD"}]
        assert is_rate_limit_error(exc) is False

    def test_handles_non_list_content_attribute(self):
        from src.salesforce_client import is_rate_limit_error

        exc = Exception("other")
        exc.content = "not a list"
        assert is_rate_limit_error(exc) is False


# ---------------------------------------------------------------------------
# coerce_is_active — shared active-flag normalization (P3 fix)
# ---------------------------------------------------------------------------

class TestCoerceIsActive:
    """Regression for live-org bug: bool('No') is True but 'No' means inactive."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            (True, True),
            (False, False),
            (None, True),  # missing defaults to active
            ("Yes", True),
            ("No", False),
            ("yes", True),
            ("no", False),
            ("Y", True),
            ("N", False),
            ("true", True),
            ("false", False),
            ("True", True),
            ("False", False),
            ("1", True),
            ("0", False),
            ("", False),
            ("  yes  ", True),
            (1, True),
            (0, False),
        ],
    )
    def test_covers_all_variants(self, raw, expected):
        from src.salesforce_client import coerce_is_active

        assert coerce_is_active(raw) is expected


# ---------------------------------------------------------------------------
# is_expired_session_error — detects expired SF session for auto-reauth
# ---------------------------------------------------------------------------

class TestIsExpiredSessionError:
    """Detector for expired SF sessions used by sf_call auto-reauth wrapper.

    Catches both the canonical 401 path (SalesforceExpiredSession) and the
    observed production 302→404 path where SF redirects the query endpoint
    and simple-salesforce reports it as SalesforceResourceNotFound with
    resource_name='query' or INVALID_SESSION_ID in the error content.
    """

    def test_detects_native_expired_session(self):
        from simple_salesforce.exceptions import SalesforceExpiredSession

        from src.salesforce_client import is_expired_session_error

        exc = SalesforceExpiredSession(
            "https://example.salesforce.com/services/data/v59.0/query/",
            401,
            "query",
            b'[{"errorCode":"INVALID_SESSION_ID","message":"Session expired"}]',
        )
        assert is_expired_session_error(exc) is True

    def test_detects_invalid_session_id_in_content(self):
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import is_expired_session_error

        exc = SalesforceResourceNotFound(
            "https://example.salesforce.com/services/data/v59.0/query/",
            404,
            "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired or invalid"}],
        )
        assert is_expired_session_error(exc) is True

    def test_query_404_with_empty_body_is_not_session_expired(self):
        """Regression: empty-body 404 on /query is NOT session-expired.

        A previous heuristic classified these as expired, which caused a
        false-positive reauth loop in production (~40min cadence) when SF
        returned transient empty-body 404s on the /query endpoint. Real
        session expiry is covered by SalesforceExpiredSession or
        INVALID_SESSION_ID in the content list. Empty-body 404s are
        handled separately by `is_transient_query_404` + a polling-side
        WARNING log (no reauth). See commit 5686f19.
        """
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import is_expired_session_error

        exc = SalesforceResourceNotFound(
            "https://example.salesforce.com/services/data/v59.0/query/",
            404,
            "query",
            b"",
        )
        assert is_expired_session_error(exc) is False

    def test_ignores_404_on_unrelated_resource(self):
        """A genuine 404 (e.g. deleted custom endpoint) must NOT reauth-loop."""
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import is_expired_session_error

        exc = SalesforceResourceNotFound(
            "https://example.salesforce.com/services/data/v59.0/sobjects/Nonexistent__c/",
            404,
            "sobjects/Nonexistent__c",
            [{"errorCode": "NOT_FOUND", "message": "The requested resource does not exist"}],
        )
        assert is_expired_session_error(exc) is False

    def test_ignores_generic_exception(self):
        from src.salesforce_client import is_expired_session_error

        assert is_expired_session_error(ValueError("bad value")) is False


# ---------------------------------------------------------------------------
# is_transient_query_404 — log-only signal for polling, separate from reauth
# ---------------------------------------------------------------------------

class TestIsTransientQuery404:
    """Detector for the empty-body 404 SF emits on /query during polling.

    This signal MUST stay decoupled from `is_expired_session_error` —
    reauth on it caused a documented production loop (commit 5686f19).
    The polling cycle handles it via WARNING + skip.
    """

    def _make_404(
        self,
        *,
        url: str = "https://example.salesforce.com/services/data/v59.0/query/",
        status: int = 404,
        name: str = "query",
        content: object = b"",
    ):
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        return SalesforceResourceNotFound(url, status, name, content)

    def test_detects_empty_body_404_on_query(self):
        from src.salesforce_client import is_transient_query_404

        assert is_transient_query_404(self._make_404()) is True

    def test_detects_when_only_url_indicates_query(self):
        from src.salesforce_client import is_transient_query_404

        # resource_name absent / generic, but URL ends with /query
        exc = self._make_404(name="")
        assert is_transient_query_404(exc) is True

    def test_does_not_match_query_all_endpoint(self):
        """`/queryAll` is a different endpoint and must not match.

        The substring check ``/query`` would have matched ``/queryAll``;
        the strict ``endswith("/query")`` rule prevents that.
        """
        from src.salesforce_client import is_transient_query_404

        exc = self._make_404(
            url="https://example.salesforce.com/services/data/v59.0/queryAll/",
            name="queryAll",
        )
        assert is_transient_query_404(exc) is False

    def test_does_not_match_when_content_is_non_empty(self):
        """Non-empty content means SF returned a real REST error body."""
        from src.salesforce_client import is_transient_query_404

        exc = self._make_404(content=[{"errorCode": "MALFORMED_QUERY"}])
        assert is_transient_query_404(exc) is False

    def test_does_not_match_unrelated_resource(self):
        from src.salesforce_client import is_transient_query_404

        exc = self._make_404(
            url="https://example.salesforce.com/services/data/v59.0/sobjects/Contact/",
            name="sobjects/Contact",
        )
        assert is_transient_query_404(exc) is False

    def test_does_not_match_non_404_exception(self):
        from src.salesforce_client import is_transient_query_404

        assert is_transient_query_404(ValueError("oops")) is False


# ---------------------------------------------------------------------------
# SalesforceSession + sf_call — auto-reauth wrapper for long-lived polling
# ---------------------------------------------------------------------------

class TestSfCall:
    """sf_call wraps SF calls with one-shot reauth on expired session."""

    @pytest.mark.asyncio
    async def test_returns_result_on_first_try_success(self):
        from src.salesforce_client import SalesforceSession, sf_call

        fake_sf = MagicMock()
        fake_sf.query.return_value = {"records": [{"Id": "001"}]}

        session = SalesforceSession(
            fake_sf, config=MagicMock(), shutdown_event=None,
        )

        result = await sf_call(session, lambda sf: sf.query("SELECT Id FROM Contact"))

        assert result == {"records": [{"Id": "001"}]}
        fake_sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_reauths_and_retries_once_on_expired_session(self, monkeypatch):
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import SalesforceSession, sf_call

        old_sf = MagicMock()
        old_sf.query.side_effect = SalesforceResourceNotFound(
            "https://example/query/",
            404,
            "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}],
        )

        new_sf = MagicMock()
        new_sf.query.return_value = {"records": []}

        async def fake_get_client(config, shutdown_event=None):
            return new_sf

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_get_client,
        )

        session = SalesforceSession(old_sf, config=MagicMock(), shutdown_event=None)

        result = await sf_call(session, lambda sf: sf.query("SELECT Id FROM Contact"))

        assert result == {"records": []}
        assert session.sf is new_sf  # reauth mutated the container
        old_sf.query.assert_called_once()
        new_sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_reauth_on_empty_body_query_404(self, monkeypatch):
        """Regression: empty-body 404 on /query must NOT trigger reauth.

        Polling handles this transient via WARNING log + skip-cycle. Reauth
        on this signal caused a documented ~40min false-positive loop
        (commit 5686f19); sf_call must propagate the error so the polling
        outer handler can downgrade it.
        """
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import SalesforceSession, sf_call

        reauth_calls: list[int] = []

        async def fake_get_client(config, shutdown_event=None):
            reauth_calls.append(1)
            return MagicMock()

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_get_client,
        )

        old_sf = MagicMock()
        old_sf.query_all.side_effect = SalesforceResourceNotFound(
            "https://example.salesforce.com/services/data/v59.0/query/",
            404,
            "query",
            b"",
        )

        session = SalesforceSession(old_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(SalesforceResourceNotFound):
            await sf_call(session, lambda sf: sf.query_all("SELECT Id FROM Contact"))

        assert reauth_calls == []
        old_sf.query_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_propagates_rate_limit_without_reauth(self, monkeypatch):
        from src.salesforce_client import SalesforceSession, sf_call

        reauth_calls = []

        async def fake_get_client(config, shutdown_event=None):
            reauth_calls.append(1)
            return MagicMock()

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_get_client,
        )

        fake_sf = MagicMock()
        rate_limit_exc = Exception("REQUEST_LIMIT_EXCEEDED: daily API cap")
        fake_sf.query.side_effect = rate_limit_exc

        session = SalesforceSession(fake_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(Exception, match="REQUEST_LIMIT_EXCEEDED"):
            await sf_call(session, lambda sf: sf.query("soql"))

        # No reauth triggered — rate-limit must surface to outer handler
        assert reauth_calls == []
        fake_sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_propagates_non_session_error_without_reauth(self, monkeypatch):
        from src.salesforce_client import SalesforceSession, sf_call

        reauth_calls = []

        async def fake_get_client(config, shutdown_event=None):
            reauth_calls.append(1)
            return MagicMock()

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_get_client,
        )

        fake_sf = MagicMock()
        fake_sf.query.side_effect = ValueError("malformed SOQL")

        session = SalesforceSession(fake_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(ValueError, match="malformed SOQL"):
            await sf_call(session, lambda sf: sf.query("soql"))

        assert reauth_calls == []

    @pytest.mark.asyncio
    async def test_propagates_after_second_expired_failure(self, monkeypatch):
        """Max 1 reauth: if both try and retry fail with expired, propagate."""
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import SalesforceSession, sf_call

        persistent_exc = SalesforceResourceNotFound(
            "https://example/query/",
            404,
            "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}],
        )

        old_sf = MagicMock()
        old_sf.query.side_effect = persistent_exc

        new_sf = MagicMock()
        new_sf.query.side_effect = persistent_exc

        reauth_call_count = {"n": 0}

        async def fake_get_client(config, shutdown_event=None):
            reauth_call_count["n"] += 1
            return new_sf

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_get_client,
        )

        session = SalesforceSession(old_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(SalesforceResourceNotFound):
            await sf_call(session, lambda sf: sf.query("soql"))

        assert reauth_call_count["n"] == 1  # exactly one reauth attempted
        old_sf.query.assert_called_once()
        new_sf.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_sf_call_converts_shutdown_runtime_error_to_cancelled(self, monkeypatch):
        """Shutdown RuntimeError from get_salesforce_client → CancelledError.

        Hardening for H1: if `get_salesforce_client` raises RuntimeError on
        shutdown signal during reauth, sf_call must translate to
        CancelledError so run_polling's outer `except asyncio.CancelledError`
        re-raises cleanly instead of logging "cycle failed" and sleeping.
        """
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import SalesforceSession, sf_call

        expired_exc = SalesforceResourceNotFound(
            "https://example/query/",
            404,
            "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}],
        )

        old_sf = MagicMock()
        old_sf.query.side_effect = expired_exc

        async def fake_reauth_shutdown(config, shutdown_event=None):
            raise RuntimeError("Salesforce connection cancelled by shutdown signal")

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_reauth_shutdown,
        )

        session = SalesforceSession(old_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(asyncio.CancelledError):
            await sf_call(session, lambda sf: sf.query("soql"))

    @pytest.mark.asyncio
    async def test_sf_call_propagates_reauth_auth_failure(self, monkeypatch):
        """SalesforceAuthenticationFailed during reauth propagates unchanged.

        Hardening for H1: when credentials are rotated mid-run, the first
        expired-session triggers a reauth that raises AuthFailed. We must
        let it propagate so run_polling's outer handler logs the real root
        cause — not translate it silently.
        """
        from simple_salesforce.exceptions import (
            SalesforceAuthenticationFailed,
            SalesforceResourceNotFound,
        )

        from src.salesforce_client import SalesforceSession, sf_call

        expired_exc = SalesforceResourceNotFound(
            "https://example/query/",
            404,
            "query",
            [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired"}],
        )

        old_sf = MagicMock()
        old_sf.query.side_effect = expired_exc

        async def fake_reauth_bad_creds(config, shutdown_event=None):
            raise SalesforceAuthenticationFailed("INVALID_LOGIN", "bad password")

        monkeypatch.setattr(
            "src.salesforce_client.get_salesforce_client", fake_reauth_bad_creds,
        )

        session = SalesforceSession(old_sf, config=MagicMock(), shutdown_event=None)

        with pytest.raises(SalesforceAuthenticationFailed):
            await sf_call(session, lambda sf: sf.query("soql"))


class TestIsExpiredSessionErrorContentGuard:
    """Ensure 404s on /query with non-session content do not trigger reauth."""

    def test_ignores_query_404_with_non_session_errorcode(self):
        """A real 404 response with MALFORMED_QUERY content is NOT expired."""
        from simple_salesforce.exceptions import SalesforceResourceNotFound

        from src.salesforce_client import is_expired_session_error

        exc = SalesforceResourceNotFound(
            "https://example/services/data/v59.0/query/",
            404,
            "query",
            [{"errorCode": "MALFORMED_QUERY", "message": "unexpected token"}],
        )
        assert is_expired_session_error(exc) is False
