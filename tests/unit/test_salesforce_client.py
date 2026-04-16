from unittest.mock import MagicMock

import pytest
from simple_salesforce import SalesforceError

import src.salesforce_client as salesforce_client_module
from src.salesforce_client import (
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    create_account,
    create_contact,
    deactivate_contact,
    ensure_contact_identifiers,
    find_unique_contact_by_email,
    get_account_by_vat,
    get_contact_by_crm_id,
    get_contact_by_email,
    get_contact_match_by_email,
    get_contact_match_by_planning_id,
    get_unpaid_contacts,
    has_contact_mailing_id_field,
    has_contact_planning_id_field,
    update_mailing_contact,
    update_payment_status,
    upsert_account_by_vat,
    upsert_contact_by_email,
)


@pytest.fixture
def sf(monkeypatch):
    salesforce_client_module._active_field_cache = None
    salesforce_client_module._mailing_id_field_supported_cache = None
    salesforce_client_module._planning_id_field_supported_cache = None

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
    sf.query = MagicMock()
    sf.query_all = MagicMock()
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
# get_unpaid_contacts (Contract 17)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_unpaid_contacts_returns_sorted_mapped_persons(sf):
    sf.query_all.return_value = {
        "records": [
            {
                "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440102",
                "FirstName": "Zara",
                "LastName": "Alpha",
                "Email": "zara@example.com",
                "AccountId": "001-company",
                "Account": {"Name": "Acme NV"},
                "IsActive__c": True,
            },
            {
                "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440101",
                "FirstName": "Anna",
                "LastName": "Alpha",
                "Email": "anna@example.com",
                "AccountId": None,
                "IsActive__c": True,
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
    assert "Paid_At__c = NULL" in sf.query_all.call_args.args[0]
    assert "Account.Name" in sf.query_all.call_args.args[0]


@pytest.mark.asyncio
async def test_get_unpaid_contacts_skips_inactive_records(sf):
    sf.query_all.return_value = {
        "records": [
            {
                "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440103",
                "FirstName": "Inactive",
                "LastName": "User",
                "Email": "inactive@example.com",
                "IsActive__c": False,
            },
            {
                "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440104",
                "FirstName": "Active",
                "LastName": "User",
                "Email": "active@example.com",
                "IsActive__c": None,
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
            {"CRM_ID__c": None, "Email": "missing-id@example.com", "IsActive__c": True},
            {"CRM_ID__c": "crm-missing-email", "Email": None, "IsActive__c": True},
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
                "CRM_ID__c": "550e8400-e29b-41d4-a716-446655440105",
                "FirstName": "Legacy",
                "LastName": "Contact",
                "Email": "legacy@example.com",
                "AccountId": None,
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
    assert "IsActive__c" not in query
    assert "Active__c" not in query
    assert "Is_Active__c" not in query


@pytest.mark.asyncio
async def test_get_unpaid_contacts_skips_invalid_crm_ids(sf, caplog):
    sf.query_all.return_value = {
        "records": [
            {
                "CRM_ID__c": "legacy-text-id",
                "FirstName": "Legacy",
                "LastName": "Broken",
                "Email": "legacy.broken@example.com",
                "AccountId": None,
                "IsActive__c": True,
            },
            {
                "CRM_ID__c": "550E8400-E29B-41D4-A716-446655440099",
                "FirstName": "Upper",
                "LastName": "Case",
                "Email": "upper.case@example.com",
                "AccountId": None,
                "IsActive__c": True,
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
            "Registration_ID__c": "REG-123",
        },
        {
            "Id": "003000000000040",
            "CRM_ID__c": "crm-user-1",
            "Email": "john@example.com",
            "Registration_ID__c": "REG-123",
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
    assert result["Paid_At__c"] == "2026-04-02T10:00:00Z"


@pytest.mark.asyncio
async def test_update_payment_status_falls_back_to_unique_email(sf):
    sf.query.side_effect = [
        {"totalSize": 1, "records": [{"Id": "003000000000041"}]},
    ]
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
    sf.query.return_value = {"totalSize": 1, "records": [{"Id": "003000000000043"}]}
    sf.Contact.get.return_value = {
        "Id": "003000000000043",
        "CRM_ID__c": "crm-user-3",
        "Email": "mismatch@example.com",
        "Registration_ID__c": "REG-OLD",
    }

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
