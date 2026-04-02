from unittest.mock import MagicMock

import pytest
from simple_salesforce import SalesforceError

import src.salesforce_client as salesforce_client_module
from src.salesforce_client import (
    create_account,
    create_contact,
    deactivate_contact,
    find_unique_contact_by_email,
    get_account_by_vat,
    get_contact_by_crm_id,
    get_contact_by_email,
    update_payment_status,
    upsert_account_by_vat,
    upsert_contact_by_email,
)


@pytest.fixture
def sf(monkeypatch):
    salesforce_client_module._active_field_cache = None

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
