import pytest
from unittest.mock import MagicMock
from simple_salesforce import SalesforceError

from src.salesforce_client import (
    create_contact,
    upsert_contact_by_email,
    get_contact_by_email,
    create_account,
    upsert_account_by_vat,
    get_account_by_vat,
)


@pytest.fixture
def sf():
    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.Account = MagicMock()
    sf.query = MagicMock()
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
    sf.Contact.upsert.return_value = {"id": "003000000000002"}
    sf.Contact.get.return_value = {"Id": "003000000000002", "Email": "a@a.com"}

    payload = {"FirstName": "Bob"}
    result = await upsert_contact_by_email(sf, "a@a.com", payload)

    sf.Contact.upsert.assert_called_once()
    assert result == {"Id": "003000000000002", "Email": "a@a.com"}


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