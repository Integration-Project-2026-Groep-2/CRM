from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sf(monkeypatch):
    import src.salesforce.contacts.utils as utils_module

    utils_module._describe_cache.clear()

    async def immediate_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(utils_module.asyncio, "to_thread", immediate_to_thread)

    sf = MagicMock()
    sf.Contact = MagicMock()
    sf.query = MagicMock()
    return sf


def _describe(fields: list[str]) -> dict:
    return {"fields": [{"name": name} for name in fields]}


async def test_returns_company_id_when_describe_and_query_expose_it(sf):
    from src.salesforce.contacts.utils import get_full_contact_record

    sf.Contact.describe.return_value = _describe(
        ["Id", "CRM_ID__c", "FirstName", "LastName", "Email", "Company_ID__c"]
    )
    sf.query.return_value = {
        "totalSize": 1,
        "records": [
            {
                "Id": "003xxx",
                "CRM_ID__c": "uuid-1",
                "FirstName": "Jarvis",
                "LastName": "Bot",
                "Email": "j@b.com",
                "Company_ID__c": "company-uuid-1",
            }
        ],
    }

    record = await get_full_contact_record(sf, "003xxx")

    assert record["Company_ID__c"] == "company-uuid-1"
    soql = sf.query.call_args[0][0]
    assert "Company_ID__c" in soql
    assert "WHERE Id = '003xxx'" in soql


async def test_filters_optional_fields_missing_from_describe(sf):
    from src.salesforce.contacts.utils import get_full_contact_record

    sf.Contact.describe.return_value = _describe(
        ["Id", "CRM_ID__c", "FirstName", "LastName", "Email"]
    )
    sf.query.return_value = {
        "totalSize": 1,
        "records": [
            {
                "Id": "003xxx",
                "CRM_ID__c": "uuid-1",
                "FirstName": "F",
                "LastName": "L",
                "Email": "e@x",
            }
        ],
    }

    await get_full_contact_record(sf, "003xxx")

    soql = sf.query.call_args[0][0]
    assert "Company_ID__c" not in soql
    assert "Phone" not in soql


async def test_raises_when_required_field_missing_from_org(sf):
    from src.salesforce.contacts.utils import get_full_contact_record

    sf.Contact.describe.return_value = _describe(
        ["Id", "FirstName", "LastName", "Email"]
    )

    with pytest.raises(RuntimeError, match="CRM_ID__c"):
        await get_full_contact_record(sf, "003xxx")


async def test_raises_when_contact_not_found(sf):
    from src.salesforce.contacts.utils import get_full_contact_record

    sf.Contact.describe.return_value = _describe(
        ["Id", "CRM_ID__c", "FirstName", "LastName", "Email"]
    )
    sf.query.return_value = {"totalSize": 0, "records": []}

    with pytest.raises(RuntimeError, match="not found"):
        await get_full_contact_record(sf, "003missing")


async def test_describe_cached_between_calls(sf):
    from src.salesforce.contacts.utils import get_full_contact_record

    sf.Contact.describe.return_value = _describe(
        ["Id", "CRM_ID__c", "FirstName", "LastName", "Email", "Company_ID__c"]
    )
    sf.query.return_value = {
        "totalSize": 1,
        "records": [
            {
                "Id": "003xxx",
                "CRM_ID__c": "u",
                "FirstName": "F",
                "LastName": "L",
                "Email": "e@x",
                "Company_ID__c": "c",
            }
        ],
    }

    await get_full_contact_record(sf, "003xxx")
    await get_full_contact_record(sf, "003yyy")

    assert sf.Contact.describe.call_count == 1
