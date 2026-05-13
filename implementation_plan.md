# Fix Missing Salesforce Fields (Company_ID__c)

The `companyId` (linked to Salesforce `Company_ID__c`) is missing from `crm.user.confirmed` messages because the CRM uses `sf.Contact.get(id)` to refresh records after creation or lookup. In Salesforce, the REST API `get()` method is sensitive to the integration user's default Page Layout. If a custom field is not in the layout, it is omitted from the response, even if it was successfully written.

## User Review Required

> [!IMPORTANT]
> This change replaces standard Salesforce `get()` calls with explicit SOQL queries to ensure all required fields are retrieved regardless of Page Layout configuration.

## Proposed Changes

### [NEW] [utils.py](file:///c:/Users/lucas/Documents/Ehb%20local/Integration%20Project/CRM/src/salesforce/contacts/utils.py)
Create a central utility to retrieve a Contact with an explicit field list, probing the org schema first to avoid `INVALID_FIELD` errors on missing custom fields.

### [MODIFY] [client.py](file:///c:/Users/lucas/Documents/Ehb%20local/Integration%20Project/CRM/src/salesforce/contacts/client.py)
Update `create_contact` to use `get_full_contact_record` instead of `sf.Contact.get`.

### [MODIFY] [matching.py](file:///c:/Users/lucas/Documents/Ehb%20local/Integration%20Project/CRM/src/salesforce/contacts/matching.py)
Update all lookup functions to use `get_full_contact_record`.

### [MODIFY] [updates.py](file:///c:/Users/lucas/Documents/Ehb%20local/Integration%20Project/CRM/src/salesforce/contacts/updates.py)
Update `ensure_contact_identifiers`, `backfill_*`, and `update_*` functions to use `get_full_contact_record`.

## Verification Plan

### Automated Tests
- I will verify that `get_full_contact_record` correctly handles missing fields by mocking a `describe` response.

### Manual Verification
- The added logging in `client.py` and `facturatie_user_created.py` (already applied) will confirm that `Company_ID__c` is now present in the retrieved records.
