"""Facade shim — re-exports from the src.salesforce package.

Kept so existing imports (`from src.salesforce_client import foo`) keep
working while the codebase migrates to the per-SObject sub-modules in
src.salesforce.{client,contacts,accounts,sessions,payments}.

New code should import directly from the sub-modules.
"""

# Kept so tests that patch `salesforce_client.asyncio.to_thread` still
# resolve the attribute. Python's module-singleton semantics mean that
# patching asyncio.to_thread anywhere affects every caller globally.
import asyncio  # noqa: F401

from src.salesforce.accounts import (  # noqa: F401
    apply_account_is_active,
    create_account,
    deactivate_account_by_crm_id,
    deactivate_account_record,
    get_account_by_crm_id,
    get_account_by_vat,
    get_account_match_by_crm_id,
    get_account_match_by_email,
    update_facturatie_account,
    upsert_account_by_vat,
)
from src.salesforce.client import (  # noqa: F401
    SalesforceSession,
    _escape_soql,
    _normalize_optional_field_value,
    _normalize_uuid_v4,
    _parse_iso_datetime_utc,
    _resolve_account_active_field,
    _resolve_account_active_field_optional,
    _resolve_account_country_field,
    _resolve_account_email_field,
    _resolve_contact_active_field,
    _resolve_contact_active_field_optional,
    coerce_is_active,
    escape_soql,
    get_salesforce_client,
    has_account_house_number_field,
    has_contact_kassa_id_field,
    has_contact_mailing_id_field,
    has_contact_planning_id_field,
    has_session_registration_object,
    is_expired_session_error,
    is_rate_limit_error,
    is_transient_query_404,
    sf_call,
)
from src.salesforce.contacts import (  # noqa: F401
    apply_is_active,
    backfill_kassa_contact_fields,
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    count_active_contacts_for_company,
    create_contact,
    deactivate_contact,
    deactivate_contact_record,
    ensure_contact_identifiers,
    find_unique_contact_by_email,
    get_contact_by_crm_id,
    get_contact_by_email,
    get_contact_for_person_lookup,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    get_contact_match_by_mailing_id,
    get_contact_match_by_planning_id,
    update_facturatie_contact,
    update_kassa_contact,
    update_mailing_contact,
    update_planning_contact,
    upsert_contact_by_email,
)
from src.salesforce.payments import (  # noqa: F401
    get_unpaid_contacts,
    update_payment_status,
)
from src.salesforce.sessions import (  # noqa: F401
    count_active_session_registrations,
    deactivate_session_registration,
    ensure_session_registration_active,
    get_active_session_participants,
    get_session_registration_by_contact_and_session,
    get_session_registration_by_registration_id,
    get_unique_active_session_registration_for_contact,
    upsert_session_registration,
)
