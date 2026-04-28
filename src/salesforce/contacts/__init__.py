"""Salesforce Contact operations.

Split across mapping (record→dict builders), client (CRUD + active-field),
matching (lookups/match classifiers), and updates (authoritative writes
and deactivation). Kept as a package with __init__ re-exports so existing
imports (`from src.salesforce.contacts import create_contact`) keep
working unchanged.
"""

from src.salesforce.contacts.client import (  # noqa: F401
    _ensure_contact_active,
    apply_is_active,
    count_active_contacts_for_company,
    create_contact,
    upsert_contact_by_email,
)
from src.salesforce.contacts.mapping import (  # noqa: F401
    _SPECIALIZED_ROLES,
    _build_updated_user_data,
    _build_user_data,
    _build_user_deactivation_data,
    _get_contact_is_active,
)
from src.salesforce.contacts.matching import (  # noqa: F401
    find_unique_contact_by_email,
    get_contact_by_crm_id,
    get_contact_by_email,
    get_contact_for_person_lookup,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    get_contact_match_by_mailing_id,
    get_contact_match_by_planning_id,
)
from src.salesforce.contacts.updates import (  # noqa: F401
    backfill_kassa_contact_fields,
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    deactivate_contact,
    deactivate_contact_record,
    ensure_contact_identifiers,
    update_facturatie_contact,
    update_kassa_contact,
    update_mailing_contact,
    update_planning_contact,
)
