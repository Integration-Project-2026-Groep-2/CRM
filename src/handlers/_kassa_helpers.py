"""Kassa-specific helpers used by the Kassa user handlers (C36-C38)."""

from datetime import datetime, timezone

from src.handlers._helpers import _build_conflict_value


def _build_kassa_user_conflict_data(
    email: str,
    source_contact: dict,
    incoming_first_name: str,
    incoming_last_name: str,
    incoming_company_id: str | None,
) -> dict:
    """Build a Contract 15 payload from a source Contact and incoming Kassa payload.

    The `source_contact` is whichever Contact represents the *existing* side of
    the conflict — usually the CRM_ID-resolved Contact, but for an
    email-collision conflict it is the Contact already bound to the incoming
    email.
    """
    return {
        "email": email,
        "existingValue": _build_conflict_value(
            source_contact.get("FirstName"),
            source_contact.get("LastName"),
            source_contact.get("Company_ID__c"),
        ),
        "incomingValue": _build_conflict_value(
            incoming_first_name, incoming_last_name, incoming_company_id
        ),
        "detectedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
