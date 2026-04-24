"""Session_Registration__c SObject operations.

Covers the per-participant session registration lifecycle used by Contracts
1 (registration created), 2 (update/cancel), 11 (session updated), and 16
(payment confirmed). Session_Registration__c is the canonical per-participant
record; Contacts aggregate information across multiple registrations.
"""

import asyncio
import logging
from typing import Any

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import (
    _escape_soql,
    _resolve_contact_active_field_optional,
)

logger = logging.getLogger(__name__)


async def _get_session_registration_by_id(
    sf: Salesforce, session_registration_id: str
) -> dict[str, Any]:
    """Return one Session_Registration__c row by Salesforce record id."""
    return await asyncio.to_thread(sf.Session_Registration__c.get, session_registration_id)


async def get_session_registration_by_registration_id(
    sf: Salesforce, registration_id: str
) -> dict[str, Any] | None:
    """Return one session registration row by registrationId, if it exists."""
    try:
        escaped_registration_id = _escape_soql(registration_id)
        query = (
            "SELECT Id FROM Session_Registration__c "
            f"WHERE Registration_ID__c = '{escaped_registration_id}'"
        )
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return None
        if result["totalSize"] > 1:
            logger.warning(
                "Multiple Session_Registration__c rows found for Registration_ID__c %s",
                registration_id,
            )
            return None

        return await _get_session_registration_by_id(sf, result["records"][0]["Id"])
    except SalesforceError as e:
        logger.error(
            "Failed to get Session_Registration__c by Registration_ID__c %s: %s",
            registration_id,
            str(e),
        )
        raise


async def get_session_registration_by_contact_and_session(
    sf: Salesforce,
    *,
    contact_id: str,
    session_id: str,
    active_only: bool = False,
) -> dict[str, Any] | None:
    """Return a unique session registration for one contact/session pair."""
    escaped_contact_id = _escape_soql(contact_id)
    escaped_session_id = _escape_soql(session_id)
    query = (
        "SELECT Id FROM Session_Registration__c "
        f"WHERE Contact__c = '{escaped_contact_id}' "
        f"AND Session_ID__c = '{escaped_session_id}'"
    )
    if active_only:
        query += " AND Is_Active__c = true"

    try:
        result = await asyncio.to_thread(sf.query, query)
    except SalesforceError as e:
        logger.error(
            "Failed to get Session_Registration__c by Contact__c %s and Session_ID__c %s: %s",
            contact_id,
            session_id,
            str(e),
        )
        raise

    if result["totalSize"] == 0:
        return None
    if result["totalSize"] > 1:
        logger.warning(
            "Multiple Session_Registration__c rows found for Contact__c %s and Session_ID__c %s",
            contact_id,
            session_id,
        )
        return None

    return await _get_session_registration_by_id(sf, result["records"][0]["Id"])


async def get_unique_active_session_registration_for_contact(
    sf: Salesforce, contact_id: str
) -> dict[str, Any] | None:
    """Return the unique active session registration for one Contact, if any."""
    escaped_contact_id = _escape_soql(contact_id)
    query = (
        "SELECT Id FROM Session_Registration__c "
        f"WHERE Contact__c = '{escaped_contact_id}' AND Is_Active__c = true"
    )

    try:
        result = await asyncio.to_thread(sf.query, query)
    except SalesforceError as e:
        logger.error(
            "Failed to get active Session_Registration__c for Contact__c %s: %s",
            contact_id,
            str(e),
        )
        raise

    if result["totalSize"] == 0:
        logger.warning(
            "No active Session_Registration__c row found for Contact__c %s",
            contact_id,
        )
        return None
    if result["totalSize"] > 1:
        logger.warning(
            "Multiple active Session_Registration__c rows found for Contact__c %s",
            contact_id,
        )
        return None

    return await _get_session_registration_by_id(sf, result["records"][0]["Id"])


async def upsert_session_registration(
    sf: Salesforce,
    *,
    registration_id: str,
    session_id: str,
    contact_id: str,
    paid_at: str | None = None,
) -> dict[str, Any]:
    """Create or reactivate a session registration link for one participant."""
    payload: dict[str, Any] = {
        "Registration_ID__c": registration_id,
        "Session_ID__c": session_id,
        "Contact__c": contact_id,
        "Is_Active__c": True,
    }
    if paid_at is not None:
        payload["Paid_At__c"] = paid_at

    try:
        result = await asyncio.to_thread(
            sf.Session_Registration__c.upsert,
            f"Registration_ID__c/{registration_id}",
            payload,
        )

        session_registration = await get_session_registration_by_registration_id(sf, registration_id)
        if session_registration is not None:
            return session_registration

        if isinstance(result, dict) and result.get("id"):
            return await _get_session_registration_by_id(sf, result["id"])

        raise RuntimeError(
            "Upsert succeeded but Session_Registration__c row was not retrievable "
            f"for registrationId {registration_id}"
        )
    except SalesforceError as e:
        logger.error(
            "Failed to upsert Session_Registration__c for registrationId %s: %s",
            registration_id,
            str(e),
        )
        raise


async def ensure_session_registration_active(
    sf: Salesforce,
    *,
    contact_id: str,
    session_id: str,
    registration_id: str | None = None,
) -> dict[str, Any] | None:
    """Ensure the session registration exists and is active."""
    if registration_id:
        return await upsert_session_registration(
            sf,
            registration_id=registration_id,
            session_id=session_id,
            contact_id=contact_id,
        )

    session_registration = await get_session_registration_by_contact_and_session(
        sf,
        contact_id=contact_id,
        session_id=session_id,
        active_only=False,
    )
    if session_registration is None:
        logger.warning(
            "Cannot ensure Session_Registration__c without registrationId for Contact__c %s session %s",
            contact_id,
            session_id,
        )
        return None

    if session_registration.get("Is_Active__c") is True:
        return session_registration

    session_registration_id = session_registration["Id"]
    await asyncio.to_thread(
        sf.Session_Registration__c.update,
        session_registration_id,
        {"Is_Active__c": True},
    )
    logger.info("Reactivated Session_Registration__c row %s", session_registration_id)
    return await _get_session_registration_by_id(sf, session_registration_id)


async def get_active_session_participants(
    sf: Salesforce, session_id: str
) -> list[dict[str, Any]]:
    """Return active Contacts linked to the given active session registrations."""
    active_field = await _resolve_contact_active_field_optional(sf)
    escaped_session_id = _escape_soql(session_id)
    select_fields = [
        "Id",
        "Registration_ID__c",
        "Session_ID__c",
        "Contact__c",
        "Contact__r.Id",
        "Contact__r.CRM_ID__c",
        "Contact__r.Email",
        "Contact__r.FirstName",
        "Contact__r.LastName",
    ]
    if active_field is not None:
        select_fields.append(f"Contact__r.{active_field}")

    query = (
        f"SELECT {', '.join(select_fields)} FROM Session_Registration__c "
        f"WHERE Session_ID__c = '{escaped_session_id}' AND Is_Active__c = true"
    )

    try:
        result = await asyncio.to_thread(sf.query_all, query)
    except SalesforceError as e:
        logger.error(
            "Failed to get active Session_Registration__c participants for sessionId %s: %s",
            session_id,
            str(e),
        )
        raise

    participants: list[dict[str, Any]] = []
    for record in result.get("records", []):
        contact = record.get("Contact__r") or {}
        if record.get("Contact__c") and "Id" not in contact:
            contact["Id"] = record["Contact__c"]

        if not contact:
            logger.warning(
                "Skipping Session_Registration__c row %s without linked Contact__r",
                record.get("Id"),
            )
            continue

        if active_field is not None and contact.get(active_field) is False:
            continue

        participants.append(contact)

    participants.sort(
        key=lambda contact: (
            str(contact.get("LastName") or ""),
            str(contact.get("FirstName") or ""),
            str(contact.get("Email") or ""),
        )
    )
    return participants


async def deactivate_session_registration(
    sf: Salesforce,
    *,
    registration_id: str | None = None,
    contact_id: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any] | None:
    """Deactivate one session registration row."""
    session_registration: dict[str, Any] | None = None

    if registration_id:
        session_registration = await get_session_registration_by_registration_id(sf, registration_id)

    if session_registration is None and contact_id and session_id:
        session_registration = await get_session_registration_by_contact_and_session(
            sf,
            contact_id=contact_id,
            session_id=session_id,
            active_only=True,
        )

    if session_registration is None:
        return None

    session_registration_id = session_registration["Id"]
    await asyncio.to_thread(
        sf.Session_Registration__c.update,
        session_registration_id,
        {"Is_Active__c": False},
    )
    logger.info("Deactivated Session_Registration__c row %s", session_registration_id)
    return await _get_session_registration_by_id(sf, session_registration_id)


async def count_active_session_registrations(sf: Salesforce, contact_id: str) -> int:
    """Return the number of active session registrations for one Contact."""
    escaped_contact_id = _escape_soql(contact_id)
    query = (
        "SELECT Id FROM Session_Registration__c "
        f"WHERE Contact__c = '{escaped_contact_id}' AND Is_Active__c = true"
    )

    try:
        result = await asyncio.to_thread(sf.query, query)
    except SalesforceError as e:
        logger.error(
            "Failed to count active Session_Registration__c rows for Contact %s: %s",
            contact_id,
            str(e),
        )
        raise

    return int(result["totalSize"])
