"""Salesforce REST API client wrapper using simple-salesforce.

CUSTOM FIELDS REFERENCE (must be created in Salesforce Setup):
- Contact.CRM_ID__c (Text, Unique) — UUID v4 for Contract 13, 18, 22
- Contact.GDPR_Consent__c (Checkbox) — For Contract 1
- Contact.Registration_ID__c (Text) — Deduplication key for Contract 1
- Contact.Paid_At__c (DateTime) — Compatibility timestamp derived from the latest
  paid session registration for Contracts 16 / 17 legacy consumers
- Contact.Mailing_ID__c (Text, Unique) — Native Mailing UUID for Contracts 27-29
- Contact.Planning_ID__c (Text, Unique) — Native Planning UUID for Contracts 30-32
- Contact.Role__c (Picklist: VISITOR | COMPANY_CONTACT) — For Contract 1, 13, 18

- Session_Registration__c.Registration_ID__c (Text, External ID, Unique) —
  Canonical registration identifier for Contracts 1, 2, 11, 16
- Session_Registration__c.Session_ID__c (Text) — Planning session identifier
- Session_Registration__c.Contact__c (Lookup(Contact)) — Canonical Contact link
- Session_Registration__c.Is_Active__c (Checkbox) — Soft delete flag per registration
- Session_Registration__c.Paid_At__c (DateTime) — Payment timestamp per registration

- Account.CRM_ID__c (Text, Unique) — UUID v4 for Contract 14, 19, 23
- Account.VAT_Number__c (Text, External ID, Unique) — For Contract 3, 5a, 5b, 14

All functions return complete records as dicts for XML serialization.
All SF calls are wrapped in asyncio.to_thread() to prevent blocking the event loop.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from simple_salesforce import Salesforce, SalesforceError
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from src.config import Config

logger = logging.getLogger(__name__)

_PAYMENT_TIMESTAMP_FIELD = "Paid_At__c"

# Startup retry configuration for Salesforce login. Mirrors the RabbitMQ
# connect retry pattern in src/connection.py so a transient Salesforce
# outage (SERVER_UNAVAILABLE / network blip) does not leave the receiver
# task silently dead while heartbeat keeps the container "alive".
_SF_STARTUP_DELAY: float = 1.0
_SF_STARTUP_MAX_DELAY: float = 60.0
_TRANSIENT_SF_AUTH_CODES = frozenset({"SERVER_UNAVAILABLE", "SERVICE_UNAVAILABLE"})


def escape_soql(value: str) -> str:
    """Escape single quotes for SOQL to prevent injection attacks."""
    return value.replace("'", "''")


# Backwards-compatibility alias — existing callers in this module use the
# private form; remove once everyone has migrated.
_escape_soql = escape_soql


def coerce_is_active(raw_value: Any) -> bool:
    """Normalize an active-flag value from Salesforce into a bool.

    Salesforce custom active fields come in multiple forms across orgs:
    - Boolean (`IsActive__c` / `Is_Active__c`) → True / False / None
    - Picklist (`Active__c`) → "Yes" / "No"
    - Text → "true" / "false" / empty string

    Python's `bool()` treats any non-empty string as True, so `bool("No")`
    is True — exactly the opposite of what we want for a picklist field.
    Callers should always route active-field values through this helper.

    Missing (`None`) defaults to True: records without a flag are treated as
    active. This mirrors the receiver's legacy behaviour.
    """
    if raw_value is None:
        return True
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return bool(raw_value)
    text = str(raw_value).strip().lower()
    if text in ("yes", "true", "1", "y"):
        return True
    if text in ("no", "false", "0", "n", ""):
        return False
    # Unknown non-empty string → best-effort fall-through.
    return bool(raw_value)


def is_rate_limit_error(exc: Exception) -> bool:
    """Detect Salesforce REQUEST_LIMIT_EXCEEDED via content attribute or message.

    Shared between the receiver (drop-and-sleep on rate limit) and the polling
    task (skip cycle on rate limit). The Salesforce REST API surfaces the error
    both as a structured `content` attribute on SalesforceError subclasses and
    occasionally as a plain message, hence both checks.
    """
    content = getattr(exc, "content", None)
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("errorCode") == "REQUEST_LIMIT_EXCEEDED":
                return True
    return "REQUEST_LIMIT_EXCEEDED" in str(exc)


def _normalize_uuid_v4(value: Any) -> str | None:
    """Return a canonical UUID v4 string or None when invalid."""
    try:
        parsed = uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None

    if parsed.version != 4:
        return None
    return str(parsed)


def _parse_iso_datetime_utc(value: Any) -> datetime | None:
    """Parse an ISO-8601 datetime into UTC, or return None when invalid/missing."""
    normalized = _normalize_optional_field_value(value)
    if normalized is None:
        return None

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _wait_retry_or_shutdown(
    delay: float, shutdown_event: asyncio.Event | None = None,
) -> bool:
    """Sleep for `delay` seconds unless shutdown fires first.

    Returns True when shutdown was requested during the wait, False otherwise.
    Mirrors the helper in src/connection.py so both retry loops behave the
    same way (see plan: feature/sf-startup-retry).
    """
    if shutdown_event is None:
        await asyncio.sleep(delay)
        return False

    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
        return True
    except asyncio.TimeoutError:
        return False


async def get_salesforce_client(
    config: Config, shutdown_event: asyncio.Event | None = None,
) -> Salesforce:
    """Create an authenticated Salesforce client, retrying transient failures.

    Retries with exponential backoff (1s → 60s cap) on transient Salesforce
    auth codes (SERVER_UNAVAILABLE, SERVICE_UNAVAILABLE) and on generic
    network errors. Honours `shutdown_event` so a graceful shutdown during
    the retry backoff does not hang the container.

    Permanent authentication errors (INVALID_LOGIN, PASSWORD_LOCKOUT, bad
    security token) are re-raised immediately so the receiver task crashes
    visibly and the operator can tell the difference between "Salesforce is
    flaky" and "credentials are wrong".

    Args:
        config: Application configuration with SF credentials.
        shutdown_event: Optional event that aborts the retry loop when set.

    Returns:
        Authenticated Salesforce instance.

    Raises:
        SalesforceAuthenticationFailed: Permanent auth failure (bad creds).
        RuntimeError: Shutdown fired during the retry backoff.
    """
    delay = _SF_STARTUP_DELAY
    while True:
        if shutdown_event is not None and shutdown_event.is_set():
            raise RuntimeError(
                "Salesforce connection cancelled by shutdown signal",
            )

        try:
            logger.info(
                "Connecting to Salesforce as %s...", config.salesforce_username,
            )
            sf = await asyncio.to_thread(
                Salesforce,
                username=config.salesforce_username,
                password=config.salesforce_password,
                security_token=config.salesforce_security_token,
                domain=config.salesforce_domain,
            )
            logger.info("Connected to Salesforce.")
            return sf
        except SalesforceAuthenticationFailed as exc:
            if exc.code not in _TRANSIENT_SF_AUTH_CODES:
                logger.error(
                    "Salesforce authentication failed permanently "
                    "(code=%s): %s",
                    exc.code, exc,
                )
                raise
            logger.warning(
                "Salesforce transient auth failure (code=%s); "
                "retrying in %.1fs",
                exc.code, delay,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Salesforce connection failed (%s); retrying in %.1fs",
                exc, delay,
            )

        shutdown_requested = await _wait_retry_or_shutdown(
            delay, shutdown_event,
        )
        if shutdown_requested:
            raise RuntimeError(
                "Salesforce connection cancelled by shutdown signal",
            )
        delay = min(delay * 2, _SF_STARTUP_MAX_DELAY)


async def create_contact(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Contact in Salesforce (Contracts 1 and 24).

    Maps XML Registration fields to Contact fields and returns complete record
    for crm.user.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Contact fields (FirstName, LastName, Email, Role__c, GDPR_Consent__c, etc.).

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If Contact creation fails.
    """
    try:
        # Shallow copy to avoid mutating input dict
        data = {**data}
        # Generate UUID v4 for crm.user.confirmed (Contract 13)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id
        data = await _ensure_contact_active(sf, data)

        # Create Contact
        result = await asyncio.to_thread(sf.Contact.create, data)
        contact_id = result["id"]
        logger.info("Created Contact with ID %s (CRM_ID: %s)", contact_id, crm_id)

        # Retrieve and return complete record for XML serialization
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to create contact: %s", str(e))
        raise


async def upsert_contact_by_email(
    sf: Salesforce, email: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update a Contact by email (Contract 2).

    Uses native Salesforce upsert with Email as external ID to avoid race conditions.
    Returns complete record for XML serialization.

    REQUIREMENT: Email must be configured as External ID in Salesforce Setup.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email (lookup key).
        data: Contact fields to create/update.

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    data = {**data}

    existing = await get_contact_by_email(sf, email)
    if existing and existing.get("CRM_ID__c"):
        data["CRM_ID__c"] = existing["CRM_ID__c"]
    if existing is None:
        data = await _ensure_contact_active(sf, data)

    try:
        result = await asyncio.to_thread(sf.Contact.upsert, f"Email/{email}", data)

        # Salesforce upsert may return a dict for creates and an int status code
        # for updates, depending on backend/API behavior.
        created = isinstance(result, dict) and result.get("created", False)
        contact_id = result.get("id") if isinstance(result, dict) else None

        if contact_id is None and existing is not None:
            contact_id = existing.get("Id")

        if created and contact_id and not data.get("CRM_ID__c"):
            crm_id = str(uuid.uuid4())
            await asyncio.to_thread(
                sf.Contact.update, contact_id, {"CRM_ID__c": crm_id}
            )

        if contact_id is None:
            refreshed = await get_contact_by_email(sf, email)
            if refreshed is None:
                raise RuntimeError(f"Upsert succeeded but contact not found for email {email}")
            return refreshed

        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to upsert contact by email %s: %s", email, str(e))
        raise


_active_field_cache: str | None = None
_mailing_id_field_supported_cache: bool | None = None
_planning_id_field_supported_cache: bool | None = None
_session_registration_object_supported_cache: bool | None = None


def _normalize_optional_field_value(value: Any) -> str | None:
    """Normalize optional Salesforce/text values so blanks behave like absence."""
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


async def _ensure_contact_active(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Ensure active Contacts are explicitly marked active in Salesforce."""
    if any(field in data for field in ("IsActive__c", "Active__c", "Is_Active__c")):
        return data

    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = True
    return data


async def apply_is_active(
sf: Salesforce, data: dict[str, Any], is_active: bool,
) -> dict[str, Any]:
    """Set the resolved Contact active field on a payload dict.

    Used by producer-sync receivers (Mailing/Facturatie) that carry
    authoritative `isActive` state from the source system.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is None:
        return data

    data[active_field] = is_active
    return data

async def _resolve_contact_active_field_optional(sf: Salesforce) -> str | None:
    """Resolve the optional Contact active field without requiring the migration."""
    global _active_field_cache  # noqa: PLW0603
    if _active_field_cache is not None:
        return _active_field_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}

    for candidate in ("IsActive__c", "Active__c", "Is_Active__c"):
        if candidate in available_fields:
            _active_field_cache = candidate
            return candidate

    return None


async def _resolve_contact_active_field(sf: Salesforce) -> str:
    """Resolve which custom field is used as contact active flag in this org.

    Result is cached after first call - custom fields don't change at runtime.
    """
    active_field = await _resolve_contact_active_field_optional(sf)
    if active_field is not None:
        return active_field

    raise RuntimeError(
        "No supported Contact active field found. Expected one of: "
        "IsActive__c, Active__c, Is_Active__c"
    )


async def has_contact_mailing_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Mailing_ID__c."""
    global _mailing_id_field_supported_cache  # noqa: PLW0603
    if _mailing_id_field_supported_cache is not None:
        return _mailing_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _mailing_id_field_supported_cache = "Mailing_ID__c" in available_fields
    return _mailing_id_field_supported_cache


async def has_contact_planning_id_field(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Contact.Planning_ID__c."""
    global _planning_id_field_supported_cache  # noqa: PLW0603
    if _planning_id_field_supported_cache is not None:
        return _planning_id_field_supported_cache

    describe = await asyncio.to_thread(sf.Contact.describe)
    available_fields = {field["name"] for field in describe.get("fields", [])}
    _planning_id_field_supported_cache = "Planning_ID__c" in available_fields
    return _planning_id_field_supported_cache


async def has_session_registration_object(sf: Salesforce) -> bool:
    """Return whether the Salesforce org exposes Session_Registration__c."""
    global _session_registration_object_supported_cache  # noqa: PLW0603
    if _session_registration_object_supported_cache is not None:
        return _session_registration_object_supported_cache

    describe = await asyncio.to_thread(sf.describe)
    available_objects = {
        sobject.get("name")
        for sobject in describe.get("sobjects", [])
        if isinstance(sobject, dict)
    }
    _session_registration_object_supported_cache = "Session_Registration__c" in available_objects
    return _session_registration_object_supported_cache


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


async def get_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email (Contracts 5a, 20).

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Complete Contact record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            contact_id = result["records"][0]["Id"]
            contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info("Found Contact by email: %s", email)
            return contact_record
        else:
            logger.info("Contact not found by email: %s", email)
            return None
    except SalesforceError as e:
        logger.error("Failed to get contact by email %s: %s", email, str(e))
        raise


async def get_contact_for_person_lookup(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email for Contract 10a (kassa.person.lookup.requested).

    Returns Contact with Account relationship fields expanded so the
    handler can build the PersonLookupResponse in one round-trip.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Record with ``Id``, ``CRM_ID__c``, ``AccountId`` and (when the Contact
        is linked to an Account) a nested ``Account`` dict with ``Name`` and
        ``CRM_ID__c``. Returns ``None`` if no Contact matches the email.

    Raises:
        SalesforceError: If the SOQL query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = (
            "SELECT Id, CRM_ID__c, AccountId, Account.Name, Account.CRM_ID__c "
            f"FROM Contact WHERE Email = '{escaped_email}'"
        )
        result = await asyncio.to_thread(sf.query, query)
        if result["totalSize"] == 0:
            logger.info("Person lookup — no Contact for email %s", email)
            return None
        logger.info("Person lookup — found Contact for email %s", email)
        return result["records"][0]
    except SalesforceError as e:
        logger.error("Person lookup query failed for %s: %s", email, str(e))
        raise


async def get_contact_by_crm_id(
    sf: Salesforce, crm_id: str
) -> dict[str, Any] | None:
    """Look up a Contact by CRM UUID."""
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return None
        if result["totalSize"] > 1:
            logger.warning("Multiple Contacts found for CRM_ID__c %s", crm_id)
            return None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found Contact by CRM_ID__c: %s", crm_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def find_unique_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email, but only return a unique match."""
    match_status, contact = await get_contact_match_by_email(sf, email)
    if match_status == "none":
        logger.warning("No Contact found for payment confirmation email %s", email)
        return None
    if match_status == "ambiguous":
        logger.warning("Multiple Contacts found for payment confirmation email %s", email)
        return None
    return contact


async def get_contact_match_by_email(
    sf: Salesforce, email: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify an email lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by email: %s", email)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by email %s: %s", email, str(e))
        raise


async def get_contact_match_by_crm_id(
    sf: Salesforce, crm_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a CRM_ID__c lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by CRM_ID__c: %s", crm_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def get_contact_match_by_mailing_id(
    sf: Salesforce, mailing_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Mailing native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_mailing_id = _escape_soql(mailing_id)
        query = f"SELECT Id FROM Contact WHERE Mailing_ID__c = '{escaped_mailing_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by Mailing_ID__c: %s", mailing_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Mailing_ID__c %s: %s", mailing_id, str(e))
        raise


async def get_contact_match_by_planning_id(
    sf: Salesforce, planning_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Planning native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_planning_id = _escape_soql(planning_id)
        query = f"SELECT Id FROM Contact WHERE Planning_ID__c = '{escaped_planning_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        logger.info("Found unique Contact by Planning_ID__c: %s", planning_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Planning_ID__c %s: %s", planning_id, str(e))
        raise



async def ensure_contact_identifiers(
    sf: Salesforce,
    contact: dict[str, Any],
    registration_id: str | None = None,
    mailing_id: str | None = None,
    planning_id: str | None = None,
) -> dict[str, Any]:
    """Ensure a Contact has the canonical identifiers needed by CRM contracts.

    - Always ensure CRM_ID__c exists.
    - Only set Registration_ID__c when it is currently empty and an inbound
      registration_id is provided.
    - Only set Mailing_ID__c when it is currently empty and an inbound
      mailing_id is provided.
        - Only set Planning_ID__c when it is currently empty and an inbound
            planning_id is provided.
    """
    updates: dict[str, Any] = {}

    if not contact.get("CRM_ID__c"):
        updates["CRM_ID__c"] = str(uuid.uuid4())

    if registration_id and not contact.get("Registration_ID__c"):
        updates["Registration_ID__c"] = registration_id

    if mailing_id and not contact.get("Mailing_ID__c"):
        updates["Mailing_ID__c"] = mailing_id

    if planning_id and not contact.get("Planning_ID__c"):
        updates["Planning_ID__c"] = planning_id

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Normalized Contact %s identifiers: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def backfill_mailing_contact_fields(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    company_id: str | None = None,
    role: str | None = None,
    gdpr_consent: bool | None = None,
) -> dict[str, Any]:
    """Backfill Mailing-owned fields on a compatible existing Contact.

    Text fields are only filled when missing. Role enrichment is narrower:
    Contract 27 may promote a blank/VISITOR role to COMPANY_CONTACT when the
    incoming Mailing payload links the user to a company, but it must not
    overwrite unrelated CRM roles such as ADMIN or SPEAKER.
    """
    updates: dict[str, Any] = {}

    if first_name and _normalize_optional_field_value(contact.get("FirstName")) is None:
        updates["FirstName"] = first_name

    if last_name and _normalize_optional_field_value(contact.get("LastName")) is None:
        updates["LastName"] = last_name

    requested_role = _normalize_optional_field_value(role)
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))

    if (
        company_id
        and _normalize_optional_field_value(contact.get("Company_ID__c")) is None
        and existing_role in (None, "VISITOR", "COMPANY_CONTACT")
    ):
        updates["Company_ID__c"] = company_id

    if requested_role == "VISITOR" and existing_role is None:
        updates["Role__c"] = requested_role
    elif requested_role == "COMPANY_CONTACT" and existing_role in (None, "VISITOR"):
        if existing_role != "COMPANY_CONTACT":
            updates["Role__c"] = requested_role

    if gdpr_consent is True and contact.get("GDPR_Consent__c") is None:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Backfilled Mailing Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def backfill_planning_contact_fields(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    first_name: str,
    last_name: str,
    role: str,
    phone_number: str | None = None,
    gdpr_consent: bool | None = None,
) -> dict[str, Any]:
    """Backfill Planning-owned fields on a compatible existing Contact.

    Contract 30 links by native Planning ID, then enriches a matching Contact
    without overwriting non-empty CRM values.
    """
    updates: dict[str, Any] = {}

    if _normalize_optional_field_value(contact.get("FirstName")) is None:
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) is None:
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Role__c")) is None:
        updates["Role__c"] = role

    normalized_phone = _normalize_optional_field_value(phone_number)
    if normalized_phone is not None and _normalize_optional_field_value(contact.get("Phone")) is None:
        updates["Phone"] = normalized_phone

    if gdpr_consent is True and contact.get("GDPR_Consent__c") is None:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Backfilled Planning Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def update_mailing_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str | None,
    last_name: str,
    company_id: str | None,
) -> dict[str, Any]:
    """Authoritatively update Mailing-owned fields on an existing Contact.

    Contract 28 sends the full Mailing-side object. CRM therefore overwrites the
    Mailing-owned Contact fields to match that payload, while preserving
    CRM-owned fields such as active state, badge data, and registration
    identifiers. Specialized existing roles keep their role and company linkage.
    """
    updates: dict[str, Any] = {}
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))
    can_manage_company_link = existing_role in (None, "VISITOR", "COMPANY_CONTACT")

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if (
        can_manage_company_link
        and _normalize_optional_field_value(contact.get("Company_ID__c"))
        != _normalize_optional_field_value(company_id)
    ):
        updates["Company_ID__c"] = company_id

    if contact.get("GDPR_Consent__c") is not True:
        updates["GDPR_Consent__c"] = True

    if can_manage_company_link:
        desired_role = "COMPANY_CONTACT" if company_id else "VISITOR"
        if existing_role != desired_role:
            updates["Role__c"] = desired_role

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Mailing Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)

async def update_facturatie_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str,
    last_name: str,
    phone: str | None,
    street: str | None,
    house_number: str | None,
    postal_code: str | None,
    city: str | None,
    country: str | None,
    role: str,
    company_id: str | None,
) -> dict[str, Any]:
    """Authoritatively update Facturatie-owned fields on an existing Contact.

    Contract 25 sends the full Facturatie-side user object. CRM therefore
    overwrites Facturatie-owned Contact fields to match the payload while
    preserving unrelated CRM-owned fields (badge, CRM_ID__c, Registration_ID__c,
    active flag).

    Role protection: specialized existing roles (ADMIN, SPEAKER, EVENT_MANAGER,
    CASHIER, BAR_STAFF) keep their Role__c and Company_ID__c untouched, because
    Facturatie is not authoritative for those role assignments. Roles VISITOR
    and COMPANY_CONTACT may be overwritten by the Facturatie payload.
    """
    updates: dict[str, Any] = {}
    existing_role = _normalize_optional_field_value(contact.get("Role__c"))
    can_manage_role_and_company = existing_role in (None, "VISITOR", "COMPANY_CONTACT")

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Phone")) != _normalize_optional_field_value(phone):
        updates["Phone"] = phone

    address_mapping = {
        "MailingStreet": street,
        "House_Number__c": house_number,
        "MailingPostalCode": postal_code,
        "MailingCity": city,
        "MailingCountry": country,
    }
    for sf_field, incoming_value in address_mapping.items():
        if _normalize_optional_field_value(contact.get(sf_field)) != _normalize_optional_field_value(incoming_value):
            updates[sf_field] = incoming_value

    if can_manage_role_and_company:
        normalized_incoming_role = _normalize_optional_field_value(role)
        if (
            normalized_incoming_role is not None
            and normalized_incoming_role != existing_role
        ):
            updates["Role__c"] = normalized_incoming_role

        if (
            _normalize_optional_field_value(contact.get("Company_ID__c"))
            != _normalize_optional_field_value(company_id)
        ):
            updates["Company_ID__c"] = company_id

    if contact.get("GDPR_Consent__c") is not True:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Facturatie Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)

async def update_planning_contact(
    sf: Salesforce,
    contact: dict[str, Any],
    *,
    email: str,
    first_name: str,
    last_name: str,
    role: str,
    phone_number: str | None,
) -> dict[str, Any]:
    """Authoritatively update Planning-owned fields on an existing Contact.

    Contract 31 sends the full Planning-side user object. CRM therefore
    overwrites Planning-owned fields to match the payload while preserving
    unrelated CRM-owned fields.
    """
    updates: dict[str, Any] = {}

    if contact.get("Email") != email:
        updates["Email"] = email

    if _normalize_optional_field_value(contact.get("FirstName")) != _normalize_optional_field_value(first_name):
        updates["FirstName"] = first_name

    if _normalize_optional_field_value(contact.get("LastName")) != _normalize_optional_field_value(last_name):
        updates["LastName"] = last_name

    if _normalize_optional_field_value(contact.get("Role__c")) != _normalize_optional_field_value(role):
        updates["Role__c"] = role

    if _normalize_optional_field_value(contact.get("Phone")) != _normalize_optional_field_value(phone_number):
        updates["Phone"] = phone_number

    if contact.get("GDPR_Consent__c") is not True:
        updates["GDPR_Consent__c"] = True

    if not updates:
        return contact

    contact_id = contact["Id"]
    await asyncio.to_thread(sf.Contact.update, contact_id, updates)
    logger.info(
        "Updated Planning Contact %s fields: %s",
        contact_id,
        ", ".join(sorted(updates.keys())),
    )
    return await asyncio.to_thread(sf.Contact.get, contact_id)


async def get_unpaid_contacts(sf: Salesforce) -> list[dict[str, Any]]:
    """Return active Contacts with at least one unpaid active registration.

    Records missing CRM_ID__c or Email are skipped because they cannot be
    serialized into the outbound XML contract safely.
    """
    if not await has_session_registration_object(sf):
        raise RuntimeError(
            "Session_Registration__c is required to resolve unpaid registrations"
        )

    active_field = await _resolve_contact_active_field_optional(sf)
    select_fields = [
        "Id",
        "Contact__c",
        "Contact__r.CRM_ID__c",
        "Contact__r.FirstName",
        "Contact__r.LastName",
        "Contact__r.Email",
        "Contact__r.AccountId",
        "Contact__r.Account.Name",
    ]
    if active_field is not None:
        select_fields.append(f"Contact__r.{active_field}")

    query = (
        f"SELECT {', '.join(select_fields)} FROM Session_Registration__c "
        "WHERE Is_Active__c = true AND Paid_At__c = NULL"
    )

    try:
        result = await asyncio.to_thread(sf.query_all, query)
    except SalesforceError as e:
        logger.error("Failed to get unpaid contacts: %s", str(e))
        raise

    persons_by_id: dict[str, dict[str, Any]] = {}
    for record in result.get("records", []):
        contact = record.get("Contact__r") or {}
        # Older contacts may not have the active field populated yet; treat
        # missing values as active to avoid hiding valid unpaid participants.
        if active_field is not None and contact.get(active_field) is False:
            continue

        crm_id = contact.get("CRM_ID__c")
        email = contact.get("Email")
        if not crm_id or not email:
            logger.warning(
                "Skipping unpaid contact without required fields: CRM_ID__c=%s Email=%s",
                crm_id,
                email,
            )
            continue
        normalized_crm_id = _normalize_uuid_v4(crm_id)
        if normalized_crm_id is None:
            logger.warning("Skipping unpaid contact with invalid CRM_ID__c: %s", crm_id)
            continue

        if normalized_crm_id in persons_by_id:
            continue

        account = contact.get("Account") or {}
        linked_to_company = bool(contact.get("AccountId"))

        person = {
            "id": normalized_crm_id,
            "firstName": contact.get("FirstName") or "",
            "lastName": contact.get("LastName") or "",
            "email": email,
            "linkedToCompany": linked_to_company,
        }
        if linked_to_company and account.get("Name"):
            # Company linkage currently comes from the standard Account relation.
            person["companyName"] = account["Name"]

        persons_by_id[normalized_crm_id] = person

    persons = list(persons_by_id.values())
    persons.sort(key=lambda person: (person["lastName"], person["firstName"], person["email"]))
    return persons


async def update_payment_status(
    sf: Salesforce,
    user_id: str | None,
    email: str,
    registration_id: str | None,
    paid_at: str,
) -> dict[str, Any] | None:
    """Update payment state on the canonical session registration for Contract 16."""
    if not await has_session_registration_object(sf):
        raise RuntimeError(
            "Session_Registration__c is required to update payment state"
        )

    session_registration: dict[str, Any] | None = None

    if registration_id:
        session_registration = await get_session_registration_by_registration_id(sf, registration_id)
        if session_registration is None:
            logger.warning(
                "PaymentConfirmed ignored — no Session_Registration__c found for registrationId %s",
                registration_id,
            )
            return None
        if session_registration.get("Is_Active__c") is False:
            logger.warning(
                "PaymentConfirmed ignored — Session_Registration__c %s is inactive",
                session_registration["Id"],
            )
            return None

    contact: dict[str, Any] | None
    if session_registration is not None:
        contact = await asyncio.to_thread(sf.Contact.get, session_registration["Contact__c"])
    elif user_id:
        contact = await get_contact_by_crm_id(sf, user_id)
        if contact is None:
            logger.warning("PaymentConfirmed ignored — no Contact found for userId %s", user_id)
            return None
    else:
        contact = await find_unique_contact_by_email(sf, email)
        if contact is None:
            return None

    if user_id and contact.get("CRM_ID__c") != user_id:
        logger.warning(
            "PaymentConfirmed ignored — userId mismatch for registrationId %s: incoming=%s resolved=%s",
            registration_id,
            user_id,
            contact.get("CRM_ID__c"),
        )
        return None

    if email and contact.get("Email") and contact.get("Email") != email:
        logger.warning(
            "PaymentConfirmed email mismatch — resolved %s but payload contained %s; proceeding with canonical registration match",
            contact.get("Email"),
            email,
        )

    if session_registration is None:
        session_registration = await get_unique_active_session_registration_for_contact(
            sf,
            contact["Id"],
        )
        if session_registration is None:
            return None

    if session_registration.get("Contact__c") and session_registration["Contact__c"] != contact["Id"]:
        logger.warning(
            "PaymentConfirmed ignored — Session_Registration__c %s belongs to Contact %s, not %s",
            session_registration["Id"],
            session_registration["Contact__c"],
            contact["Id"],
        )
        return None

    await asyncio.to_thread(
        sf.Session_Registration__c.update,
        session_registration["Id"],
        {"Paid_At__c": paid_at},
    )
    incoming_paid_at = _parse_iso_datetime_utc(paid_at)
    if incoming_paid_at is None:
        raise ValueError(f"Invalid paid_at timestamp: {paid_at}")

    existing_contact_paid_at = _parse_iso_datetime_utc(contact.get(_PAYMENT_TIMESTAMP_FIELD))
    if contact.get(_PAYMENT_TIMESTAMP_FIELD) and existing_contact_paid_at is None:
        logger.warning(
            "Contact %s has invalid %s value %r; overwriting with %s",
            contact["Id"],
            _PAYMENT_TIMESTAMP_FIELD,
            contact.get(_PAYMENT_TIMESTAMP_FIELD),
            paid_at,
        )

    should_sync_contact_paid_at = (
        existing_contact_paid_at is None or incoming_paid_at > existing_contact_paid_at
    )
    if should_sync_contact_paid_at:
        await asyncio.to_thread(
            sf.Contact.update,
            contact["Id"],
            {_PAYMENT_TIMESTAMP_FIELD: paid_at},
        )
        logger.info(
            "Updated Session_Registration__c %s payment and advanced Contact %s compatibility timestamp",
            session_registration["Id"],
            contact["Id"],
        )
    else:
        logger.info(
            "Updated Session_Registration__c %s payment without moving Contact %s compatibility timestamp backwards",
            session_registration["Id"],
            contact["Id"],
        )

    return await asyncio.to_thread(sf.Contact.get, contact["Id"])


async def deactivate_contact(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Soft-delete a Contact by setting IsActive__c=False (Contract 2, GDPR).

    NEVER physically delete a Contact — GDPR requires soft delete only.
    If the Contact does not exist, returns None and logs a warning.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to deactivate.

    Returns:
        Updated Contact record as dict, or None if not found.

    Raises:
        SalesforceError: If update fails.
    """
    contact = await get_contact_by_email(sf, email)
    if contact is None:
        logger.warning("Cannot deactivate — Contact not found for email: %s", email)
        return None

    return await deactivate_contact_record(sf, contact, log_value=email)


async def deactivate_contact_record(
    sf: Salesforce, contact: dict[str, Any], *, log_value: str | None = None
) -> dict[str, Any]:
    """Soft-delete an already resolved Contact by setting its active flag to False.

    NEVER physically delete a Contact — GDPR requires soft delete only.

    Args:
        sf: Authenticated Salesforce client.
        contact: The already resolved Salesforce Contact record.
        log_value: Optional identifier to include in logs.

    Returns:
        Updated Contact record as dict.

    Raises:
        SalesforceError: If update fails.
    """
    contact_id = contact["Id"]
    log_target = log_value or contact.get("Email") or contact_id

    try:
        active_field = await _resolve_contact_active_field(sf)

        await asyncio.to_thread(
            sf.Contact.update, contact_id, {active_field: False}
        )
        logger.info("Deactivated Contact %s (%s)", contact_id, log_target)

        # Re-fetch to get the complete updated record
        updated_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        if active_field != "IsActive__c" and "IsActive__c" not in updated_record:
            # Normalize key so downstream payload builders remain stable.
            updated_record["IsActive__c"] = updated_record.get(active_field, False)
        return updated_record
    except SalesforceError as e:
        logger.error("Failed to deactivate contact %s: %s", log_target, str(e))
        raise


async def create_account(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Account (company) in Salesforce (Contract 3).

    Maps XML CompanyRequest fields to Account fields and returns complete record
    for crm.company.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Account fields (Name, VAT_Number__c, Email, Phone, etc.).

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If Account creation fails.
    """
    try:
        # Shallow copy to avoid mutating input dict
        data = {**data}
        # Generate UUID v4 for crm.company.confirmed (Contract 14)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id

        # Create Account
        result = await asyncio.to_thread(sf.Account.create, data)
        account_id = result["id"]
        logger.info("Created Account with ID %s (CRM_ID: %s)", account_id, crm_id)

        # Retrieve and return complete record for XML serialization
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to create account: %s", str(e))
        raise


async def upsert_account_by_vat(
    sf: Salesforce, vat_number: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update an Account by VAT number (Contracts 3, 5a).

    Uses native Salesforce upsert with VAT_Number__c as external ID to avoid race conditions.
    Returns complete record for XML serialization.

    REQUIREMENT: VAT_Number__c must be configured as External ID in Salesforce Setup.

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number (lookup key, should be in data as VAT_Number__c).
        data: Account fields to create/update.

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    data = {**data}

    existing = await get_account_by_vat(sf, vat_number)
    if existing and existing.get("CRM_ID__c"):
        data["CRM_ID__c"] = existing["CRM_ID__c"]

    try:
        result = await asyncio.to_thread(
            sf.Account.upsert, f"VAT_Number__c/{vat_number}", data
        )
        account_id = result["id"]

        if result.get("created", False):
            if not data.get("CRM_ID__c"):
                crm_id = str(uuid.uuid4())
                await asyncio.to_thread(
                    sf.Account.update, account_id, {"CRM_ID__c": crm_id}
                )

        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to upsert account by VAT %s: %s", vat_number, str(e))
        raise


async def get_account_by_vat(
    sf: Salesforce, vat_number: str
) -> dict[str, Any] | None:
    """Look up an Account by VAT number (Contracts 5a, 5b).

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number to search for.

    Returns:
        Complete Account record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_vat = _escape_soql(vat_number)
        query = f"SELECT Id FROM Account WHERE VAT_Number__c = '{escaped_vat}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            account_id = result["records"][0]["Id"]
            account_record = await asyncio.to_thread(sf.Account.get, account_id)
            logger.info("Found Account by VAT: %s", vat_number)
            return account_record
        else:
            logger.info("Account not found by VAT: %s", vat_number)
            return None
    except SalesforceError as e:
        logger.error("Failed to get account by VAT %s: %s", vat_number, str(e))
        raise
