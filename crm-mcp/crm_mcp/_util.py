"""Internal helpers shared across tool modules."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

_VAT_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{2,12}$")
_SF_ID_CHARSET = re.compile(r"^[A-Za-z0-9]+$")
_KNOWN_ROLES = frozenset({"VISITOR", "COMPANY_CONTACT"})
VALID_USER_ROLES: frozenset[str] = frozenset({
    "VISITOR", "COMPANY_CONTACT", "SPEAKER",
    "EVENT_MANAGER", "CASHIER", "BAR_STAFF", "ADMIN",
})


def parse_sf_datetime(raw: str | None) -> datetime | None:
    """Parse a Salesforce ISO-8601 datetime string. Returns None on empty/invalid input.

    Python 3.11+ `datetime.fromisoformat` natively handles `Z` and millisecond
    fractions, so we delegate to it.
    """
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def format_soql_datetime(dt: datetime) -> str:
    """Format a datetime as a SOQL date-time literal in UTC.

    SOQL accepts unquoted ISO-8601 datetime literals like `2026-05-04T10:00:00Z`.
    Naive datetimes are rejected because silent wall-clock formatting causes
    off-by-hours bugs (silent timezone shift).
    """
    if dt.tzinfo is None:
        raise ValueError(
            "datetime must be timezone-aware; pass UTC datetime to avoid "
            "silent off-by-hours bugs"
        )
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_valid_sf_id(value: str | None, prefix: str) -> bool:
    """Validate a Salesforce Id: 15 or 18 alphanumerics with a known object prefix."""
    if not value or not value.startswith(prefix):
        return False
    if len(value) not in (15, 18):
        return False
    return bool(_SF_ID_CHARSET.fullmatch(value))


def is_uuid_format(value: str | None) -> bool:
    """True if value parses as RFC 4122 UUID. Lenient on case + version."""
    if not value:
        return False
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError):
        return False


def validate_vat_number(vat: str) -> None:
    """Validate VAT-number shape; raises ValueError on bad format.

    Accepts EU-style VAT numbers like `BE0123456789`, `NL123456789B01`,
    `FR12345678901`. Pattern: 2 letters followed by 2-12 alphanumerics.
    """
    if not _VAT_PATTERN.fullmatch(vat):
        raise ValueError(
            "vat_number must match pattern <2 letters><2-12 alphanumerics>, "
            "e.g. BE0123456789"
        )


def coerce_known_role(raw: Any) -> str | None:
    """Coerce a Salesforce Role__c picklist value to a known role or None.

    Defends against legacy/unknown picklist values that would otherwise crash
    Pydantic's Literal validation. Unknown values become None so the tool can
    still return a useful response.
    """
    return raw if raw in _KNOWN_ROLES else None


def coerce_is_active(raw: Any) -> bool:
    """Normalize an active-flag value into a bool.

    Salesforce custom active fields come in multiple forms:
    - Boolean (`IsActive__c` / `Is_Active__c`) → True / False / None
    - Picklist (`Active__c`) → "Yes" / "No"
    - Text → "true" / "false" / empty string

    Missing (`None`) defaults to True; mirrors the receiver's behaviour.
    """
    if raw is None:
        return True
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    text = str(raw).strip().lower()
    if text in ("yes", "true", "1", "y"):
        return True
    if text in ("no", "false", "0", "n", ""):
        return False
    return bool(raw)
