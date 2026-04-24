"""Country value normalisation to ISO 3166-1 alpha-2.

Used by polling and receiver builders to convert Salesforce country fields
(BillingCountry/MailingCountry may hold derived labels like "Belgium",
BillingCountryCode/MailingCountryCode hold ISO-2) into the "[A-Z]{2}" format
required by the XSD CountryCodeType.
"""
from __future__ import annotations

import logging
from functools import lru_cache

import pycountry

logger = logging.getLogger(__name__)


@lru_cache(maxsize=256)
def to_iso_alpha2(value: str | None) -> str | None:
    """Normalize a country name or code to ISO 3166-1 alpha-2.

    Returns None for empty or unresolvable input, and logs a warning on
    unresolvable non-empty input. The @lru_cache means each distinct input
    warns at most once per container lifetime — intended behavior, prevents
    log spam from polling loops that re-read the same "Belgium" every 60s.
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 2 and text.isalpha():
        candidate = text.upper()
        if pycountry.countries.get(alpha_2=candidate) is not None:
            return candidate
    try:
        return pycountry.countries.lookup(text).alpha_2
    except LookupError:
        logger.warning("Country: cannot normalize %r to ISO alpha-2", value)
        return None
