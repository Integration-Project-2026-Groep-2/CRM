"""Country value normalisation to ISO 3166-1 alpha-2 for XSD CountryCodeType."""
from __future__ import annotations

import logging
from functools import lru_cache

import pycountry

logger = logging.getLogger(__name__)


# @lru_cache also suppresses repeated warnings for the same unresolvable
# input — polling reads the same "Belgium" every 60s and we want one log.
@lru_cache(maxsize=256)
def to_iso_alpha2(value: str | None) -> str | None:
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
