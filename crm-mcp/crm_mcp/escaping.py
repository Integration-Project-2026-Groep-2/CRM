"""SOQL string-literal escaping. Mirrors src/salesforce/client.py.escape_soql."""

from __future__ import annotations


def escape_soql(value: str) -> str:
    """Escape SOQL string-literal metacharacters to prevent injection.

    SOQL string literals honour `\\` as escape, so a lone backslash followed
    by the doubling-quote trick (`\\''`) would close the literal early.
    Order matters: backslash first, then single quote.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


def escape_soql_like(value: str) -> str:
    """Escape SOQL LIKE-pattern wildcards plus base string metacharacters.

    LIKE in SOQL treats `%` and `_` as wildcards. Escape with backslash to
    match literally. Run AFTER `escape_soql` because that step doubles
    backslashes.
    """
    base = escape_soql(value)
    return base.replace("%", "\\%").replace("_", "\\_")
