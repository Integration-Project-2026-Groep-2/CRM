"""Unit tests for SOQL escaping helpers."""

from __future__ import annotations

from crm_mcp.escaping import escape_soql, escape_soql_like


def test_escape_soql_quotes_single_quote() -> None:
    assert escape_soql("O'Brien") == "O\\'Brien"


def test_escape_soql_doubles_backslashes_first() -> None:
    assert escape_soql("a\\b") == "a\\\\b"


def test_escape_soql_handles_backslash_quote_combo() -> None:
    assert escape_soql("a\\'b") == "a\\\\\\'b"


def test_escape_soql_like_escapes_wildcards() -> None:
    assert escape_soql_like("100%") == "100\\%"
    assert escape_soql_like("under_score") == "under\\_score"


def test_escape_soql_like_combines_with_base_escape() -> None:
    assert escape_soql_like("O'Brien_50%") == "O\\'Brien\\_50\\%"
