"""Tests for crm_mcp._util helpers."""

from __future__ import annotations

from crm_mcp._util import is_uuid_format, is_valid_sf_id


def test_is_uuid_format_accepts_canonical_uuid() -> None:
    assert is_uuid_format("4c82edf9-0eec-47dc-80ef-15cd3c85a106") is True


def test_is_uuid_format_accepts_uppercase() -> None:
    assert is_uuid_format("4C82EDF9-0EEC-47DC-80EF-15CD3C85A106") is True


def test_is_uuid_format_rejects_sf_id() -> None:
    assert is_uuid_format("003dM00001wmNGXQA2") is False


def test_is_uuid_format_rejects_empty_and_none() -> None:
    assert is_uuid_format("") is False
    assert is_uuid_format(None) is False


def test_is_uuid_and_is_valid_sf_id_are_disjoint() -> None:
    """A valid string is exactly one of UUID or SF Id, never both."""
    uuid = "4c82edf9-0eec-47dc-80ef-15cd3c85a106"
    sf_contact = "003dM00001wmNGXQA2"
    sf_account = "001dM00003kxOrvQAE"
    assert is_uuid_format(uuid) and not is_valid_sf_id(uuid, prefix="003")
    assert is_valid_sf_id(sf_contact, prefix="003") and not is_uuid_format(sf_contact)
    assert is_valid_sf_id(sf_account, prefix="001") and not is_uuid_format(sf_account)
