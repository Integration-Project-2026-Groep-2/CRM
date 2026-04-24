"""Tests for src.country_code.to_iso_alpha2."""
from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset the lru_cache between tests so warning/call counts are per-test."""
    from src.country_code import to_iso_alpha2

    to_iso_alpha2.cache_clear()
    yield
    to_iso_alpha2.cache_clear()


class TestToIsoAlpha2:
    def test_alpha2_pass_through(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("BE") == "BE"

    def test_alpha2_lowercase_normalized(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("be") == "BE"

    def test_alpha2_mixed_case_normalized(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("Be") == "BE"

    def test_full_english_name_resolved(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("Belgium") == "BE"

    def test_alpha3_resolved(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("BEL") == "BE"

    def test_none_returns_none(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2(None) is None

    def test_empty_string_returns_none(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("") is None

    def test_whitespace_only_returns_none(self):
        from src.country_code import to_iso_alpha2

        assert to_iso_alpha2("   ") is None

    def test_invalid_alpha2_returns_none(self, caplog):
        """'XX' is a 2-letter string but not a real ISO code — must reject."""
        from src.country_code import to_iso_alpha2

        caplog.set_level(logging.WARNING, logger="src.country_code")
        assert to_iso_alpha2("XX") is None
        assert any("XX" in rec.message for rec in caplog.records)

    def test_unresolvable_name_returns_none_with_warning(self, caplog):
        from src.country_code import to_iso_alpha2

        caplog.set_level(logging.WARNING, logger="src.country_code")
        assert to_iso_alpha2("Atlantis") is None
        assert any("Atlantis" in rec.message for rec in caplog.records)

    def test_cache_suppresses_repeat_lookup(self, monkeypatch):
        """@lru_cache means pycountry.countries.lookup is invoked once per value."""
        import pycountry

        from src.country_code import to_iso_alpha2

        call_count = {"n": 0}
        real_lookup = pycountry.countries.lookup

        def counting_lookup(value):
            call_count["n"] += 1
            return real_lookup(value)

        monkeypatch.setattr(pycountry.countries, "lookup", counting_lookup)

        assert to_iso_alpha2("Belgium") == "BE"
        assert to_iso_alpha2("Belgium") == "BE"

        assert call_count["n"] == 1

    def test_cache_suppresses_repeat_warning(self, caplog):
        """Second call with same unresolvable value is silent (intended)."""
        from src.country_code import to_iso_alpha2

        caplog.set_level(logging.WARNING, logger="src.country_code")
        assert to_iso_alpha2("Atlantis") is None
        assert to_iso_alpha2("Atlantis") is None

        atlantis_warnings = [r for r in caplog.records if "Atlantis" in r.message]
        assert len(atlantis_warnings) == 1
