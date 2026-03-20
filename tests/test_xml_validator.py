"""Tests for src.xml_validator."""

from pathlib import Path

import pytest

from src.xml_validator import load_schema


class TestLoadSchema:
    """Tests for load_schema()."""

    def test_raises_when_schema_file_missing(self, tmp_path: Path) -> None:
        """load_schema raises FileNotFoundError for a nonexistent path."""
        fake_path = tmp_path / "nonexistent.xsd"

        with pytest.raises(FileNotFoundError, match="XSD schema not found"):
            load_schema(fake_path)

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "schema" / "crm_v1.1.xsd").exists(),
        reason="XSD schema file not yet provided",
    )
    def test_loads_valid_schema(self, schema_path: Path) -> None:
        """load_schema returns an XMLSchema instance for a valid .xsd file."""
        from lxml import etree

        schema = load_schema(schema_path)

        assert isinstance(schema, etree.XMLSchema)
