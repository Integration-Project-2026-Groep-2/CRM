"""Tests for src.xml_validator."""

from pathlib import Path

import pytest
from lxml import etree

from src.xml_validator import load_schema, validate


class TestLoadSchema:
    """Tests for load_schema()."""

    def test_raises_when_schema_file_missing(self, tmp_path: Path) -> None:
        """load_schema raises FileNotFoundError for a nonexistent path."""
        fake_path = tmp_path / "nonexistent.xsd"

        with pytest.raises(FileNotFoundError, match="XSD schema not found"):
            load_schema(fake_path)

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent.parent / "src" / "schema" / "crm-schema-v1.xsd").exists(),
        reason="XSD schema file not yet provided",
    )
    def test_loads_valid_schema(self, schema_path: Path) -> None:
        """load_schema returns an XMLSchema instance for a valid .xsd file."""
        schema = load_schema(schema_path)

        assert isinstance(schema, etree.XMLSchema)


class TestValidate:
    def test_accepts_valid_planning_user_deactivated_payload(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserDeactivated>
    <id>423e4567-e89b-42d3-a456-426614174030</id>
    <email>sofie.declercq@example.com</email>
    <deactivatedAt>2026-04-15T16:00:00Z</deactivatedAt>
</PlanningUserDeactivated>"""

        doc = validate(xml)

        assert doc.tag == "PlanningUserDeactivated"

    def test_rejects_planning_user_deactivated_without_deactivated_at(self) -> None:
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<PlanningUserDeactivated>
    <id>423e4567-e89b-42d3-a456-426614174030</id>
    <email>sofie.declercq@example.com</email>
</PlanningUserDeactivated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)
