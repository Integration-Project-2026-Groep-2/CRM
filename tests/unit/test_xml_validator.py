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

    def test_accepts_facturatie_user_updated_without_gdpr_or_badge(self) -> None:
        """Contract 25 — Facturatie root has no gdprConsent or badgeCode fields."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieUserUpdated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <role>COMPANY_CONTACT</role>
    <companyId>f4e5d6c7-b8a9-4012-8f34-ab5678cd9012</companyId>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</FacturatieUserUpdated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieUserUpdated"

    def test_rejects_facturatie_user_updated_with_badge_code(self) -> None:
        """Contract 25 — badgeCode is not a valid field on FacturatieUserUpdated."""
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieUserUpdated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <role>COMPANY_CONTACT</role>
    <badgeCode>BADGE-123</badgeCode>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</FacturatieUserUpdated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_accepts_contract_18_user_updated_with_badge_and_gdpr(self) -> None:
        """Contract 18 — outbound UserUpdated still carries badgeCode and gdprConsent."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <role>COMPANY_CONTACT</role>
    <badgeCode>BADGE-123</badgeCode>
    <isActive>true</isActive>
    <gdprConsent>true</gdprConsent>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</UserUpdated>"""

        doc = validate(xml)

        assert doc.tag == "UserUpdated"

    def test_accepts_facturatie_user_deactivated(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieUserDeactivated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.peeters@example.com</email>
    <deactivatedAt>2026-04-21T16:00:00Z</deactivatedAt>
</FacturatieUserDeactivated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieUserDeactivated"
