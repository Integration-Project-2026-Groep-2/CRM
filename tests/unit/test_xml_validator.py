"""Tests for src.xml_validator."""

from pathlib import Path

import pytest
from lxml import etree

from src.xml_validator import load_schema, validate, validate_kassa


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
    def test_accepts_namespaced_registration_with_lowercase_role(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<Registration xmlns='urn:frontend:crm:contract'>
    <registrationId>drupal-98765</registrationId>
    <firstName>Jan</firstName>
    <lastName>Peeters</lastName>
    <email>jan.peeters@example.com</email>
    <sessionId>planning-session-123</sessionId>
    <role>visitor</role>
    <gdprConsent>true</gdprConsent>
    <phone>+32470123456</phone>
    <company>Acme BV</company>
</Registration>"""

        doc = validate(xml)

        assert doc.tag == "Registration"
        assert doc.findtext("role") == "visitor"

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

    def test_accepts_user_updated_without_gdpr_or_badge(self) -> None:
        """Contract 25 — Facturatie inbound uses shared <UserUpdated> root.

        Regression guard for the 2026-04-21 production bug where Facturatie's
        payload was rejected because the split <FacturatieUserUpdated> root
        did not match their actual <UserUpdated> XML root. Now gdprConsent
        and badgeCode are optional on the shared root so Facturatie's slim
        payload validates and CRM outbound can still include them.
        """
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName>Updated</lastName>
    <role>COMPANY_CONTACT</role>
    <companyId>f4e5d6c7-b8a9-4012-8f34-ab5678cd9012</companyId>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</UserUpdated>"""

        doc = validate(xml)

        assert doc.tag == "UserUpdated"

    def test_accepts_contract_18_user_updated_with_badge_and_gdpr(self) -> None:
        """Contract 18 — outbound UserUpdated still carries badgeCode and gdprConsent.

        Shared root accepts both the full outbound payload (CRM publishes with
        all fields) and the Facturatie inbound subset.
        """
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

    def test_accepts_user_deactivated(self) -> None:
        """Contracts 22 + 26 share the <UserDeactivated> root with identical fields."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserDeactivated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.peeters@example.com</email>
    <deactivatedAt>2026-04-21T16:00:00Z</deactivatedAt>
</UserDeactivated>"""

        doc = validate(xml)

        assert doc.tag == "UserDeactivated"

    def test_rejects_user_created_with_empty_last_name(self) -> None:
        """Contract 24 — lastName must be non-empty (NonEmptyStringType, 2026-04-21).

        Reproduces the 2026-04-21 production bug where Facturatie sent
        `<lastName></lastName>` which passed `xs:string` validation but hit
        Salesforce as REQUIRED_FIELD_MISSING and cycled five retries.
        """
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserCreated>
    <facturatieCustomerId>FB-2042</facturatieCustomerId>
    <firstName>Els</firstName>
    <lastName></lastName>
    <email>els.peeters@example.com</email>
    <role>COMPANY_CONTACT</role>
    <isActive>true</isActive>
    <createdAt>2026-04-21T09:00:00Z</createdAt>
</UserCreated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_rejects_user_created_with_empty_first_name(self) -> None:
        """Contract 24 — firstName must be non-empty (NonEmptyStringType)."""
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserCreated>
    <facturatieCustomerId>FB-2042</facturatieCustomerId>
    <firstName></firstName>
    <lastName>Peeters</lastName>
    <email>els.peeters@example.com</email>
    <role>COMPANY_CONTACT</role>
    <isActive>true</isActive>
    <createdAt>2026-04-21T09:00:00Z</createdAt>
</UserCreated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_rejects_user_updated_with_empty_last_name(self) -> None:
        """Contract 25 — lastName must be non-empty (NonEmptyStringType).

        Applies to the shared <UserUpdated> root used by both C18 outbound
        and C25 inbound (Facturatie).
        """
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserUpdated>
    <id>223e4567-e89b-42d3-a456-426614174024</id>
    <email>els.updated@example.com</email>
    <firstName>Els</firstName>
    <lastName></lastName>
    <role>COMPANY_CONTACT</role>
    <isActive>true</isActive>
    <updatedAt>2026-04-21T10:00:00Z</updatedAt>
</UserUpdated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_accepts_user_created_with_valid_names(self) -> None:
        """Contract 24 — positive control: non-empty firstName and lastName are accepted."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<UserCreated>
    <facturatieCustomerId>FB-2042</facturatieCustomerId>
    <firstName>Els</firstName>
    <lastName>Peeters</lastName>
    <email>els.peeters@example.com</email>
    <role>COMPANY_CONTACT</role>
    <isActive>true</isActive>
    <createdAt>2026-04-21T09:00:00Z</createdAt>
</UserCreated>"""

        doc = validate(xml)

        assert doc.tag == "UserCreated"
        assert doc.findtext("firstName") == "Els"
        assert doc.findtext("lastName") == "Peeters"

    def test_accepts_kassa_user_created_with_valid_names(self) -> None:
        """Kassa producer schema accepts the dedicated KassaUserCreated root."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserCreated>
    <userId>523e4567-e89b-42d3-a456-426614174036</userId>
    <firstName>Karel</firstName>
    <lastName>Kassa</lastName>
    <email>karel.kassa@example.com</email>
    <badgeCode>BADGE-036</badgeCode>
    <role>VISITOR</role>
    <createdAt>2026-04-25T09:30:00Z</createdAt>
</KassaUserCreated>"""

        doc = validate_kassa(xml)

        assert doc.tag == "KassaUserCreated"

    def test_rejects_kassa_user_created_with_empty_badge_code(self) -> None:
        """Kassa producer schema rejects empty badgeCode values."""
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<KassaUserCreated>
    <userId>523e4567-e89b-42d3-a456-426614174036</userId>
    <firstName>Karel</firstName>
    <lastName>Kassa</lastName>
    <email>karel.kassa@example.com</email>
    <badgeCode></badgeCode>
    <role>VISITOR</role>
    <createdAt>2026-04-25T09:30:00Z</createdAt>
</KassaUserCreated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate_kassa(invalid_xml)


class TestFacturatieCompanySync:
    """Contracts 33/34/35 — Facturatie → CRM company lifecycle sync (v1.9.0).

    Facturatie publishes to the company.topic exchange with dedicated root
    elements to avoid collision with <CompanyCreated> (C3 frontend) and with
    CRM outbound roots <CompanyUpdated>/<CompanyDeactivated> (C19/C23).
    """

    def test_accepts_facturatie_company_created_full_payload(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Acme NV</name>
    <vatNumber>BE0123456789</vatNumber>
    <email>billing@acme.example</email>
    <phone>+32 2 123 45 67</phone>
    <street>Kerkstraat</street>
    <houseNumber>12</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussels</city>
    <country>BE</country>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieCompanyCreated"
        assert doc.findtext("name") == "Acme NV"
        assert doc.findtext("vatNumber") == "BE0123456789"

    def test_accepts_facturatie_company_created_minimal_payload(self) -> None:
        """vatNumber and address fields are optional — only name, email, createdAt required."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Beta BVBA</name>
    <email>contact@beta.example</email>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieCompanyCreated"

    def test_rejects_facturatie_company_created_without_email(self) -> None:
        """email is required on FacturatieCompanyCreated (differs from C3 where it's optional)."""
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Acme NV</name>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_rejects_facturatie_company_created_with_non_belgian_vat(self) -> None:
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Acme NV</name>
    <vatNumber>FR12345678901</vatNumber>
    <email>billing@acme.example</email>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_accepts_facturatie_company_updated_full_payload(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyUpdated>
    <id>123e4567-e89b-42d3-a456-426614174000</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme NV</name>
    <email>billing@acme.example</email>
    <phone>+32 2 123 45 67</phone>
    <street>Kerkstraat</street>
    <houseNumber>12</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussels</city>
    <country>BE</country>
    <isActive>true</isActive>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</FacturatieCompanyUpdated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieCompanyUpdated"
        assert doc.findtext("id") == "123e4567-e89b-42d3-a456-426614174000"
        assert doc.findtext("isActive") == "true"

    def test_accepts_company_confirmed_with_full_address(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyConfirmed>
    <id>660e8400-e29b-41d4-a716-446655440001</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme NV</name>
    <email>info@acme.be</email>
    <street>Kerkstraat</street>
    <houseNumber>12</houseNumber>
    <postalCode>1000</postalCode>
    <city>Brussel</city>
    <country>BE</country>
    <isActive>true</isActive>
    <confirmedAt>2026-04-22T10:00:00Z</confirmedAt>
</CompanyConfirmed>"""

        doc = validate(xml)

        assert doc.tag == "CompanyConfirmed"
        assert doc.findtext("name") == "Acme NV"
        assert doc.findtext("country") == "BE"

    def test_rejects_company_confirmed_without_address(self) -> None:
        """Contract v1.X.0: address fields are now required on C14."""
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<CompanyConfirmed>
    <id>660e8400-e29b-41d4-a716-446655440001</id>
    <vatNumber>BE0123456789</vatNumber>
    <name>Acme NV</name>
    <email>info@acme.be</email>
    <isActive>true</isActive>
    <confirmedAt>2026-04-22T10:00:00Z</confirmedAt>
</CompanyConfirmed>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_rejects_facturatie_company_updated_without_id(self) -> None:
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyUpdated>
    <name>Acme NV</name>
    <email>billing@acme.example</email>
    <isActive>true</isActive>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</FacturatieCompanyUpdated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_rejects_facturatie_company_updated_without_is_active(self) -> None:
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyUpdated>
    <id>123e4567-e89b-42d3-a456-426614174000</id>
    <name>Acme NV</name>
    <email>billing@acme.example</email>
    <updatedAt>2026-04-22T10:00:00Z</updatedAt>
</FacturatieCompanyUpdated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)

    def test_accepts_facturatie_company_deactivated(self) -> None:
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyDeactivated>
    <id>123e4567-e89b-42d3-a456-426614174000</id>
    <email>billing@acme.example</email>
    <deactivatedAt>2026-04-22T11:00:00Z</deactivatedAt>
</FacturatieCompanyDeactivated>"""

        doc = validate(xml)

        assert doc.tag == "FacturatieCompanyDeactivated"
        assert doc.findtext("id") == "123e4567-e89b-42d3-a456-426614174000"

    def test_rejects_facturatie_company_deactivated_without_deactivated_at(self) -> None:
        invalid_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyDeactivated>
    <id>123e4567-e89b-42d3-a456-426614174000</id>
    <email>billing@acme.example</email>
</FacturatieCompanyDeactivated>"""

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(invalid_xml)


class TestXmlParserHardening:
    """Regression tests — parser must reject DTD entity expansion attacks.

    With `resolve_entities=False` on the shared parser, lxml does NOT expand
    `&big;` into its replacement text. Unresolved entity references become a
    distinct child node inside the target element (not text), which then
    fails XSD validation because the content model expects pure character
    data. Either way, a malicious DOCTYPE payload is rejected before it
    reaches Salesforce or SOQL.
    """

    def test_rejects_internal_dtd_entity_expansion(self) -> None:
        """Billion-laughs / payload smuggling via internal DTD entity."""
        malicious_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE FacturatieCompanyCreated [
  <!ENTITY big "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">
]>
<FacturatieCompanyCreated>
    <name>&big;</name>
    <email>x@y.example</email>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        with pytest.raises((etree.XMLSyntaxError, etree.XMLSchemaValidateError, ValueError)):
            validate(malicious_xml)

    def test_rejects_undefined_entity_reference(self) -> None:
        """Any undefined `&foo;` must raise at parse or validate time."""
        malicious_xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>&undefined;</name>
    <email>x@y.example</email>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        with pytest.raises((etree.XMLSyntaxError, etree.XMLSchemaValidateError, ValueError)):
            validate(malicious_xml)

    def test_entity_text_is_not_expanded_on_happy_path(self) -> None:
        """Positive control: valid payload without DTD/entities still validates."""
        xml = b"""<?xml version='1.0' encoding='utf-8'?>
<FacturatieCompanyCreated>
    <name>Normaal bedrijf &amp; co</name>
    <email>x@y.example</email>
    <createdAt>2026-04-22T09:30:00Z</createdAt>
</FacturatieCompanyCreated>"""

        doc = validate(xml)
        # `&amp;` is a built-in predefined entity — still allowed, treated as
        # the `&` character because lxml handles the five XML built-ins even
        # with resolve_entities=False.
        assert doc.findtext("name") == "Normaal bedrijf & co"
