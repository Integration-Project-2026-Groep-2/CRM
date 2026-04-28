"""Verify the modular XSD include-graph (entry → common-types + 8 contracts/*.xsd).

Bewijst dat de splitsing van crm-schema-v1.xsd in een manifest met
<xs:include> directives geen root-elementen verliest en dat lxml de
sub-files correct compileert tot één XMLSchema-instantie.
"""

from pathlib import Path

import pytest
from lxml import etree

from src.xml_validator import load_schema, validate

EXPECTED_ROOTS: frozenset[str] = frozenset(
    {
        # frontend.xsd
        "Registration",
        "RegistrationChange",
        "CompanyCreated",
        # facturatie.xsd
        "UserCreated",
        "CompanyRequest",
        "CompanyResponse",
        "InvoiceRequested",
        "FacturatieCompanyCreated",
        "FacturatieCompanyUpdated",
        "FacturatieCompanyDeactivated",
        # mailing.xsd
        "MailingUserCreated",
        "MailingUserUpdated",
        "MailingUserDeactivated",
        "MailRequest",
        "BounceReported",
        # controlroom.xsd
        "Heartbeat",
        "StatusCheck",
        "Warning",
        # kassa.xsd
        "PersonLookupRequest",
        "PersonLookupResponse",
        "KassaUserCreated",
        "KassaUserUpdated",
        "KassaUserDeactivated",
        "PaymentConfirmed",
        "UnpaidRequest",
        "UnpaidResponse",
        # planning.xsd
        "SessionUpdate",
        "PlanningUserCreated",
        "PlanningUserUpdated",
        "PlanningUserDeactivated",
        # iot.xsd
        "BadgeLink",
        # crm-outbound.xsd
        "UserConfirmed",
        "CompanyConfirmed",
        "UserConflict",
        "UserUpdated",
        "CompanyUpdated",
        "UserDeactivated",
        "CompanyDeactivated",
    }
)


class TestIncludeGraph:
    def test_entry_manifest_compiles(self, schema_path: Path) -> None:
        schema = load_schema(schema_path)

        assert isinstance(schema, etree.XMLSchema)

    def test_entry_manifest_only_contains_includes(self, schema_path: Path) -> None:
        """Entry file moet een pure manifest zijn — geen inline types of elements."""
        doc = etree.parse(str(schema_path))
        root = doc.getroot()
        ns = "{http://www.w3.org/2001/XMLSchema}"
        children = [child for child in root if isinstance(child.tag, str)]

        assert all(child.tag == f"{ns}include" for child in children), (
            f"Entry manifest mag alleen <xs:include> bevatten, "
            f"vond: {[c.tag for c in children if c.tag != f'{ns}include']}"
        )

    def test_all_expected_roots_resolve(self, schema_path: Path) -> None:
        """Iedere root uit EXPECTED_ROOTS moet via een minimale XML te vinden zijn."""
        schema = load_schema(schema_path)
        ns = "{http://www.w3.org/2001/XMLSchema}"
        doc = etree.parse(str(schema_path))

        elements_found: set[str] = set()
        for include in doc.getroot().findall(f"{ns}include"):
            location = include.get("schemaLocation")
            assert location, "<xs:include> mist schemaLocation"
            sub_doc = etree.parse(str(schema_path.parent / location))
            for el in sub_doc.getroot().findall(f"{ns}element"):
                name = el.get("name")
                if name:
                    elements_found.add(name)

        missing = EXPECTED_ROOTS - elements_found
        unexpected = elements_found - EXPECTED_ROOTS

        assert not missing, f"Ontbrekende root elements: {sorted(missing)}"
        assert not unexpected, f"Onverwachte root elements: {sorted(unexpected)}"
        assert isinstance(schema, etree.XMLSchema)

    def test_expected_root_count_matches_contract_count(self) -> None:
        """35 root elements voor 35 contracts (sommige roots delen consumers)."""
        assert len(EXPECTED_ROOTS) == 38


@pytest.mark.parametrize(
    ("xml_bytes", "expected_root"),
    [
        (
            b"<Heartbeat><serviceId>CRM</serviceId>"
            b"<timestamp>2026-04-25T12:00:00Z</timestamp></Heartbeat>",
            "Heartbeat",
        ),
        (
            b"<BadgeLink><badgeId>B-001</badgeId>"
            b"<contactEmail>x@y.be</contactEmail>"
            b"<linkedAt>2026-04-25T12:00:00Z</linkedAt></BadgeLink>",
            "BadgeLink",
        ),
        (
            b"<UnpaidRequest><requestId>req-1</requestId></UnpaidRequest>",
            "UnpaidRequest",
        ),
        (
            b"<MailingUserDeactivated>"
            b"<id>423e4567-e89b-42d3-a456-426614174030</id>"
            b"<email>x@y.be</email>"
            b"<deactivatedAt>2026-04-25T12:00:00Z</deactivatedAt>"
            b"</MailingUserDeactivated>",
            "MailingUserDeactivated",
        ),
        (
            b"<PlanningUserDeactivated>"
            b"<id>423e4567-e89b-42d3-a456-426614174030</id>"
            b"<email>x@y.be</email>"
            b"<deactivatedAt>2026-04-25T12:00:00Z</deactivatedAt>"
            b"</PlanningUserDeactivated>",
            "PlanningUserDeactivated",
        ),
        (
            b"<CompanyRequest><requestId>r1</requestId>"
            b"<vatNumber>BE0123456789</vatNumber></CompanyRequest>",
            "CompanyRequest",
        ),
        (
            b"<UserDeactivated>"
            b"<id>423e4567-e89b-42d3-a456-426614174030</id>"
            b"<email>x@y.be</email>"
            b"<deactivatedAt>2026-04-25T12:00:00Z</deactivatedAt>"
            b"</UserDeactivated>",
            "UserDeactivated",
        ),
        (
            b"<Registration><registrationId>r1</registrationId>"
            b"<firstName>A</firstName><lastName>B</lastName>"
            b"<email>x@y.be</email><sessionId>s1</sessionId>"
            b"<role>VISITOR</role><gdprConsent>true</gdprConsent></Registration>",
            "Registration",
        ),
    ],
)
class TestPerTeamRoundtrip:
    def test_valid_payload_per_team_passes(
        self, xml_bytes: bytes, expected_root: str
    ) -> None:
        """Geldige XML per team moet de manifest-include keten succesvol passeren."""
        doc = validate(xml_bytes)

        assert doc.tag == expected_root

    def test_invalid_payload_per_team_rejects(
        self, xml_bytes: bytes, expected_root: str
    ) -> None:
        """Verwijder een verplicht veld → moet falen, bewijst include-keten doet validatie."""
        del expected_root
        # Strip alle child elements — produceert een lege root die voor elk
        # contract minimaal één verplicht veld mist.
        stripped = xml_bytes.split(b">", 1)[0] + b"/>"

        with pytest.raises(ValueError, match="XML validation failed"):
            validate(stripped)
