"""Tests for src.heartbeat."""

from lxml import etree

from src import xml_validator
from src.heartbeat import _build_heartbeat_xml


class TestBuildHeartbeatXml:
    """Tests for _build_heartbeat_xml()."""

    def test_builds_valid_xml(self) -> None:
        """_build_heartbeat_xml returns bytes with correct structure."""
        xml_bytes = _build_heartbeat_xml("CRM")

        # Parse output to verify structure
        root = etree.fromstring(xml_bytes)

        assert root.tag == "Heartbeat"
        assert root.findtext("serviceId") == "CRM"
        assert root.findtext("timestamp") is not None
        assert root.findtext("timestamp").endswith("Z")  # Verify UTC suffix

    def test_heartbeat_xml_passes_xsd(self) -> None:
        """_build_heartbeat_xml output passes strict XSD validation."""
        xml_bytes = _build_heartbeat_xml("CRM")
        # Should not raise any ValueError
        xml_validator.validate(xml_bytes)
