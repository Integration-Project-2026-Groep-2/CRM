"""Tests for src.heartbeat."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

from src import xml_validator
from src.config import Config
from src.heartbeat import _build_heartbeat_xml, run_heartbeat


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


class TestRunHeartbeat:
    """Tests for run_heartbeat()."""

    @pytest.mark.asyncio
    async def test_publishes_heartbeat(self, config: Config) -> None:
        """run_heartbeat declares queue, builds XML, validates, and publishes."""
        # Setup mocks
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_connection.channel.return_value = mock_channel
        
        # Terminate iteration cleanly when publish is called
        mock_channel.default_exchange.publish.side_effect = asyncio.CancelledError()
        
        try:
            await run_heartbeat(mock_connection, config)
        except asyncio.CancelledError:
            pass

        # Verification
        mock_connection.channel.assert_called_once()
        mock_channel.declare_queue.assert_called_once_with("crm.heartbeat", durable=False)
        mock_channel.default_exchange.publish.assert_called_once()
        
        # Verify the published message is what we expect
        args, kwargs = mock_channel.default_exchange.publish.call_args
        message = args[0]
        assert kwargs["routing_key"] == "crm.heartbeat"
        
        # The body should be valid XML with serviceId CRM
        root = etree.fromstring(message.body)
        assert root.tag == "Heartbeat"
        assert root.findtext("serviceId") == "CRM"

    @pytest.mark.asyncio
    @patch("src.heartbeat._build_heartbeat_xml")
    async def test_recovers_from_exception(self, mock_build: MagicMock, config: Config) -> None:
        """run_heartbeat catches exceptions and continues the loop."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_connection.channel.return_value = mock_channel
        
        # Make build_xml fail on the first call, succeed on the second
        mock_build.side_effect = [Exception("Simulation of failure"), b"<Heartbeat></Heartbeat>"]
        
        # Terminate loop on the second iteration when publish is called
        mock_channel.default_exchange.publish.side_effect = asyncio.CancelledError()
        
        # We patch xml_validator so it doesn't crash on the fake bytes
        with patch("src.heartbeat.xml_validator.validate"):
            try:
                await run_heartbeat(mock_connection, config)
            except asyncio.CancelledError:
                pass

        # Since it ran multiple iterations and failed on the first,
        # it should have called build_xml exactly twice
        assert mock_build.call_count == 2
        
        # Connection channel should be called again after failure to recover
        assert mock_connection.channel.call_count == 2
        
        # Verify publish was indeed called after recovery
        mock_channel.default_exchange.publish.assert_called_once()
