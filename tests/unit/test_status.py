"""Tests for src.status."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

from src import xml_validator
from src.config import Config
from src.status import _build_status_xml, _determine_status, _get_system_metrics, run_status


class TestDetermineStatus:
    """Tests for status determination logic."""

    def test_healthy(self):
        """Test healthy status (all metrics < 0.8)."""
        assert _determine_status(0.5, 0.6, 0.79) == "healthy"
        assert _determine_status(0.0, 0.0, 0.0) == "healthy"

    def test_degraded(self):
        """Test degraded status (any metric >= 0.8, none >= 0.95)."""
        assert _determine_status(0.8, 0.2, 0.2) == "degraded"
        assert _determine_status(0.5, 0.94, 0.5) == "degraded"
        assert _determine_status(0.5, 0.5, 0.8) == "degraded"

    def test_unhealthy(self):
        """Test unhealthy status (any metric >= 0.95)."""
        assert _determine_status(0.95, 0.5, 0.5) == "unhealthy"
        assert _determine_status(0.5, 0.96, 0.5) == "unhealthy"
        assert _determine_status(0.5, 0.5, 0.99) == "unhealthy"
        assert _determine_status(0.95, 0.96, 0.99) == "unhealthy"


class TestGetSystemMetrics:
    """Tests for gathering system metrics via psutil."""

    @patch("src.status.psutil")
    @patch("src.status.time.monotonic")
    def test_get_metrics_success(self, mock_monotonic, mock_psutil):
        """Metrics are correctly fetched and percentages scaled to 0.0-1.0."""
        mock_monotonic.return_value = 1000.0
        # psutil returns 23.0 for 23%
        mock_psutil.cpu_percent.return_value = 23.0
        mock_psutil.virtual_memory.return_value.percent = 41.0
        mock_psutil.disk_usage.return_value.percent = 15.0

        # simulate started at 900.0 (100 sec ago)
        metrics = _get_system_metrics(start_time=900.0)

        assert metrics["cpu"] == 0.23
        assert metrics["memory"] == 0.41
        assert metrics["disk"] == 0.15
        assert metrics["uptime"] == 100
        assert metrics["status"] == "healthy"

    @patch("src.status.psutil")
    @patch("src.status.time.monotonic")
    def test_get_metrics_exception(self, mock_monotonic, mock_psutil):
        """Returns unknown status when psutil throws an exception."""
        mock_psutil.cpu_percent.side_effect = Exception("Permission Denied")
        mock_monotonic.return_value = 200.0

        metrics = _get_system_metrics(start_time=100.0)

        assert metrics["status"] == "unknown"
        assert metrics["uptime"] == 100


class TestBuildStatusXml:
    """Tests for building the StatusCheck XML message."""

    def test_builds_valid_xml_structure(self) -> None:
        """Output matches the expected XML structure."""
        metrics = {
            "status": "healthy",
            "uptime": 3600,
            "cpu": 0.23,
            "memory": 0.41,
            "disk": 0.15
        }
        xml_bytes = _build_status_xml("CRM", metrics)
        
        # Test structure with lxml
        root = etree.fromstring(xml_bytes)
        assert root.tag == "StatusCheck"
        assert root.findtext("serviceId") == "CRM"
        assert root.findtext("status") == "healthy"
        assert root.findtext("uptime") == "3600"
        
        # Test nested systemLoad
        assert root.find("systemLoad/cpu").text == "0.23"
        assert root.find("systemLoad/memory").text == "0.41"
        assert root.find("systemLoad/disk").text == "0.15"

        # Timestamp should be present
        assert root.findtext("timestamp") is not None
        assert root.findtext("timestamp").endswith("Z")

    def test_status_xml_passes_validation(self) -> None:
        """If XSD schema is present, output passes strict XSD validation."""
        metrics = {
            "status": "degraded",
            "uptime": 1234,
            "cpu": 0.85,
            "memory": 0.41,
            "disk": 0.15
        }
        xml_bytes = _build_status_xml("CRM", metrics)
        # Should not raise any ValueError
        xml_validator.validate(xml_bytes)


class TestRunStatus:
    """Tests for the asynchronous run_status loop."""

    @pytest.mark.asyncio
    @patch("src.status.asyncio.sleep", new_callable=AsyncMock)
    @patch("src.status._get_system_metrics")
    async def test_publishes_status(self, mock_get_metrics: MagicMock, mock_sleep: AsyncMock, config: Config) -> None:
        """Should connect, get metrics, build XML, and publish to crm.status.checked."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_connection.channel.return_value = mock_channel

        mock_get_metrics.return_value = {
            "status": "healthy",
            "uptime": 3600,
            "cpu": 0.23,
            "memory": 0.41,
            "disk": 0.15
        }

        # Stop the loop after one iteration when publish is called
        mock_channel.default_exchange.publish.side_effect = asyncio.CancelledError()

        try:
            await run_status(mock_connection, config)
        except asyncio.CancelledError:
            pass

        # Assert queue is correct and durable=False
        mock_channel.declare_queue.assert_called_once_with("crm.status.checked", durable=False)
        
        # Assert publish is called using the correct routing key
        mock_channel.default_exchange.publish.assert_called_once()
        args, kwargs = mock_channel.default_exchange.publish.call_args
        assert kwargs["routing_key"] == "crm.status.checked"
        
        # Verify the published message is valid XML and has correct serviceId
        message = args[0]
        root = etree.fromstring(message.body)
        assert root.tag == "StatusCheck"
        assert root.findtext("serviceId") == "CRM"

    @pytest.mark.asyncio
    @patch("src.status.asyncio.sleep", new_callable=AsyncMock)
    @patch("src.status._get_system_metrics")
    async def test_loop_recovery_on_exception(self, mock_get_metrics: MagicMock, mock_sleep: AsyncMock, config: Config) -> None:
        """Ensure exception within iteration does not terminate loop (like in heartbeat)."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_connection.channel.return_value = mock_channel

        # First call fails, second succeeds
        mock_get_metrics.side_effect = [
            Exception("Simulation failure"), 
            {
                "status": "unknown",
                "uptime": 100,
                "cpu": 0.0,
                "memory": 0.0,
                "disk": 0.0
            }
        ]

        # Stop the loop when it successfully publishes for the first time
        mock_channel.default_exchange.publish.side_effect = asyncio.CancelledError()

        try:
            await run_status(mock_connection, config)
        except asyncio.CancelledError:
            pass

        # Since it ran multiple iterations, get_metrics was called twice
        assert mock_get_metrics.call_count == 2
        # Verify channel was opened twice (initial + recovery)
        assert mock_connection.channel.call_count == 2
        mock_channel.default_exchange.publish.assert_called_once()
