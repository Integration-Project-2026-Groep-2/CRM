"""Tests for src.status (Contract 8)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika import ExchangeType
from lxml import etree

from src import xml_validator
from src.config import Config
from src.status import _build_status_xml, _clip_fraction, run_status_check


@pytest.fixture(autouse=True)
def mock_psutil(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide deterministic psutil values."""
    monkeypatch.setattr("src.status.psutil.virtual_memory", lambda: MagicMock(percent=50.0))
    monkeypatch.setattr("src.status.psutil.disk_usage", lambda path: MagicMock(percent=60.0))


class TestClipFraction:
    """Tests for _clip_fraction()."""

    def test_clips(self) -> None:
        assert _clip_fraction(42.0) == pytest.approx(0.42)
        assert _clip_fraction(105.0) == 1.0
        assert _clip_fraction(-5.0) == 0.0


class TestBuildStatusXml:
    """Tests for _build_status_xml()."""

    def test_builds_correct_structure(self) -> None:
        xml_bytes = _build_status_xml("CRM")
        root = etree.fromstring(xml_bytes)

        assert root.tag == "StatusCheck"
        assert root.findtext("serviceId") == "CRM"
        assert int(root.findtext("uptime")) >= 0
        assert root.findtext("memory") == "0.5000"
        assert root.findtext("disk") == "0.6000"

    def test_passes_xsd_validation(self) -> None:
        xml_bytes = _build_status_xml("CRM")
        xml_validator.validate(xml_bytes)


class TestRunStatusCheck:
    """Tests for run_status_check()."""

    @pytest.mark.asyncio
    async def test_publishes_to_exchange(self, config: Config) -> None:
        mock_connection = AsyncMock()
        mock_channel = MagicMock()
        mock_channel.is_closed = False
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        with patch("src.status.asyncio.sleep", side_effect=asyncio.CancelledError):
            try:
                await run_status_check(mock_connection, config)
            except asyncio.CancelledError:
                pass

        mock_channel.declare_exchange.assert_called_once_with(
            "statuscheck.direct", type=ExchangeType.DIRECT, durable=True
        )
        mock_exchange.publish.assert_called_once()
        args, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "routing.statuscheck"

        root = etree.fromstring(args[0].body)
        assert root.tag == "StatusCheck"
