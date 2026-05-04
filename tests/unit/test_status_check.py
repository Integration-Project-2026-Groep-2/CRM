"""Tests for src.status_check (Contract 8)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika import ExchangeType
from lxml import etree

from src import xml_validator
from src.config import Config
from src.status_check import (
    _build_status_check_xml,
    _clip_fraction,
    _on_unrouted,
    run_status_check,
)


@pytest.fixture(autouse=True)
def mock_psutil(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide deterministic, cross-platform psutil values for every test.

    autouse=True so we don't have to pass it in to each test; psutil access
    on Windows-dev machines is otherwise non-deterministic and can hit
    a non-existent root path.
    """
    monkeypatch.setattr(
        "src.status_check.psutil.virtual_memory",
        lambda: MagicMock(percent=42.0),
    )
    monkeypatch.setattr(
        "src.status_check.psutil.disk_usage",
        lambda path: MagicMock(percent=71.0),
    )


class TestClipFraction:
    """Tests for _clip_fraction()."""

    def test_normal_value_in_range(self) -> None:
        assert _clip_fraction(42.0) == pytest.approx(0.42)

    def test_clips_above_one(self) -> None:
        """psutil can report >100% on bursty hosts (zswap, disk overcommit)."""
        assert _clip_fraction(105.0) == 1.0

    def test_clips_below_zero(self) -> None:
        assert _clip_fraction(-5.0) == 0.0

    def test_zero(self) -> None:
        assert _clip_fraction(0.0) == 0.0

    def test_one_hundred(self) -> None:
        assert _clip_fraction(100.0) == 1.0


class TestBuildStatusCheckXml:
    """Tests for _build_status_check_xml()."""

    def test_builds_correct_structure(self) -> None:
        """Output has root <StatusCheck> with the 5 required children in order."""
        xml_bytes = _build_status_check_xml("CRM")
        root = etree.fromstring(xml_bytes)

        assert root.tag == "StatusCheck"

        children = [child.tag for child in root]
        assert children == ["serviceId", "timestamp", "uptime", "memory", "disk"]

        assert root.findtext("serviceId") == "CRM"
        assert root.findtext("timestamp").endswith("Z")
        assert int(root.findtext("uptime")) >= 0
        assert root.findtext("memory") == "0.4200"
        assert root.findtext("disk") == "0.7100"

    def test_passes_xsd_validation(self) -> None:
        """The built XML is accepted by the strict XSD."""
        xml_bytes = _build_status_check_xml("CRM")
        xml_validator.validate(xml_bytes)  # must not raise

    def test_clips_above_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """percent > 100 is clipped so the XSD bound (max 1.0) holds."""
        monkeypatch.setattr(
            "src.status_check.psutil.virtual_memory",
            lambda: MagicMock(percent=105.0),
        )
        xml_bytes = _build_status_check_xml("CRM")
        assert etree.fromstring(xml_bytes).findtext("memory") == "1.0000"
        # Still passes XSD with the clipped value
        xml_validator.validate(xml_bytes)


class TestOnUnrouted:
    """Tests for _on_unrouted callback."""

    def test_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unrouted messages emit a WARNING with the routing key."""
        message = MagicMock()
        message.routing_key = "routing.statuscheck"
        message.reply_text = "NO_ROUTE"

        with caplog.at_level("WARNING", logger="src.status_check"):
            _on_unrouted(message)

        assert any(
            "Status check unrouted" in record.message
            and record.levelname == "WARNING"
            for record in caplog.records
        )


class TestRunStatusCheck:
    """Tests for run_status_check()."""

    @pytest.mark.asyncio
    async def test_publishes_with_mandatory_flag(self, config: Config) -> None:
        """run_status_check declares the right exchange, publishes mandatory=True."""
        mock_connection = AsyncMock()
        mock_channel = MagicMock()
        mock_channel.is_closed = False
        # connection.channel is awaited
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        mock_exchange.publish.side_effect = asyncio.CancelledError()

        try:
            await run_status_check(mock_connection, config)
        except asyncio.CancelledError:
            pass

        mock_connection.channel.assert_called_once_with(publisher_confirms=True)
        mock_channel.return_callbacks.add.assert_called_once()
        mock_channel.declare_exchange.assert_called_once_with(
            "statuscheck.direct", type=ExchangeType.DIRECT, durable=True
        )

        mock_exchange.publish.assert_called_once()
        args, kwargs = mock_exchange.publish.call_args
        assert kwargs["routing_key"] == "routing.statuscheck"
        assert kwargs["mandatory"] is True

        root = etree.fromstring(args[0].body)
        assert root.tag == "StatusCheck"
        assert root.findtext("serviceId") == "CRM"

    @pytest.mark.asyncio
    @patch("src.status_check._build_status_check_xml")
    async def test_recovers_from_channel_failure(
        self, mock_build: MagicMock, config: Config
    ) -> None:
        """After a publish exception, channel is recreated on next iteration."""
        mock_connection = AsyncMock()
        mock_channel = MagicMock()
        mock_channel.is_closed = False
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        mock_build.side_effect = [
            b"<StatusCheck/>",  # first iteration: build OK, publish raises
            b"<StatusCheck/>",  # second iteration: build OK, publish raises CancelledError
        ]
        mock_exchange.publish.side_effect = [
            Exception("transient broker failure"),
            asyncio.CancelledError(),
        ]

        with patch("src.status_check.xml_validator.validate"):
            try:
                await run_status_check(mock_connection, config)
            except asyncio.CancelledError:
                pass

        assert mock_connection.channel.call_count == 2
        assert mock_exchange.publish.call_count == 2
