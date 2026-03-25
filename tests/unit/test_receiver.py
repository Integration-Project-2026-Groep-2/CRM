"""Tests for src.receiver."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aio_pika
import pytest
from lxml import etree

from src import receiver
from src.config import Config


class TestRunReceiver:
    """Tests for the run_receiver() setup logic."""

    @pytest.mark.asyncio
    async def test_declares_warning_queue_and_consumes_with_handler(
        self, config: Config, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """run_receiver declares the warning queue and wires handle_warning consumer."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_connection.channel.return_value = mock_channel
        mock_channel.declare_queue.return_value = mock_queue

        # Make the endless Future() immediately cancellable
        async def fake_forever() -> None:
            raise asyncio.CancelledError()

        monkeypatch.setattr("src.receiver.asyncio.Future", lambda: fake_forever())

        # Patch logger to avoid noisy output
        with patch.object(receiver, "logger") as mock_logger:
            try:
                await receiver.run_receiver(mock_connection, config)
            except asyncio.CancelledError:
                # Expected termination from fake_forever
                pass

        mock_connection.channel.assert_awaited_once()
        mock_channel.declare_queue.assert_awaited_once_with(
            "controlroom.warning.issued", durable=False
        )
        # Consumer should be registered with handle_warning callback
        mock_queue.consume.assert_awaited_once()
        args, _ = mock_queue.consume.call_args
        assert args[0] is receiver.handle_warning

        mock_logger.info.assert_called()  # at least one info log


class TestHandleWarning:
    """Tests for the handle_warning() contract handler."""

    @pytest.mark.asyncio
    async def test_validates_xml_and_logs_error(self, sample_warning_xml: bytes) -> None:
        """handle_warning validates XML and logs it as error without crashing."""
        # IncomingMessage is from aio_pika; we emulate minimal API
        message = MagicMock(spec=aio_pika.IncomingMessage)

        class DummyCM:
            async def __aenter__(self):
                return None

            async def __aexit__(self, exc_type, exc, tb):  # type: ignore[override]
                return False

        message.process.return_value = DummyCM()
        message.body = sample_warning_xml

        with patch("src.receiver.xml_validator.validate") as mock_validate, patch.object(
            receiver, "logger"
        ) as mock_logger:
            # xml_validator returns parsed element
            xml_elem = etree.fromstring(sample_warning_xml)
            mock_validate.return_value = xml_elem

            await receiver.handle_warning(message)

            mock_validate.assert_called_once_with(sample_warning_xml)
            # logger.error should be called with the rendered XML string
            mock_logger.error.assert_called_once()
            args, _ = mock_logger.error.call_args
            assert "Controlroom warning received" in args[0]
            # Second argument is the XML string
            xml_string = args[1]
            assert "Warning" in xml_string
            assert "Duplicate heartbeat detected" in xml_string

        # message.process() context manager should have been used
        message.process.assert_called_once()  # type: ignore[call-arg]
