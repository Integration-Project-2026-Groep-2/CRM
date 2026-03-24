"""Tests for src.connection."""

import asyncio
from unittest.mock import AsyncMock

import aio_pika
import pytest

from src.connection import STARTUP_MAX_DELAY, _wait_retry_or_shutdown, get_rabbitmq_connection


class TestWaitRetryOrShutdown:
    """Tests for _wait_retry_or_shutdown()."""

    @pytest.mark.asyncio
    async def test_waits_full_delay_without_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns False when delay expires and no shutdown event exists."""
        sleep_mock = AsyncMock()
        monkeypatch.setattr("src.connection.asyncio.sleep", sleep_mock)

        result = await _wait_retry_or_shutdown(1.5, shutdown_event=None)

        assert result is False
        sleep_mock.assert_awaited_once_with(1.5)

    @pytest.mark.asyncio
    async def test_returns_true_when_shutdown_event_set(self) -> None:
        """Returns True when shutdown is requested during wait."""
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        result = await _wait_retry_or_shutdown(5.0, shutdown_event=shutdown_event)

        assert result is True


class TestGetRabbitMqConnection:
    """Tests for get_rabbitmq_connection()."""

    @pytest.mark.asyncio
    async def test_connects_successfully_first_try(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Returns connection immediately when first attempt succeeds."""
        mock_connection = AsyncMock()
        connect_mock = AsyncMock(return_value=mock_connection)
        monkeypatch.setattr("src.connection.aio_pika.connect_robust", connect_mock)

        connection = await get_rabbitmq_connection("amqp://test")

        assert connection is mock_connection
        connect_mock.assert_awaited_once_with("amqp://test")

    @pytest.mark.asyncio
    async def test_retries_with_exponential_backoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retries with 1s then 2s delay before succeeding."""
        mock_connection = AsyncMock()
        connect_mock = AsyncMock(
            side_effect=[Exception("fail 1"), Exception("fail 2"), mock_connection]
        )
        wait_mock = AsyncMock(side_effect=[False, False])
        monkeypatch.setattr("src.connection.aio_pika.connect_robust", connect_mock)
        monkeypatch.setattr("src.connection._wait_retry_or_shutdown", wait_mock)

        connection = await get_rabbitmq_connection("amqp://test")

        assert connection is mock_connection
        assert connect_mock.await_count == 3
        assert wait_mock.await_args_list[0].args == (1.0, None)
        assert wait_mock.await_args_list[1].args == (2.0, None)

    @pytest.mark.asyncio
    async def test_raises_when_shutdown_requested_before_connect(self) -> None:
        """Raises RuntimeError if shutdown is already requested."""
        shutdown_event = asyncio.Event()
        shutdown_event.set()

        with pytest.raises(
            aio_pika.exceptions.AMQPConnectionError, match="Shutdown requested before RabbitMQ connection established."
        ):
            await get_rabbitmq_connection("amqp://test", shutdown_event=shutdown_event)

    @pytest.mark.asyncio
    async def test_raises_when_shutdown_requested_during_backoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Raises RuntimeError when shutdown is requested during retry wait."""
        connect_mock = AsyncMock(side_effect=Exception("fail"))
        wait_mock = AsyncMock(return_value=True)
        shutdown_event = asyncio.Event()
        monkeypatch.setattr("src.connection.aio_pika.connect_robust", connect_mock)
        monkeypatch.setattr("src.connection._wait_retry_or_shutdown", wait_mock)

        with pytest.raises(aio_pika.exceptions.AMQPConnectionError, match="Shutdown requested during RabbitMQ retry backoff."):
            await get_rabbitmq_connection("amqp://test", shutdown_event=shutdown_event)

        connect_mock.assert_awaited_once_with("amqp://test")
        wait_mock.assert_awaited_once_with(1.0, shutdown_event)

    @pytest.mark.asyncio
    async def test_backoff_caps_at_max_delay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After exponential growth, retry wait uses STARTUP_MAX_DELAY (60s)."""
        mock_connection = AsyncMock()
        # Fails with waits: 1, 2, 4, 8, 16, 32, 60 — then two more failures at cap 60 before success.
        fail_count = 9
        connect_mock = AsyncMock(
            side_effect=[*(Exception("fail"),) * fail_count, mock_connection]
        )
        wait_mock = AsyncMock(return_value=False)
        monkeypatch.setattr("src.connection.aio_pika.connect_robust", connect_mock)
        monkeypatch.setattr("src.connection._wait_retry_or_shutdown", wait_mock)

        connection = await get_rabbitmq_connection("amqp://test")

        assert connection is mock_connection
        assert connect_mock.await_count == fail_count + 1
        waits = [call.args[0] for call in wait_mock.await_args_list]
        assert waits == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
        assert all(w <= STARTUP_MAX_DELAY for w in waits)
