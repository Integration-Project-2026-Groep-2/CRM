import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import Config
from src.main import _install_signal_handlers, main


def test_sigterm_sets_shutdown_event():
    loop = MagicMock()
    shutdown_event = asyncio.Event()
    _install_signal_handlers(loop, shutdown_event)

    for added_signal, handler, *_ in (call.args for call in loop.add_signal_handler.call_args_list):
        if added_signal == signal.SIGTERM:
            handler(signal.SIGTERM)
            break
    else:
        raise AssertionError("SIGTERM handler was not registered")

    assert shutdown_event.is_set()


def test_sigint_sets_shutdown_event():
    loop = MagicMock()
    shutdown_event = asyncio.Event()
    _install_signal_handlers(loop, shutdown_event)

    for added_signal, handler, *_ in (call.args for call in loop.add_signal_handler.call_args_list):
        if added_signal == signal.SIGINT:
            handler(signal.SIGINT)
            break
    else:
        raise AssertionError("SIGINT handler was not registered")

    assert shutdown_event.is_set()


def test_signal_handler_survives_not_implemented():
    loop = MagicMock()
    loop.add_signal_handler.side_effect = NotImplementedError
    shutdown_event = asyncio.Event()

    # Should not raise.
    _install_signal_handlers(loop, shutdown_event)


@pytest.mark.asyncio
async def test_main_stops_on_shutdown_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = Config(
        rabbitmq_url="amqp://test",
        salesforce_username="test",
        salesforce_password="test",
        salesforce_security_token="test",
        salesforce_domain="login",
        heartbeat_interval_seconds=1,
        system_name="CRM",
        polling_interval_seconds=60,
        polling_state_path="/tmp/polling_checkpoint_test.json",
        polling_integration_user_id=None,
        log_level="INFO",
    )
    never = asyncio.Event()

    async def fake_run_heartbeat(*_args: object, **_kwargs: object) -> None:
        await never.wait()

    async def fake_run_receiver(*_args: object, **_kwargs: object) -> None:
        await never.wait()

    async def fake_run_polling(*_args: object, **_kwargs: object) -> None:
        await never.wait()

    def set_shutdown_immediately(
        _loop: asyncio.AbstractEventLoop,
        shutdown_event: asyncio.Event,
    ) -> None:
        shutdown_event.set()

    mock_connection = AsyncMock()
    mock_connection.channel.return_value = AsyncMock()
    mock_connection.close = AsyncMock()

    monkeypatch.setattr("src.main.load_dotenv", lambda: None)
    monkeypatch.setattr("src.main.load_config", lambda: cfg)
    monkeypatch.setattr("src.main.setup_logging", lambda _level: None)
    monkeypatch.setattr("src.main._install_signal_handlers", set_shutdown_immediately)
    monkeypatch.setattr(
        "src.main.get_rabbitmq_connection",
        AsyncMock(return_value=mock_connection),
    )
    monkeypatch.setattr("src.main.sender.init", AsyncMock())
    monkeypatch.setattr("src.main.run_heartbeat", fake_run_heartbeat)
    monkeypatch.setattr("src.main.run_receiver", fake_run_receiver)
    monkeypatch.setattr("src.main.run_polling", fake_run_polling)

    await main()

    mock_connection.close.assert_awaited_once()
