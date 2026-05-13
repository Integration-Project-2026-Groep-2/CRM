"""Tests for src.main."""

import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

from src.config import Config
from src.main import main


@pytest.mark.asyncio
async def test_main_starts_five_tasks_concurrently(monkeypatch: pytest.MonkeyPatch) -> None:
    """main() starts heartbeat, status_check, receiver, polling, and log_publisher tasks concurrently."""
    heartbeat_started = asyncio.Event()
    status_check_started = asyncio.Event()
    receiver_started = asyncio.Event()
    polling_started = asyncio.Event()
    log_publisher_started = asyncio.Event()
    never = asyncio.Event()

    async def fake_run_heartbeat(*_args: object, **_kwargs: object) -> None:
        heartbeat_started.set()
        await never.wait()

    async def fake_run_status_check(*_args: object, **_kwargs: object) -> None:
        status_check_started.set()
        await never.wait()

    async def fake_run_receiver(*_args: object, **_kwargs: object) -> None:
        receiver_started.set()
        await never.wait()

    async def fake_run_polling(*_args: object, **_kwargs: object) -> None:
        polling_started.set()
        await never.wait()

    async def fake_run_log_publisher(*_args: object, **_kwargs: object) -> None:
        log_publisher_started.set()
        await never.wait()

    mock_connection = AsyncMock()
    mock_connection.channel.return_value = AsyncMock()
    mock_connection.close = AsyncMock()

    mock_sender_init = AsyncMock()

    cfg = Config(
        rabbitmq_url="amqp://test",
        salesforce_username="test",
        salesforce_password="test",
        salesforce_security_token="test",
        salesforce_domain="login",
        heartbeat_interval_seconds=1,
        status_check_interval_seconds=120,
        system_name="CRM",
        polling_interval_seconds=60,
        polling_state_path="/tmp/polling_checkpoint_test.json",
        polling_integration_user_id=None,
        log_level="INFO",
        log_service_name="crm",
        log_rabbitmq_level="INFO",
    )

    monkeypatch.setattr("src.main.load_dotenv", lambda: None)
    monkeypatch.setattr("src.main.setup_logging", lambda _level: None)
    monkeypatch.setattr("src.main.load_config", lambda: cfg)
    monkeypatch.setattr(
        "src.main._install_signal_handlers",
        lambda _loop, _shutdown_event: None,
    )
    monkeypatch.setattr("src.main.get_rabbitmq_connection", AsyncMock(return_value=mock_connection))
    monkeypatch.setattr("src.main.sender.init", mock_sender_init)
    monkeypatch.setattr("src.main.run_heartbeat", fake_run_heartbeat)
    monkeypatch.setattr("src.main.run_status_check", fake_run_status_check)
    monkeypatch.setattr("src.main.run_receiver", fake_run_receiver)
    monkeypatch.setattr("src.main.run_polling", fake_run_polling)
    monkeypatch.setattr("src.main.attach_rabbitmq_log_handler", lambda *_a, **_kw: None)
    monkeypatch.setattr("src.main.run_log_publisher", fake_run_log_publisher)

    main_task = asyncio.create_task(main())
    try:
        await asyncio.wait_for(heartbeat_started.wait(), timeout=1.0)
        await asyncio.wait_for(status_check_started.wait(), timeout=1.0)
        await asyncio.wait_for(receiver_started.wait(), timeout=1.0)
        await asyncio.wait_for(polling_started.wait(), timeout=1.0)
        await asyncio.wait_for(log_publisher_started.wait(), timeout=1.0)

        assert heartbeat_started.is_set()
        assert status_check_started.is_set()
        assert receiver_started.is_set()
        assert polling_started.is_set()
        assert log_publisher_started.is_set()
        assert not main_task.done()
    finally:
        main_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await main_task


@pytest.mark.asyncio
async def test_supervised_task_clears_receiver_alive_on_crash() -> None:
    """When the receiver factory raises, _supervised_task must clear the event."""
    from src.main import _receiver_alive_event, _supervised_task

    _receiver_alive_event.set()

    async def crashing_factory() -> None:
        raise RuntimeError("boom")

    await _supervised_task("receiver", crashing_factory)

    assert not _receiver_alive_event.is_set()


@pytest.mark.asyncio
async def test_supervised_task_does_not_touch_event_for_non_receiver() -> None:
    from src.main import _receiver_alive_event, _supervised_task

    _receiver_alive_event.set()

    async def crashing_factory() -> None:
        raise RuntimeError("boom")

    await _supervised_task("heartbeat", crashing_factory)

    # Heartbeat crashes must not flip receiver-aliveness.
    assert _receiver_alive_event.is_set()
    _receiver_alive_event.clear()
