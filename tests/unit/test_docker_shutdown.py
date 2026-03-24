import asyncio
import signal
from unittest.mock import MagicMock, call
from src.main import _install_signal_handlers


def test_sigterm_sets_shutdown_event():
    loop = MagicMock()
    shutdown_event = asyncio.Event()
    _install_signal_handlers(loop, shutdown_event)

    # Haal de handler op die meegegeven werd aan add_signal_handler voor SIGTERM
    handler = loop.add_signal_handler.call_args_list[0][0][1]
    handler(signal.SIGTERM)

    assert shutdown_event.is_set()


def test_sigint_sets_shutdown_event():
    loop = MagicMock()
    shutdown_event = asyncio.Event()
    _install_signal_handlers(loop, shutdown_event)

    # Haal de handler op die meegegeven werd aan add_signal_handler voor SIGINT
    handler = loop.add_signal_handler.call_args_list[1][0][1]
    handler(signal.SIGINT)

    assert shutdown_event.is_set()


def test_signal_handler_survives_not_implemented():
    loop = MagicMock()
    loop.add_signal_handler.side_effect = NotImplementedError
    shutdown_event = asyncio.Event()

    # mag niet crashen
    _install_signal_handlers(loop, shutdown_event)