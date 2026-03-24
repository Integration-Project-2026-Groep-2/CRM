import asyncio
import signal
from unittest.mock import MagicMock, call , AsyncMock, patch
from src.main import _install_signal_handlers
import pytest

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

async def test_main_stops_on_shutdown_signal():
    shutdown_event = asyncio.Event()

    async def fake_gather(*args, **kwargs):
        shutdown_event.set()  # Simuleer dat shutdown event wordt gezet
    
    with patch("src.main.load_config"), \
         patch("src.main.setup_logging"), \
         patch("src.main.connect_with_retry", new_callable=AsyncMock), \
         patch("src.main.sender.init", new_callable=AsyncMock), \
         patch("src.main.asyncio.gather", side_effect=fake_gather), \
         patch("src.main._install_signal_handlers"):
        from src.main import main
        await main()