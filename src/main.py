"""CRM integration service entrypoint.

Runs 3 asyncio tasks concurrently:
- heartbeat: XML heartbeat every second (Contract 7)
- status: CPU/mem/disk status to Controlroom (Contract 8)
- receiver: listens on 11 RabbitMQ queues

The sender module is a utility library (not a task) — receiver handlers
call sender.publish_*() functions to publish outbound messages.
"""

import asyncio
import logging
from collections.abc import Coroutine
import signal
from typing import Any

from dotenv import load_dotenv

from src import sender
from src.config import load_config, setup_logging
from src.connection import get_rabbitmq_connection
from src.heartbeat import run_heartbeat
from src.receiver import run_receiver
from src.status import run_status

logger = logging.getLogger(__name__)


async def _supervised_task(name: str, coro: Coroutine[Any, Any, None]) -> None:
    """Run a task with crash isolation — log errors, never propagate."""
    try:
        await coro
    except Exception:
        logger.exception("Task '%s' crashed", name)

STARTUP_DELAY = 2.0 # seconnds before retrying RabbitMQ connection on failure
STARTUP_MAX_ATTEMPTS = 10
STARTUP_MAX_DELAY = 60.0 # seconds

async def connect_with_retry(rabbitmq_url: str):
    delay = STARTUP_DELAY
    for attempt in range(1, STARTUP_MAX_ATTEMPTS + 1):  # Try up to 5 times
        try:
            connection = await get_rabbitmq_connection(rabbitmq_url)
            logger.info("Successfully connected to RabbitMQ on attempt %d", attempt)
            return connection
        except Exception as e:
            if attempt == STARTUP_MAX_ATTEMPTS:
                logger.critical("RabbitMQ connection failed after %d attempts: %s", attempt, e)
                raise
            logger.warning("RabbitMQ connection attempt %d failed: %s. Retrying in %.1f seconds...", attempt, e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, STARTUP_MAX_DELAY)

def _install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event) -> None:
    def handle_signal(sig: signal.Signals) -> None:
        logger.info("Signal %s received, shutting down...", sig.name)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_signal, sig)
        except (NotImplementedError, RuntimeError):
            logger.warning("Could not install signal handler for %s", sig.name)

async def main() -> None:
    """Start all CRM integration tasks."""
    load_dotenv()
    config = load_config()
    setup_logging(config.log_level)

    logger.info("Starting CRM integration service...")

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    _install_signal_handlers(loop, shutdown_event)

    connection = await connect_with_retry(config.rabbitmq_url)
    channel = await connection.channel()
    await sender.init(channel)

    try:
        await asyncio.gather(
            _supervised_task("heartbeat", run_heartbeat(connection, config)),
            _supervised_task("status", run_status(connection, config)),
            _supervised_task("receiver", run_receiver(connection, config)),
            shutdown_event.wait(),  # Wait until shutdown signal is received
        )
    finally:
        await connection.close()
        logger.info("CRM integration service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
