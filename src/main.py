"""CRM integration service entrypoint.

Runs 3 asyncio tasks concurrently:
- heartbeat: XML heartbeat every second (Contract 7)
- receiver: listens on the configured inbound RabbitMQ queues
- polling: detects out-of-band Salesforce UI edits, publishes
  crm.user.* / crm.company.* contracts so other teams stay in sync

The sender module is a utility library (not a task) — receiver and polling
handlers call sender.publish_*() functions to publish outbound messages.
"""

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

from dotenv import load_dotenv

from src import sender
from src.config import load_config, setup_logging
from src.connection import get_rabbitmq_connection
from src.heartbeat import run_heartbeat
from src.polling import run_polling
from src.receiver import run_receiver

logger = logging.getLogger(__name__)


async def _supervised_task(
    name: str,
    task_factory: Callable[[], Awaitable[None]],
) -> None:
    """Run a task with crash isolation — log errors, never propagate."""
    try:
        await task_factory()
    except Exception:
        logger.exception("Task '%s' crashed", name)


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

    connection = await get_rabbitmq_connection(config.rabbitmq_url, shutdown_event)
    channel = await connection.channel()
    await sender.init(channel)

    # Polling creates its OWN Salesforce client inside run_polling (same pattern
    # as run_receiver). Doing the SF login here would block heartbeat/status
    # startup while we wait for SF, which defeats the point of having those
    # liveness signals.
    tasks = [
        asyncio.create_task(_supervised_task("heartbeat", lambda: run_heartbeat(connection, config))),
        asyncio.create_task(_supervised_task("receiver", lambda: run_receiver(connection, config, shutdown_event))),
        asyncio.create_task(_supervised_task("polling", lambda: run_polling(config, shutdown_event))),
    ]
    try:
        await shutdown_event.wait()  # Wait until shutdown signal is received
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await connection.close()
        logger.info("CRM integration service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
