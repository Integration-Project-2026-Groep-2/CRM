"""CRM integration service entrypoint.

Runs 5 asyncio tasks concurrently:
- heartbeat: XML heartbeat every second (Contract 7)
- status_check: XML status check every 120 seconds (Contract 8)
- receiver: listens on the configured inbound RabbitMQ queues
- polling: detects out-of-band Salesforce UI edits, publishes
  crm.user.* / crm.company.* contracts so other teams stay in sync
- log_publisher: drains the in-process log queue and publishes LogEvent XML
  to logs.direct so Controlroom can aggregate service logs (Contract LogEvent)

The sender module is a utility library (not a task) — receiver and polling
handlers call sender.publish_*() functions to publish outbound messages.

Alongside these asyncio tasks, the bundled CRM MCP server runs in its own
daemon thread (see src/mcp_thread.py). It cannot be a 6th asyncio task
because FastMCP starts uvicorn with its own event loop.
"""

import asyncio
import logging
import signal
import threading
from collections.abc import Awaitable, Callable

from dotenv import load_dotenv

from src import sender
from src.config import load_config, setup_logging
from src.connection import get_rabbitmq_connection
from src.heartbeat import run_heartbeat
from src.logging_rabbitmq import attach_rabbitmq_log_handler, run_log_publisher
from src.mcp_thread import start_mcp_thread
from src.polling import run_polling
from src.receiver import run_receiver
from src.status import run_status_check

logger = logging.getLogger(__name__)

# Set inside run_receiver once consumer-tags are wired for every queue, cleared
# in _supervised_task when the receiver task ends. The MCP /health endpoint
# (which runs in a daemon thread) reads it so an external monitor sees 503 when
# the asyncio receiver dies silently.
_receiver_alive_event: threading.Event = threading.Event()


async def _supervised_task(
    name: str,
    task_factory: Callable[[], Awaitable[None]],
    *,
    restartable: bool = False,
    max_restarts: int = 10,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> None:
    """Run a task with crash isolation — log errors, never propagate.

    When `restartable=True` the task is retried on failure with exponential
    backoff (1, 2, 4, ..., capped at `max_delay`) up to `max_restarts` times.
    `asyncio.CancelledError` is always re-raised so graceful shutdown still
    works. Use restartable=True only for tasks without their own outer loop —
    heartbeat/status_check already retry internally.
    """
    if not restartable:
        try:
            await task_factory()
        except Exception:
            logger.exception("Task '%s' crashed", name)
        finally:
            if name == "receiver":
                _receiver_alive_event.clear()
        return

    delay = base_delay
    attempt = 0
    while True:
        try:
            await task_factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            if name == "receiver":
                _receiver_alive_event.clear()
            logger.exception(
                "Task '%s' crashed (attempt %d/%d)", name, attempt + 1, max_restarts + 1,
            )
            if attempt >= max_restarts:
                logger.error(
                    "Task '%s' exceeded max_restarts=%d; giving up — operator must restart container",
                    name, max_restarts,
                )
                return
            attempt += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


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

    # Start the bundled MCP server on its own daemon thread BEFORE the RabbitMQ
    # connect-with-retry so that MCP is reachable even if RabbitMQ is briefly
    # unavailable. MCP only needs Salesforce REST (independent of RMQ).
    # Failures inside the MCP thread are logged but never crash main.
    mcp_handle = start_mcp_thread(receiver_alive_event=_receiver_alive_event)

    connection = await get_rabbitmq_connection(config.rabbitmq_url, shutdown_event)
    channel = await connection.channel()
    await sender.init(channel, connection=connection)

    if mcp_handle is not None:
        _, mcp_publisher = mcp_handle
        mcp_publisher.bind(loop, sender)
        logger.info("MCP write-tools enabled (publisher bound to main loop)")

    # Attach the RabbitMQ log handler AFTER the connection + sender are up so
    # connect-time logs flow only through stdout and can never be re-routed to
    # an exchange that may not yet be reachable.
    attach_rabbitmq_log_handler(config.log_service_name, config.log_rabbitmq_level)

    # Polling creates its OWN Salesforce client inside run_polling (same pattern
    # as run_receiver). Doing the SF login here would block heartbeat/status
    # startup while we wait for SF, which defeats the point of having those
    # liveness signals.
    tasks = [
        asyncio.create_task(_supervised_task("heartbeat", lambda: run_heartbeat(connection, config))),
        asyncio.create_task(_supervised_task("status_check", lambda: run_status_check(connection, config))),
        asyncio.create_task(_supervised_task("receiver", lambda: run_receiver(connection, config, shutdown_event, started_event=_receiver_alive_event), restartable=True)),
        asyncio.create_task(_supervised_task("polling", lambda: run_polling(config, shutdown_event), restartable=True)),
        asyncio.create_task(_supervised_task("log_publisher", lambda: run_log_publisher(connection, config.log_service_name))),
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
