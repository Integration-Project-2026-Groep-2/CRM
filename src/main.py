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
from typing import Any

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


async def main() -> None:
    """Start all CRM integration tasks."""
    config = load_config()
    setup_logging(config.log_level)

    logger.info("Starting CRM integration service...")

    connection = await get_rabbitmq_connection(config.rabbitmq_url)
    channel = await connection.channel()
    await sender.init(channel)

    try:
        await asyncio.gather(
            _supervised_task("heartbeat", run_heartbeat(connection, config)),
            _supervised_task("status", run_status(connection, config)),
            _supervised_task("receiver", run_receiver(connection, config)),
        )
    finally:
        await connection.close()
        logger.info("CRM integration service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
