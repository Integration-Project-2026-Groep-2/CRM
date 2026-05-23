import asyncio
import logging

from src.rabbit import publish_heartbeat

logger = logging.getLogger(__name__)


async def run_heartbeat() -> None:
    """Publish a periodic heartbeat event to RabbitMQ.

    Runs indefinitely; intended to be supervised by the task-runner in main.py.
    """
    while True:
        try:
            await publish_heartbeat()
            logger.debug("Heartbeat published successfully.")
        except Exception as exc:  # noqa: BLE001
            logger.error("Heartbeat publish failed: %s", exc)
        await asyncio.sleep(30)
