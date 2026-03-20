"""CRM integration service entrypoint.

Runs 4 asyncio tasks concurrently:
- heartbeat: XML heartbeat every second (Contract 7)
- status: CPU/mem/disk status to Controlroom (Contract 8)
- receiver: listens on 11 RabbitMQ queues
- sender: publishes events from Salesforce to RabbitMQ
"""

import asyncio
import logging

from src.config import load_config, setup_logging
from src.connection import get_rabbitmq_connection
from src.heartbeat import run_heartbeat
from src.receiver import run_receiver
from src.sender import run_sender
from src.status import run_status

logger = logging.getLogger(__name__)


async def main() -> None:
    """Start all CRM integration tasks."""
    config = load_config()
    setup_logging(config.log_level)

    logger.info("Starting CRM integration service...")

    connection = await get_rabbitmq_connection(config.rabbitmq_url)

    try:
        await asyncio.gather(
            run_heartbeat(connection, config),
            run_status(connection, config),
            run_receiver(connection, config),
            run_sender(connection, config),
        )
    finally:
        await connection.close()
        logger.info("CRM integration service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
