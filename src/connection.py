"""RabbitMQ connection management using aio-pika."""

import logging
import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection

logger = logging.getLogger(__name__)

STARTUP_DELAY: float = 1.0 # seconds before retrying RabbitMQ connection on failure
STARTUP_MAX_DELAY: float = 60.0 # seconds

async def get_rabbitmq_connection(rabbitmq_url: str, shutdown_event: asyncio.Event = None) -> AbstractRobustConnection:
    """Create a robust RabbitMQ connection that auto-reconnects.

    Args:
        rabbitmq_url: AMQP connection string.

    Returns:
        A robust connection instance.

    Raises:
        aio_pika.exceptions.AMQPConnectionError: If connection fails.
    """
    delay: float = STARTUP_DELAY

    while True:
        # Stop retrying if shutdown requested
        if shutdown_event and shutdown_event.is_set():
            raise RuntimeError("Shutdown requested before RabbitMQ connection established.")
        
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = await aio_pika.connect_robust(rabbitmq_url)
            logger.info("Connected to RabbitMQ.")
            return connection
        except Exception as e:
            logger.warning("Failed to connect to RabbitMQ: %s. Retrying in %f seconds.", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, STARTUP_MAX_DELAY)  # Exponential backoff: 1 → 2 → 4 → ... → 60

