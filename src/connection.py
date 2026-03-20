"""RabbitMQ connection management using aio-pika."""

import logging

import aio_pika
from aio_pika.abc import AbstractRobustConnection

logger = logging.getLogger(__name__)


async def get_rabbitmq_connection(rabbitmq_url: str) -> AbstractRobustConnection:
    """Create a robust RabbitMQ connection that auto-reconnects.

    Args:
        rabbitmq_url: AMQP connection string.

    Returns:
        A robust connection instance.

    Raises:
        aio_pika.exceptions.AMQPConnectionError: If connection fails.
    """
    logger.info("Connecting to RabbitMQ...")
    connection = await aio_pika.connect_robust(rabbitmq_url)
    logger.info("Connected to RabbitMQ.")
    return connection
