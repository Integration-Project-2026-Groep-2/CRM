"""RabbitMQ publisher — sends events from Salesforce to other teams."""

import logging

from aio_pika.abc import AbstractRobustConnection

from src.config import Config

logger = logging.getLogger(__name__)


async def run_sender(connection: AbstractRobustConnection, config: Config) -> None:
    """Publish CRM events to RabbitMQ queues/exchanges.

    TODO: Implement event publishing per contract.
    """
    raise NotImplementedError("sender task not yet implemented")
