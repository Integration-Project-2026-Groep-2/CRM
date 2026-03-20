"""RabbitMQ queue consumer — listens on 11 queues from other teams."""

import logging

from aio_pika.abc import AbstractRobustConnection

from src.config import Config

logger = logging.getLogger(__name__)


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume messages from all inbound queues, validate XML, process in Salesforce.

    TODO: Implement queue bindings and message handlers per contract.
    """
    raise NotImplementedError("receiver task not yet implemented")
