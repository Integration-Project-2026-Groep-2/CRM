"""Contract 7 — CRM → Controlroom: heartbeat elke 1 seconde."""

import logging

from aio_pika.abc import AbstractRobustConnection

from src.config import Config

logger = logging.getLogger(__name__)


async def run_heartbeat(connection: AbstractRobustConnection, config: Config) -> None:
    """Publish XML heartbeat to crm.heartbeat every HEARTBEAT_INTERVAL_SECONDS.

    TODO: Implement heartbeat XML construction and publishing.
    """
    raise NotImplementedError("heartbeat task not yet implemented")
