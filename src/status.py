"""Contract 8 — CRM → Controlroom: status check (CPU, memory, disk)."""

import logging

from aio_pika.abc import AbstractRobustConnection

from src.config import Config

logger = logging.getLogger(__name__)


async def run_status(connection: AbstractRobustConnection, config: Config) -> None:
    """Publish XML status to crm.status.checked every STATUS_CHECK_INTERVAL_SECONDS.

    TODO: Implement status XML construction and publishing.
    """
    raise NotImplementedError("status task not yet implemented")
