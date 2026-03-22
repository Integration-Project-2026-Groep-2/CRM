"""Contract 7 — CRM → Controlroom: heartbeat elke 1 seconde."""

import asyncio
import logging
from datetime import datetime, timezone

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from lxml import etree

from src import xml_validator
from src.config import Config

logger = logging.getLogger(__name__)


def _build_heartbeat_xml(service_id: str) -> bytes:
    """Build a Heartbeat XML message.

    Uses lxml Element/SubElement — no string formatting.
    """
    root = etree.Element("Heartbeat")
    etree.SubElement(root, "serviceId").text = service_id
    etree.SubElement(root, "timestamp").text = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


async def _get_channel(connection: AbstractRobustConnection) -> AbstractChannel:
    """Open a new channel and declare the heartbeat queue.

    Separated so it can be called again after a channel failure.
    """
    channel = await connection.channel()
    await channel.declare_queue("crm.heartbeat", durable=False)
    return channel


async def run_heartbeat(connection: AbstractRobustConnection, config: Config) -> None:
    """Publish XML heartbeat to crm.heartbeat every HEARTBEAT_INTERVAL_SECONDS.

    - Declares queue crm.heartbeat (durable=False)
    - Automatically recreates channel on failure
    - Per-iteration try/except: logs error, skips iteration, loop continues
    - Heartbeat mag NOOIT stoppen
    """
    channel: AbstractChannel | None = None

    logger.info("Heartbeat task started (interval=%ds)", config.heartbeat_interval_seconds)

    while True:
        try:
            # (Re)create channel if needed — handles first start + recovery
            if channel is None or channel.is_closed:
                logger.info("Opening heartbeat channel...")
                channel = await _get_channel(connection)

            xml_bytes = _build_heartbeat_xml("CRM")
            xml_validator.validate(xml_bytes)

            await channel.default_exchange.publish(
                aio_pika.Message(body=xml_bytes),
                routing_key="crm.heartbeat",
            )
            logger.debug("Heartbeat published")
        except Exception:
            logger.exception("Heartbeat iteration failed")
            # Force channel re-creation on next iteration
            channel = None

        await asyncio.sleep(config.heartbeat_interval_seconds)
