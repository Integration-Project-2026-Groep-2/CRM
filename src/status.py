"""Contract 8 — CRM → Controlroom: periodieke statuscheck.

Publiceert periodiek systeemmetrieken naar statuscheck.direct exchange.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aio_pika
import psutil
from aio_pika import ExchangeType
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)
from lxml import etree

from src import xml_validator
from src.config import Config

logger = logging.getLogger(__name__)

_PROCESS_START_MONOTONIC = time.monotonic()


def _clip_fraction(percent: float) -> float:
    """Clip a 0-100 percentage into a 0.0-1.0 fraction."""
    return max(0.0, min(1.0, percent / 100.0))


def _build_status_xml(service_id: str) -> bytes:
    """Build a StatusCheck XML message."""
    root = etree.Element("StatusCheck")
    etree.SubElement(root, "serviceId").text = service_id
    etree.SubElement(root, "timestamp").text = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    etree.SubElement(root, "uptime").text = str(
        int(time.monotonic() - _PROCESS_START_MONOTONIC)
    )

    # CPU, Memory and Disk as decimals (fractions 0.0-1.0)
    cpu = _clip_fraction(psutil.cpu_percent(interval=0.1))
    memory = _clip_fraction(psutil.virtual_memory().percent)
    disk = _clip_fraction(psutil.disk_usage("/").percent)

    etree.SubElement(root, "cpu").text = f"{cpu:.4f}"
    etree.SubElement(root, "memory").text = f"{memory:.4f}"
    etree.SubElement(root, "disk").text = f"{disk:.4f}"

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def _on_unrouted(message: AbstractIncomingMessage) -> None:
    """Log unrouted status checks so silent drops are visible.

    Guards against the LogEvent rollout 2026-05-04 failure mode where 25/26
    messages were silently lost because of a routing-key mix-up. Without this
    callback + mandatory=True, a renamed/unbound consumer queue produces zero
    log signal on the publisher side.
    """
    logger.warning(
        "Status check unrouted by broker: rk=%s reply=%s",
        message.routing_key,
        getattr(message, "reply_text", None),
    )


async def _get_channel(
    connection: AbstractRobustConnection,
) -> tuple[AbstractChannel, AbstractExchange]:
    """Open a new channel and declare the statuscheck exchange."""
    channel = await connection.channel()
    channel.return_callbacks.add(_on_unrouted)
    exchange = await channel.declare_exchange(
        "statuscheck.direct", type=ExchangeType.DIRECT, durable=True
    )
    return channel, exchange


async def run_status_check(
    connection: AbstractRobustConnection, config: Config
) -> None:
    """Publish XML status check via statuscheck.direct every interval.

    - Exchange: statuscheck.direct (DIRECT, durable=True)
    - Routing key: routing.statuscheck
    - Interval: config.status_check_interval_seconds (standaard 120)
    """
    channel: AbstractChannel | None = None
    exchange: AbstractExchange | None = None

    logger.info(
        "Status check task started (interval=%ds)", config.status_check_interval_seconds
    )

    while True:
        try:
            if channel is None or channel.is_closed:
                logger.info("Opening status check channel...")
                channel, exchange = await _get_channel(connection)

            xml_bytes = _build_status_xml(config.system_name)
            xml_validator.validate(xml_bytes)

            await exchange.publish(
                aio_pika.Message(body=xml_bytes),
                routing_key="routing.statuscheck",
                mandatory=True,
            )
            logger.debug("Status check published")
        except (ValueError, etree.XMLSyntaxError):
            logger.exception("Status check XML validation failed")
        except Exception:
            logger.exception("Status check iteration failed")
            channel = None
            exchange = None

        await asyncio.sleep(config.status_check_interval_seconds)
