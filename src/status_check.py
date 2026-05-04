"""Contract 8 — CRM → Controlroom: periodic status check.

Publishes a `<StatusCheck>` XML message every STATUS_CHECK_INTERVAL_SECONDS
(default 120s) on the `statuscheck.direct` exchange with routing key
`routing.statuscheck`. Controlroom binds `controlroom.statuscheck.queue`
(durable, with DLQ `controlroom.statuscheck.queue.dlq`) to receive these.

Design notes:
- Uses `mandatory=True` on publish + an on-return callback so unrouted
  messages surface as WARN logs instead of vanishing silently. The LogEvent
  rollout (2026-05-04) lost 25/26 messages because of a labels mix-up in the
  ClickUp spec — this guard makes that failure mode loud.
- `_PROCESS_START_MONOTONIC` is captured at module import: uptime resets on
  container restart (correct behaviour) but not on hot-reload during tests.
- `_DISK_PATH` defaults to '/' on Linux (the production target) and 'C:\\'
  on Windows so local dev runs do not crash. Override via STATUS_CHECK_DISK_PATH.
"""

import asyncio
import logging
import os
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
_DISK_PATH = os.getenv("STATUS_CHECK_DISK_PATH") or ("C:\\" if os.name == "nt" else "/")


def _clip_fraction(percent: float) -> float:
    """Clip a 0-100 percentage into a 0.0-1.0 fraction respecting XSD bounds."""
    return max(0.0, min(1.0, percent / 100.0))


def _build_status_check_xml(service_id: str) -> bytes:
    """Build a StatusCheck XML message.

    Field order follows the XSD xs:sequence: serviceId, timestamp, uptime,
    memory, disk. memory and disk are emitted with 4 decimals so the XSD
    decimal bound is unambiguous.
    """
    root = etree.Element("StatusCheck")
    etree.SubElement(root, "serviceId").text = service_id
    etree.SubElement(root, "timestamp").text = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    etree.SubElement(root, "uptime").text = str(
        int(time.monotonic() - _PROCESS_START_MONOTONIC)
    )
    etree.SubElement(root, "memory").text = (
        f"{_clip_fraction(psutil.virtual_memory().percent):.4f}"
    )
    etree.SubElement(root, "disk").text = (
        f"{_clip_fraction(psutil.disk_usage(_DISK_PATH).percent):.4f}"
    )
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


def _on_unrouted(message: AbstractIncomingMessage) -> None:
    """Log unrouted status checks so silent drops are visible."""
    logger.warning(
        "Status check unrouted by broker: rk=%s reply=%s",
        message.routing_key,
        getattr(message, "reply_text", None),
    )


async def _get_channel(
    connection: AbstractRobustConnection,
) -> tuple[AbstractChannel, AbstractExchange]:
    """Open a new channel, register the unrouted-return callback, and declare
    the statuscheck.direct exchange.

    Separated so it can be called again after a channel failure.
    """
    channel = await connection.channel(publisher_confirms=True)
    channel.return_callbacks.add(_on_unrouted)
    exchange = await channel.declare_exchange(
        "statuscheck.direct", type=ExchangeType.DIRECT, durable=True
    )
    return channel, exchange


async def run_status_check(
    connection: AbstractRobustConnection, config: Config
) -> None:
    """Publish XML status check via statuscheck.direct every STATUS_CHECK_INTERVAL_SECONDS.

    - Declares exchange statuscheck.direct (direct, durable=True)
    - Publishes with mandatory=True so unrouted messages trigger _on_unrouted
    - Recreates channel on transport failure; validation errors do not
    - Per-iteration try/except: status check mag NOOIT stoppen
    """
    channel: AbstractChannel | None = None
    exchange: AbstractExchange | None = None

    logger.info(
        "Status check task started (interval=%ds)",
        config.status_check_interval_seconds,
    )

    while True:
        try:
            if channel is None or channel.is_closed:
                logger.info("Opening status check channel...")
                channel, exchange = await _get_channel(connection)

            xml_bytes = _build_status_check_xml(config.system_name)
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
