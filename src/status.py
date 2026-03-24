"""Contract 8 — CRM → Controlroom: status check (CPU, memory, disk)."""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aio_pika
import psutil
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from lxml import etree

from src import xml_validator
from src.config import Config

logger = logging.getLogger(__name__)


def _determine_status(cpu: float, memory: float, disk: float) -> str:
    """Determine status based on system metrics."""
    if cpu >= 0.95 or memory >= 0.95 or disk >= 0.95:
        return "unhealthy"
    if cpu >= 0.8 or memory >= 0.8 or disk >= 0.8:
        return "degraded"
    return "healthy"


def _get_system_metrics(start_time: float) -> dict:
    """Retrieve system metrics and calculate uptime."""
    uptime = int(time.monotonic() - start_time)
    
    try:
        cpu = psutil.cpu_percent(interval=None) / 100.0
        memory = psutil.virtual_memory().percent / 100.0
        disk = psutil.disk_usage('/').percent / 100.0
        status = _determine_status(cpu, memory, disk)
        
        return {
            "cpu": cpu,
            "memory": memory,
            "disk": disk,
            "uptime": uptime,
            "status": status
        }
    except Exception as e:
        logger.error("Failed to retrieve system metrics: %s", e)
        return {
            "cpu": 0.0,
            "memory": 0.0,
            "disk": 0.0,
            "uptime": uptime,
            "status": "unknown"
        }


def _build_status_xml(service_id: str, metrics: dict) -> bytes:
    """Build a StatusCheck XML message."""
    root = etree.Element("StatusCheck")
    etree.SubElement(root, "serviceId").text = service_id
    etree.SubElement(root, "timestamp").text = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    etree.SubElement(root, "status").text = metrics["status"]
    etree.SubElement(root, "uptime").text = str(metrics["uptime"])
    
    system_load = etree.SubElement(root, "systemLoad")
    etree.SubElement(system_load, "cpu").text = f"{metrics['cpu']:.2f}"
    etree.SubElement(system_load, "memory").text = f"{metrics['memory']:.2f}"
    etree.SubElement(system_load, "disk").text = f"{metrics['disk']:.2f}"
    
    return etree.tostring(root, xml_declaration=True, encoding="UTF-8")


async def _get_channel(connection: AbstractRobustConnection) -> AbstractChannel:
    """Open a new channel and declare the status queue."""
    channel = await connection.channel()
    await channel.declare_queue("crm.status.checked", durable=False)
    return channel


async def run_status(connection: AbstractRobustConnection, config: Config) -> None:
    """Publish XML status to crm.status.checked every STATUS_CHECK_INTERVAL_SECONDS."""
    channel: AbstractChannel | None = None
    
    logger.info("Status task started (interval=%ds)", config.status_check_interval_seconds)
    
    start_time = time.monotonic()

    while True:
        try:
            if channel is None or channel.is_closed:
                logger.info("Opening status channel...")
                channel = await _get_channel(connection)

            metrics = _get_system_metrics(start_time)
            xml_bytes = _build_status_xml(config.system_name, metrics)
            xml_validator.validate(xml_bytes)

            await channel.default_exchange.publish(
                aio_pika.Message(body=xml_bytes),
                routing_key="crm.status.checked",
            )
            logger.debug("Status published: %s", metrics["status"])
        except (ValueError, etree.XMLSyntaxError):
            logger.exception("Status XML validation failed")
        except Exception:
            logger.exception("Status iteration failed")
            channel = None

        await asyncio.sleep(config.status_check_interval_seconds)
