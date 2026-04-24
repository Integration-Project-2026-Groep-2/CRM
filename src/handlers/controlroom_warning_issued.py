"""Handler for Contract 9 — Controlroom → CRM: system warning.

Queue: controlroom.warning.issued | Exchange: planning.topic | durable: false | US-26
"""

import logging

import aio_pika
from lxml import etree

from src import xml_validator

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage) -> None:
    """Contract 9 — Controlroom → CRM: system warning.

    Behaviour:
    - Validates incoming XML against XSD
    - Logs as logger.error() — no crash
    - No Salesforce action required
    - Invalid XML: exception is caught, logged, and the message is rejected
      (not requeued — a structurally invalid message will never become valid)
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("Controlroom warning — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    logger.error(
        "Controlroom warning received: %s",
        etree.tostring(xml, encoding="unicode"),
    )
    await message.ack()
