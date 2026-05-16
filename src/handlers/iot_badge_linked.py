"""Handler for Contract 12 - IoT -> CRM: badge linked.

Queue: iot.badge.linked | Exchange: planning.topic | durable: true | US-28, US-29
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import xml_validator
from src.salesforce_client import update_contact_badge_code_by_email

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 12 - IoT -> CRM: persist a linked badge on Contact."""
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("BadgeLinked - invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    badge_code = xml.findtext("badgeId") or ""
    email = xml.findtext("contactEmail") or ""

    contact = await update_contact_badge_code_by_email(
        sf,
        email=email,
        badge_code=badge_code,
    )
    if contact is None:
        await message.ack()
        return

    logger.info(
        "Processed badge link for Contact %s (%s)",
        contact.get("Id"),
        contact.get("Email", email),
    )
    await message.ack()
