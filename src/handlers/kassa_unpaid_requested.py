"""Handler for Contract 17a — Kassa → CRM: unpaid persons request.

Queue: kassa.unpaid.requested | Exchange: payment.topic | durable: true | US-07
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.salesforce_client import get_unpaid_contacts

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 17a — Kassa -> CRM: unpaid persons request.

    Behaviour:
    - Validate XML against schema.
    - Query Salesforce for unpaid Contacts.
    - Publish crm.unpaid.responded with the same requestId.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("UnpaidRequest — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    request_id = xml.findtext("requestId") or ""
    persons = await get_unpaid_contacts(sf)
    await sender.publish_unpaid_responded(request_id, persons)
    logger.info("Processed unpaid request %s with %d unpaid contacts", request_id, len(persons))
    await message.ack()
