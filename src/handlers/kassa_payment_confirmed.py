"""Handler for Contract 16 — Kassa → CRM: payment confirmed.

Queue: kassa.payment.confirmed | Exchange: payment.topic | durable: true | US-08, US-21
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import xml_validator
from src.handlers._transport import _handle_processing_error
from src.salesforce_client import update_payment_status

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 16 — Kassa -> CRM: payment confirmed.

    Behaviour:
    - Validate XML against schema.
    - Update the canonical session registration payment state in Salesforce.
    - Sync Contact.Paid_At__c as a compatibility field.
    - Invalid XML: rejected without requeue.
    - Unknown/ambiguous Contact or registrationId mismatch: ack without retry.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PaymentConfirmed — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        user_id = xml.findtext("userId")
        email = xml.findtext("email") or ""
        registration_id = xml.findtext("registrationId")
        paid_at = xml.findtext("paidAt") or ""

        contact = await update_payment_status(
            sf,
            user_id=user_id,
            email=email,
            registration_id=registration_id,
            paid_at=paid_at,
        )
        if contact is None:
            await message.ack()
            return

        logger.info("Processed payment confirmation for %s", contact.get("Email", email))
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PaymentConfirmed", message, exc)
