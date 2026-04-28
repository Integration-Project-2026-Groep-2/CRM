"""Handler for Contract 11 — Planning → CRM: session update notification.

Queue: planning.session.updated | Exchange: planning.topic | durable: true | US-23, US-16, US-53
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import _build_mail_display_name, _normalize_optional_text
from src.salesforce_client import (
    get_active_session_participants,
    has_session_registration_object,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 11 — Planning -> CRM: notify all active participants of a session change."""
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("SessionUpdate — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    if not await has_session_registration_object(sf):
        logger.error(
            "SessionUpdate rejected — Salesforce object Session_Registration__c is missing",
        )
        await message.reject(requeue=False)
        return

    session_id = xml.findtext("sessionId") or ""
    session_name = xml.findtext("sessionName") or ""
    change_type = xml.findtext("changeType") or ""
    new_time = _normalize_optional_text(xml.findtext("newTime"))
    new_location = _normalize_optional_text(xml.findtext("newLocation"))

    participants = await get_active_session_participants(sf, session_id)
    if not participants:
        logger.info(
            "SessionUpdate for sessionId=%s changeType=%s has no active participants",
            session_id,
            change_type,
        )
        await message.ack()
        return

    published_count = 0
    for participant in participants:
        email = _normalize_optional_text(participant.get("Email"))
        if email is None:
            logger.warning(
                "Skipping session update notification for sessionId=%s because participant %s has no email",
                session_id,
                participant.get("Id"),
            )
            continue

        display_name = _build_mail_display_name(
            participant.get("FirstName"),
            participant.get("LastName"),
            email,
        )
        recipient = {"email": email, "name": display_name}
        dynamic_data = {
            "guest_name": display_name,
            "session_name": session_name,
        }
        if new_time is not None:
            dynamic_data["session_time"] = new_time
        if new_location is not None:
            dynamic_data["session_location"] = new_location

        await sender.publish_mail_requested("session_change", recipient, dynamic_data)
        published_count += 1

    logger.info(
        "Published %s crm.mail.requested messages for sessionId=%s changeType=%s",
        published_count,
        session_id,
        change_type,
    )
    await message.ack()
