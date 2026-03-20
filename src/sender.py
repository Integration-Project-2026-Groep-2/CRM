"""RabbitMQ publisher — utility module for sending CRM events to other teams.

This module is called by receiver handlers after processing inbound messages.
All outbound publishing goes through these functions — no direct aio_pika
publish calls elsewhere in the codebase.
"""

import logging
from typing import Any

from aio_pika.abc import AbstractChannel

logger = logging.getLogger(__name__)

_channel: AbstractChannel | None = None


async def init(channel: AbstractChannel) -> None:
    """Initialize the sender with a RabbitMQ channel.

    Must be called once at startup before any publish function is used.
    """
    global _channel  # noqa: PLW0603
    _channel = channel
    logger.info("Sender initialized.")


async def publish_user_confirmed(user_data: dict[str, Any]) -> None:
    """Contract 13 — Publish user confirmation after registration processing.

    TODO: Build XML, validate against XSD, publish to crm.user.confirmed.
    """
    raise NotImplementedError("publish_user_confirmed not yet implemented")


async def publish_company_confirmed(company_data: dict[str, Any]) -> None:
    """Contract 14 — Publish company confirmation after creation.

    TODO: Build XML, validate against XSD, publish to crm.company.confirmed.
    """
    raise NotImplementedError("publish_company_confirmed not yet implemented")


async def publish_company_responded(request_id: str, company_data: dict[str, Any]) -> None:
    """Contract 5b — Respond to facturatie company data request.

    TODO: Build XML with requestId, validate, publish to crm.company.responded.
    """
    raise NotImplementedError("publish_company_responded not yet implemented")


async def publish_person_lookup_responded(request_id: str, person_data: dict[str, Any]) -> None:
    """Contract 10b — Respond to kassa person lookup request.

    TODO: Build XML with requestId, validate, publish to crm.person.lookup.responded.
    """
    raise NotImplementedError("publish_person_lookup_responded not yet implemented")


async def publish_unpaid_responded(request_id: str, persons: list[dict[str, Any]]) -> None:
    """Contract 17b — Respond to kassa unpaid persons request.

    TODO: Build XML with requestId, validate, publish to crm.unpaid.responded.
    """
    raise NotImplementedError("publish_unpaid_responded not yet implemented")


async def publish_mail_requested(
    mail_type: str, recipient: dict[str, Any], dynamic_data: dict[str, Any]
) -> None:
    """Contract 6 — Request mailing team to send notification.

    TODO: Build XML, validate, publish to crm.mail.requested.
    """
    raise NotImplementedError("publish_mail_requested not yet implemented")
