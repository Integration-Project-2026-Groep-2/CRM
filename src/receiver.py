"""RabbitMQ queue consumer — listens on 11 queues from other teams."""

import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from lxml import etree

from src import xml_validator
from src.config import Config

logger = logging.getLogger(__name__)


async def run_receiver(connection: AbstractRobustConnection, config: Config) -> None:
    """Consume messages from all inbound queues, validate XML, process in Salesforce.

    Contract 9 is the first implemented handler and establishes the base structure
    for all future contract handlers.
    """
    channel = await connection.channel()

    # Contract 9 — Controlroom → CRM: system warning
    # Queue: controlroom.warning.issued | durable: false | US-26
    queue_warning = await channel.declare_queue(
        "controlroom.warning.issued", durable=False
    )
    await queue_warning.consume(handle_warning)

    # Future contracts — uncomment and implement per sprint:
    # Contract 1 — Frontend → CRM: new registration
    # queue_registration = await channel.declare_queue(
    #     "frontend.registration.created", durable=True
    # )
    # await queue_registration.consume(handle_registration)

    # Contract 2 — Frontend → CRM: update/cancel registration
    # queue_reg_updated = await channel.declare_queue(
    #     "frontend.registration.updated", durable=True
    # )
    # await queue_reg_updated.consume(handle_registration_updated)

    # Contract 3 — Frontend → CRM: create company
    # queue_company = await channel.declare_queue(
    #     "frontend.company.created", durable=True
    # )
    # await queue_company.consume(handle_company_created)

    # Contract 5a — Facturatie → CRM: request company data
    # queue_company_req = await channel.declare_queue(
    #     "facturatie.company.requested", durable=True
    # )
    # await queue_company_req.consume(handle_company_requested)

    # Contract 10a — Kassa → CRM: person lookup request
    # queue_person_lookup = await channel.declare_queue(
    #     "kassa.person.lookup.requested", durable=True
    # )
    # await queue_person_lookup.consume(handle_person_lookup)

    # Contract 16 — Kassa → CRM: payment confirmed
    # queue_payment = await channel.declare_queue(
    #     "kassa.payment.confirmed", durable=True
    # )
    # await queue_payment.consume(handle_payment_confirmed)

    # Contract 17a — Kassa → CRM: unpaid persons request
    # queue_unpaid = await channel.declare_queue(
    #     "kassa.unpaid.requested", durable=True
    # )
    # await queue_unpaid.consume(handle_unpaid_requested)

    # Contract 11 — Planning → CRM: session update (Release 2)
    # queue_session = await channel.declare_queue(
    #     "planning.session.updated", durable=True
    # )
    # await queue_session.consume(handle_session_updated)

    # Contract 12 — IoT → CRM: badge linked (Release 2)
    # queue_badge = await channel.declare_queue(
    #     "iot.badge.linked", durable=True
    # )
    # await queue_badge.consume(handle_badge_linked)

    logger.info("Receiver started. Listening on all configured queues.")
    await asyncio.Future()  # run forever


async def handle_warning(message: aio_pika.IncomingMessage) -> None:
    """Contract 9 — Controlroom → CRM: system warning.

    Queue: controlroom.warning.issued | durable: false | US-26

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