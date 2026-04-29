"""Handler for Contract 3 — Frontend → CRM: new company created.

Queue: crm.frontend.company.created (routing key: frontend.company.created)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._facturatie_helpers import _build_facturatie_account_data
from src.handlers._helpers import _build_company_data, _normalize_optional_text
from src.salesforce_client import upsert_account_by_vat

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(message: aio_pika.IncomingMessage, sf: "Salesforce") -> None:
    """Contract 3 — Frontend → CRM: new company created via Frontend.

    Behaviour:
    - Validate XML against schema.
    - Upsert Account by VAT (frontend contract requires VAT).
    - Publish crm.company.confirmed after persistence.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("Frontend CompanyCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    vat_number = _normalize_optional_text(xml.findtext("vatNumber"))
    name = xml.findtext("name") or ""

    if not vat_number:
        logger.error("Frontend CompanyCreated missing vatNumber — rejecting message")
        await message.reject(requeue=False)
        return

    account_data = await _build_facturatie_account_data(xml, sf)

    account = await upsert_account_by_vat(sf, vat_number, account_data)
    await sender.publish_company_confirmed(_build_company_data(account))
    logger.info(
        "Published crm.company.confirmed for Frontend company %s (vatNumber=%s)",
        name,
        vat_number,
    )
    await message.ack()
