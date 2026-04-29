"""Handler for Contract 3 — Frontend → CRM: new company created.

Queue: crm.frontend.company.created (routing key: frontend.company.created)
Exchange: user.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING, Any

import aio_pika
from lxml import etree

from src import sender, xml_validator
from src.handlers._helpers import _build_company_data, _normalize_optional_text
from src.salesforce_client import upsert_account_by_vat

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


def _build_frontend_account_data(xml: etree._Element) -> dict[str, Any]:
    """Map Frontend inbound CompanyCreated XML to a Salesforce Account payload."""
    data: dict[str, Any] = {
        "Name": xml.findtext("name") or "",
        "VAT_Number__c": xml.findtext("vatNumber") or "",
    }

    email = _normalize_optional_text(xml.findtext("email"))
    if email:
        data["Email__c"] = email

    phone = _normalize_optional_text(xml.findtext("phone"))
    if phone:
        data["Phone"] = phone

    street = _normalize_optional_text(xml.findtext("street"))
    if street:
        data["BillingStreet"] = street

    house_number = _normalize_optional_text(xml.findtext("houseNumber"))
    if house_number:
        data["House_Number__c"] = house_number

    postal_code = _normalize_optional_text(xml.findtext("postalCode"))
    if postal_code:
        data["BillingPostalCode"] = postal_code

    city = _normalize_optional_text(xml.findtext("city"))
    if city:
        data["BillingCity"] = city

    country = _normalize_optional_text(xml.findtext("country"))
    if country:
        data["BillingCountry"] = country

    return data


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 3 — Frontend → CRM: new company created.

    Behaviour:
    - Validate XML against schema.
    - Upsert Salesforce Account on VAT_Number__c (idempotent via XSD-required vatNumber).
    - Publish C14 crm.company.confirmed after persistence.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.

    Note: no hijack-guard or email-fallback — Frontend always provides vatNumber
    (XSD-required), so neither path applies. _build_company_data may raise
    ValueError when address fields are missing; this is intentional — let it
    bubble so _wrap_handler routes the message to retry/DLQ. The polling task
    will resurface the record once an admin completes the address in Salesforce.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FrontendCompanyCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    vat_number = xml.findtext("vatNumber") or ""
    name = xml.findtext("name") or ""

    account_data = _build_frontend_account_data(xml)
    account = await upsert_account_by_vat(sf, vat_number, account_data)
    await sender.publish_company_confirmed(_build_company_data(account))
    logger.info(
        "Published crm.company.confirmed for Frontend company %s (vatNumber=%s)",
        name,
        vat_number,
    )
    await message.ack()
