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
from src.salesforce.client import (
    _resolve_account_country_field,
    _resolve_account_email_field,
    has_account_house_number_field,
)
from src.salesforce_client import upsert_account_by_vat

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def _build_frontend_account_data(
    xml: etree._Element, sf: "Salesforce"
) -> dict[str, Any]:
    """Map Frontend inbound CompanyCreated XML to a Salesforce Account payload.

    Resolves org-specific Account field names dynamically (Email__c vs Email,
    BillingCountryCode vs BillingCountry, optional House_Number__c) — same
    pattern as `_build_facturatie_account_data`. Hardcoding these caused a
    `FIELD_INTEGRITY_EXCEPTION` regression in production on 2026-04-22.
    """
    data: dict[str, Any] = {
        "Name": xml.findtext("name") or "",
        "VAT_Number__c": xml.findtext("vatNumber") or "",
    }

    email = _normalize_optional_text(xml.findtext("email"))
    if email:
        email_field = await _resolve_account_email_field(sf)
        if email_field is not None:
            data[email_field] = email

    phone = _normalize_optional_text(xml.findtext("phone"))
    if phone:
        data["Phone"] = phone

    street = _normalize_optional_text(xml.findtext("street"))
    if street:
        data["BillingStreet"] = street

    house_number = _normalize_optional_text(xml.findtext("houseNumber"))
    if house_number and await has_account_house_number_field(sf):
        data["House_Number__c"] = house_number

    postal_code = _normalize_optional_text(xml.findtext("postalCode"))
    if postal_code:
        data["BillingPostalCode"] = postal_code

    city = _normalize_optional_text(xml.findtext("city"))
    if city:
        data["BillingCity"] = city

    country = _normalize_optional_text(xml.findtext("country"))
    if country:
        country_field = await _resolve_account_country_field(sf)
        data[country_field] = country

    return data


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 3 — Frontend → CRM: new company created.

    Behaviour:
    - Validate XML against schema (invalid → reject without requeue).
    - Upsert Salesforce Account on VAT_Number__c (idempotent).
    - If the resulting Account satisfies all C14 prerequisites
      (email + complete address) → publish C14 crm.company.confirmed.
    - If C14 prerequisites are missing → ack and defer publication to the
      polling task, which will emit C14 once an admin completes the record.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.

    Note: no hijack-guard or email-fallback — Frontend always provides
    vatNumber (XSD-required), so neither path applies. The C3 inbound XSD
    makes addresses optional, but C14 (since PR #111) requires them. The
    deferred-publish path covers that asymmetry without DLQ-storming legitimate
    address-less Frontend payloads.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FrontendCompanyCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    name = _normalize_optional_text(xml.findtext("name"))
    vat_number = _normalize_optional_text(xml.findtext("vatNumber"))
    if not name or not vat_number:
        logger.error(
            "FrontendCompanyCreated — empty name or vatNumber after normalization, "
            "rejecting message",
        )
        await message.reject(requeue=False)
        return

    account_data = await _build_frontend_account_data(xml, sf)
    account = await upsert_account_by_vat(sf, vat_number, account_data)

    try:
        company_data = _build_company_data(account)
    except ValueError as exc:
        logger.warning(
            "FrontendCompanyCreated — Account upserted (vatNumber=%s) but C14 "
            "deferred to polling: %s",
            vat_number,
            exc,
        )
        await message.ack()
        return

    await sender.publish_company_confirmed(company_data)
    logger.info(
        "Published crm.company.confirmed for Frontend company %s (vatNumber=%s)",
        name,
        vat_number,
    )
    await message.ack()
