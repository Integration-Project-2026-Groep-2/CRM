"""Handler for Contract 33 — Facturatie → CRM: new company created.

Queue: crm.facturatie.company.created (routing key: facturatie.company.created)
Exchange: company.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._facturatie_helpers import _build_facturatie_account_data
from src.handlers._helpers import _build_company_data, _normalize_optional_text
from src.handlers._transport import _handle_processing_error
from src.salesforce_client import (
    create_account,
    get_account_match_by_email,
    upsert_account_by_vat,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 33 — Facturatie → CRM: new company created in FOSSBilling.

    Behaviour:
    - Validate XML against schema.
    - If vatNumber is present, upsert on VAT_Number__c (preserves CRM_ID__c).
    - Otherwise match on email: unique → reuse, none → create, ambiguous → ack.
    - Publish C14 crm.company.confirmed after persistence.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieCompanyCreated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        vat_number = _normalize_optional_text(xml.findtext("vatNumber"))
        email = xml.findtext("email") or ""
        name = xml.findtext("name") or ""

        account_data = await _build_facturatie_account_data(xml, sf)

        if vat_number:
            account = await upsert_account_by_vat(sf, vat_number, account_data)
            await sender.publish_company_confirmed(_build_company_data(account))
            logger.info(
                "Published crm.company.confirmed for Facturatie company %s (vatNumber=%s)",
                name,
                vat_number,
            )
            await message.ack()
            return

        email_match_status, existing_account = await get_account_match_by_email(sf, email)
        if email_match_status == "ambiguous":
            logger.warning(
                "FacturatieCompanyCreated ignored — ambiguous email %s in Salesforce",
                email,
            )
            await message.ack()
            return

        if email_match_status == "unique" and existing_account is not None:
            # Account hijack guard: email is a weak identity signal (generic
            # addresses like info@bigcorp.com can collide with unrelated
            # companies). Only reuse when the existing Account has no VAT —
            # i.e. it was not created via the authoritative VAT-upsert path.
            # Otherwise ack with a warning so the ambiguity is visible but we
            # never silently bind Facturatie's company to someone else's
            # CRM_ID__c (which a subsequent C34 would then overwrite).
            if _normalize_optional_text(existing_account.get("VAT_Number__c")):
                logger.warning(
                    "FacturatieCompanyCreated ignored — email %s matches existing "
                    "Account with VAT_Number__c %s; refusing to bind new Facturatie "
                    "company (no vatNumber in payload) to established CRM_ID__c",
                    email,
                    existing_account.get("VAT_Number__c"),
                )
                await message.ack()
                return
            await sender.publish_company_confirmed(_build_company_data(existing_account))
            logger.info(
                "Published crm.company.confirmed for existing Facturatie company %s (email=%s)",
                name,
                email,
            )
            await message.ack()
            return

        account = await create_account(sf, account_data)
        await sender.publish_company_confirmed(_build_company_data(account))
        logger.info(
            "Published crm.company.confirmed for new Facturatie company %s (email=%s)",
            name,
            email,
        )
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieCompanyCreated", message, exc)
