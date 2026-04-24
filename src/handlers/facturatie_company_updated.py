"""Handler for Contract 34 — Facturatie → CRM: update existing CRM-linked company.

Queue: crm.facturatie.company.updated (routing key: facturatie.company.updated)
Exchange: company.topic | durable: true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._helpers import (
    _build_company_deactivation_data,
    _build_updated_company_data,
    _get_account_is_active,
    _normalize_optional_text,
)
from src.handlers._transport import _handle_out_of_order_deferral, _handle_processing_error
from src.salesforce_client import (
    apply_account_is_active,
    deactivate_account_record,
    get_account_match_by_crm_id,
    update_facturatie_account,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 34 — Facturatie → CRM: update existing CRM-linked company.

    The `<id>` field carries the CRM master UUID (received by Facturatie in
    crm.company.confirmed). CRM resolves the Account by CRM_ID__c.

    Behaviour:
    - Validate XML against schema.
    - Resolve strictly by CRM_ID__c.
    - Requeue unknown identities (create may still be in flight).
    - Ack ambiguous identities without retry.
    - If isActive=false, soft-delete and publish crm.company.deactivated.
    - Otherwise update Facturatie-owned fields authoritatively and publish
      crm.company.updated.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieCompanyUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        crm_id = xml.findtext("id") or ""
        is_active = xml.findtext("isActive") in ("true", "1")

        crm_match_status, existing_account = await get_account_match_by_crm_id(sf, crm_id)
        if crm_match_status == "none":
            await _handle_out_of_order_deferral(
                "FacturatieCompanyUpdated",
                message,
                identifier_label="CRM_ID__c",
                identifier_value=crm_id,
            )
            return

        if crm_match_status == "ambiguous":
            logger.warning(
                "FacturatieCompanyUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
                crm_id,
            )
            await message.ack()
            return

        account = existing_account
        if not is_active:
            account = await deactivate_account_record(
                sf, account, log_value=f"CRM_ID__c {crm_id}",
            )
            deactivated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            await sender.publish_company_deactivated(
                _build_company_deactivation_data(account, deactivated_at),
            )
            logger.info(
                "Published crm.company.deactivated for Facturatie company CRM_ID__c %s (isActive=false on update)",
                crm_id,
            )
            await message.ack()
            return

        account = await update_facturatie_account(
            sf,
            account,
            vat_number=_normalize_optional_text(xml.findtext("vatNumber")),
            name=xml.findtext("name") or "",
            email=xml.findtext("email") or "",
            phone=_normalize_optional_text(xml.findtext("phone")),
            street=_normalize_optional_text(xml.findtext("street")),
            house_number=_normalize_optional_text(xml.findtext("houseNumber")),
            postal_code=_normalize_optional_text(xml.findtext("postalCode")),
            city=_normalize_optional_text(xml.findtext("city")),
            country=_normalize_optional_text(xml.findtext("country")),
        )
        if not _get_account_is_active(account):
            reactivation_update = await apply_account_is_active(sf, {}, True)
            if reactivation_update:
                account_id = account["Id"]
                await asyncio.to_thread(sf.Account.update, account_id, reactivation_update)
                account = await asyncio.to_thread(sf.Account.get, account_id)
                logger.info(
                    "Reactivated Account %s for Facturatie company CRM_ID__c %s (isActive=true on update)",
                    account_id,
                    crm_id,
                )
        await sender.publish_company_updated(_build_updated_company_data(account))
        logger.info(
            "Published crm.company.updated for Facturatie company CRM_ID__c %s",
            crm_id,
        )
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("FacturatieCompanyUpdated", message, exc)
