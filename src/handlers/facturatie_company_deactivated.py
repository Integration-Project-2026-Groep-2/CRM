"""Handler for Contract 35 — Facturatie → CRM: deactivate existing CRM-linked company.

Queue: crm.facturatie.company.deactivated (routing key: facturatie.company.deactivated)
Exchange: company.topic | durable: true
"""

import logging
from typing import TYPE_CHECKING

import aio_pika

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import (
    _build_company_deactivation_data,
    _get_account_email,
    _normalize_email_for_compare,
)
from src.salesforce_client import (
    deactivate_account_record,
    get_account_match_by_crm_id,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 35 — Facturatie → CRM: deactivate existing CRM-linked company.

    The `<id>` field carries the CRM master UUID. CRM resolves the Account by
    CRM_ID__c and performs a soft delete only (audit trail).

    Behaviour:
    - Validate XML against schema.
    - Resolve strictly by CRM_ID__c.
    - Raise MissingDependencyError on unknown identities (TTL-DLX deferral).
    - Ack ambiguous identities without retry.
    - Trust CRM_ID__c over a stale payload email, but log the mismatch.
    - Soft delete and publish crm.company.deactivated.
    - Invalid XML: rejected without requeue.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FacturatieCompanyDeactivated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    email = xml.findtext("email") or ""
    crm_id = xml.findtext("id") or ""
    deactivated_at = xml.findtext("deactivatedAt") or ""

    crm_match_status, existing_account = await get_account_match_by_crm_id(sf, crm_id)
    if crm_match_status == "none":
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if crm_match_status == "ambiguous":
        logger.warning(
            "FacturatieCompanyDeactivated ignored — ambiguous CRM_ID__c %s in Salesforce",
            crm_id,
        )
        await message.ack()
        return

    account = existing_account
    existing_email = _normalize_email_for_compare(_get_account_email(account))
    incoming_email = _normalize_email_for_compare(email)
    if existing_email is not None and incoming_email is not None and existing_email != incoming_email:
        logger.warning(
            "FacturatieCompanyDeactivated email mismatch — CRM_ID__c %s resolved to %s but payload contained %s; proceeding with soft delete",
            crm_id,
            _get_account_email(account),
            email,
        )

    account = await deactivate_account_record(
        sf,
        account,
        log_value=f"CRM_ID__c {crm_id}",
    )
    await sender.publish_company_deactivated(
        _build_company_deactivation_data(account, deactivated_at),
    )
    logger.info(
        "Published crm.company.deactivated for Facturatie company CRM_ID__c %s",
        crm_id,
    )
    await message.ack()
