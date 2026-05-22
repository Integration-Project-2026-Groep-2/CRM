"""Handler — Frontend → CRM: update an existing company.

Queue: crm.frontend.company.updated (routing key: frontend.company.updated)
Exchange: user.topic | durable: true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import aio_pika
from lxml import etree

from src import sender, xml_validator
from src.handlers._exceptions import MissingDependencyError
from src.handlers._helpers import (
    _build_company_deactivation_data,
    _build_updated_company_data,
    _get_account_is_active,
    _normalize_optional_text,
)
from src.salesforce.client import (
    _resolve_account_country_field,
    _resolve_account_email_field,
    has_account_house_number_field,
)
from src.salesforce_client import (
    apply_account_is_active,
    deactivate_account_record,
    get_account_match_by_crm_id,
    patch_account_fields,
)

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def _build_frontend_account_patch(
    xml: etree._Element, sf: "Salesforce"
) -> dict[str, Any]:
    """Map present Frontend CompanyUpdated elements to a Salesforce Account patch.

    Partial-merge semantics: an optional field is included only when its element
    is present in the payload (an empty element → None → clears the column); an
    omitted element is left out so patch_account_fields never touches it.
    Required fields (name, vatNumber, email) are always present per the XSD.
    Resolves org-specific field names the same way _build_frontend_account_data
    does for the create path.
    """
    data: dict[str, Any] = {
        "Name": _normalize_optional_text(xml.findtext("name")),
        "VAT_Number__c": _normalize_optional_text(xml.findtext("vatNumber")),
    }

    email_field = await _resolve_account_email_field(sf)
    if email_field is not None:
        data[email_field] = xml.findtext("email") or ""

    if xml.find("phone") is not None:
        data["Phone"] = _normalize_optional_text(xml.findtext("phone"))

    if xml.find("street") is not None:
        data["BillingStreet"] = _normalize_optional_text(xml.findtext("street"))

    if xml.find("houseNumber") is not None and await has_account_house_number_field(sf):
        data["House_Number__c"] = _normalize_optional_text(xml.findtext("houseNumber"))

    if xml.find("postalCode") is not None:
        data["BillingPostalCode"] = _normalize_optional_text(xml.findtext("postalCode"))

    if xml.find("city") is not None:
        data["BillingCity"] = _normalize_optional_text(xml.findtext("city"))

    if xml.find("country") is not None:
        country_field = await _resolve_account_country_field(sf)
        data[country_field] = _normalize_optional_text(xml.findtext("country"))

    return data


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Frontend → CRM: update an existing company.

    The `<id>` field carries the CRM master UUID (received by the Frontend in
    crm.company.confirmed). CRM resolves the Account by CRM_ID__c.

    Behaviour:
    - Validate XML against schema (invalid → reject without requeue).
    - Reject without requeue when `name` is blank after normalization.
    - Resolve strictly by CRM_ID__c; unknown → MissingDependencyError
      (TTL-DLX deferral), ambiguous → ack without retry.
    - isActive=false → soft-delete and publish crm.company.deactivated.
    - Otherwise patch only the fields the payload carries (partial merge) and
      publish crm.company.updated.
    - Other errors: bubble to _wrap_handler for retry/DLQ routing.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("FrontendCompanyUpdated — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    name = _normalize_optional_text(xml.findtext("name"))
    if not name:
        logger.error(
            "FrontendCompanyUpdated — empty name after normalization, rejecting message",
        )
        await message.reject(requeue=False)
        return

    crm_id = xml.findtext("id") or ""
    is_active = (xml.findtext("isActive") or "").strip() in ("true", "1")

    crm_match_status, existing_account = await get_account_match_by_crm_id(sf, crm_id)
    if crm_match_status == "none":
        raise MissingDependencyError("CRM_ID__c", crm_id)

    if crm_match_status == "ambiguous":
        logger.warning(
            "FrontendCompanyUpdated ignored — ambiguous CRM_ID__c %s in Salesforce",
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
            "Published crm.company.deactivated for Frontend company CRM_ID__c %s (isActive=false on update)",
            crm_id,
        )
        await message.ack()
        return

    patch = await _build_frontend_account_patch(xml, sf)
    account = await patch_account_fields(sf, account, patch)
    if not _get_account_is_active(account):
        reactivation_update = await apply_account_is_active(sf, {}, True)
        if reactivation_update:
            account_id = account["Id"]
            await asyncio.to_thread(sf.Account.update, account_id, reactivation_update)
            account = await asyncio.to_thread(sf.Account.get, account_id)
            logger.info(
                "Reactivated Account %s for Frontend company CRM_ID__c %s (isActive=true on update)",
                account_id,
                crm_id,
            )
    await sender.publish_company_updated(_build_updated_company_data(account))
    logger.info(
        "Published crm.company.updated for Frontend company CRM_ID__c %s",
        crm_id,
    )
    await message.ack()
