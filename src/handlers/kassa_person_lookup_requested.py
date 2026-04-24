"""Handler for Contract 10a — Kassa → CRM: person lookup request.

Queue: kassa.person.lookup.requested | Exchange: payment.topic | durable: true | US-47
"""

import logging
from typing import TYPE_CHECKING, Any

import aio_pika

from src import sender, xml_validator
from src.handlers._transport import _handle_processing_error
from src.salesforce_client import get_contact_for_person_lookup

if TYPE_CHECKING:
    from simple_salesforce import Salesforce

logger = logging.getLogger(__name__)


async def handle(
    message: aio_pika.IncomingMessage, sf: "Salesforce"
) -> None:
    """Contract 10a — Kassa -> CRM: person lookup request.

    Behaviour:
    - Validate XML against schema (root: <PersonLookupRequest>).
    - Resolve the Contact by email; include Account relation for company info.
    - Publish crm.person.lookup.responded with the same requestId.
    - Invalid XML: rejected without requeue.
    - Other errors: requeued via _handle_processing_error.

    ``linkedToCompany`` follows the same convention as Contract 17b: the
    Contact is considered linked to a company iff ``Contact.AccountId`` is set.
    When linked, the company's canonical CRM UUID (``Account.CRM_ID__c``) is
    returned so Kassa can join with their local cache without a second lookup.
    """
    try:
        xml = xml_validator.validate(message.body)
    except Exception as exc:  # noqa: BLE001
        logger.error("PersonLookupRequest — invalid XML, rejecting message: %s", exc)
        await message.reject(requeue=False)
        return

    try:
        request_id = xml.findtext("requestId") or ""
        email = xml.findtext("email") or ""

        contact = await get_contact_for_person_lookup(sf, email)

        if contact is None:
            person_data: dict[str, Any] = {"found": False, "linkedToCompany": False}
        else:
            account = contact.get("Account") or {}
            account_id = contact.get("AccountId")
            linked_to_company = bool(account_id)
            person_data = {
                "found": True,
                "linkedToCompany": linked_to_company,
            }
            crm_id = contact.get("CRM_ID__c")
            if crm_id:
                person_data["id"] = crm_id
            if linked_to_company:
                company_name = account.get("Name")
                company_id = account.get("CRM_ID__c")
                if company_name:
                    person_data["companyName"] = company_name
                if company_id:
                    person_data["companyId"] = company_id

        await sender.publish_person_lookup_responded(request_id, person_data)
        logger.info(
            "Processed person lookup %s (email=%s, found=%s, linkedToCompany=%s)",
            request_id,
            email,
            person_data["found"],
            person_data["linkedToCompany"],
        )
        await message.ack()
    except Exception as exc:  # noqa: BLE001
        await _handle_processing_error("PersonLookupRequest", message, exc)
