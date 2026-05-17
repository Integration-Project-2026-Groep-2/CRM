"""Contact lookup and match-classification operations.

Read-only SOQL queries that resolve a Contact by various identifiers
(email, CRM_ID__c, Mailing_ID__c, Planning_ID__c, Kassa_ID__c) and classify the match
as "none", "unique", or "ambiguous". Used by handlers to decide whether
to create, reuse, or conflict-publish.
"""

import asyncio
import logging
from typing import Any, Literal

from simple_salesforce import Salesforce, SalesforceError

from src.salesforce.client import _escape_soql
from src.salesforce.contacts.utils import get_full_contact_record

logger = logging.getLogger(__name__)


async def get_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email (Contracts 5a, 20).

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Complete Contact record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            contact_id = result["records"][0]["Id"]
            contact_record = await get_full_contact_record(sf, contact_id)
            logger.info("Found Contact by email: %s", email)
            return contact_record
        else:
            logger.info("Contact not found by email: %s", email)
            return None
    except SalesforceError as e:
        logger.error("Failed to get contact by email %s: %s", email, str(e))
        raise


async def get_contact_for_person_lookup(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email for Contract 10a (kassa.person.lookup.requested).

    Returns Contact with Account relationship fields expanded so the
    handler can build the PersonLookupResponse in one round-trip.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email to search for.

    Returns:
        Record with ``Id``, ``CRM_ID__c``, ``AccountId`` and (when the Contact
        is linked to an Account) a nested ``Account`` dict with ``Name`` and
        ``CRM_ID__c``. Returns ``None`` if no Contact matches the email.

    Raises:
        SalesforceError: If the SOQL query fails.
    """
    try:
        escaped_email = _escape_soql(email)
        query = (
            "SELECT Id, CRM_ID__c, Company_ID__c, AccountId, Account.Name, Account.CRM_ID__c "
            f"FROM Contact WHERE Email = '{escaped_email}'"
        )
        result = await asyncio.to_thread(sf.query, query)
        if result["totalSize"] == 0:
            logger.info("Person lookup — no Contact for email %s", email)
            return None
        logger.info("Person lookup — found Contact for email %s", email)
        return result["records"][0]
    except SalesforceError as e:
        logger.error("Person lookup query failed for %s: %s", email, str(e))
        raise


async def get_contact_by_crm_id(
    sf: Salesforce, crm_id: str
) -> dict[str, Any] | None:
    """Look up a Contact by CRM UUID."""
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return None
        if result["totalSize"] > 1:
            logger.warning("Multiple Contacts found for CRM_ID__c %s", crm_id)
            return None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found Contact by CRM_ID__c: %s", crm_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact by CRM_ID__c %s: %s", crm_id, str(e))
        raise


async def find_unique_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email, but only return a unique match."""
    match_status, contact = await get_contact_match_by_email(sf, email)
    if match_status == "none":
        logger.warning("No Contact found for payment confirmation email %s", email)
        return None
    if match_status == "ambiguous":
        logger.warning("Multiple Contacts found for payment confirmation email %s", email)
        return None
    return contact


async def get_contact_match_by_email(
    sf: Salesforce, email: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify an email lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found unique Contact by email: %s", email)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by email %s: %s", email, str(e))
        raise


async def get_contact_match_by_mailing_id(
    sf: Salesforce, mailing_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Mailing native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_mailing_id = _escape_soql(mailing_id)
        query = f"SELECT Id FROM Contact WHERE Mailing_ID__c = '{escaped_mailing_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found unique Contact by Mailing_ID__c: %s", mailing_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Mailing_ID__c %s: %s", mailing_id, str(e))
        raise


async def get_contact_match_by_planning_id(
    sf: Salesforce, planning_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Planning native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_planning_id = _escape_soql(planning_id)
        query = f"SELECT Id FROM Contact WHERE Planning_ID__c = '{escaped_planning_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found unique Contact by Planning_ID__c: %s", planning_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Planning_ID__c %s: %s", planning_id, str(e))
        raise


async def get_contact_match_by_kassa_id(
    sf: Salesforce, kassa_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a Kassa native-ID lookup as no match, unique match, or ambiguous match."""
    try:
        escaped_kassa_id = _escape_soql(kassa_id)
        query = f"SELECT Id FROM Contact WHERE Kassa_ID__c = '{escaped_kassa_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found unique Contact by Kassa_ID__c: %s", kassa_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by Kassa_ID__c %s: %s", kassa_id, str(e))
        raise


async def get_contact_match_by_crm_id(
    sf: Salesforce, crm_id: str
) -> tuple[Literal["none", "unique", "ambiguous"], dict[str, Any] | None]:
    """Classify a CRM master UUID lookup as no match, unique match, or ambiguous match.

    Used by update/deactivate handlers where consumers send back the canonical
    CRM UUID received in `crm.user.confirmed` (Option 2 UUID strategy).
    """
    try:
        escaped_crm_id = _escape_soql(crm_id)
        query = f"SELECT Id FROM Contact WHERE CRM_ID__c = '{escaped_crm_id}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] == 0:
            return "none", None
        if result["totalSize"] > 1:
            return "ambiguous", None

        contact_id = result["records"][0]["Id"]
        contact_record = await get_full_contact_record(sf, contact_id)
        logger.info("Found unique Contact by CRM_ID__c: %s", crm_id)
        return "unique", contact_record
    except SalesforceError as e:
        logger.error("Failed to get contact match by CRM_ID__c %s: %s", crm_id, str(e))
        raise
