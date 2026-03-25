"""Salesforce REST API client wrapper using simple-salesforce.

CUSTOM FIELDS REFERENCE (must be created in Salesforce Setup):
- Contact.CRM_ID__c (Text, Unique) — UUID v4 for Contract 13, 18, 22
- Contact.GDPR_Consent__c (Checkbox) — For Contract 1
- Contact.Registration_ID__c (Text) — Deduplication key for Contract 1
- Contact.Role__c (Picklist: VISITOR | COMPANY_CONTACT) — For Contract 1, 13, 18

- Account.CRM_ID__c (Text, Unique) — UUID v4 for Contract 14, 19, 23
- Account.VAT_Number__c (Text, External ID, Unique) — For Contract 3, 5a, 5b, 14

All functions return complete records as dicts for XML serialization.
All SF calls are wrapped in asyncio.to_thread() to prevent blocking the event loop.
"""

import asyncio
import logging
import uuid
from typing import Any

from simple_salesforce import Salesforce, SalesforceError

from src.config import Config

logger = logging.getLogger(__name__)


def _escape_soql(value: str) -> str:
    """Escape single quotes for SOQL to prevent injection attacks."""
    return value.replace("'", "''")


async def get_salesforce_client(config: Config) -> Salesforce:
    """Create an authenticated Salesforce client.

    Args:
        config: Application configuration with SF credentials.

    Returns:
        Authenticated Salesforce instance.
    """
    logger.info("Connecting to Salesforce as %s...", config.salesforce_username)
    sf = await asyncio.to_thread(
        Salesforce,
        username=config.salesforce_username,
        password=config.salesforce_password,
        security_token=config.salesforce_security_token,
        domain=config.salesforce_domain,
    )
    logger.info("Connected to Salesforce.")
    return sf


async def create_contact(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Contact in Salesforce (Contract 1).

    Maps XML Registration fields to Contact fields and returns complete record
    for crm.user.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Contact fields (FirstName, LastName, Email, Role__c, GDPR_Consent__c, etc.).

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If Contact creation fails.
    """
    try:
        # Generate UUID v4 for crm.user.confirmed (Contract 13)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id

        # Create Contact
        result = await asyncio.to_thread(sf.Contact.create, data)
        contact_id = result["id"]
        logger.info("Created Contact with ID %s (CRM_ID: %s)", contact_id, crm_id)

        # Retrieve and return complete record for XML serialization
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to create contact: %s", str(e))
        raise


async def upsert_contact_by_email(
    sf: Salesforce, email: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update a Contact by email (Contract 2).

    Uses SOQL to find existing Contact, then updates or creates.
    Returns complete record for XML serialization.

    Args:
        sf: Authenticated Salesforce client.
        email: Contact email (lookup key).
        data: Contact fields to create/update.

    Returns:
        Complete Contact record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    try:
        # SOQL query to find by email (escaped)
        escaped_email = _escape_soql(email)
        query = f"SELECT Id FROM Contact WHERE Email = '{escaped_email}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            # Contact exists — update it
            contact_id = result["records"][0]["Id"]
            await asyncio.to_thread(sf.Contact.update, contact_id, data)
            logger.info("Updated Contact by email %s (ID: %s)", email, contact_id)
        else:
            # Contact does not exist — create it
            crm_id = str(uuid.uuid4())
            data["CRM_ID__c"] = crm_id
            data["Email"] = email
            create_result = await asyncio.to_thread(sf.Contact.create, data)
            contact_id = create_result["id"]
            logger.info("Created Contact by email %s (ID: %s, CRM_ID: %s)",
                        email, contact_id, crm_id)

        # Retrieve and return complete record
        contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
        return contact_record
    except SalesforceError as e:
        logger.error("Failed to upsert contact by email %s: %s", email, str(e))
        raise


async def get_contact_by_email(
    sf: Salesforce, email: str
) -> dict[str, Any] | None:
    """Look up a Contact by email (Contracts 5a, 10a, 20).

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
            contact_record = await asyncio.to_thread(sf.Contact.get, contact_id)
            logger.info("Found Contact by email: %s", email)
            return contact_record
        else:
            logger.info("Contact not found by email: %s", email)
            return None
    except SalesforceError as e:
        logger.error("Failed to get contact by email %s: %s", email, str(e))
        raise


async def create_account(sf: Salesforce, data: dict[str, Any]) -> dict[str, Any]:
    """Create a new Account (company) in Salesforce (Contract 3).

    Maps XML CompanyRequest fields to Account fields and returns complete record
    for crm.company.confirmed serialization.

    Args:
        sf: Authenticated Salesforce client.
        data: Account fields (Name, VAT_Number__c, Email, Phone, etc.).

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If Account creation fails.
    """
    try:
        # Generate UUID v4 for crm.company.confirmed (Contract 14)
        crm_id = str(uuid.uuid4())
        data["CRM_ID__c"] = crm_id

        # Create Account
        result = await asyncio.to_thread(sf.Account.create, data)
        account_id = result["id"]
        logger.info("Created Account with ID %s (CRM_ID: %s)", account_id, crm_id)

        # Retrieve and return complete record for XML serialization
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to create account: %s", str(e))
        raise


async def upsert_account_by_vat(
    sf: Salesforce, vat_number: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Create or update an Account by VAT number (Contracts 3, 5a).

    Uses SOQL to find existing Account, then updates or creates.
    Returns complete record for XML serialization.

    REQUIREMENT: VAT_Number__c must be configured as External ID in Salesforce Setup.

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number (lookup key, should be in data as VAT_Number__c).
        data: Account fields to create/update.

    Returns:
        Complete Account record as dictionary.

    Raises:
        SalesforceError: If operation fails.
    """
    try:
        # Ensure VAT_Number__c is in data
        data["VAT_Number__c"] = vat_number

        # SOQL query to find by VAT (escaped)
        escaped_vat = _escape_soql(vat_number)
        query = f"SELECT Id FROM Account WHERE VAT_Number__c = '{escaped_vat}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            # Account exists — update it
            account_id = result["records"][0]["Id"]
            await asyncio.to_thread(sf.Account.update, account_id, data)
            logger.info("Updated Account by VAT %s (ID: %s)", vat_number, account_id)
        else:
            # Account does not exist — create it
            crm_id = str(uuid.uuid4())
            data["CRM_ID__c"] = crm_id
            create_result = await asyncio.to_thread(sf.Account.create, data)
            account_id = create_result["id"]
            logger.info("Created Account by VAT %s (ID: %s, CRM_ID: %s)",
                        vat_number, account_id, crm_id)

        # Retrieve and return complete record
        account_record = await asyncio.to_thread(sf.Account.get, account_id)
        return account_record
    except SalesforceError as e:
        logger.error("Failed to upsert account by VAT %s: %s", vat_number, str(e))
        raise


async def get_account_by_vat(
    sf: Salesforce, vat_number: str
) -> dict[str, Any] | None:
    """Look up an Account by VAT number (Contracts 5a, 5b).

    Args:
        sf: Authenticated Salesforce client.
        vat_number: VAT number to search for.

    Returns:
        Complete Account record as dict, or None if not found.

    Raises:
        SalesforceError: If query fails.
    """
    try:
        escaped_vat = _escape_soql(vat_number)
        query = f"SELECT Id FROM Account WHERE VAT_Number__c = '{escaped_vat}'"
        result = await asyncio.to_thread(sf.query, query)

        if result["totalSize"] > 0:
            account_id = result["records"][0]["Id"]
            account_record = await asyncio.to_thread(sf.Account.get, account_id)
            logger.info("Found Account by VAT: %s", vat_number)
            return account_record
        else:
            logger.info("Account not found by VAT: %s", vat_number)
            return None
    except SalesforceError as e:
        logger.error("Failed to get account by VAT %s: %s", vat_number, str(e))
        raise
