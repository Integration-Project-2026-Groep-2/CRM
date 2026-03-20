"""Salesforce REST API client wrapper using simple-salesforce."""

import logging
from typing import Any

from simple_salesforce import Salesforce

from src.config import Config

logger = logging.getLogger(__name__)


def get_salesforce_client(config: Config) -> Salesforce:
    """Create an authenticated Salesforce client.

    Args:
        config: Application configuration with SF credentials.

    Returns:
        Authenticated Salesforce instance.
    """
    logger.info("Connecting to Salesforce as %s...", config.salesforce_username)
    sf = Salesforce(
        username=config.salesforce_username,
        password=config.salesforce_password,
        security_token=config.salesforce_security_token,
        domain=config.salesforce_domain,
    )
    logger.info("Connected to Salesforce.")
    return sf


def create_contact(sf: Salesforce, data: dict[str, Any]) -> str:
    """Create a new Contact in Salesforce.

    TODO: Map XML fields to Salesforce Contact fields, return Contact Id.
    """
    raise NotImplementedError("create_contact not yet implemented")


def upsert_contact_by_email(sf: Salesforce, email: str, data: dict[str, Any]) -> str:
    """Create or update a Contact by email address.

    TODO: Use sf.Contact.upsert() with Email as external ID.
    """
    raise NotImplementedError("upsert_contact_by_email not yet implemented")


def get_contact_by_email(sf: Salesforce, email: str) -> dict[str, Any] | None:
    """Look up a Contact by email address.

    TODO: SOQL query on Contact.Email, return dict or None if not found.
    """
    raise NotImplementedError("get_contact_by_email not yet implemented")


def create_account(sf: Salesforce, data: dict[str, Any]) -> str:
    """Create a new Account (company) in Salesforce.

    TODO: Map XML fields to Salesforce Account fields, return Account Id.
    """
    raise NotImplementedError("create_account not yet implemented")


def upsert_account_by_vat(sf: Salesforce, vat_number: str, data: dict[str, Any]) -> str:
    """Create or update an Account by VAT number.

    TODO: Use sf.Account.upsert() with VAT_Number__c as external ID.
    """
    raise NotImplementedError("upsert_account_by_vat not yet implemented")


def get_account_by_vat(sf: Salesforce, vat_number: str) -> dict[str, Any] | None:
    """Look up an Account by VAT number.

    TODO: SOQL query on Account.VAT_Number__c, return dict or None.
    """
    raise NotImplementedError("get_account_by_vat not yet implemented")
