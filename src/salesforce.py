"""Salesforce REST API client — all Salesforce operations go through this module.

Receiver handlers call these functions. No direct HTTP calls to Salesforce
anywhere else in the codebase.

Raises SalesforceUnavailableError when the Salesforce API is unreachable or
returns a server-side error (5xx). Callers requeue the message on this error.
"""

import logging
from typing import Any

from src.config import Config

logger = logging.getLogger(__name__)

_config: Config | None = None
_access_token: str | None = None
_instance_url: str | None = None


class SalesforceUnavailableError(Exception):
    """Raised when Salesforce is down or unreachable.

    The receiver requeues the message so it can be retried once SF is back.
    """


def _get_aiohttp():
    """Import aiohttp on demand.

    This keeps unit tests (and tooling that imports `src.salesforce`) working
    even if the optional runtime dependency isn't installed.
    """
    try:
        import aiohttp  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency 'aiohttp'. Install it with `pip install -r requirements.txt`."
        ) from exc
    return aiohttp


async def init(config: Config) -> None:
    """Authenticate with Salesforce and store the access token.

    Must be called once at startup before any Salesforce function is used.
    Uses the OAuth 2.0 Username-Password flow.
    """
    global _config, _access_token, _instance_url  # noqa: PLW0603
    _config = config

    aiohttp = _get_aiohttp()

    token_url = f"{config.sf_login_url}/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": config.sf_client_id,
        "client_secret": config.sf_client_secret,
        "username": config.sf_username,
        "password": config.sf_password + config.sf_security_token,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise SalesforceUnavailableError(
                        f"Salesforce auth failed (HTTP {resp.status}): {body}"
                    )
                data = await resp.json()
                _access_token = data["access_token"]
                _instance_url = data["instance_url"]
    except aiohttp.ClientError as exc:
        raise SalesforceUnavailableError(
            f"Salesforce auth network error: {exc}"
        ) from exc

    logger.info("Salesforce authenticated. Instance: %s", _instance_url)


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_access_token}",
        "Content-Type": "application/json",
    }


def _api(path: str) -> str:
    return f"{_instance_url}/services/data/v59.0/{path}"


# ---------------------------------------------------------------------------
# Contact operations (used by Contract 1)
# ---------------------------------------------------------------------------

async def registration_exists(registration_id: str) -> bool:
    """Return True if a Contact with this Registration_ID__c already exists.

    Used for idempotency on Contract 1. registrationId is the dedup key,
    not email (email is used for conflict detection).

    Raises SalesforceUnavailableError on network/server errors.
    """
    query = (
        f"SELECT Id FROM Contact "
        f"WHERE Registration_ID__c = '{_escape(registration_id)}' "
        f"LIMIT 1"
    )
    results = await _soql(query)
    return results["totalSize"] > 0


async def find_contact_by_email(email: str) -> dict[str, Any] | None:
    """Return the Contact record for this email, or None if not found.

    Used for email conflict detection on Contract 1.

    Raises SalesforceUnavailableError on network/server errors.
    """
    query = (
        f"SELECT Id, CRM_ID__c, Email, FirstName, LastName "
        f"FROM Contact "
        f"WHERE Email = '{_escape(email)}' "
        f"LIMIT 1"
    )
    results = await _soql(query)
    if results["totalSize"] == 0:
        return None
    return results["records"][0]


async def create_contact(payload: dict[str, Any]) -> str:
    """Create a new Contact in Salesforce. Returns the Salesforce record Id.

    Raises SalesforceUnavailableError on network/server errors.
    """
    return await _create("Contact", payload)


# ---------------------------------------------------------------------------
# Account operations (used by Contract 3)
# ---------------------------------------------------------------------------

async def find_account_by_vat(vat_number: str) -> dict[str, Any] | None:
    """Return the Account record for this VAT number, or None if not found.

    vatNumber is the dedup key for companies (Contract 3).

    Raises SalesforceUnavailableError on network/server errors.
    """
    query = (
        f"SELECT Id, CRM_ID__c, Name, VAT_Number__c "
        f"FROM Account "
        f"WHERE VAT_Number__c = '{_escape(vat_number)}' "
        f"LIMIT 1"
    )
    results = await _soql(query)
    if results["totalSize"] == 0:
        return None
    return results["records"][0]


async def create_account(payload: dict[str, Any]) -> str:
    """Create a new Account in Salesforce. Returns the Salesforce record Id.

    Raises SalesforceUnavailableError on network/server errors.
    """
    return await _create("Account", payload)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _soql(query: str) -> dict[str, Any]:
    """Execute a SOQL query and return the parsed JSON response.

    Raises SalesforceUnavailableError on network or server-side errors.
    """
    url = _api(f"query/?q={query}")
    aiohttp = _get_aiohttp()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=_headers()) as resp:
                if resp.status >= 500:
                    body = await resp.text()
                    raise SalesforceUnavailableError(
                        f"Salesforce SOQL server error (HTTP {resp.status}): {body}"
                    )
                if resp.status >= 400:
                    body = await resp.text()
                    raise ValueError(
                        f"Salesforce SOQL client error (HTTP {resp.status}): {body}"
                    )
                return await resp.json()
    except aiohttp.ClientError as exc:
        raise SalesforceUnavailableError(
            f"Salesforce SOQL network error: {exc}"
        ) from exc


async def _create(sobject: str, payload: dict[str, Any]) -> str:
    """POST to the sObject API to create a record. Returns the new record Id.

    Raises SalesforceUnavailableError on network or server-side errors.
    """
    url = _api(f"sobjects/{sobject}/")
    aiohttp = _get_aiohttp()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=_headers(), json=payload) as resp:
                if resp.status >= 500:
                    body = await resp.text()
                    raise SalesforceUnavailableError(
                        f"Salesforce create {sobject} server error "
                        f"(HTTP {resp.status}): {body}"
                    )
                if resp.status not in (200, 201):
                    body = await resp.text()
                    raise ValueError(
                        f"Salesforce create {sobject} failed "
                        f"(HTTP {resp.status}): {body}"
                    )
                data = await resp.json()
                record_id = data["id"]
                logger.debug("Created %s with id=%s.", sobject, record_id)
                return record_id
    except aiohttp.ClientError as exc:
        raise SalesforceUnavailableError(
            f"Salesforce create {sobject} network error: {exc}"
        ) from exc


def _escape(value: str) -> str:
    """Escape a string value for safe embedding in a SOQL query.

    Prevents SOQL injection by escaping single quotes.
    """
    return value.replace("'", "\\'")