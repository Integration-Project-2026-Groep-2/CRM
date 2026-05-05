"""Environment-based configuration for the CRM MCP server.

Reads `.env` from the crm-mcp folder first, then falls back to the parent
CRM project's `.env` so existing Salesforce credentials are reused.
Variable names mirror the parent project's `SALESFORCE_*` naming.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_PARENT_ENV = Path(__file__).resolve().parent.parent.parent / ".env"


def _load_env_files() -> None:
    """Load .env files: local crm-mcp first, parent CRM second (no override)."""
    load_dotenv()
    if _PARENT_ENV.exists():
        load_dotenv(_PARENT_ENV, override=False)


@dataclass(frozen=True)
class SalesforceConfig:
    """Salesforce connection settings, loaded from environment variables."""

    username: str
    password: str
    security_token: str
    domain: str = "login"

    @classmethod
    def from_env(cls) -> "SalesforceConfig":
        _load_env_files()
        return cls(
            username=_require_env("SALESFORCE_USERNAME"),
            password=_require_env("SALESFORCE_PASSWORD"),
            security_token=_require_env("SALESFORCE_SECURITY_TOKEN"),
            domain=os.getenv("SALESFORCE_DOMAIN", "login"),
        )


@dataclass(frozen=True)
class ServerConfig:
    """MCP server transport settings."""

    transport: str
    host: str
    port: int

    @classmethod
    def from_env(cls) -> "ServerConfig":
        _load_env_files()
        return cls(
            transport=os.getenv("CRM_MCP_TRANSPORT", "streamable-http"),
            host=os.getenv("CRM_MCP_HOST", "127.0.0.1"),
            port=int(os.getenv("CRM_MCP_PORT", "7001")),
        )


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    return value
