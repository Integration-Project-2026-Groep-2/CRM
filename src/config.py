"""Application configuration loaded from environment variables."""

import logging
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Typed, immutable application configuration."""

    rabbitmq_url: str
    salesforce_username: str
    salesforce_password: str
    salesforce_security_token: str
    salesforce_domain: str
    heartbeat_interval_seconds: int
    system_name: str
    log_level: str


def load_config() -> Config:
    """Load configuration from environment variables.

    Raises:
        ValueError: If a required environment variable is missing.
    """
    required = {
        "RABBITMQ_URL": os.getenv("RABBITMQ_URL"),
        "SALESFORCE_USERNAME": os.getenv("SALESFORCE_USERNAME"),
        "SALESFORCE_PASSWORD": os.getenv("SALESFORCE_PASSWORD"),
        "SALESFORCE_SECURITY_TOKEN": os.getenv("SALESFORCE_SECURITY_TOKEN"),
    }

    missing = [key for key, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return Config(
        rabbitmq_url=required["RABBITMQ_URL"],  # type: ignore[arg-type]
        salesforce_username=required["SALESFORCE_USERNAME"],  # type: ignore[arg-type]
        salesforce_password=required["SALESFORCE_PASSWORD"],  # type: ignore[arg-type]
        salesforce_security_token=required["SALESFORCE_SECURITY_TOKEN"],  # type: ignore[arg-type]
        salesforce_domain=os.getenv("SALESFORCE_DOMAIN", "login"),
        heartbeat_interval_seconds=int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "1")),
        system_name=os.getenv("SYSTEM_NAME", "CRM"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def setup_logging(level: str) -> None:
    """Configure application-wide logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
