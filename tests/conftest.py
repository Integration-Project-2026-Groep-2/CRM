"""Shared test fixtures."""

from pathlib import Path

import pytest

from src.config import Config


@pytest.fixture()
def schema_path() -> Path:
    """Path to the XSD schema file."""
    return Path(__file__).parent.parent / "src" / "schema" / "crm-schema-v1.xsd"


@pytest.fixture()
def config() -> Config:
    """Fixture providing a default Config instance."""
    return Config(
        rabbitmq_url="amqp://test",
        salesforce_username="test",
        salesforce_password="test",
        salesforce_security_token="test",
        salesforce_domain="login",
        heartbeat_interval_seconds=0,  # 0 for fast tests
        system_name="CRM",
        log_level="INFO",
    )


@pytest.fixture()
def sample_heartbeat_xml() -> bytes:
    """Valid heartbeat XML for Contract 7."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Heartbeat>
  <serviceId>CRM</serviceId>
  <timestamp>2026-03-14T10:00:00Z</timestamp>
</Heartbeat>"""


@pytest.fixture()
def sample_warning_xml() -> bytes:
    """Valid warning XML for Contract 9."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Warning>
  <serviceId>CRM</serviceId>
  <message>Duplicate heartbeat detected</message>
  <type>heartbeat</type>
</Warning>"""
