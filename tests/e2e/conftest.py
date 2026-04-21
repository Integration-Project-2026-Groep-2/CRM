"""E2E test configuration."""

import asyncio
import os

import aio_pika
import pytest
from aio_pika import ExchangeType
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

_raw_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
RABBITMQ_URL = _raw_url.replace("@rabbitmq:", "@localhost:") if "@rabbitmq:" in _raw_url else _raw_url


def pytest_configure(config):
    config.addinivalue_line("markers", "salesforce: test requires a running Salesforce connection")


def pytest_collection_modifyitems(config, items):
    """Skip Salesforce-dependent tests when --skip-sf is passed."""
    if config.getoption("--skip-sf", default=False):
        skip_sf = pytest.mark.skip(reason="--skip-sf: skipping Salesforce-dependent tests")
        for item in items:
            if "salesforce" in item.keywords:
                item.add_marker(skip_sf)


def pytest_addoption(parser):
    parser.addoption("--skip-sf", action="store_true", help="Skip Salesforce-dependent e2e tests")


# ---------------------------------------------------------------------------
# Shared fixtures for E2E suites (test_contracts_e2e.py and test_polling_e2e.py)
# test_contracts_e2e.py defines these locally too; pytest prefers the closer
# definition so both fixture sets coexist without conflict.
# ---------------------------------------------------------------------------


@pytest.fixture
async def connection():
    """RabbitMQ connection per test."""
    conn = await aio_pika.connect_robust(RABBITMQ_URL)
    yield conn
    await conn.close()


@pytest.fixture
async def channel(connection):
    ch = await connection.channel()
    yield ch


@pytest.fixture
async def outbound_exchange(channel):
    """Declare contact.topic to consume CRM's outbound messages."""
    return await channel.declare_exchange(
        "contact.topic", type=ExchangeType.TOPIC, durable=True,
    )


@pytest.fixture
async def sf_client():
    """Create a real Salesforce client for e2e verification."""
    username = os.getenv("SALESFORCE_USERNAME")
    password = os.getenv("SALESFORCE_PASSWORD")
    security_token = os.getenv("SALESFORCE_SECURITY_TOKEN")
    domain = os.getenv("SALESFORCE_DOMAIN", "login")

    if not username or not password or not security_token:
        pytest.skip("Salesforce credentials missing in environment for e2e test")

    return await asyncio.to_thread(
        Salesforce,
        username=username,
        password=password,
        security_token=security_token,
        domain=domain,
    )
