"""E2E test configuration."""

import pytest


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
