"""Unit tests for registration tools."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from crm_mcp.tools import registrations as registration_tools


@pytest.mark.asyncio
async def test_count_registrations_breakdown_no_paid_filter(fake_sf_client) -> None:
    # Sequential calls: total, paid, unpaid.
    fake_sf_client.query_count = AsyncMock(side_effect=[89, 71, 18])

    result = await registration_tools.count_registrations(fake_sf_client)

    assert result.total == 89
    assert result.paid == 71
    assert result.unpaid == 18
    assert fake_sf_client.query_count.await_count == 3


@pytest.mark.asyncio
async def test_count_registrations_paid_only_branches_to_one_query(fake_sf_client) -> None:
    """Regression: when paid=True, we should only issue 1 query, not 3."""
    fake_sf_client.query_count = AsyncMock(side_effect=[80])

    result = await registration_tools.count_registrations(fake_sf_client, paid=True)

    assert result.total == 80
    assert result.paid == 80
    assert result.unpaid == 0
    assert fake_sf_client.query_count.await_count == 1


@pytest.mark.asyncio
async def test_count_registrations_unpaid_only_branches_to_one_query(fake_sf_client) -> None:
    """Regression: when paid=False, we should only issue 1 query."""
    fake_sf_client.query_count = AsyncMock(side_effect=[20])

    result = await registration_tools.count_registrations(fake_sf_client, paid=False)

    assert result.total == 20
    assert result.paid == 0
    assert result.unpaid == 20
    assert fake_sf_client.query_count.await_count == 1


@pytest.mark.asyncio
async def test_count_registrations_rejects_naive_datetime(fake_sf_client) -> None:
    """Regression: naive datetimes cause silent off-by-hours bugs in SOQL."""
    fake_sf_client.query_count = AsyncMock(return_value=0)
    naive = datetime(2026, 5, 4, 10, 0, 0)  # no tzinfo

    with pytest.raises(ValueError, match="timezone-aware"):
        await registration_tools.count_registrations(fake_sf_client, since=naive)


@pytest.mark.asyncio
async def test_count_registrations_accepts_utc_datetime(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(side_effect=[10, 7, 3])
    utc_since = datetime(2026, 5, 4, 10, 0, 0, tzinfo=timezone.utc)

    result = await registration_tools.count_registrations(fake_sf_client, since=utc_since)

    assert result.total == 10
    soqls = [call.args[0] for call in fake_sf_client.query_count.await_args_list]
    assert all("LastModifiedDate >= 2026-05-04T10:00:00Z" in s for s in soqls)


@pytest.mark.asyncio
async def test_count_registrations_with_session_filter_escapes(fake_sf_client) -> None:
    fake_sf_client.query_count = AsyncMock(side_effect=[5, 4, 1])

    await registration_tools.count_registrations(
        fake_sf_client, session_id="SES'-injection"
    )

    soqls = [call.args[0] for call in fake_sf_client.query_count.await_args_list]
    assert all(r"Session_ID__c = 'SES\'-injection'" in s for s in soqls)
