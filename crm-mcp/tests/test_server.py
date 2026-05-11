"""Tests for the FastMCP server's `/health` endpoint."""

from __future__ import annotations

import json
import logging

import pytest

from crm_mcp.server import _HealthCheckAccessLogFilter, _HealthState, build_server


@pytest.mark.asyncio
async def test_health_state_check_sf_returns_true_on_success(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    state = _HealthState(fake_sf_client, fake_publisher)

    assert await state.check_sf() is True


@pytest.mark.asyncio
async def test_health_state_check_sf_returns_false_on_failure(
    fake_sf_client, fake_publisher
) -> None:
    fake_sf_client.query.side_effect = RuntimeError("Salesforce login failed")
    state = _HealthState(fake_sf_client, fake_publisher)

    assert await state.check_sf() is False


@pytest.mark.asyncio
async def test_health_state_caches_result_within_ttl(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """Burst of healthcheck calls must NOT multiply Salesforce API calls."""
    fake_sf_client.query.return_value = make_query_response([])
    state = _HealthState(fake_sf_client, fake_publisher)

    for _ in range(5):
        assert await state.check_sf() is True

    assert fake_sf_client.query.await_count == 1


def test_health_state_check_publisher_reflects_is_ready(
    fake_sf_client, fake_publisher
) -> None:
    state = _HealthState(fake_sf_client, fake_publisher)

    fake_publisher.is_ready.return_value = True
    assert state.check_publisher() is True

    fake_publisher.is_ready.return_value = False
    assert state.check_publisher() is False


@pytest.mark.asyncio
async def test_health_endpoint_returns_200_when_all_ok(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    """End-to-end: /health route returns 200 + ok body when SF + publisher are healthy."""
    fake_sf_client.query.return_value = make_query_response([])
    fake_publisher.is_ready.return_value = True

    mcp = build_server(fake_sf_client, fake_publisher)
    handler = _find_health_handler(mcp)
    response = await handler(_StubRequest())

    assert response.status_code == 200
    body = json.loads(bytes(response.body).decode())
    assert body["status"] == "ok"
    assert body["checks"] == {"sf_connected": True, "publisher_bound": True}
    assert "uptime_seconds" in body


@pytest.mark.asyncio
async def test_health_endpoint_returns_503_when_sf_down(
    fake_sf_client, fake_publisher
) -> None:
    fake_sf_client.query.side_effect = RuntimeError("connection refused")
    fake_publisher.is_ready.return_value = True

    mcp = build_server(fake_sf_client, fake_publisher)
    handler = _find_health_handler(mcp)
    response = await handler(_StubRequest())

    assert response.status_code == 503
    body = json.loads(bytes(response.body).decode())
    assert body["status"] == "degraded"
    assert body["checks"]["sf_connected"] is False
    assert body["checks"]["publisher_bound"] is True


@pytest.mark.asyncio
async def test_health_endpoint_returns_503_when_publisher_unbound(
    fake_sf_client, fake_publisher, make_query_response
) -> None:
    fake_sf_client.query.return_value = make_query_response([])
    fake_publisher.is_ready.return_value = False

    mcp = build_server(fake_sf_client, fake_publisher)
    handler = _find_health_handler(mcp)
    response = await handler(_StubRequest())

    assert response.status_code == 503
    body = json.loads(bytes(response.body).decode())
    assert body["status"] == "degraded"
    assert body["checks"]["sf_connected"] is True
    assert body["checks"]["publisher_bound"] is False


def _uvicorn_access_record(request_line: str) -> logging.LogRecord:
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg='%s - "%s" %d',
        args=("127.0.0.1:12345", request_line, 200),
        exc_info=None,
    )
    return record


def test_healthcheck_filter_drops_health_access_log() -> None:
    f = _HealthCheckAccessLogFilter()
    assert f.filter(_uvicorn_access_record("GET /health HTTP/1.1")) is False


def test_healthcheck_filter_keeps_mcp_access_log() -> None:
    f = _HealthCheckAccessLogFilter()
    assert f.filter(_uvicorn_access_record("POST /mcp HTTP/1.1")) is True


def _find_health_handler(mcp):
    """Locate the /health route handler registered via FastMCP custom_route."""
    for route in mcp._custom_starlette_routes:  # type: ignore[attr-defined]
        if getattr(route, "path", None) == "/health":
            return route.endpoint
    raise AssertionError("/health route not registered on FastMCP server")


class _StubRequest:
    """Minimal stand-in for starlette.Request — endpoint doesn't read body."""

    pass
