"""Tests for the MessagePublisher main-loop marshaling wrapper."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crm_mcp.messaging import MessagePublisher, PublisherNotReadyError


@pytest.mark.asyncio
async def test_publisher_unbound_raises() -> None:
    publisher = MessagePublisher()

    assert publisher.is_ready() is False
    with pytest.raises(PublisherNotReadyError):
        publisher.publish_user_confirmed({"id": "x"})


@pytest.mark.asyncio
async def test_publisher_bound_marshals_to_main_loop() -> None:
    """Bound publisher invokes sender coroutines on the bound loop."""
    publisher = MessagePublisher()
    loop = asyncio.get_running_loop()
    sender_stub = SimpleNamespace(
        publish_user_confirmed=AsyncMock(return_value=None),
        publish_user_updated=AsyncMock(return_value=None),
        publish_user_deactivated=AsyncMock(return_value=None),
        publish_company_confirmed=AsyncMock(return_value=None),
        publish_company_updated=AsyncMock(return_value=None),
        publish_company_deactivated=AsyncMock(return_value=None),
    )
    publisher.bind(loop, sender_stub)
    assert publisher.is_ready() is True

    # Run the blocking marshal calls in a worker thread so we don't deadlock
    # the running loop (publisher.publish_* calls future.result() which blocks).
    payload = {"id": "abc"}
    await asyncio.to_thread(publisher.publish_user_confirmed, payload)
    await asyncio.to_thread(publisher.publish_user_updated, payload)
    await asyncio.to_thread(publisher.publish_user_deactivated, payload)
    await asyncio.to_thread(publisher.publish_company_confirmed, payload)
    await asyncio.to_thread(publisher.publish_company_updated, payload)
    await asyncio.to_thread(publisher.publish_company_deactivated, payload)

    sender_stub.publish_user_confirmed.assert_awaited_once_with(payload)
    sender_stub.publish_user_updated.assert_awaited_once_with(payload)
    sender_stub.publish_user_deactivated.assert_awaited_once_with(payload)
    sender_stub.publish_company_confirmed.assert_awaited_once_with(payload)
    sender_stub.publish_company_updated.assert_awaited_once_with(payload)
    sender_stub.publish_company_deactivated.assert_awaited_once_with(payload)


@pytest.mark.asyncio
async def test_publisher_propagates_sender_exceptions() -> None:
    """Errors raised by sender.publish_* surface on the marshaling thread."""
    publisher = MessagePublisher()
    loop = asyncio.get_running_loop()
    sender_stub = SimpleNamespace(
        publish_company_confirmed=AsyncMock(side_effect=ValueError("XSD invalid")),
    )
    publisher.bind(loop, sender_stub)

    with pytest.raises(ValueError, match="XSD invalid"):
        await asyncio.to_thread(publisher.publish_company_confirmed, {"id": "x"})
