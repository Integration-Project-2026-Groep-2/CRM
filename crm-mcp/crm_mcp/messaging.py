"""Thread-safe wrapper around `src.sender` for MCP write-tools.

The CRM MCP server runs on its own daemon thread (see `src/mcp_thread.py`)
because FastMCP starts a uvicorn ASGI server with its own event loop. The
`src.sender` publisher functions are coroutines that must run on the *main*
asyncio loop where `sender._channel` lives.

`MessagePublisher` marshals coroutines from the MCP-thread back to the main
loop via `asyncio.run_coroutine_threadsafe`. It supports late-binding so the
MCP server can boot before RabbitMQ is connected — read-only tools stay
reachable during broker outages; write-tools raise a clear error until the
publisher is bound.
"""

from __future__ import annotations

import asyncio
import logging
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)

_PUBLISH_TIMEOUT_SECONDS = 10.0


class PublisherNotReadyError(RuntimeError):
    """Raised when a write-tool runs before the publisher is bound."""


class MessagePublisher:
    """Marshal `src.sender.publish_*` coroutines from the MCP-thread to main loop.

    Late-binding: instantiate without args at MCP-thread startup, then call
    `bind(loop, sender_module)` once `await sender.init(channel)` has completed
    on the main loop. Until bound, any `publish_*` raises `PublisherNotReadyError`.
    """

    def __init__(self) -> None:
        # Single-attribute state to make bind() atomic for cross-thread reads.
        # Two separate attributes risk torn-state where _marshal sees a partially
        # updated publisher (loop set, sender still None).
        self._state: tuple[asyncio.AbstractEventLoop, ModuleType] | None = None

    def bind(self, loop: asyncio.AbstractEventLoop, sender_module: ModuleType) -> None:
        """Attach the main asyncio loop and the initialised sender module."""
        self._state = (loop, sender_module)
        logger.info("MessagePublisher bound to main loop and sender module")

    def is_ready(self) -> bool:
        return self._state is not None

    def publish_user_confirmed(self, data: dict[str, Any]) -> None:
        self._marshal("publish_user_confirmed", data)

    def publish_user_updated(self, data: dict[str, Any]) -> None:
        self._marshal("publish_user_updated", data)

    def publish_user_deactivated(self, data: dict[str, Any]) -> None:
        self._marshal("publish_user_deactivated", data)

    def publish_company_confirmed(self, data: dict[str, Any]) -> None:
        self._marshal("publish_company_confirmed", data)

    def publish_company_updated(self, data: dict[str, Any]) -> None:
        self._marshal("publish_company_updated", data)

    def publish_company_deactivated(self, data: dict[str, Any]) -> None:
        self._marshal("publish_company_deactivated", data)

    def _marshal(self, method_name: str, data: dict[str, Any]) -> None:
        state = self._state
        if state is None:
            raise PublisherNotReadyError(
                "RabbitMQ publisher not yet bound — write-tools unavailable "
                "until `sender.init(channel)` completes on the main loop."
            )
        loop, sender_module = state
        coro = getattr(sender_module, method_name)(data)
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        future.result(timeout=_PUBLISH_TIMEOUT_SECONDS)
