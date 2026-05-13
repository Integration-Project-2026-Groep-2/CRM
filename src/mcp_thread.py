"""Bootstrap the bundled crm-mcp server in a daemon thread.

The CRM MCP server is intra-team tooling for the (Rust) master agent —
read access to Salesforce queries plus CRUD write-tools that broadcast
XSD-validated XML on RabbitMQ (see `src/sender.py`). We bundle it into the
same container as the 5 supervised asyncio tasks (heartbeat, status_check,
receiver, polling, log_publisher) but run it on its own OS-thread because
FastMCP starts an internal uvicorn ASGI server with its own event loop.

Why a thread and not a 6th asyncio task:
    `mcp.run(transport="streamable-http")` is blocking and creates its own event
    loop via uvicorn. Co-hosting in the existing loop would either require
    rewriting the MCP server's transport or running two event loops concurrently
    on one thread (impossible). A daemon thread lets uvicorn own its loop while
    asyncio.run(main()) keeps the 5 supervised tasks running on the main thread.

Why optional ImportError-guard:
    Local dev without `pip install ./crm-mcp` shouldn't break `python -m src.main`.
    In Docker the package is always installed (see Dockerfile), so production
    always boots with MCP enabled.

Why the publisher is returned (not bound) here:
    MCP must boot before the RabbitMQ connect-with-retry so read-only tools stay
    reachable during broker outages. Main.py binds the publisher to the running
    loop + sender module *after* `sender.init(channel)` succeeds — write-tools
    raise `PublisherNotReadyError` until that bind completes.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crm_mcp.messaging import MessagePublisher

logger = logging.getLogger(__name__)


def start_mcp_thread(
    receiver_alive_event: threading.Event | None = None,
) -> tuple[threading.Thread, MessagePublisher] | None:
    """Start the CRM MCP server in a daemon thread.

    Returns (thread, publisher) on success, or None if the package is not
    installed or if `CRM_MCP_ENABLED=false`. The caller must call
    `publisher.bind(loop, sender_module)` after `sender.init(channel)` succeeds
    on the main loop, or write-tools will raise `PublisherNotReadyError`.

    `receiver_alive_event` is forwarded to `build_server` so `/health` flips
    to 503 when the asyncio receiver task dies.

    Errors during MCP startup or runtime are logged but never propagate -- the
    main asyncio loop keeps running even if MCP fails (defense-in-depth: a
    Salesforce auth failure should not take down the heartbeat).
    """
    if os.getenv("CRM_MCP_ENABLED", "true").lower() != "true":
        logger.info("CRM MCP server disabled via CRM_MCP_ENABLED=false")
        return None

    try:
        from crm_mcp.config import SalesforceConfig, ServerConfig
        from crm_mcp.messaging import MessagePublisher
        from crm_mcp.salesforce import CrmSalesforceClient
        from crm_mcp.server import build_server
    except ImportError as exc:
        logger.warning(
            "crm-mcp package not installed (%s) -- skipping MCP thread; "
            "run `pip install ./crm-mcp` to enable",
            exc,
        )
        return None

    server_config = ServerConfig.from_env()
    sf_config = SalesforceConfig.from_env()
    client = CrmSalesforceClient(sf_config)
    publisher = MessagePublisher()
    mcp = build_server(
        client,
        publisher,
        host=server_config.host,
        port=server_config.port,
        receiver_alive_event=receiver_alive_event,
    )

    def _run() -> None:
        logger.info(
            "Starting CRM MCP server thread (transport=%s host=%s port=%d)",
            server_config.transport,
            server_config.host,
            server_config.port,
        )
        try:
            mcp.run(transport=server_config.transport)
        except Exception:
            logger.exception("CRM MCP server thread crashed")

    thread = threading.Thread(target=_run, daemon=True, name="crm-mcp-server")
    thread.start()
    return thread, publisher
