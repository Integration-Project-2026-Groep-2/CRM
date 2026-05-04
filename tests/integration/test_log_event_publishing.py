"""Integration test: LogEvent — CRM service-logs → Controlroom via logs.direct.

End-to-end:
1. Connect to local broker.
2. sender.init(channel) declares logs.direct exchange.
3. Bind a throwaway queue with rk routing.log to logs.direct.
4. attach_rabbitmq_log_handler installs the handler on the root logger.
5. Emit a logger.error("integration-test-marker") and let run_log_publisher
   drain one item.
6. Assert the message arrives, parses as XML, validates against XSD,
   carries <service>crm-test</service> and <level>ERROR</level>.

Skipped when broker unreachable. Per project rule: never skip in CI — start
local broker first: `docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

import aio_pika
import pytest

from src import logging_rabbitmq, sender
from src.logging_rabbitmq import (
    RabbitMQLogHandler,
    attach_rabbitmq_log_handler,
    run_log_publisher,
)
from src.xml_validator import validate

RABBITMQ_URL = os.getenv("CRM_TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5675/")


async def _broker_reachable() -> bool:
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=2)
    except Exception:  # noqa: BLE001
        return False
    await connection.close()
    return True


@pytest.fixture
def _skip_if_no_broker():
    if not asyncio.run(_broker_reachable()):
        pytest.skip(
            f"RabbitMQ not reachable at {RABBITMQ_URL}. "
            "Start a local broker: docker run -d --name crm-test-rabbitmq "
            "-p 5675:5672 rabbitmq:3.13-alpine",
        )


@pytest.fixture
def _reset_log_state():
    """Detach RabbitMQLogHandler from root before & after the test."""
    original_queue = logging_rabbitmq._log_queue
    original_dropped = logging_rabbitmq._dropped_count
    root = logging.getLogger()
    original_level = root.level
    original_handlers = list(root.handlers)

    for h in list(root.handlers):
        if isinstance(h, RabbitMQLogHandler):
            root.removeHandler(h)
    logging_rabbitmq._log_queue = None
    logging_rabbitmq._dropped_count = 0
    root.setLevel(logging.DEBUG)

    yield

    for h in list(root.handlers):
        if isinstance(h, RabbitMQLogHandler):
            root.removeHandler(h)
    for h in original_handlers:
        if h not in root.handlers:
            root.addHandler(h)
    root.setLevel(original_level)
    logging_rabbitmq._log_queue = original_queue
    logging_rabbitmq._dropped_count = original_dropped


@pytest.mark.asyncio
async def test_log_event_publishes_to_logs_direct_exchange(
    _skip_if_no_broker,
    _reset_log_state,
):
    """Emit a logger.error → drain → publish → consume → validate end-to-end."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await sender.init(channel)

        # Bind a throwaway queue to logs.direct so we can read what was published.
        queue_name = f"test-log-event-{uuid.uuid4().hex[:8]}"
        queue = await channel.declare_queue(queue_name, durable=False, auto_delete=True)
        await queue.bind(sender._logs_exchange, routing_key="routing.log")

        # Install handler. Force min_level=DEBUG so any record reaches the queue.
        attach_rabbitmq_log_handler("crm-test", min_level="DEBUG")

        marker = f"integration-test-marker-{uuid.uuid4().hex[:6]}"
        logger = logging.getLogger("src.handlers.integration_dummy")
        logger.error(marker)

        # Drive one publish iteration manually so the test does not race the
        # supervised log_publisher task (which we don't start here).
        xml_bytes = await asyncio.wait_for(
            logging_rabbitmq._log_queue.get(),
            timeout=2.0,
        )
        await sender.publish_log_event_raw(xml_bytes)

        # Consume back what just landed on logs.direct.
        incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            received = incoming.body
        finally:
            await incoming.ack()
    finally:
        await connection.close()

    doc = validate(received)
    assert doc.tag == "LogEvent"
    assert doc.findtext("service") == "crm-test"
    assert doc.findtext("level") == "ERROR"
    assert marker in doc.findtext("data")


@pytest.mark.asyncio
async def test_run_log_publisher_drains_queue_against_real_broker(
    _skip_if_no_broker,
    _reset_log_state,
):
    """run_log_publisher continuously drains the queue against a live broker."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()
        await sender.init(channel)

        queue_name = f"test-log-publisher-{uuid.uuid4().hex[:8]}"
        queue = await channel.declare_queue(queue_name, durable=False, auto_delete=True)
        await queue.bind(sender._logs_exchange, routing_key="routing.log")

        attach_rabbitmq_log_handler("crm-runner-test", min_level="DEBUG")

        publisher_task = asyncio.create_task(
            run_log_publisher(connection, "crm-runner-test")
        )

        try:
            marker = f"runner-marker-{uuid.uuid4().hex[:6]}"
            logging.getLogger("src.handlers.integration_runner").warning(marker)

            incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
            try:
                received = incoming.body
            finally:
                await incoming.ack()
        finally:
            publisher_task.cancel()
            try:
                await publisher_task
            except asyncio.CancelledError:
                pass
    finally:
        await connection.close()

    doc = validate(received)
    assert doc.tag == "LogEvent"
    assert doc.findtext("service") == "crm-runner-test"
    assert doc.findtext("level") == "WARN"  # Python WARNING → XSD WARN
    assert marker in doc.findtext("data")
