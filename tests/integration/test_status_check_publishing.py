"""Integration test: Contract 8 — status check → Controlroom via statuscheck.direct.

End-to-end:
1. Connect to local broker.
2. Declare statuscheck.direct (DIRECT, durable=true) and bind a throwaway
   queue with rk routing.statuscheck.
3. Run one iteration of run_status_check (terminate via CancelledError after
   the first publish).
4. Consume the message back, validate against XSD, assert root + bounded
   memory/disk fractions.

Skipped when broker unreachable. Per project rule: never skip in CI — start
local broker first: `docker run -d --name crm-test-rabbitmq -p 5675:5672 rabbitmq:3.13-alpine`.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import aio_pika
import pytest
from aio_pika import ExchangeType

from src.config import Config
from src.status_check import run_status_check
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


def _make_config() -> Config:
    return Config(
        rabbitmq_url=RABBITMQ_URL,
        salesforce_username="test",
        salesforce_password="test",
        salesforce_security_token="test",
        salesforce_domain="login",
        heartbeat_interval_seconds=0,
        status_check_interval_seconds=0,
        system_name="CRM",
        polling_interval_seconds=0,
        polling_state_path="/tmp/polling_checkpoint_test.json",
        polling_integration_user_id=None,
        log_level="INFO",
        log_service_name="crm",
        log_rabbitmq_level="INFO",
    )


@pytest.mark.asyncio
async def test_status_check_publishes_to_statuscheck_direct(
    _skip_if_no_broker,
):
    """run_status_check declares statuscheck.direct, publishes mandatory=True,
    and the message round-trips through routing.statuscheck → consumer queue."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    try:
        # The publisher (run_status_check) declares its own channel; the test
        # uses a separate channel for the consumer-side queue + binding.
        consumer_channel = await connection.channel()
        exchange = await consumer_channel.declare_exchange(
            "statuscheck.direct", type=ExchangeType.DIRECT, durable=True
        )

        queue_name = f"test-statuscheck-{uuid.uuid4().hex[:8]}"
        queue = await consumer_channel.declare_queue(
            queue_name, durable=False, auto_delete=True
        )
        await queue.bind(exchange, routing_key="routing.statuscheck")

        # Drive exactly one publish iteration. asyncio.sleep at the end of
        # run_status_check is patched to raise CancelledError so the loop
        # terminates after publishing once.
        config = _make_config()

        async def _stop_after_first_publish(*_args, **_kwargs):
            raise asyncio.CancelledError()

        from unittest.mock import patch

        with patch("src.status_check.asyncio.sleep", side_effect=_stop_after_first_publish):
            try:
                await run_status_check(connection, config)
            except asyncio.CancelledError:
                pass

        incoming = await asyncio.wait_for(queue.get(timeout=5), timeout=5.0)
        try:
            received = incoming.body
        finally:
            await incoming.ack()
    finally:
        await connection.close()

    # XSD validation passes
    doc = validate(received)
    assert doc.tag == "StatusCheck"
    assert doc.findtext("serviceId") == "CRM"

    timestamp = doc.findtext("timestamp")
    assert timestamp.endswith("Z")

    uptime = int(doc.findtext("uptime"))
    assert uptime >= 0

    memory = float(doc.findtext("memory"))
    disk = float(doc.findtext("disk"))
    assert 0.0 <= memory <= 1.0
    assert 0.0 <= disk <= 1.0

    # Validate field order matches XSD xs:sequence
    children = [child.tag for child in doc]
    assert children == ["serviceId", "timestamp", "uptime", "memory", "disk"]
