"""RabbitMQ transport primitives: retry counting, backoff, requeue, error handling.

Extracted from receiver.py so handler modules can import them without
creating a handlers → receiver import cycle. `run_receiver` also imports
from here.
"""

import asyncio
import logging

import aio_pika
import aiormq

from src.salesforce.client import is_rate_limit_error

logger = logging.getLogger(__name__)

_MAX_REQUEUE_ATTEMPTS = 5
_MAX_DEFERRAL_ATTEMPTS = 10
_RATE_LIMIT_SLEEP_SECONDS = 60
_BACKOFF_CAP_SECONDS = 30


def _delivery_attempt_count(message: aio_pika.IncomingMessage) -> int:
    """Return the retry count stored in our custom `x-retry-count` header.

    RabbitMQ does not track retry counts on classic queues when messages
    are requeued via basic.reject(requeue=True). We therefore maintain the
    counter ourselves: each time a handler requeues a message, it ack's
    the original and publishes a fresh copy with `x-retry-count` bumped
    by one (see `_republish_with_retry_count`).
    """
    headers = message.headers or {}
    try:
        return int(headers.get("x-retry-count", 0))
    except (TypeError, ValueError):
        return 0


def _exponential_backoff_seconds(attempt: int, cap: int = _BACKOFF_CAP_SECONDS) -> float:
    """Exponential backoff doubling on each attempt, capped.

    attempt 0 → 1s, 1 → 2s, 2 → 4s, 3 → 8s, 4 → 16s, 5+ → cap (30s by default).
    """
    if attempt < 0:
        attempt = 0
    return float(min(2 ** attempt, cap))


async def _republish_with_retry_count(
    message: aio_pika.IncomingMessage, new_count: int,
) -> None:
    """Ack the current message and publish a fresh copy with updated retry count.

    RabbitMQ's `basic.reject(requeue=True)` does not add tracking metadata
    to the requeued message, so we cannot count retries via the AMQP
    `x-death` header unless dead-lettering is configured. Instead, we ack
    the current delivery and publish a new copy of the payload to the same
    exchange+routing_key with an incremented `x-retry-count` header. The
    fresh message lands back in the same queue (RabbitMQ routes it via the
    queue's binding) and the next handler invocation reads the updated
    counter via `_delivery_attempt_count`.

    Implementation note: `aio_pika.IncomingMessage.channel` returns the raw
    `aiormq.Channel` (not the aio-pika wrapper), so we use the low-level
    `basic_publish` directly with an `aiormq.spec.Basic.Properties` struct.
    No exchange declare is needed — the broker already knows the exchange.

    Trade-off: message_id and timestamp are regenerated per retry, and the
    AMQP `redelivered` flag no longer reflects retries. We don't rely on
    either, so this is acceptable.
    """
    headers = dict(message.headers or {})
    headers["x-retry-count"] = new_count

    properties = aiormq.spec.Basic.Properties(
        content_type=message.content_type,
        content_encoding=message.content_encoding,
        headers=headers,
        delivery_mode=message.delivery_mode,
    )

    await message.channel.basic_publish(
        body=message.body,
        exchange=message.exchange or "",
        routing_key=message.routing_key or "",
        properties=properties,
    )
    await message.ack()


async def _handle_processing_error(
    contract: str, message: aio_pika.IncomingMessage, exc: Exception,
) -> None:
    """Centralised transient-error handling for receiver handlers.

    - Salesforce rate-limit → sleep then drop (no requeue, no self-DOS).
    - Max retries exceeded → drop without requeue.
    - Otherwise: exponential backoff, then republish with incremented retry
      count so the next attempt reads the correct counter.
    """
    if is_rate_limit_error(exc):
        logger.error(
            "%s — Salesforce rate limit hit; sleeping %ss then dropping: %s",
            contract, _RATE_LIMIT_SLEEP_SECONDS, exc,
        )
        await asyncio.sleep(_RATE_LIMIT_SLEEP_SECONDS)
        await message.reject(requeue=False)
        return
    attempts = _delivery_attempt_count(message)
    if attempts >= _MAX_REQUEUE_ATTEMPTS:
        logger.error(
            "%s — max retries (%d) exceeded; dropping: %s",
            contract, _MAX_REQUEUE_ATTEMPTS, exc,
        )
        await message.reject(requeue=False)
        return
    sleep_s = _exponential_backoff_seconds(attempts)
    logger.error(
        "%s — error (attempt %d/%d); sleeping %ss before requeue: %s",
        contract, attempts + 1, _MAX_REQUEUE_ATTEMPTS, sleep_s, exc,
    )
    await asyncio.sleep(sleep_s)
    await _republish_with_retry_count(message, attempts + 1)


async def _handle_out_of_order_deferral(
    contract: str,
    message: aio_pika.IncomingMessage,
    *,
    identifier_label: str,
    identifier_value: str,
) -> None:
    """Requeue with exponential backoff for create/update ordering races.

    When an update/deactivate arrives before the matching create is processed,
    we retry with increasing delays instead of tight-looping. This gives the
    create-handler breathing room and prevents burning Salesforce API quota
    on queries that will keep returning empty results.

    Drops the message after _MAX_DEFERRAL_ATTEMPTS retries — at that point the
    create almost certainly never arrived and further retries are pointless.

    Uses `_republish_with_retry_count` so the retry counter survives across
    deliveries (see that helper's docstring for rationale).
    """
    attempts = _delivery_attempt_count(message)
    if attempts >= _MAX_DEFERRAL_ATTEMPTS:
        logger.warning(
            "%s — deferred %d times for %s=%s; dropping (upstream create never arrived)",
            contract, attempts, identifier_label, identifier_value,
        )
        await message.reject(requeue=False)
        return
    sleep_s = _exponential_backoff_seconds(attempts)
    logger.warning(
        "%s deferred (attempt %d/%d) — no match for %s=%s; sleeping %ss before requeue",
        contract, attempts + 1, _MAX_DEFERRAL_ATTEMPTS,
        identifier_label, identifier_value, sleep_s,
    )
    await asyncio.sleep(sleep_s)
    await _republish_with_retry_count(message, attempts + 1)
