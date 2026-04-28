"""RabbitMQ transport primitives: TTL-DLX retry + DLQ publishing.

Implements Lucas's (TL Facturatie/Mailing) TTL-DLX pattern as measured on the
prod broker on 2026-04-28: each work-queue has a sibling `<queue>.retry` queue
bound to `contact.retry` (DIRECT) with `x-message-ttl=30000` and a DLX back to
the producer-exchange. Terminal failures publish explicitly to `contact.dlq`
(TOPIC) so the receiver does not depend on per-queue `x-dead-letter-exchange`
arguments.

Handlers raise `MissingDependencyError` (typed) for missing-Salesforce-entity
race conditions or any other `Exception` for transient errors; the receiver's
`_wrap_handler` routes both via `_handle_failure`. Custom headers
(`x-retry-count`, `x-error`, `x-missing-<label>`, `x-original-routing-key`,
`x-retry-queue`, `x-source-queue`, `x-error-class`, `x-error-message`) provide
cross-team observability so Control Room can pivot on failure cause without
parsing payloads.
"""

import asyncio
import logging
from typing import Any

import aio_pika
import aiormq

from src.handlers._exceptions import MissingDependencyError
from src.salesforce.client import is_rate_limit_error

logger = logging.getLogger(__name__)

_MAX_REQUEUE_ATTEMPTS = 5
_MAX_DEFERRAL_ATTEMPTS = 15
_RATE_LIMIT_SLEEP_SECONDS = 60
_BACKOFF_CAP_SECONDS = 30  # legacy shim path only

_RETRY_EXCHANGE = "contact.retry"
_DLQ_EXCHANGE = "contact.dlq"


def _delivery_attempt_count(message: aio_pika.IncomingMessage) -> int:
    """Return the retry count stored in our custom `x-retry-count` header."""
    headers = message.headers or {}
    try:
        return int(headers.get("x-retry-count", 0))
    except (TypeError, ValueError):
        return 0


def _exponential_backoff_seconds(attempt: int, cap: int = _BACKOFF_CAP_SECONDS) -> float:
    if attempt < 0:
        attempt = 0
    return float(min(2 ** attempt, cap))


async def _publish_to_retry_exchange(
    message: aio_pika.IncomingMessage,
    *,
    work_queue: str,
    next_retry_count: int,
    error_tag: str,
    extra_headers: dict[str, Any] | None = None,
) -> None:
    """Publish to `contact.retry` (rk=`<work_queue>.retry`); ack original.

    The retry-queue (declared by `_declare_and_bind`) holds the message for its
    queue-level `x-message-ttl=30000`. After expiry, RabbitMQ DLXes the message
    back to the producer-exchange with the original producer routing-key, which
    re-binds to the work-queue and re-delivers to the handler.

    Uses raw `aiormq.spec.Basic.Properties` because `IncomingMessage.channel`
    returns the unwrapped `aiormq.Channel` — see the 2026-04-20 prod regression
    documented in `tests/integration/test_republish_with_retry_count.py`.
    """
    headers = dict(message.headers or {})
    headers["x-retry-count"] = next_retry_count
    headers["x-error"] = error_tag
    headers["x-original-routing-key"] = message.routing_key or ""
    headers["x-retry-queue"] = work_queue
    if extra_headers:
        headers.update(extra_headers)
    properties = aiormq.spec.Basic.Properties(
        content_type=message.content_type,
        content_encoding=message.content_encoding,
        delivery_mode=message.delivery_mode,
        headers=headers,
    )
    await message.channel.basic_publish(
        body=message.body,
        exchange=_RETRY_EXCHANGE,
        routing_key=f"{work_queue}.retry",
        properties=properties,
    )
    await message.ack()


async def _publish_to_dlq(
    message: aio_pika.IncomingMessage,
    *,
    work_queue: str,
    error_tag: str,
    extra_headers: dict[str, Any] | None = None,
) -> None:
    """Publish to `contact.dlq` (rk=`<work_queue>.dlq`); ack original.

    Self-contained: works regardless of whether the source work-queue carries
    an `x-dead-letter-exchange` argument. We publish explicitly so we control
    the headers and so PR2 does not depend on PR3 (queue-args activation).
    """
    headers = dict(message.headers or {})
    headers["x-error"] = error_tag
    headers["x-source-queue"] = work_queue
    headers["x-original-routing-key"] = message.routing_key or ""
    if extra_headers:
        headers.update(extra_headers)
    properties = aiormq.spec.Basic.Properties(
        content_type=message.content_type,
        content_encoding=message.content_encoding,
        delivery_mode=message.delivery_mode,
        headers=headers,
    )
    await message.channel.basic_publish(
        body=message.body,
        exchange=_DLQ_EXCHANGE,
        routing_key=f"{work_queue}.dlq",
        properties=properties,
    )
    await message.ack()


async def _handle_failure(
    contract: str,
    message: aio_pika.IncomingMessage,
    exc: Exception,
    *,
    work_queue: str,
) -> None:
    """Branch on exception type, publish to retry-exchange or DLQ accordingly."""
    if is_rate_limit_error(exc):
        logger.error(
            "%s — Salesforce rate limit hit; sleeping %ss then dead-lettering: %s",
            contract, _RATE_LIMIT_SLEEP_SECONDS, exc,
        )
        await asyncio.sleep(_RATE_LIMIT_SLEEP_SECONDS)
        await _publish_to_dlq(
            message, work_queue=work_queue, error_tag="rate-limit-dropped",
            extra_headers={"x-error-class": exc.__class__.__name__},
        )
        return

    attempts = _delivery_attempt_count(message)
    if isinstance(exc, MissingDependencyError):
        max_attempts = _MAX_DEFERRAL_ATTEMPTS
        error_tag = f"missing-{exc.identifier_label}"
        extra: dict[str, Any] = {
            f"x-missing-{exc.identifier_label}": str(exc.identifier_value),
        }
    else:
        max_attempts = _MAX_REQUEUE_ATTEMPTS
        error_tag = "processing-error"
        extra = {
            "x-error-class": exc.__class__.__name__,
            "x-error-message": str(exc)[:512],
        }

    if attempts >= max_attempts:
        logger.error(
            "%s — max retries (%d) exceeded; routing to %s: %s",
            contract, max_attempts, _DLQ_EXCHANGE, exc,
        )
        await _publish_to_dlq(
            message, work_queue=work_queue, error_tag=error_tag, extra_headers=extra,
        )
        return

    logger.warning(
        "%s — retry %d/%d via %s: %s",
        contract, attempts + 1, max_attempts, _RETRY_EXCHANGE, exc,
    )
    await _publish_to_retry_exchange(
        message, work_queue=work_queue,
        next_retry_count=attempts + 1, error_tag=error_tag,
        extra_headers=extra,
    )


async def _republish_with_retry_count(
    message: aio_pika.IncomingMessage, new_count: int,
) -> None:
    """DEPRECATED — retained only for the 2026-04-20 prod-bug regression test.

    Exercises the raw `aiormq.Channel.basic_publish` code path that broke in
    prod when wrapped helpers tried `IncomingMessage.channel.declare_exchange`
    (the channel is `aiormq.Channel`, not the aio-pika wrapper). New code MUST
    use `_publish_to_retry_exchange` or `_handle_failure`.
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
    """DEPRECATED shim — delegates to `_handle_failure`.

    New handlers let exceptions bubble to `_wrap_handler`. Kept so import
    statements in older tests do not break; tests that mock this name keep
    working transparently.
    """
    work_queue = message.routing_key or ""
    await _handle_failure(contract, message, exc, work_queue=work_queue)


async def _handle_out_of_order_deferral(
    contract: str,
    message: aio_pika.IncomingMessage,
    *,
    identifier_label: str,
    identifier_value: str,
) -> None:
    """DEPRECATED shim — synthesises `MissingDependencyError` and delegates.

    New handlers `raise MissingDependencyError(label, value)` directly and let
    `_wrap_handler` route the failure.
    """
    exc = MissingDependencyError(identifier_label, str(identifier_value))
    work_queue = message.routing_key or ""
    await _handle_failure(contract, message, exc, work_queue=work_queue)
