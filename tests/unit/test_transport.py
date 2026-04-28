"""Unit tests for src.handlers._transport — TTL-DLX failure routing.

Covers the four branches of `_handle_failure`:
  1. rate-limit → sleep + publish-to-DLQ
  2. MissingDependencyError, attempts < max → publish-to-retry
  3. generic Exception, attempts < max → publish-to-retry
  4. attempts >= max → publish-to-DLQ
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.handlers._exceptions import MissingDependencyError
from src.handlers._transport import (
    _DLQ_EXCHANGE,
    _MAX_DEFERRAL_ATTEMPTS,
    _MAX_REQUEUE_ATTEMPTS,
    _RETRY_EXCHANGE,
    _handle_failure,
)


@pytest.fixture
def message():
    msg = MagicMock()
    msg.body = b"<Probe/>"
    msg.headers = {}
    msg.content_type = "application/xml"
    msg.content_encoding = None
    msg.delivery_mode = 2
    msg.routing_key = "facturatie.user.updated"
    msg.exchange = "user.topic"
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    msg.channel = MagicMock()
    msg.channel.basic_publish = AsyncMock()
    return msg


class TestHandleFailureBranches:
    @pytest.mark.asyncio
    async def test_missing_dependency_publishes_to_retry_with_label_header(self, message):
        exc = MissingDependencyError("CRM_ID__c", "uuid-123")

        await _handle_failure(
            "FacturatieUserUpdated", message, exc,
            work_queue="crm.facturatie.user.updated",
        )

        message.channel.basic_publish.assert_awaited_once()
        kwargs = message.channel.basic_publish.await_args.kwargs
        assert kwargs["exchange"] == _RETRY_EXCHANGE
        assert kwargs["routing_key"] == "crm.facturatie.user.updated.retry"
        headers = kwargs["properties"].headers
        assert headers["x-retry-count"] == 1
        assert headers["x-error"] == "missing-CRM_ID__c"
        assert headers["x-missing-CRM_ID__c"] == "uuid-123"
        assert headers["x-retry-queue"] == "crm.facturatie.user.updated"
        message.ack.assert_awaited_once()
        message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_generic_exception_publishes_to_retry_with_error_class(self, message):
        exc = RuntimeError("boom")

        await _handle_failure(
            "FacturatieUserUpdated", message, exc,
            work_queue="crm.facturatie.user.updated",
        )

        message.channel.basic_publish.assert_awaited_once()
        headers = message.channel.basic_publish.await_args.kwargs["properties"].headers
        assert headers["x-retry-count"] == 1
        assert headers["x-error"] == "processing-error"
        assert headers["x-error-class"] == "RuntimeError"
        assert headers["x-error-message"] == "boom"
        message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_increment_retry_count_from_existing_header(self, message):
        message.headers = {"x-retry-count": 3}

        await _handle_failure(
            "FacturatieUserUpdated", message, RuntimeError("still broken"),
            work_queue="crm.facturatie.user.updated",
        )

        headers = message.channel.basic_publish.await_args.kwargs["properties"].headers
        assert headers["x-retry-count"] == 4

    @pytest.mark.asyncio
    async def test_processing_error_max_retries_publishes_to_dlq(self, message, caplog):
        message.headers = {"x-retry-count": _MAX_REQUEUE_ATTEMPTS}

        with caplog.at_level(logging.ERROR):
            await _handle_failure(
                "FacturatieUserUpdated", message, RuntimeError("persistent"),
                work_queue="crm.facturatie.user.updated",
            )

        kwargs = message.channel.basic_publish.await_args.kwargs
        assert kwargs["exchange"] == _DLQ_EXCHANGE
        assert kwargs["routing_key"] == "crm.facturatie.user.updated.dlq"
        message.ack.assert_awaited_once()
        assert "max retries" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_missing_dependency_max_retries_publishes_to_dlq(self, message, caplog):
        message.headers = {"x-retry-count": _MAX_DEFERRAL_ATTEMPTS}
        exc = MissingDependencyError("CRM_ID__c", "uuid-123")

        with caplog.at_level(logging.ERROR):
            await _handle_failure(
                "FacturatieUserUpdated", message, exc,
                work_queue="crm.facturatie.user.updated",
            )

        kwargs = message.channel.basic_publish.await_args.kwargs
        assert kwargs["exchange"] == _DLQ_EXCHANGE
        headers = kwargs["properties"].headers
        assert headers["x-error"] == "missing-CRM_ID__c"
        assert headers["x-missing-CRM_ID__c"] == "uuid-123"

    @pytest.mark.asyncio
    async def test_rate_limit_sleeps_then_publishes_to_dlq(self, message):
        # is_rate_limit_error checks for REQUEST_LIMIT_EXCEEDED in the message text.
        exc = RuntimeError("Salesforce: REQUEST_LIMIT_EXCEEDED TotalRequests Limit exceeded.")

        with patch("src.handlers._transport.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await _handle_failure(
                "FacturatieUserUpdated", message, exc,
                work_queue="crm.facturatie.user.updated",
            )

        mock_sleep.assert_awaited_once_with(60)
        kwargs = message.channel.basic_publish.await_args.kwargs
        assert kwargs["exchange"] == _DLQ_EXCHANGE
        headers = kwargs["properties"].headers
        assert headers["x-error"] == "rate-limit-dropped"
        assert headers["x-error-class"] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_delivery_mode_propagates_to_retry_publish(self, message):
        message.delivery_mode = 2

        await _handle_failure(
            "FacturatieUserUpdated", message, RuntimeError("boom"),
            work_queue="crm.facturatie.user.updated",
        )

        properties = message.channel.basic_publish.await_args.kwargs["properties"]
        assert properties.delivery_mode == 2

    @pytest.mark.asyncio
    async def test_long_error_message_is_truncated(self, message):
        long_msg = "x" * 1000
        await _handle_failure(
            "FacturatieUserUpdated", message, RuntimeError(long_msg),
            work_queue="crm.facturatie.user.updated",
        )

        headers = message.channel.basic_publish.await_args.kwargs["properties"].headers
        assert len(headers["x-error-message"]) == 512
