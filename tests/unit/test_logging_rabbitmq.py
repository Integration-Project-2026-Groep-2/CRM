"""Unit tests for src.logging_rabbitmq.

Covers:
- _build_log_event_xml: XML shape, level mapping, XSD validation
- RabbitMQLogHandler: emit, recursion guard, queue overflow, name filter
- run_log_publisher: drains queue, recovery, cancellation
- attach_rabbitmq_log_handler: queue init + handler installation
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from lxml import etree

from src import logging_rabbitmq, xml_validator
from src.logging_rabbitmq import (
    RabbitMQLogHandler,
    _build_log_event_xml,
    attach_rabbitmq_log_handler,
    run_log_publisher,
)


def _make_record(
    *,
    name: str = "src.handlers.foo",
    level: int = logging.INFO,
    msg: str = "hello",
    exc_info=None,
) -> logging.LogRecord:
    """Helper for building LogRecord instances with predictable timestamps."""
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=None,
        exc_info=exc_info,
    )
    record.created = 1746355902.0  # 2025-05-04T10:51:42Z — fixed for assertions
    return record


@pytest.fixture(autouse=True)
def reset_module_state():
    """Reset module-level singletons so each test starts clean."""
    original_queue = logging_rabbitmq._log_queue
    original_dropped = logging_rabbitmq._dropped_count
    logging_rabbitmq._log_queue = None
    logging_rabbitmq._dropped_count = 0

    root = logging.getLogger()
    original_handlers = list(root.handlers)

    yield

    # Tear down: detach test-installed handlers
    for h in list(root.handlers):
        if isinstance(h, RabbitMQLogHandler):
            root.removeHandler(h)
    for h in original_handlers:
        if h not in root.handlers:
            root.addHandler(h)

    logging_rabbitmq._log_queue = original_queue
    logging_rabbitmq._dropped_count = original_dropped


# ---------------------------------------------------------------------------
# _build_log_event_xml
# ---------------------------------------------------------------------------

class TestBuildLogEventXml:

    def test_root_element_is_log_event(self):
        record = _make_record()
        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        assert xml.tag == "LogEvent"

    def test_xml_declaration_uses_double_quotes(self):
        """Some downstream parsers (Logstash xml codec) reject single-quoted
        declarations. Emit double quotes for maximum interoperability.
        """
        record = _make_record()
        xml = _build_log_event_xml(record, "crm")
        assert xml.startswith(b'<?xml version="1.0" encoding="UTF-8"?>')

    def test_declaration_and_root_are_on_one_line(self):
        """No inter-element newline between `?>` and `<LogEvent>` — keeps the
        body a single XML chunk for line-based or streaming parsers.
        """
        record = _make_record()
        xml = _build_log_event_xml(record, "crm")
        assert b'?><LogEvent' in xml
        assert b'?>\n<LogEvent' not in xml

    def test_field_order_matches_xsd_sequence(self):
        record = _make_record()
        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        assert [child.tag for child in xml] == ["level", "timestamp", "service", "data"]

    def test_service_name_propagates(self):
        record = _make_record()
        xml = etree.fromstring(_build_log_event_xml(record, "crm-test"))
        assert xml.findtext("service") == "crm-test"

    def test_message_with_args_is_resolved(self):
        record = logging.LogRecord(
            name="src.handlers.foo",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="user %s registered with id %d",
            args=("jan@example.com", 42),
            exc_info=None,
        )
        record.created = 1746355902.0
        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        assert xml.findtext("data") == "user jan@example.com registered with id 42"

    @pytest.mark.parametrize(
        ("python_level", "expected_xsd_level"),
        [
            (logging.DEBUG, "DEBUG"),
            (logging.INFO, "INFO"),
            (logging.WARNING, "WARN"),
            (logging.ERROR, "ERROR"),
            (logging.CRITICAL, "FATAL"),
        ],
    )
    def test_level_mapping(self, python_level, expected_xsd_level):
        record = _make_record(level=python_level)
        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        assert xml.findtext("level") == expected_xsd_level

    def test_timestamp_is_iso8601_utc(self):
        record = _make_record()
        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        ts = xml.findtext("timestamp")
        assert ts.endswith("Z")
        assert ts == "2025-05-04T10:51:42Z"

    def test_xml_is_xsd_valid(self):
        record = _make_record()
        xml_bytes = _build_log_event_xml(record, "crm")
        xml_validator.validate(xml_bytes)

    def test_exception_info_appended_to_data(self):
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            record = _make_record(exc_info=sys.exc_info())

        xml = etree.fromstring(_build_log_event_xml(record, "crm"))
        data = xml.findtext("data")
        assert "hello" in data
        assert "ValueError" in data
        assert "boom" in data


# ---------------------------------------------------------------------------
# _build_log_event_xml — <data> hardening (CDATA + control-char strip)
# ---------------------------------------------------------------------------

class TestBuildLogEventXmlDataHardening:
    """Structural fix: CDATA-wrapped <data> + XML-1.0-illegal char stripping.

    Controlroom reported "unparseable log body" on traceback records. Root
    cause: lxml entity-escapes `<` and `&` in element text but not `"` or
    newlines, producing a mix that downstream parsers (Logstash xml codec,
    Java SAX) handle inconsistently. Plus any control char in record text
    would silently drop the record (lxml ValueError caught by emit()).
    """

    def test_multiline_traceback_roundtrips(self):
        try:
            raise RuntimeError('error with "quotes" and\nnewlines')
        except RuntimeError:
            import sys
            record = _make_record(exc_info=sys.exc_info())

        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        assert "RuntimeError" in data
        assert '"quotes"' in data
        assert "\n" in data

    def test_literal_double_quotes_survive(self):
        record = _make_record(msg='File "/app/src/polling.py", line 1')
        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        assert data == 'File "/app/src/polling.py", line 1'

    def test_lambda_and_angle_brackets_survive_verbatim_in_cdata(self):
        """CDATA does not entity-escape `<` so `<lambda>` arrives intact."""
        record = _make_record(msg='in <lambda> at <module>')
        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        assert data == "in <lambda> at <module>"

    @pytest.mark.parametrize(
        "ctrl",
        ["\x00", "\x01", "\x08", "\x0B", "\x0C", "\x1F"],
    )
    def test_control_chars_are_stripped_not_dropped(self, ctrl):
        """Without the strip, the record would silently disappear via emit()."""
        record = _make_record(msg=f"before{ctrl}after")
        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        assert data == "beforeafter"

    def test_tab_newline_carriage_return_preserved(self):
        record = _make_record(msg="a\tb\nc d")
        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        assert "a\tb\nc" in data
        assert "d" in data

    def test_data_is_serialized_as_cdata_section(self):
        """Raw bytes contain <![CDATA[...]]> — not entity-escaped."""
        record = _make_record(msg="payload with <tag> and &amp;")
        xml = _build_log_event_xml(record, "crm")
        assert b"<![CDATA[" in xml
        assert b"]]>" in xml
        # Verbatim content inside CDATA, no entity escaping
        assert b"payload with <tag> and &amp;" in xml
        # Sanity: not double-escaped
        assert b"&lt;tag&gt;" not in xml

    def test_xsd_validates_with_cdata_data(self):
        try:
            raise ValueError('boom "x" <y>')
        except ValueError:
            import sys
            record = _make_record(exc_info=sys.exc_info())
        # Must not raise — XSD treats CDATA as xs:string content
        xml_validator.validate(_build_log_event_xml(record, "crm"))

    def test_empty_data_after_strip_does_not_crash(self):
        """All-control-chars input becomes empty <data><![CDATA[]]></data>."""
        record = _make_record(msg="\x00\x01\x02\x1F")
        data = etree.fromstring(_build_log_event_xml(record, "crm")).findtext("data")
        # findtext returns None for empty text in some lxml versions
        assert data in (None, "")

    def test_cdata_terminator_in_data_does_not_drop_record(self):
        """`]]>` would raise ValueError in etree.CDATA — must be neutralised.

        Without the `]]>` → `]] >` replacement, a log message like
        `logger.error("got payload: %s", xml_string)` containing `]]>`
        anywhere would silently drop the record via emit()'s ValueError
        catch-all — the very failure mode CDATA was supposed to fix.
        """
        record = _make_record(msg="payload contains ]]> sequence")
        xml_bytes = _build_log_event_xml(record, "crm")
        # Bytes are well-formed (no premature CDATA termination)
        assert b"<![CDATA[payload contains ]] > sequence]]></data>" in xml_bytes
        # Round-trips cleanly through XSD validation
        xml_validator.validate(xml_bytes)
        # Content survives with single-space mutation
        data = etree.fromstring(xml_bytes).findtext("data")
        assert data == "payload contains ]] > sequence"

    def test_multiple_cdata_terminators_in_data(self):
        """Repeated `]]>` sequences are all neutralised."""
        record = _make_record(msg="]]> first ]]> second ]]> third")
        xml_bytes = _build_log_event_xml(record, "crm")
        xml_validator.validate(xml_bytes)
        data = etree.fromstring(xml_bytes).findtext("data")
        assert data == "]] > first ]] > second ]] > third"
        # No raw `]]>` survives inside the CDATA section
        cdata_start = xml_bytes.index(b"<![CDATA[")
        cdata_end = xml_bytes.index(b"]]></data>")
        assert b"]]>" not in xml_bytes[cdata_start + len(b"<![CDATA["):cdata_end]


# ---------------------------------------------------------------------------
# RabbitMQLogHandler.emit
# ---------------------------------------------------------------------------

class TestRabbitMQLogHandlerEmit:

    def test_emit_enqueues_xml_bytes(self):
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        handler = RabbitMQLogHandler("crm")

        record = _make_record()
        handler.emit(record)

        assert logging_rabbitmq._log_queue.qsize() == 1
        xml = etree.fromstring(logging_rabbitmq._log_queue.get_nowait())
        assert xml.tag == "LogEvent"
        assert xml.findtext("service") == "crm"

    def test_emit_skips_records_from_own_module(self):
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        handler = RabbitMQLogHandler("crm")

        record = _make_record(name="src.logging_rabbitmq")
        handler.emit(record)

        assert logging_rabbitmq._log_queue.qsize() == 0

    @pytest.mark.parametrize(
        "logger_name",
        [
            "src.logging_rabbitmq",
            "src.logging_rabbitmq.publisher",  # sub-logger
            "src.sender",                       # sender debug-logs after each publish → recursion source
            "aio_pika",
            "aio_pika.connection",              # aio-pika sub-logger
            "aiormq",
            "aiormq.channel",
            "pamqp",
        ],
    )
    def test_emit_skips_recursion_prefixes(self, logger_name):
        """Records from publish-pipeline modules MUST NOT be forwarded — anti-amplification."""
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        handler = RabbitMQLogHandler("crm")

        handler.emit(_make_record(name=logger_name))

        assert logging_rabbitmq._log_queue.qsize() == 0

    def test_emit_skips_when_queue_uninitialised(self):
        logging_rabbitmq._log_queue = None
        handler = RabbitMQLogHandler("crm")

        # Must not raise
        handler.emit(_make_record())

    def test_emit_increments_dropped_counter_on_full_queue(self):
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=1)
        handler = RabbitMQLogHandler("crm")

        # Fill the queue
        handler.emit(_make_record())
        assert logging_rabbitmq._dropped_count == 0

        # Second emit cannot enqueue → drop counter ticks up
        handler.emit(_make_record())
        assert logging_rabbitmq._dropped_count == 1

    def test_emit_re_entrancy_guard(self):
        """Setting _in_emit short-circuits the call (defence in depth)."""
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        handler = RabbitMQLogHandler("crm")

        token = logging_rabbitmq._in_emit.set(True)
        try:
            handler.emit(_make_record())
        finally:
            logging_rabbitmq._in_emit.reset(token)

        assert logging_rabbitmq._log_queue.qsize() == 0

    def test_emit_invalid_level_does_not_raise(self):
        """Validation failure (unmapped custom levelname) calls handleError, not bubble."""
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        handler = RabbitMQLogHandler("crm")

        record = _make_record()
        record.levelname = "TRACE"  # not in SeverityType enum

        with patch.object(handler, "handleError") as he:
            handler.emit(record)
            he.assert_called_once_with(record)

        assert logging_rabbitmq._log_queue.qsize() == 0


# ---------------------------------------------------------------------------
# run_log_publisher
# ---------------------------------------------------------------------------

class TestRunLogPublisher:

    @pytest.mark.asyncio
    async def test_drains_queue_and_calls_publish_log_event_raw(self):
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        sample = b"<LogEvent><level>INFO</level></LogEvent>"
        await logging_rabbitmq._log_queue.put(sample)

        with patch("src.logging_rabbitmq.sender.publish_log_event_raw", new=AsyncMock()) as pub:
            # CancelledError on first publish breaks the loop after one iteration.
            pub.side_effect = asyncio.CancelledError()

            mock_connection = MagicMock()
            try:
                await run_log_publisher(mock_connection, "crm")
            except asyncio.CancelledError:
                pass

            pub.assert_any_await(sample)

    @pytest.mark.asyncio
    async def test_continues_loop_when_publish_fails(self):
        logging_rabbitmq._log_queue = asyncio.Queue(maxsize=10)
        await logging_rabbitmq._log_queue.put(b"<LogEvent><level>INFO</level></LogEvent>")
        await logging_rabbitmq._log_queue.put(b"<LogEvent><level>ERROR</level></LogEvent>")

        with patch("src.logging_rabbitmq.sender.publish_log_event_raw", new=AsyncMock()) as pub:
            pub.side_effect = [Exception("transient"), asyncio.CancelledError()]

            mock_connection = MagicMock()
            try:
                await run_log_publisher(mock_connection, "crm")
            except asyncio.CancelledError:
                pass

            assert pub.await_count == 2

    @pytest.mark.asyncio
    async def test_aborts_when_queue_not_initialised(self):
        logging_rabbitmq._log_queue = None
        mock_connection = MagicMock()

        # Should return immediately without raising
        await run_log_publisher(mock_connection, "crm")


# ---------------------------------------------------------------------------
# attach_rabbitmq_log_handler
# ---------------------------------------------------------------------------

class TestAttachRabbitmqLogHandler:

    def test_creates_queue_and_attaches_handler(self):
        assert logging_rabbitmq._log_queue is None

        handler = attach_rabbitmq_log_handler("crm", min_level="INFO")

        assert logging_rabbitmq._log_queue is not None
        assert handler in logging.getLogger().handlers
        assert handler.level == logging.INFO

    def test_min_level_is_respected(self):
        handler = attach_rabbitmq_log_handler("crm", min_level="WARNING")
        assert handler.level == logging.WARNING

    def test_unknown_min_level_falls_back_to_info(self):
        handler = attach_rabbitmq_log_handler("crm", min_level="not-a-level")
        assert handler.level == logging.INFO

    def test_emitting_after_attach_publishes_via_logger_pipeline(self):
        # Default root logger level is WARNING — lower it so INFO records reach handlers.
        root = logging.getLogger()
        original_level = root.level
        root.setLevel(logging.DEBUG)
        try:
            attach_rabbitmq_log_handler("crm-test", min_level="DEBUG")

            logger = logging.getLogger("src.handlers.dummy")
            logger.info("integration-marker")

            assert logging_rabbitmq._log_queue.qsize() >= 1
            xml = etree.fromstring(logging_rabbitmq._log_queue.get_nowait())
            assert xml.findtext("service") == "crm-test"
            assert xml.findtext("level") == "INFO"
            assert "integration-marker" in xml.findtext("data")
        finally:
            root.setLevel(original_level)
