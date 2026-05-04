"""Contract LogEvent — CRM service-logs naar Controlroom via logs.direct.

Pipeline:
    logger.info(...)  ──▶  RabbitMQLogHandler.emit()    (sync)
                              │
                              │ build XML, validate XSD, put_nowait
                              ▼
                       asyncio.Queue (bounded)
                              │
                              ▼
                       run_log_publisher()              (async worker task)
                              │
                              ▼
                       sender.publish_log_event_raw()
                              │
                              ▼
                       logs.direct ─[rk: routing.log]─▶ controlroom.logs.queue (Controlroom)

Bootstrap-volgorde in main.py garandeert dat connectie-fouten niet via deze
handler proberen te publiceren — attach_rabbitmq_log_handler wordt pas
aangeroepen nadat de RabbitMQ-connectie staat. Tot die tijd loopt alles via
de stdout StreamHandler die setup_logging() installeert.
"""

import asyncio
import contextvars
import logging
import re
from datetime import datetime, timezone

from aio_pika.abc import AbstractRobustConnection
from lxml import etree

from src import sender, xml_validator

_module_logger = logging.getLogger(__name__)
_module_logger.propagate = False

_LOG_QUEUE_MAXSIZE = 10000

# Logger-name prefixes whose records MUST NOT be forwarded to logs.direct.
# Forwarding them creates an infinite amplification loop because:
#   - src.logging_rabbitmq itself logs publish failures
#   - src.sender logs at DEBUG after every successful publish (incl. our own)
#   - aio_pika / aiormq log channel/connection events during publish
# Each of those records would, without this guard, generate yet another
# LogEvent which generates another debug log which generates another LogEvent.
_RECURSION_BLOCKED_PREFIXES: tuple[str, ...] = (
    "src.logging_rabbitmq",
    "src.sender",
    "aio_pika",
    "aiormq",
    "pamqp",
)

_log_queue: asyncio.Queue[bytes] | None = None
_dropped_count: int = 0

_in_emit: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "logging_rabbitmq_in_emit", default=False
)

_PY_TO_XSD_LEVEL: dict[str, str] = {
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "WARNING": "WARN",
    "ERROR": "ERROR",
    "CRITICAL": "FATAL",
}

# XML 1.0 forbids most C0 control chars in text content. lxml enforces this:
# `etree.SubElement(...).text = "\x00"` raises ValueError, which our emit()
# would silently catch and drop the record via handleError(). Tracebacks
# from third-party libs occasionally contain such bytes (terminal escape
# sequences, mangled UTF-8, raw bytes from binary SF fields). We strip
# them defensively — \t \n \r are kept because they are legal XML chars
# and preserve traceback readability for Controlroom / Logstash / Kibana.
_ILLEGAL_XML_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _build_log_event_xml(record: logging.LogRecord, service_name: str) -> bytes:
    """Build a LogEvent XML message from a logging.LogRecord.

    Maps Python levelname → SeverityType enum (DEBUG/INFO/WARN/ERROR/FATAL/PANIC).
    Unknown levelnames pass through; XSD validation rejects invalid values.

    Serialization notes
    -------------------
    The ``<data>`` element is CDATA-wrapped because the field carries
    arbitrary text (log message + optional traceback) which can include
    real ``\\n`` newlines, literal ``"`` characters and entity-needing
    chars like ``<lambda>``. Without CDATA, lxml entity-escapes ``<`` and
    ``&`` while leaving ``"`` and newlines literal — a combination that
    Logstash's xml codec and some Java SAX parsers handle inconsistently
    (Controlroom reported "unparseable log body" on traceback records).
    CDATA forces a single verbatim text node which every conformant XML
    parser materialises identically. XSD-wise CDATA is equivalent to
    ``xs:string`` so the contract semantics post-parse are unchanged.

    XML-1.0-illegal control chars in ``data`` are stripped before they
    reach lxml — otherwise a single ``\\x00`` (e.g. from a mangled UTF-8
    byte) raises ``ValueError`` deep inside ``etree.SubElement.text =``
    and ``emit()`` silently drops the record.

    The literal sequence ``]]>`` cannot appear inside a CDATA section —
    ``etree.CDATA(text)`` raises ``ValueError`` if it does, which would
    then be caught by emit() and silently drop the record (the very
    failure mode we set out to fix). Triggers in real logs: XML payloads,
    regex patterns containing ``]]>``, or stack traces from XML/HTML
    parser libs. We neutralise the sequence by inserting a single space:
    ``]]>`` → ``]] >``. Minimal visual mutation, no content loss, no drop.
    """
    level = _PY_TO_XSD_LEVEL.get(record.levelname, record.levelname)
    timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    data = record.getMessage()
    if record.exc_info:
        data = f"{data}\n{logging.Formatter().formatException(record.exc_info)}"

    # Defence-in-depth — see Serialization notes above.
    data = _ILLEGAL_XML_CHARS_RE.sub("", data)

    # CDATA cannot contain the terminator sequence `]]>`. Insert a space
    # to neutralise it while preserving readability.
    data = data.replace("]]>", "]] >")

    root = etree.Element("LogEvent")
    etree.SubElement(root, "level").text = level
    etree.SubElement(root, "timestamp").text = timestamp
    etree.SubElement(root, "service").text = service_name

    data_element = etree.SubElement(root, "data")
    data_element.text = etree.CDATA(data)

    # lxml's xml_declaration=True emits single-quoted attributes
    # (`<?xml version='1.0' ...?>`) followed by a newline. Some Logstash /
    # Java XML codecs choke on either the single quotes or the inter-element
    # newline. Emit our own double-quoted declaration without trailing newline
    # so the body is a single line: <?xml...?><LogEvent>...</LogEvent>.
    body = etree.tostring(root, xml_declaration=False, encoding="UTF-8")
    return b'<?xml version="1.0" encoding="UTF-8"?>' + body


class RabbitMQLogHandler(logging.Handler):
    """Logging handler that enqueues LogEvent XML for async RabbitMQ publication.

    Sync-side only: builds XML, validates against XSD, and puts on a bounded
    asyncio.Queue. The async run_log_publisher task drains the queue and
    publishes via sender.publish_log_event_raw.

    Recursion-safe via two guards:
      1. Records originating from this module are filtered out — prevents
         self-feedback when sender/aio-pika emit log records during publish.
      2. ContextVar _in_emit short-circuits re-entrant calls within the same
         async/thread context.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        global _dropped_count  # noqa: PLW0603

        if record.name.startswith(_RECURSION_BLOCKED_PREFIXES):
            return
        if _in_emit.get():
            return
        if _log_queue is None:
            return

        token = _in_emit.set(True)
        try:
            try:
                xml_bytes = _build_log_event_xml(record, self._service_name)
                xml_validator.validate(xml_bytes)
            except (ValueError, etree.XMLSyntaxError):
                self.handleError(record)
                return

            try:
                _log_queue.put_nowait(xml_bytes)
            except asyncio.QueueFull:
                _dropped_count += 1
        finally:
            _in_emit.reset(token)


async def run_log_publisher(
    connection: AbstractRobustConnection,
    service_name: str,
) -> None:
    """Drain the log queue and publish each LogEvent via sender.publish_log_event_raw.

    Mirrors run_heartbeat shape — never crashes the loop, recreates resources
    on transport failure, idempotent against multiple calls.

    Args:
        connection: aio-pika robust connection (passed for symmetry with other
            tasks; sender owns the actual channel).
        service_name: Logged for diagnostic context only — XML payload uses the
            service name baked into RabbitMQLogHandler.
    """
    global _dropped_count  # noqa: PLW0603

    if _log_queue is None:
        _module_logger.error(
            "run_log_publisher started before attach_rabbitmq_log_handler — aborting."
        )
        return

    _module_logger.info(
        "Log publisher task started (service=%s, queue maxsize=%d)",
        service_name,
        _LOG_QUEUE_MAXSIZE,
    )

    while True:
        try:
            xml_bytes = await _log_queue.get()
        except asyncio.CancelledError:
            raise

        try:
            await sender.publish_log_event_raw(xml_bytes)
        except asyncio.CancelledError:
            raise
        except Exception:
            _module_logger.exception("Failed to publish LogEvent to logs.direct")

        if _dropped_count > 0:
            dropped = _dropped_count
            _dropped_count = 0
            _module_logger.warning(
                "Dropped %d log records due to bounded queue overflow", dropped
            )


def attach_rabbitmq_log_handler(
    service_name: str,
    min_level: str = "INFO",
) -> RabbitMQLogHandler:
    """Initialise the queue and attach the RabbitMQLogHandler to the root logger.

    Must be called only AFTER the RabbitMQ connection is up and sender.init()
    has succeeded — otherwise emitted records have nowhere to go.

    Args:
        service_name: Value placed in the <service> element of every LogEvent.
        min_level: Minimum Python log level published to RabbitMQ. Distinct
            from the stdout logger level (configured via setup_logging) so
            DEBUG can stay local without flooding Logstash.

    Returns:
        The attached handler — caller can detach it on shutdown if needed.
    """
    global _log_queue  # noqa: PLW0603

    if _log_queue is None:
        _log_queue = asyncio.Queue(maxsize=_LOG_QUEUE_MAXSIZE)

    handler = RabbitMQLogHandler(service_name)
    handler.setLevel(getattr(logging, min_level.upper(), logging.INFO))

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    _module_logger.info(
        "RabbitMQLogHandler attached (service=%s, min_level=%s)",
        service_name,
        min_level.upper(),
    )
    return handler
