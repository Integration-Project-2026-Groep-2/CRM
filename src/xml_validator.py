"""XML validation against XSD schema using lxml."""

import logging
from pathlib import Path

from lxml import etree

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema" / "crm-schema-v1.xsd"

_schema: etree.XMLSchema | None = None


def load_schema(path: Path = SCHEMA_PATH) -> etree.XMLSchema:
    """Load and parse an XSD schema file.

    Args:
        path: Path to the .xsd file.

    Returns:
        Compiled XMLSchema instance.

    Raises:
        FileNotFoundError: If the schema file does not exist.
        etree.XMLSchemaParseError: If the schema is invalid.
    """
    if not path.exists():
        raise FileNotFoundError(f"XSD schema not found: {path}")

    schema_doc = etree.parse(str(path))
    schema = etree.XMLSchema(schema_doc)
    logger.info("Loaded XSD schema from %s", path)
    return schema


def _get_schema() -> etree.XMLSchema:
    """Return the cached schema, loading it on first call."""
    global _schema  # noqa: PLW0603
    if _schema is None:
        _schema = load_schema()
    return _schema


def validate(xml_bytes: bytes) -> etree._Element:
    """Validate XML bytes against the CRM XSD schema.

    Args:
        xml_bytes: Raw XML content.

    Returns:
        Parsed XML element tree on success.

    Raises:
        ValueError: If validation fails, with the schema error log.
        etree.XMLSyntaxError: If the XML is malformed.
    """
    schema = _get_schema()
    doc = etree.fromstring(xml_bytes)
    if not schema.validate(doc):
        raise ValueError(f"XML validation failed: {schema.error_log}")
    return doc
