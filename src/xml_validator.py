"""XML validation against XSD schema using lxml."""

import logging
from pathlib import Path

from lxml import etree

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema" / "crm_v1.1.xsd"


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


def validate_xml(xml_bytes: bytes, schema: etree.XMLSchema) -> None:
    """Validate XML bytes against the loaded schema.

    Args:
        xml_bytes: Raw XML content.
        schema: Compiled XSD schema.

    Raises:
        ValueError: If validation fails, with the schema error log.
        etree.XMLSyntaxError: If the XML is malformed.
    """
    doc = etree.fromstring(xml_bytes)
    if not schema.validate(doc):
        raise ValueError(f"XML validation failed: {schema.error_log}")
