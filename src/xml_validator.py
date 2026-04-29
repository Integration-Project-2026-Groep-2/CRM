"""XML validation against XSD schema using lxml."""

import logging
from pathlib import Path

from lxml import etree

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema" / "crm-schema-v1.xsd"
KASSA_SCHEMA_PATH = Path(__file__).parent / "schema" / "contracts" / "kassa-user.xsd"

_schema: etree.XMLSchema | None = None
_kassa_schema: etree.XMLSchema | None = None

# Hardened parser for untrusted inbound XML.
#
# Defaults on lxml's standard parser block external entity resolution but still
# expand internal DTD entities — enabling a "billion-laughs"-style memory
# amplification where `<!ENTITY big "AAAA...">` followed by `&big;` in the
# payload body produces megabytes of text that passes XSD validation and then
# flows into Salesforce field writes.
#
# Settings:
#   resolve_entities=False  — refuse to expand any entity reference (&foo;)
#   no_network=True         — never fetch external resources
#   load_dtd=False          — ignore any <!DOCTYPE ...> declarations
#   huge_tree=False         — enforce libxml2's default tree-size limit
#
# We accept that well-formed payloads without DOCTYPE declarations validate
# identically; producers in this project never emit DTDs.
_SECURE_PARSER = etree.XMLParser(
    resolve_entities=False,
    no_network=True,
    load_dtd=False,
    huge_tree=False,
)


def _strip_element_namespaces(root: etree._Element) -> etree._Element:
    """Normalize an XML tree to local names so no-namespace XSDs can validate it.

    Frontend occasionally sends payloads with a default namespace on the root
    (e.g. ``xmlns=\"urn:frontend:crm:contract\"``), while our schema is defined
    without a targetNamespace. This helper strips element namespaces before
    validation so both forms remain accepted.
    """
    for element in root.iter():
        if isinstance(element.tag, str) and element.tag.startswith("{"):
            element.tag = element.tag.split("}", 1)[1]
    return root


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


def _get_kassa_schema() -> etree.XMLSchema:
    """Return the cached Kassa producer schema."""
    global _kassa_schema  # noqa: PLW0603
    if _kassa_schema is None:
        _kassa_schema = load_schema(KASSA_SCHEMA_PATH)
    return _kassa_schema


def validate(xml_bytes: bytes) -> etree._Element:
    """Validate XML bytes against the CRM XSD schema.

    Args:
        xml_bytes: Raw XML content.

    Returns:
        Parsed XML element tree on success.

    Raises:
        ValueError: If validation fails, with the schema error log.
        etree.XMLSyntaxError: If the XML is malformed or contains entity
            references when entity resolution is disabled.
    """
    schema = _get_schema()
    doc = etree.fromstring(xml_bytes, _SECURE_PARSER)
    doc = _strip_element_namespaces(doc)
    if not schema.validate(doc):
        raise ValueError(f"XML validation failed: {schema.error_log}")
    return doc


def validate_kassa(xml_bytes: bytes) -> etree._Element:
    """Validate XML bytes against the Kassa producer XSD schema."""
    schema = _get_kassa_schema()
    doc = etree.fromstring(xml_bytes, _SECURE_PARSER)
    if not schema.validate(doc):
        raise ValueError(f"XML validation failed: {schema.error_log}")
    return doc
