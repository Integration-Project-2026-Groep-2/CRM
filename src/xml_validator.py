"""XML validation against XSD schema using lxml."""

import logging
from pathlib import Path

from lxml import etree

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schema" / "crm-schema-v1.xsd"
KASSA_SCHEMA_PATH = Path(__file__).parent / "schema" / "contracts" / "kassa-user.xsd"

_schema: etree.XMLSchema | None = None
_kassa_schema: etree.XMLSchema | None = None

_ALLOWED_FRONTEND_NAMESPACE = "urn:frontend:crm:contract"
_ALLOWED_FRONTEND_NAMESPACED_ROOTS = {"Registration", "RegistrationChange"}

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


def _split_tag_namespace(tag: str) -> tuple[str | None, str]:
    """Return (namespace, localname) from an lxml tag name."""
    if tag.startswith("{"):
        namespace, local_name = tag[1:].split("}", 1)
        return namespace, local_name
    return None, tag


def _normalize_frontend_namespace_if_allowed(root: etree._Element) -> etree._Element:
    """Strip only the known frontend namespace for specific inbound roots.

    Our schemas are defined without targetNamespace. To maintain compatibility
    with Frontend payloads that still include a default namespace, we only
    normalize that single known namespace for the two frontend registration
    roots. Any other namespace usage is rejected.
    """
    if not isinstance(root.tag, str):
        return root

    root_namespace, root_local_name = _split_tag_namespace(root.tag)
    if root_namespace is None:
        return root

    if (
        root_namespace != _ALLOWED_FRONTEND_NAMESPACE
        or root_local_name not in _ALLOWED_FRONTEND_NAMESPACED_ROOTS
    ):
        raise ValueError(
            f"XML validation failed: unexpected namespace '{root_namespace}' on root '{root_local_name}'"
        )

    normalized = etree.fromstring(etree.tostring(root), _SECURE_PARSER)
    for element in normalized.iter():
        if not isinstance(element.tag, str):
            continue

        element_namespace, element_local_name = _split_tag_namespace(element.tag)
        if element_namespace is not None:
            if element_namespace != _ALLOWED_FRONTEND_NAMESPACE:
                raise ValueError(
                    "XML validation failed: mixed element namespaces are not allowed"
                )
            element.tag = element_local_name

        for attribute_name in element.attrib:
            if not isinstance(attribute_name, str):
                continue
            attribute_namespace, _ = _split_tag_namespace(attribute_name)
            if attribute_namespace is not None:
                raise ValueError(
                    "XML validation failed: namespaced attributes are not allowed"
                )

    etree.cleanup_namespaces(normalized)
    return normalized


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
    doc = _normalize_frontend_namespace_if_allowed(doc)
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
