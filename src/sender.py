"""RabbitMQ publisher — utility module for sending CRM events to other teams.

This module is called by receiver handlers after processing inbound messages.
All outbound publishing goes through these functions — no direct aio_pika
publish calls elsewhere in the codebase.

Implemented contracts:
  Contract 13 — crm.user.confirmed           (durable: true)
  Contract 18 — crm.user.updated             (durable: true)
  Contract 14 — crm.company.confirmed        (durable: true)
  Contract 5b  — crm.company.responded       (durable: false)
  Contract 10b — crm.person.lookup.responded (durable: false)
  Contract 17b — crm.unpaid.responded        (durable: false)
  Contract 6   — crm.mail.requested          (durable: true)
"""

import logging
from datetime import datetime, timezone
from typing import Any

import aio_pika
from aio_pika import DeliveryMode
from aio_pika.abc import AbstractChannel
from lxml import etree

from src import xml_validator

logger = logging.getLogger(__name__)

_channel: AbstractChannel | None = None


async def init(channel: AbstractChannel) -> None:
    """Initialize the sender with a RabbitMQ channel.

    Must be called once at startup before any publish function is used.
    """
    global _channel  # noqa: PLW0603
    _channel = channel
    logger.info("Sender initialized.")


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

async def _publish(queue_name: str, xml_bytes: bytes, persistent: bool = False) -> None:
    """Publish xml_bytes to the given queue via the default exchange.

    Args:
        queue_name: Routing key (= queue name) to publish to.
        xml_bytes:  Validated XML payload as bytes.
        persistent: If True, sets delivery_mode=PERSISTENT so the message
                    survives a broker restart. Use for durable queues.

    The sender never declares queues — the consuming team creates their own.
    """
    delivery_mode = DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT
    await _channel.default_exchange.publish(
        aio_pika.Message(body=xml_bytes, delivery_mode=delivery_mode),
        routing_key=queue_name,
    )
    logger.debug("Message published to queue '%s' (persistent=%s).", queue_name, persistent)


# ---------------------------------------------------------------------------
# Contract 13 — CRM → consumers: user confirmed
# Queue: crm.user.confirmed | durable: true | US-02, US-19
# ---------------------------------------------------------------------------

async def publish_user_confirmed(user_data: dict[str, Any]) -> None:
    """Contract 13 — Publish user confirmation after registration processing.

    Required keys in user_data:
        id, email, firstName, lastName, role, isActive, gdprConsent, confirmedAt
    Optional keys:
        phone, companyId (only if role=COMPANY_CONTACT), badgeCode

    Field order follows XSD xs:sequence exactly:
        id, email, firstName, lastName, [phone], role, [companyId], [badgeCode],
        isActive, gdprConsent, confirmedAt
    """
    root = etree.Element("UserConfirmed")

    # Fields — in XSD xs:sequence order
    etree.SubElement(root, "id").text = str(user_data["id"])
    etree.SubElement(root, "email").text = user_data["email"]
    etree.SubElement(root, "firstName").text = user_data["firstName"]
    etree.SubElement(root, "lastName").text = user_data["lastName"]
    if "phone" in user_data:
        etree.SubElement(root, "phone").text = user_data["phone"]

    etree.SubElement(root, "role").text = user_data["role"]

    if "companyId" in user_data:
        etree.SubElement(root, "companyId").text = str(user_data["companyId"])
    if "badgeCode" in user_data:
        etree.SubElement(root, "badgeCode").text = user_data["badgeCode"]

    etree.SubElement(root, "isActive").text = str(user_data["isActive"]).lower()
    etree.SubElement(root, "gdprConsent").text = str(user_data["gdprConsent"]).lower()
    etree.SubElement(root, "confirmedAt").text = user_data["confirmedAt"]

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.user.confirmed", xml_bytes, persistent=True)


# ---------------------------------------------------------------------------
# Contract 18 — CRM → consumers: user updated
# Queue: crm.user.updated | durable: true | US-21, US-41
# ---------------------------------------------------------------------------

async def publish_user_updated(user_data: dict[str, Any]) -> None:
    """Contract 18 — Publish full user profile after Contact update in Salesforce.

    Consumers replace their local copy entirely — no partial merge.

    Required keys in user_data:
        id, email, firstName, lastName, role, isActive, gdprConsent, updatedAt
    Optional keys:
        phone, companyId (only if role=COMPANY_CONTACT), badgeCode,
        street, houseNumber, postalCode, city, country

    Field order follows XSD xs:sequence exactly:
        id, email, firstName, lastName, [phone], role, [companyId], [badgeCode],
        [street], [houseNumber], [postalCode], [city], [country],
        isActive, gdprConsent, updatedAt
    """
    root = etree.Element("UserUpdated")

    # Required fields — in XSD xs:sequence order
    etree.SubElement(root, "id").text = str(user_data["id"])
    etree.SubElement(root, "email").text = user_data["email"]
    etree.SubElement(root, "firstName").text = user_data["firstName"]
    etree.SubElement(root, "lastName").text = user_data["lastName"]
    if "phone" in user_data:
        etree.SubElement(root, "phone").text = user_data["phone"]

    etree.SubElement(root, "role").text = user_data["role"]

    if "companyId" in user_data:
        etree.SubElement(root, "companyId").text = str(user_data["companyId"])
    if "badgeCode" in user_data:
        etree.SubElement(root, "badgeCode").text = user_data["badgeCode"]

    # Address fields — optional, XSD order: street, houseNumber, postalCode, city, country
    for field in ("street", "houseNumber", "postalCode", "city", "country"):
        if field in user_data:
            etree.SubElement(root, field).text = user_data[field]

    etree.SubElement(root, "isActive").text = str(user_data["isActive"]).lower()
    etree.SubElement(root, "gdprConsent").text = str(user_data["gdprConsent"]).lower()
    etree.SubElement(root, "updatedAt").text = user_data["updatedAt"]

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.user.updated", xml_bytes, persistent=True)


# ---------------------------------------------------------------------------
# Contract 14 — CRM → consumers: company confirmed
# Queue: crm.company.confirmed | durable: true | US-40, US-20
# ---------------------------------------------------------------------------

async def publish_company_confirmed(company_data: dict[str, Any]) -> None:
    """Contract 14 — Publish company confirmation after creation.

    Required keys in company_data:
        id, vatNumber, name, email, isActive, confirmedAt
    """
    root = etree.Element("CompanyConfirmed")

    etree.SubElement(root, "id").text = str(company_data["id"])
    etree.SubElement(root, "vatNumber").text = company_data["vatNumber"]
    etree.SubElement(root, "name").text = company_data["name"]
    etree.SubElement(root, "email").text = company_data["email"]
    etree.SubElement(root, "isActive").text = str(company_data["isActive"]).lower()
    etree.SubElement(root, "confirmedAt").text = company_data["confirmedAt"]

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.company.confirmed", xml_bytes, persistent=True)


# ---------------------------------------------------------------------------
# Contract 5b — CRM → Facturatie: company data response
# Queue: crm.company.responded | durable: false | US-40, US-20
# ---------------------------------------------------------------------------

async def publish_company_responded(request_id: str, company_data: dict[str, Any]) -> None:
    """Contract 5b — Respond to facturatie company data request.

    Correlates with Contract 5a via requestId.

    Required keys in company_data:
        found (bool)
    Optional keys (only if found=True):
        id, name, vatNumber, email, phone,
        street, houseNumber, postalCode, city, country
    """
    root = etree.Element("CompanyResponse")

    etree.SubElement(root, "requestId").text = str(request_id)
    found = company_data["found"]
    etree.SubElement(root, "found").text = str(found).lower()

    if found:
        optional_fields = [
            "id", "name", "vatNumber", "email", "phone",
            "street", "houseNumber", "postalCode", "city", "country",
        ]
        for field in optional_fields:
            if field in company_data:
                etree.SubElement(root, field).text = str(company_data[field])

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.company.responded", xml_bytes, persistent=False)


# ---------------------------------------------------------------------------
# Contract 10b — CRM → Kassa: person lookup response
# Queue: crm.person.lookup.responded | durable: false | US-47
# ---------------------------------------------------------------------------

async def publish_person_lookup_responded(request_id: str, person_data: dict[str, Any]) -> None:
    """Contract 10b — Respond to kassa person lookup request.

    Correlates with Contract 10a via requestId.

    Required keys in person_data:
        found (bool), linkedToCompany (bool — always False if found=False)
    Optional keys (only if found=True):
        id, companyName, companyId (only if linkedToCompany=True)
    """
    root = etree.Element("PersonResponse")

    etree.SubElement(root, "requestId").text = str(request_id)
    found = person_data["found"]
    linked = person_data["linkedToCompany"]

    etree.SubElement(root, "found").text = str(found).lower()
    etree.SubElement(root, "linkedToCompany").text = str(linked).lower()

    if found:
        if "id" in person_data:
            etree.SubElement(root, "id").text = str(person_data["id"])
        if "companyName" in person_data:
            etree.SubElement(root, "companyName").text = person_data["companyName"]
        if linked and "companyId" in person_data:
            etree.SubElement(root, "companyId").text = str(person_data["companyId"])

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.person.lookup.responded", xml_bytes, persistent=False)


# ---------------------------------------------------------------------------
# Contract 17b — CRM → Kassa: unpaid persons response
# Queue: crm.unpaid.responded | durable: false | US-07
# ---------------------------------------------------------------------------

async def publish_unpaid_responded(request_id: str, persons: list[dict[str, Any]]) -> None:
    """Contract 17b — Respond to kassa unpaid persons request.

    Correlates with Contract 17a via requestId.

    Each dict in persons requires:
        id, firstName, lastName, email, linkedToCompany (bool)
    Optional per person:
        companyName (only if linkedToCompany=True)
    """
    root = etree.Element("UnpaidResponse")
    etree.SubElement(root, "requestId").text = str(request_id)

    persons_el = etree.SubElement(root, "persons")
    for person in persons:
        person_el = etree.SubElement(persons_el, "person")
        etree.SubElement(person_el, "id").text = str(person["id"])
        etree.SubElement(person_el, "firstName").text = person["firstName"]
        etree.SubElement(person_el, "lastName").text = person["lastName"]
        etree.SubElement(person_el, "email").text = person["email"]
        linked = person["linkedToCompany"]
        etree.SubElement(person_el, "linkedToCompany").text = str(linked).lower()
        if linked and "companyName" in person:
            etree.SubElement(person_el, "companyName").text = person["companyName"]

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.unpaid.responded", xml_bytes, persistent=False)


# ---------------------------------------------------------------------------
# Contract 6 — CRM → Mailing: participant notification
# Queue: crm.mail.requested | durable: true | US-03, US-22, US-23 (R1), US-24 (R2)
# ---------------------------------------------------------------------------

async def publish_mail_requested(
    mail_type: str, recipient: dict[str, Any], dynamic_data: dict[str, Any]
) -> None:
    """Contract 6 — Request mailing team to send notification.

    One message per recipient — never multiple recipients in one XML.

    Args:
        mail_type:    registration_confirmation | session_change |
                      session_reminder | bulk_event
        recipient:    dict with keys: email, name
        dynamic_data: dict with required key: guest_name
                      Conditionally required (all mailTypes except bulk_event):
                        session_name, session_time

    The <header> is generated automatically with source=CRM and current UTC timestamp.
    """
    root = etree.Element("MailRequest")

    # Header — auto-generated
    header_el = etree.SubElement(root, "header")
    etree.SubElement(header_el, "source").text = "CRM"
    etree.SubElement(header_el, "timestamp").text = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    etree.SubElement(root, "mailType").text = mail_type

    recipient_el = etree.SubElement(root, "recipient")
    etree.SubElement(recipient_el, "email").text = recipient["email"]
    etree.SubElement(recipient_el, "name").text = recipient["name"]

    dynamic_el = etree.SubElement(root, "dynamic_data")
    etree.SubElement(dynamic_el, "guest_name").text = dynamic_data["guest_name"]

    # Conditionally required for all types except bulk_event
    if mail_type != "bulk_event":
        etree.SubElement(dynamic_el, "session_name").text = dynamic_data["session_name"]
        etree.SubElement(dynamic_el, "session_time").text = dynamic_data["session_time"]

    xml_bytes = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    xml_validator.validate(xml_bytes)
    await _publish("crm.mail.requested", xml_bytes, persistent=True)