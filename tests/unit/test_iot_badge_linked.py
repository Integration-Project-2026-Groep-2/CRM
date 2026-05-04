from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from lxml import etree


VALID_BADGE_LINK_XML = b"""<?xml version='1.0' encoding='utf-8'?>
<BadgeLink>
    <badgeId>BADGE-C12-001</badgeId>
    <contactEmail>visitor@example.com</contactEmail>
    <linkedAt>2026-04-22T09:30:00Z</linkedAt>
</BadgeLink>"""


def _make_message(body: bytes) -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.ack = AsyncMock()
    msg.reject = AsyncMock()
    return msg


class TestIotBadgeLinkedHandler:
    @pytest.mark.asyncio
    async def test_updates_contact_badge_code_and_acks(self):
        parsed_xml = etree.fromstring(VALID_BADGE_LINK_XML)
        updated_contact = {
            "Id": "003000000000012",
            "Email": "visitor@example.com",
            "Badge_Code__c": "BADGE-C12-001",
        }

        with (
            patch("src.handlers.iot_badge_linked.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.iot_badge_linked.update_contact_badge_code_by_email",
                new_callable=AsyncMock,
                return_value=updated_contact,
            ) as mock_update,
        ):
            from src.handlers.iot_badge_linked import handle

            msg = _make_message(VALID_BADGE_LINK_XML)
            await handle(msg, MagicMock())

        mock_update.assert_awaited_once_with(
            ANY,
            email="visitor@example.com",
            badge_code="BADGE-C12-001",
        )
        msg.ack.assert_awaited_once()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_contact_is_acked_without_retry(self):
        parsed_xml = etree.fromstring(VALID_BADGE_LINK_XML)
        with (
            patch("src.handlers.iot_badge_linked.xml_validator.validate", return_value=parsed_xml),
            patch(
                "src.handlers.iot_badge_linked.update_contact_badge_code_by_email",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            from src.handlers.iot_badge_linked import handle

            msg = _make_message(VALID_BADGE_LINK_XML)
            await handle(msg, MagicMock())

        msg.ack.assert_awaited_once()
        msg.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_xml_is_rejected_without_update(self):
        with (
            patch("src.handlers.iot_badge_linked.xml_validator.validate", side_effect=ValueError("bad xml")),
            patch(
                "src.handlers.iot_badge_linked.update_contact_badge_code_by_email",
                new_callable=AsyncMock,
            ) as mock_update,
        ):
            from src.handlers.iot_badge_linked import handle

            msg = _make_message(b"not xml")
            await handle(msg, MagicMock())

        mock_update.assert_not_awaited()
        msg.reject.assert_awaited_once_with(requeue=False)
        msg.ack.assert_not_called()


def test_contract_12_is_active_in_receiver_registry():
    from src.handlers.iot_badge_linked import handle
    from src.handlers._registry import PENDING_EXCHANGES, QUEUE_REGISTRY

    c12 = [
        entry for entry in QUEUE_REGISTRY
        if entry[0] == "iot.badge.linked"
    ]

    assert c12 == [("iot.badge.linked", handle, True, None, True, "planning.topic")]
    assert "iot.badge.linked" not in PENDING_EXCHANGES
