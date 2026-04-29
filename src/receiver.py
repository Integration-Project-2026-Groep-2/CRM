"""RabbitMQ queue consumer — declare queues and dispatch to handlers via registry."""

import asyncio
import logging
from functools import partial
from typing import Any

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from src import xml_validator
from src.config import Config
from src.handlers._registry import PENDING_EXCHANGES, QUEUE_REGISTRY
from src.handlers._transport import _handle_failure
from src.salesforce_client import get_salesforce_client

logger = logging.getLogger(__name__)

# Queue → topic exchange mapping derived from the single source of truth in
# handlers/_registry.py. Active queues come from QUEUE_REGISTRY (6th tuple
# element); pending/unimplemented queues (contracts 3, 5a, 12, 20) come from
# PENDING_EXCHANGES. Kept as a module-level name so integration tests that do
# `from src.receiver import _INBOUND_EXCHANGE` keep working.
_INBOUND_EXCHANGE: dict[str, str] = {
    **{
        queue: exchange
        for queue, _handler, _requires_sf, _rk, _durable, exchange in QUEUE_REGISTRY
        if exchange is not None
    },
    **PENDING_EXCHANGES,
}

# TTL-DLX failure topology — Lucas's pattern (TL Facturatie/Mailing) measured
# on the prod broker on 2026-04-28. The `contact.*` exchanges already exist on
# prod (declared by Lucas/another developer via Management UI) so our declare
# calls are no-ops at runtime; we keep them so the test broker bootstraps the
# same topology.
_RETRY_EXCHANGE = "contact.retry"      # DIRECT — rk-matched per <queue>.retry
_DLQ_EXCHANGE = "contact.dlq"          # TOPIC — bound `#` to crm.dlq.queue
_DLQ_OPS_QUEUE = "crm.dlq.queue"
_RETRY_QUEUE_TTL_MS = 30_000           # Lucas's vaste 30s queue-level TTL

# Queues whose contracts forbid retry semantics. Exceptions raised by these
# handlers are logged and ack'd; nothing flows to retry or DLQ. C9 explicitly
# states "log error, no crash"; retry-fanout would create alarm storms in
# Controlroom.
_NO_RETRY_QUEUES: frozenset[str] = frozenset({"controlroom.warning.issued"})


async def _ensure_dlq_topology(channel: AbstractChannel) -> None:
    """Idempotently declare the contact.* failure topology.

    `contact.retry` is DIRECT (rk-precise), `contact.dlq` is TOPIC so the
    shared `crm.dlq.queue` ops-queue catches every flow via `#` binding. All
    three already exist on prod; declare is a no-op when args match.
    """
    await channel.declare_exchange(_RETRY_EXCHANGE, ExchangeType.DIRECT, durable=True)
    dlq_exchange = await channel.declare_exchange(
        _DLQ_EXCHANGE, ExchangeType.TOPIC, durable=True,
    )
    dlq_queue = await channel.declare_queue(_DLQ_OPS_QUEUE, durable=True)
    await dlq_queue.bind(dlq_exchange, routing_key="#")


async def _declare_and_bind(
    channel: AbstractChannel,
    queue_name: str,
    durable: bool,
    *,
    routing_key: str | None = None,
    exchange_name: str | None = None,
) -> aio_pika.abc.AbstractQueue:
    """Declare a work-queue + sibling retry-queue and bind both.

    The retry-queue (`<queue_name>.retry`) carries a queue-level
    `x-message-ttl=30000` and dead-letters back to the producer-exchange with
    the producer's original routing-key, so a TTL expiry re-delivers the
    message via the existing producer→work-queue binding. This matches
    Lucas's pattern measured on prod (`facturatie.user.confirmed.retry`).
    """
    queue = await channel.declare_queue(queue_name, durable=durable)
    resolved_exchange = exchange_name or _INBOUND_EXCHANGE.get(queue_name)
    if resolved_exchange:
        exchange = await channel.declare_exchange(
            resolved_exchange, type=ExchangeType.TOPIC, durable=True,
        )
        effective_routing_key = routing_key or queue_name
        await queue.bind(exchange, routing_key=effective_routing_key)

        retry_queue = await channel.declare_queue(
            f"{queue_name}.retry",
            durable=durable,
            arguments={
                "x-message-ttl": _RETRY_QUEUE_TTL_MS,
                "x-dead-letter-exchange": resolved_exchange,
                "x-dead-letter-routing-key": effective_routing_key,
            },
        )
        retry_exchange = await channel.declare_exchange(
            _RETRY_EXCHANGE, type=ExchangeType.DIRECT, durable=True,
        )
        await retry_queue.bind(retry_exchange, routing_key=f"{queue_name}.retry")
    return queue


async def _wrap_handler(
    handler: Any,
    queue_name: str,
    message: aio_pika.abc.AbstractIncomingMessage,
) -> None:
    """Run a handler under the centralised failure-routing policy.

    No-retry queues (e.g. C9) ack-and-drop on any exception per contract.
    Other queues route uncaught exceptions through `_handle_failure`; if that
    helper itself raises (channel closed, bug), we last-resort
    `reject(requeue=False)` to keep the message off the work-queue.
    """
    try:
        await handler(message)
        if xml_validator.reorder_was_applied():
            logger.warning(
                "frontend.reorder_applied queue=%s message_id=%s — Frontend payload accepted after element reorder; "
                "producer is out of spec, please notify Frontend team.",
                queue_name,
                message.message_id,
            )
    except Exception as exc:  # noqa: BLE001
        if queue_name in _NO_RETRY_QUEUES:
            logger.error(
                "%s — %s; ack-and-drop (no retry by contract)", queue_name, exc,
            )
            try:
                await message.ack()
            except Exception:  # noqa: BLE001
                pass
            return
        try:
            await _handle_failure(
                queue_name, message, exc, work_queue=queue_name,
            )
        except Exception as meta_exc:  # noqa: BLE001
            logger.exception(
                "%s — meta-failure in _handle_failure: %s; force-rejecting",
                queue_name, meta_exc,
            )
            try:
                await message.reject(requeue=False)
            except Exception:  # noqa: BLE001
                pass


async def run_receiver(
    connection: AbstractRobustConnection,
    config: Config,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Consume configured inbound messages, validate XML, process in Salesforce.

    Iterates over QUEUE_REGISTRY (handlers/_registry.py) and wires each queue
    to its handler. Handlers that need the Salesforce client are wrapped with
    partial(handler, sf=sf_client).

    `shutdown_event` is forwarded to the Salesforce login retry loop so a
    graceful shutdown during a transient Salesforce outage does not hang the
    container.
    """
    channel = await connection.channel()
    await _ensure_dlq_topology(channel)
    sf_client = await get_salesforce_client(config, shutdown_event=shutdown_event)

    for queue_name, handler, requires_sf, routing_key, durable, exchange in QUEUE_REGISTRY:
        queue = await _declare_and_bind(
            channel,
            queue_name,
            durable=durable,
            routing_key=routing_key,
            exchange_name=exchange,
        )
        inner = partial(handler, sf=sf_client) if requires_sf else handler
        consumer = partial(_wrap_handler, inner, queue_name)
        await queue.consume(consumer)

    logger.info("Receiver started. Listening on all configured queues.")
    await asyncio.Future()  # run forever


# Transport re-exports for backward-compat (tests import these from src.receiver).
# Helper re-exports for backward-compat (record-mapping helpers now live in
# src.handlers._helpers and src.salesforce.contacts).
from src.handlers._helpers import (  # noqa: E402, F401
    _build_company_data,
    _build_company_deactivation_data,
    _build_updated_company_data,
)
from src.handlers._transport import (  # noqa: E402, F401
    _MAX_DEFERRAL_ATTEMPTS,
    _MAX_REQUEUE_ATTEMPTS,
    _delivery_attempt_count,
    _exponential_backoff_seconds,
    _handle_out_of_order_deferral,
    _handle_processing_error,
    _republish_with_retry_count,
)

# Handler re-exports for backward-compat: existing test code does
# `from src.receiver import handle_warning` etc. New code should import
# the `handle` function directly from the src.handlers.<event> module.
from src.handlers.controlroom_warning_issued import handle as handle_warning  # noqa: E402, F401
from src.handlers.facturatie_company_created import handle as handle_facturatie_company_created  # noqa: E402, F401
from src.handlers.facturatie_company_deactivated import (  # noqa: E402, F401
    handle as handle_facturatie_company_deactivated,
)
from src.handlers.facturatie_company_updated import handle as handle_facturatie_company_updated  # noqa: E402, F401
from src.handlers.facturatie_user_created import handle as handle_facturatie_user_created  # noqa: E402, F401
from src.handlers.facturatie_user_deactivated import handle as handle_facturatie_user_deactivated  # noqa: E402, F401
from src.handlers.facturatie_user_updated import handle as handle_facturatie_user_updated  # noqa: E402, F401
from src.handlers.frontend_company_created import handle as handle_company_created  # noqa: E402, F401
from src.handlers.frontend_registration_created import handle as handle_registration  # noqa: E402, F401
from src.handlers.frontend_registration_updated import handle as handle_registration_updated  # noqa: E402, F401
from src.handlers.kassa_payment_confirmed import handle as handle_payment_confirmed  # noqa: E402, F401
from src.handlers.kassa_person_lookup_requested import handle as handle_person_lookup  # noqa: E402, F401
from src.handlers.kassa_unpaid_requested import handle as handle_unpaid_requested  # noqa: E402, F401
from src.handlers.kassa_user_created import handle as handle_kassa_user_created  # noqa: E402, F401
from src.handlers.kassa_user_deactivated import handle as handle_kassa_user_deactivated  # noqa: E402, F401
from src.handlers.kassa_user_updated import handle as handle_kassa_user_updated  # noqa: E402, F401
from src.handlers.mailing_user_created import handle as handle_mailing_user_created  # noqa: E402, F401
from src.handlers.mailing_user_deactivated import handle as handle_mailing_user_deactivated  # noqa: E402, F401
from src.handlers.mailing_user_updated import handle as handle_mailing_user_updated  # noqa: E402, F401
from src.handlers.planning_session_updated import handle as handle_session_updated  # noqa: E402, F401
from src.handlers.planning_user_created import handle as handle_planning_user_created  # noqa: E402, F401
from src.handlers.planning_user_deactivated import handle as handle_planning_user_deactivated  # noqa: E402, F401
from src.handlers.planning_user_updated import handle as handle_planning_user_updated  # noqa: E402, F401
from src.salesforce.contacts import (  # noqa: E402, F401
    _build_updated_user_data,
    _build_user_data,
    _build_user_deactivation_data,
)

# Salesforce-client re-exports for backward-compat patches in tests.
from src.salesforce_client import (  # noqa: E402, F401
    apply_account_is_active,
    apply_is_active,
    backfill_mailing_contact_fields,
    backfill_planning_contact_fields,
    count_active_contacts_for_company,
    count_active_session_registrations,
    create_account,
    create_contact,
    deactivate_account_by_crm_id,
    deactivate_account_record,
    deactivate_contact_record,
    deactivate_session_registration,
    ensure_contact_identifiers,
    ensure_session_registration_active,
    get_account_by_crm_id,
    get_account_match_by_crm_id,
    get_account_match_by_email,
    get_active_session_participants,
    get_contact_by_email,
    get_contact_for_person_lookup,
    get_contact_match_by_crm_id,
    get_contact_match_by_email,
    get_contact_match_by_kassa_id,
    get_contact_match_by_mailing_id,
    get_contact_match_by_planning_id,
    get_unpaid_contacts,
    has_contact_kassa_id_field,
    has_contact_mailing_id_field,
    has_contact_planning_id_field,
    has_session_registration_object,
    update_facturatie_account,
    update_facturatie_contact,
    update_mailing_contact,
    update_payment_status,
    update_planning_contact,
    upsert_account_by_vat,
    upsert_contact_by_email,
)
