"""RabbitMQ queue consumer — declare queues and dispatch to handlers via registry."""

import asyncio
import logging
from functools import partial

import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from src.config import Config
from src.handlers._registry import PENDING_EXCHANGES, QUEUE_REGISTRY
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

# TTL-DLX failure topology — project-wide convention shared with Lucas (TL
# Facturatie/Mailing) and validated by Control Room. `crm.retry` carries
# delayed redelivery via per-message TTL; `crm.dlq` is the terminal failure
# stream that ops inspects via `crm.dlq.queue`. Queue-args wiring on the
# inbound work-queues themselves is added in a later PR.
_RETRY_EXCHANGE = "crm.retry"
_DLQ_EXCHANGE = "crm.dlq"
_DLQ_OPS_QUEUE = "crm.dlq.queue"


async def _ensure_dlq_topology(channel: AbstractChannel) -> None:
    """Idempotently declare the project-wide retry + DLQ infrastructure.

    Safe to call on every container start — RabbitMQ no-ops the declares when
    the exchange/queue already exists with matching args. The ops-queue uses
    routing-key `#` so any team that adopts the same `<team>.dlq` convention
    gets full coverage without per-contract bindings.
    """
    await channel.declare_exchange(_RETRY_EXCHANGE, ExchangeType.TOPIC, durable=True)
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
    """Declare a queue and bind it to a topic exchange.

    When `routing_key` is omitted, the queue-name is reused as routing key
    (point-to-point queues where the name matches the producer event). Passing
    an explicit routing_key is required when the queue-name is consumer-prefixed
    to avoid collisions while still binding to the producer's event.

    `exchange_name` falls back to the _INBOUND_EXCHANGE lookup so existing
    callers that omit it (e.g. integration tests) continue to work.
    """
    queue = await channel.declare_queue(queue_name, durable=durable)
    resolved_exchange = exchange_name or _INBOUND_EXCHANGE.get(queue_name)
    if resolved_exchange:
        exchange = await channel.declare_exchange(
            resolved_exchange, type=ExchangeType.TOPIC, durable=True,
        )
        effective_routing_key = routing_key or queue_name
        await queue.bind(exchange, routing_key=effective_routing_key)
    return queue


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
        consumer = partial(handler, sf=sf_client) if requires_sf else handler
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
from src.handlers.frontend_registration_created import handle as handle_registration  # noqa: E402, F401
from src.handlers.frontend_registration_updated import handle as handle_registration_updated  # noqa: E402, F401
from src.handlers.kassa_payment_confirmed import handle as handle_payment_confirmed  # noqa: E402, F401
from src.handlers.kassa_person_lookup_requested import handle as handle_person_lookup  # noqa: E402, F401
from src.handlers.kassa_unpaid_requested import handle as handle_unpaid_requested  # noqa: E402, F401
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
    get_contact_match_by_mailing_id,
    get_contact_match_by_planning_id,
    get_unpaid_contacts,
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
