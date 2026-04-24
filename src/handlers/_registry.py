"""Queue → handler dispatch registry.

Maps each inbound RabbitMQ queue to the handler that processes it, the
routing key (when the consumer-prefixed queue name differs from the
producer's event name), whether the handler needs a Salesforce client
injected, and the queue durability flag.

run_receiver iterates over QUEUE_REGISTRY to wire all consumers in a
single data-driven loop instead of 19 hand-written consume() calls.
"""

from typing import Awaitable, Callable

import aio_pika

from src.handlers import (
    controlroom_warning_issued,
    facturatie_company_created,
    facturatie_company_deactivated,
    facturatie_company_updated,
    facturatie_user_created,
    facturatie_user_deactivated,
    facturatie_user_updated,
    frontend_registration_created,
    frontend_registration_updated,
    kassa_payment_confirmed,
    kassa_person_lookup_requested,
    kassa_unpaid_requested,
    mailing_user_created,
    mailing_user_deactivated,
    mailing_user_updated,
    planning_session_updated,
    planning_user_created,
    planning_user_deactivated,
    planning_user_updated,
)

# A handler either takes just the message (no SF needed — e.g. handle_warning)
# or message + sf (all the rest). The registry carries `requires_sf` so
# run_receiver can wrap with functools.partial(handler, sf=...) only when
# needed.
HandlerFn = Callable[..., Awaitable[None]]

# Tuple: (queue_name, handler, requires_sf, routing_key_override, durable)
QUEUE_REGISTRY: list[tuple[str, HandlerFn, bool, str | None, bool]] = [
    # Contract 9 — Controlroom → CRM: system warning (no SF needed)
    ("controlroom.warning.issued", controlroom_warning_issued.handle, False, None, False),

    # Contract 1/2 — Frontend → CRM: registration created/updated (consumer-prefixed queues)
    ("crm.frontend.registration.created", frontend_registration_created.handle, True,
     "frontend.registration.created", True),
    ("crm.frontend.registration.updated", frontend_registration_updated.handle, True,
     "frontend.registration.updated", True),

    # Contracts 24/25/26 — Facturatie → CRM: user sync (consumer-prefixed queues)
    ("crm.facturatie.user.created", facturatie_user_created.handle, True,
     "facturatie.user.created", True),
    ("crm.facturatie.user.updated", facturatie_user_updated.handle, True,
     "facturatie.user.updated", True),
    ("crm.facturatie.user.deactivated", facturatie_user_deactivated.handle, True,
     "facturatie.user.deactivated", True),

    # Contracts 33/34/35 — Facturatie → CRM: company sync (company.topic exchange)
    ("crm.facturatie.company.created", facturatie_company_created.handle, True,
     "facturatie.company.created", True),
    ("crm.facturatie.company.updated", facturatie_company_updated.handle, True,
     "facturatie.company.updated", True),
    ("crm.facturatie.company.deactivated", facturatie_company_deactivated.handle, True,
     "facturatie.company.deactivated", True),

    # Contracts 27/28/29 — Mailing → CRM: user sync (consumer-prefixed queues)
    ("crm.mailing.user.created", mailing_user_created.handle, True,
     "mailing.user.created", True),
    ("crm.mailing.user.updated", mailing_user_updated.handle, True,
     "mailing.user.updated", True),
    ("crm.mailing.user.deactivated", mailing_user_deactivated.handle, True,
     "mailing.user.deactivated", True),

    # Contracts 30/31/32 — Planning → CRM: user sync (consumer-prefixed queues)
    ("crm.planning.user.created", planning_user_created.handle, True,
     "planning.user.created", True),
    ("crm.planning.user.updated", planning_user_updated.handle, True,
     "planning.user.updated", True),
    ("crm.planning.user.deactivated", planning_user_deactivated.handle, True,
     "planning.user.deactivated", True),

    # Contracts 10a/16/17a — Kassa → CRM
    ("kassa.person.lookup.requested", kassa_person_lookup_requested.handle, True, None, True),
    ("kassa.payment.confirmed", kassa_payment_confirmed.handle, True, None, True),
    ("kassa.unpaid.requested", kassa_unpaid_requested.handle, True, None, True),

    # Contract 11 — Planning → CRM: session update
    ("planning.session.updated", planning_session_updated.handle, True, None, True),

    # Pending implementations (contracts 3, 5a, 12):
    # ("frontend.company.created", frontend_company_created.handle, True, None, True),
    # ("facturatie.company.requested", facturatie_company_requested.handle, True, None, True),
    # ("iot.badge.linked", iot_badge_linked.handle, True, None, True),
]
