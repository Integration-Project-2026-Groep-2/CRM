"""Queue → handler dispatch registry.

Single source of truth for inbound queue configuration. Maps each queue to
its handler, routing key (when the consumer-prefixed queue name differs
from the producer's event name), whether the handler needs a Salesforce
client injected, the queue durability flag, and the topic exchange the
queue binds to.

run_receiver iterates over ACTIVE_REGISTRY to wire all consumers in a
single data-driven loop. EXCHANGE_REGISTRY (includes pending/commented
handlers) feeds `_INBOUND_EXCHANGE` in receiver.py for integration tests
that assert binding infra independent of whether a handler is activated.
"""

from typing import Awaitable, Callable

from src.handlers import (
    controlroom_warning_issued,
    facturatie_company_created,
    facturatie_company_deactivated,
    facturatie_company_updated,
    facturatie_user_created,
    facturatie_user_deactivated,
    facturatie_user_updated,
    frontend_company_created,
    frontend_registration_created,
    frontend_registration_updated,
    iot_badge_linked,
    kassa_payment_confirmed,
    kassa_person_lookup_requested,
    kassa_unpaid_requested,
    kassa_user_created,
    kassa_user_deactivated,
    kassa_user_updated,
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

# Tuple: (queue_name, handler, requires_sf, routing_key_override, durable, exchange_name)
QUEUE_REGISTRY: list[tuple[str, HandlerFn, bool, str | None, bool, str | None]] = [
    # Contract 9 — Controlroom → CRM: system warning (no SF needed)
    ("controlroom.warning.issued", controlroom_warning_issued.handle, False, None, False, "planning.topic"),

    # Contract 1/2 — Frontend → CRM: registration created/updated (consumer-prefixed queues)
    ("crm.frontend.registration.created", frontend_registration_created.handle, True,
     "frontend.registration.created", True, "user.topic"),
    ("crm.frontend.registration.updated", frontend_registration_updated.handle, True,
     "frontend.registration.updated", True, "user.topic"),

    # Contract 3 — Frontend → CRM: company created
    ("crm.frontend.company.created", frontend_company_created.handle, True,
     "frontend.company.created", True, "user.topic"),

    # Contracts 24/25/26 — Facturatie → CRM: user sync (consumer-prefixed queues)
    ("crm.facturatie.user.created", facturatie_user_created.handle, True,
     "facturatie.user.created", True, "user.topic"),
    ("crm.facturatie.user.updated", facturatie_user_updated.handle, True,
     "facturatie.user.updated", True, "user.topic"),
    ("crm.facturatie.user.deactivated", facturatie_user_deactivated.handle, True,
     "facturatie.user.deactivated", True, "user.topic"),

    # Contracts 33/34/35 — Facturatie → CRM: company sync (company.topic exchange)
    ("crm.facturatie.company.created", facturatie_company_created.handle, True,
     "facturatie.company.created", True, "company.topic"),
    ("crm.facturatie.company.updated", facturatie_company_updated.handle, True,
     "facturatie.company.updated", True, "company.topic"),
    ("crm.facturatie.company.deactivated", facturatie_company_deactivated.handle, True,
     "facturatie.company.deactivated", True, "company.topic"),

    # Contracts 27/28/29 — Mailing → CRM: user sync (consumer-prefixed queues)
    ("crm.mailing.user.created", mailing_user_created.handle, True,
     "mailing.user.created", True, "user.topic"),
    ("crm.mailing.user.updated", mailing_user_updated.handle, True,
     "mailing.user.updated", True, "user.topic"),
    ("crm.mailing.user.deactivated", mailing_user_deactivated.handle, True,
     "mailing.user.deactivated", True, "user.topic"),

    # Contracts 30/31/32 — Planning → CRM: user sync (consumer-prefixed queues)
    ("crm.planning.user.created", planning_user_created.handle, True,
     "planning.user.created", True, "user.topic"),
    ("crm.planning.user.updated", planning_user_updated.handle, True,
     "planning.user.updated", True, "user.topic"),
    ("crm.planning.user.deactivated", planning_user_deactivated.handle, True,
     "planning.user.deactivated", True, "user.topic"),

    # Contracts 10a/16/17a — Kassa → CRM
    ("kassa.person.lookup.requested", kassa_person_lookup_requested.handle, True, None, True, "payment.topic"),
    ("kassa.payment.confirmed", kassa_payment_confirmed.handle, True, None, True, "payment.topic"),
    ("kassa.unpaid.requested", kassa_unpaid_requested.handle, True, None, True, "payment.topic"),

    # Contracts 36/37/38 — Kassa → CRM: user sync (consumer-prefixed queues)
    ("crm.kassa.user.created", kassa_user_created.handle, True,
     "kassa.user.created", True, "user.topic"),
    ("crm.kassa.user.updated", kassa_user_updated.handle, True,
     "kassa.user.updated", True, "user.topic"),
    ("crm.kassa.user.deactivated", kassa_user_deactivated.handle, True,
     "kassa.user.deactivated", True, "user.topic"),

    # Contract 11 — Planning → CRM: session update
    ("planning.session.updated", planning_session_updated.handle, True, None, True, "planning.topic"),
    ("iot.badge.linked", iot_badge_linked.handle, True, None, True, "planning.topic"),
]


# Queue → exchange mapping for pending/unimplemented contracts. Used by
# _INBOUND_EXCHANGE consumers (integration tests) that want the full infra
# picture without regard to handler-activation state. Active queues also
# appear in this dict via PENDING_EXCHANGES + the QUEUE_REGISTRY-derived
# computed dict in receiver.py.
PENDING_EXCHANGES: dict[str, str] = {
    # Contract 5a — Facturatie → CRM: request company data (no handler yet)
    "facturatie.company.requested": "invoice.topic",
    # Contract 20 — Mailing → CRM: bounce reported (R2, no handler yet)
    "mailing.bounce.reported": "mail.topic",
}
