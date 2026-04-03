# RabbitMQ Exchanges — Integratieproject 2026

_Bron: ClickUp > Teams > Team CRM > Documentatie CRM > XML Contracts (AsyncAPI v1.7.0)_

---

## Overzicht

Het Infra-team beheert 6 **topic exchanges** op de centrale RabbitMQ broker. Daarnaast zijn er 2 speciale exchanges.

## Topic Exchanges

| Exchange | Eigenaar | Consumers |
|---|---|---|
| `user.topic` | Users | Frontend, Kassa, CRM, Planning, ... |
| `planning.topic` | Planning | — |
| `payment.topic` | Kassa | — |
| `invoice.topic` | Facturatie | — |
| `contact.topic` | CRM | — |
| `mail.topic` | Mailing | — |

## Speciale Exchanges

| Exchange | Type | Durable | Eigenaar | Opmerking |
|---|---|---|---|---|
| `heartbeat.direct` | direct | true | CRM | Routing key: `routing.heartbeat`. Elke 1 seconde heartbeat (Contract 7). |
| `crm.user.conflict` | fanout | true | CRM | Contract 15. Dubbele inschrijving detectie (R2). |

### Fanout binding: `crm.user.conflict`

Controlroom en Frontend moeten elk een **eigen queue** aanmaken en binden aan de fanout exchange. Zonder binding ontvangen ze geen berichten.

```python
# Voorbeeld consumer-side binding
queue = await channel.declare_queue("controlroom.user.conflict")
exchange = await channel.declare_exchange("crm.user.conflict", type=ExchangeType.FANOUT, durable=True)
await queue.bind(exchange)
```

## Queue-naar-Exchange Mapping

### CRM Outbound → `contact.topic`

Alle CRM outbound berichten gaan via `contact.topic` met `routing_key=queue_name`.

| Queue | Richting | Contract | Release |
|---|---|---|---|
| `crm.user.confirmed` | CRM → consumers | 13 | R1 |
| `crm.user.updated` | CRM → consumers | 18 | R2 |
| `crm.user.deactivated` | CRM → consumers | 22 | R3 |
| `crm.company.confirmed` | CRM → consumers | 14 | R1 |
| `crm.company.responded` | CRM → Facturatie | 5b | R1 |
| `crm.company.updated` | CRM → consumers | 19 | R2 |
| `crm.company.deactivated` | CRM → consumers | 23 | R3 |
| `crm.person.lookup.responded` | CRM → Kassa | 10b | R1 |
| `crm.unpaid.responded` | CRM → Kassa | 17b | R1 |
| `crm.mail.requested` | CRM → Mailing | 6 | R1 |
| `crm.invoice.requested` | CRM → Facturatie | 21 | R3 |
| `crm.status.checked` | CRM → Controlroom | 8 | R1 |

### CRM Inbound — per exchange van het producerende team

| Queue | Exchange | Producent | Contract |
|---|---|---|---|
| `frontend.registration.created` | `user.topic` | Frontend | 1 |
| `frontend.registration.updated` | `user.topic` | Frontend | 2 |
| `frontend.company.created` | `user.topic` | Frontend | 3 |
| `facturatie.user.created` | `user.topic` | Facturatie | 24 |
| `facturatie.user.updated` | `user.topic` | Facturatie | 25 |
| `facturatie.user.deactivated` | `user.topic` | Facturatie | 26 |
| `mailing.user.created` | `user.topic` | Mailing | 27 |
| `mailing.user.updated` | `user.topic` | Mailing | 28 |
| `mailing.user.deactivated` | `user.topic` | Mailing | 29 |
| `kassa.person.lookup.requested` | `payment.topic` | Kassa | 10a |
| `kassa.payment.confirmed` | `payment.topic` | Kassa | 16 |
| `kassa.unpaid.requested` | `payment.topic` | Kassa | 17a |
| `facturatie.company.requested` | `invoice.topic` | Facturatie | 5a |
| `planning.session.updated` | `planning.topic` | Planning | 11 |
| `controlroom.warning.issued` | `planning.topic` | Controlroom | 9 |
| `iot.badge.linked` | `planning.topic` | IoT | 12 |
| `mailing.bounce.reported` | `mail.topic` | Mailing | 20 |

**Contract 24 runtime-gedrag:** Bij een uniek bestaand Contact hergebruikt CRM dat Contact en kent zo nodig eerst een CRM UUID toe. Deze flow publiceert geen `crm.mail.requested`.
**Contracten 27-28 inbound-vereisten:** Mailing user sync gebruikt verplichte velden `id`, `email` en `gdprConsent`; `firstName`, `lastName` en `companyId` blijven optioneel volgens de Mailing XSD.

> Dezelfde exchange delen betekent **niet** dat berichten automatisch compatibel zijn. CRM moet elke routing key expliciet binden en de payload moet overeenkomen met het contract dat de receiver valideert.

### Speciale exchanges (apart van topic routing)

| Item | Exchange | Contract |
|---|---|---|
| Heartbeat | `heartbeat.direct` (direct, routing key: `routing.heartbeat`) | 7 |
| User conflict | `crm.user.conflict` (fanout) | 15 |

---

## Referenties

- ClickUp: Teams > Team CRM > Documentatie CRM > XML Contracts (AsyncAPI v1.7.0)
- Formele AsyncAPI specificatie: `docs/crm-asyncapi-v1.yaml`
- XSD schema: `src/schema/crm-schema-v1.xsd`
