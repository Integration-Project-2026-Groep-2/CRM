# RabbitMQ Exchanges — Alle Teams

_Bron: ClickUp docs per team (opgehaald 2026-04-01)_

---

## Exchange Model

Exchanges zijn **per domein**, niet per team. Een team publiceert naar de exchange die past bij het type data, niet naar "zijn eigen" exchange. Voorbeeld: Mailing publiceert user-gerelateerde berichten naar `user.topic`, bounce reports naar `mail.topic`, en heartbeats naar `control_room_topic_exchange`.

## Exchanges

| Exchange | Type | Durable | Domein | Producers |
|---|---|---|---|---|
| `user.topic` | topic | true | User lifecycle | Frontend, Mailing, Facturatie, Planning |
| `contact.topic` | topic | true | CRM master data | CRM |
| `planning.topic` | topic | true | Sessies + monitoring | Planning, Controlroom, IoT |
| `payment.topic` | topic | true | Betalingen | Kassa |
| `invoice.topic` | topic | true | Facturatie | Facturatie |
| `mail.topic` | topic | true | Mailing / bounces | Mailing |
| `company.topic` | topic | true | Bedrijfsdata (Facturatie) | Facturatie |
| `heartbeat.direct` | direct | true | Service heartbeats | CRM, Facturatie, Planning |
| `control_room_topic_exchange` | topic? | true | Monitoring | Mailing |
| `crm.user.conflict` | fanout | true | Duplicate detectie | CRM |

> **Let op:** `company.topic` en `control_room_topic_exchange` staan NIET in de oorspronkelijke Infra exchange lijst (die alleen 6 topic exchanges noemt). Dit zijn ofwel later toegevoegd, ofwel discrepanties.

---

## Per Team — Outbound Routing

### CRM

_Bron: ClickUp > Team CRM > Documentatie CRM > XML Contracts_

| Routing Key | Exchange | Richting |
|---|---|---|
| `crm.user.confirmed` | `contact.topic` | CRM → consumers |
| `crm.user.updated` | `contact.topic` | CRM → consumers |
| `crm.user.deactivated` | `contact.topic` | CRM → consumers |
| `crm.company.confirmed` | `contact.topic` | CRM → consumers |
| `crm.company.responded` | `contact.topic` | CRM → Facturatie |
| `crm.company.updated` | `contact.topic` | CRM → consumers |
| `crm.company.deactivated` | `contact.topic` | CRM → consumers |
| `crm.person.lookup.responded` | `contact.topic` | CRM → Kassa |
| `crm.unpaid.responded` | `contact.topic` | CRM → Kassa |
| `crm.mail.requested` | `contact.topic` | CRM → Mailing |
| `crm.invoice.requested` | `contact.topic` | CRM → Facturatie |
| `crm.status.checked` | `contact.topic` | CRM → Controlroom |
| `crm.user.conflict` | `crm.user.conflict` (fanout) | CRM → Controlroom + Frontend |
| heartbeat | `heartbeat.direct` | CRM → Controlroom |

### Mailing

_Bron: ClickUp > Team Facturatie/Mailing > Doc Mailing (SendGrid) > XML/XSD Contracts_

| Routing Key | Exchange | Richting |
|---|---|---|
| `mailing.bounce.reported` | `mail.topic` | Mailing → CRM |
| `mailing.user.created` | `user.topic` | Mailing → CRM |
| `mailing.user.updated` | `user.topic` | Mailing → CRM |
| `mailing.user.deactivated` | `user.topic` | Mailing → CRM |
| `heartbeat.mailing` | `control_room_topic_exchange` | Mailing → Controlroom |

### Facturatie

_Bron: ClickUp > Team Facturatie/Mailing > Doc Facturatie (FOSSBilling) > XML/XSD Contracts_

| Routing Key | Exchange | Richting |
|---|---|---|
| `facturatie.invoice.finalized` | `invoice.topic` | Facturatie → Mailing |
| `facturatie.company.requested` | `invoice.topic` | Facturatie → CRM |
| `facturatie.user.created` | `user.topic` | Facturatie → CRM |
| `facturatie.user.updated` | `user.topic` | Facturatie → CRM |
| `facturatie.user.deactivated` | `user.topic` | Facturatie → CRM |
| `facturatie.company.created` | `company.topic` | Facturatie → CRM |
| `facturatie.company.updated` | `company.topic` | Facturatie → CRM |
| `facturatie.company.deactivated` | `company.topic` | Facturatie → CRM |

**CRM Contract 24 note:** Bij een uniek bestaand Contact hergebruikt CRM dat Contact en kent zo nodig eerst een CRM UUID toe. Deze flow publiceert geen `crm.mail.requested`.
| `heartbeat.facturatie` | `heartbeat.direct` | Facturatie → Controlroom |

### Planning

_Bron: ClickUp > Team Planning > Analyse: Team Planning > XML contracts_

| Routing Key | Exchange | Richting |
|---|---|---|
| `planning.session.created` | `planning.topic` | Planning → Frontend, Controlroom |
| `planning.session.updated` | `planning.topic` | Planning → CRM, Frontend, Mailing, Controlroom |
| `planning.session.cancelled` | `planning.topic` | Planning → Frontend, Mailing, Controlroom |
| `planning.session.rescheduled` | `planning.topic` | Planning → Frontend, Mailing |
| `planning.session.full` | `planning.topic` | Planning → Frontend, Mailing |
| `planning.session.error` | `planning.topic` | Planning → Controlroom |
| `planning.participant.registered` | `planning.topic` | Planning → Controlroom |
| `planning.user.created` | `user.topic` | Planning → CRM |
| `planning.user.updated` | `user.topic` | Planning → CRM |
| `planning.user.deactivated` | `user.topic` | Planning → CRM |
| `planning.heartbeat` | `heartbeat.direct` | Planning → Controlroom |

### Kassa

_Bron: ClickUp > Team Kassa > Team Kassa - XSD (geen exchange kolom beschikbaar)_

Exchange info afgeleid uit CRM- en Facturatie-docs:

| Routing Key | Exchange | Richting |
|---|---|---|
| `kassa.person.lookup.requested` | `payment.topic` | Kassa → CRM |
| `kassa.payment.confirmed` | `payment.topic` | Kassa → CRM |
| `kassa.unpaid.requested` | `payment.topic` | Kassa → CRM |
| `kassa.invoice.requested` | _(onbekend)_ | Kassa → Facturatie |
| `kassa.closing.finalized` | _(onbekend)_ | Kassa → Facturatie |
| `kassa.transaction.created` | _(onbekend)_ | Kassa → Mailing |
| `kassa.heartbeat` | `heartbeat.direct` (aanname) | Kassa → Controlroom |
| `kassa.status.checked` | _(onbekend)_ | Kassa → Controlroom |

### Frontend

_Geen XML contracts doc gevonden op ClickUp._

Exchange info afgeleid uit CRM-docs:

| Routing Key | Exchange | Richting |
|---|---|---|
| `frontend.registration.created` | `user.topic` | Frontend → CRM |
| `frontend.registration.updated` | `user.topic` | Frontend → CRM |
| `frontend.company.created` | `user.topic` | Frontend → CRM |

### Controlroom

_Geen XML contracts doc gevonden op ClickUp._

| Routing Key | Exchange | Richting |
|---|---|---|
| `controlroom.warning.issued` | `planning.topic` | Controlroom → alle teams |

### IoT

_Geen eigen docs._

| Routing Key | Exchange | Richting |
|---|---|---|
| `iot.badge.linked` | `planning.topic` | IoT → CRM |

---

## Discrepanties

| Item | Probleem |
|---|---|
| `company.topic` | Gebruikt door Facturatie, maar niet in Infra exchange lijst |
| `control_room_topic_exchange` | Gebruikt door Mailing voor heartbeat, maar andere teams gebruiken `heartbeat.direct` |
| Kassa exchange kolom | Ontbreekt in hun ClickUp docs — afgeleid uit CRM/Facturatie docs |
| Frontend docs | Geen XML contracts doc op ClickUp |
| Controlroom docs | Geen XML contracts doc op ClickUp |

---

## Referenties

- ClickUp > Team CRM > Documentatie CRM > XML Contracts (AsyncAPI v1.8.0)
- ClickUp > Team Facturatie/Mailing > Doc Mailing (SendGrid) > XML/XSD Contracts
- ClickUp > Team Facturatie/Mailing > Doc Facturatie (FOSSBilling) > XML/XSD Contracts
- ClickUp > Team Planning > Analyse: Team Planning > XML contracts
- ClickUp > Team Kassa > Team Kassa - XSD
- Infra exchange lijst (screenshot 2026-03-18)
