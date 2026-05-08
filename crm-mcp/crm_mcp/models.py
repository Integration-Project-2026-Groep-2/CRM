"""Pydantic models for CRM MCP tool inputs and outputs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ---- Contact-related ----


class ContactSummary(BaseModel):
    """Lightweight contact info for search results and activity feeds."""

    id: str = Field(description="Salesforce 18-character Contact Id")
    name: str
    email: str | None = None
    is_active: bool
    last_modified_at: datetime | None = None


class ContactDetails(BaseModel):
    """Complete contact info from Salesforce, with account link and key flags."""

    id: str
    name: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    is_active: bool
    role: Literal["VISITOR", "COMPANY_CONTACT"] | None = None
    gdpr_consent: bool = False
    paid_at: datetime | None = None
    account_id: str | None = None
    account_name: str | None = None
    created_at: datetime
    last_modified_at: datetime


class ContactCount(BaseModel):
    """Aggregated contact counts with breakdowns."""

    total: int
    by_role: dict[str, int] = Field(
        description="Counts per Role__c picklist value plus 'UNKNOWN' for missing"
    )
    by_active: dict[str, int] = Field(description="Counts split into 'active' and 'inactive'")


# ---- Company-related (Salesforce Account) ----


class CompanySummary(BaseModel):
    id: str = Field(description="Salesforce 18-character Account Id")
    name: str
    vat_number: str | None = None
    country: str | None = Field(default=None, description="ISO 3166-1 alpha-2")


class CompanyDetails(BaseModel):
    id: str
    name: str
    vat_number: str | None = None
    street: str | None = None
    city: str | None = None
    postal_code: str | None = None
    country: str | None = Field(default=None, description="ISO 3166-1 alpha-2")
    is_active: bool = True
    created_at: datetime
    last_modified_at: datetime


# ---- Registration-related (Session_Registration__c) ----


class RegistrationCount(BaseModel):
    """Aggregated session-registration counts."""

    total: int
    paid: int
    unpaid: int


# ---- Mutation results (write-tools) ----


class MutationResult(BaseModel):
    """Result of a CRUD write-tool: SF-write outcome plus broadcast routing-key."""

    id: str = Field(description="CRM_ID__c UUID v4 stamped on the record")
    success: bool
    routing_key: str = Field(
        description="RabbitMQ routing-key of the broadcast event (e.g. 'crm.company.confirmed')"
    )
    salesforce_id: str | None = Field(
        default=None,
        description="Salesforce 18-character Id (Account or Contact) post-upsert",
    )
