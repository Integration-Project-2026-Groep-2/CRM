"""Pydantic models for CRM MCP tool inputs and outputs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ---- Contact-related ----


class ContactSummary(BaseModel):
    """Lightweight contact info for search results and activity feeds."""

    id: str = Field(description="Salesforce 18-character Contact Id")
    crm_id: str | None = Field(
        default=None, description="CRM_ID__c UUID v4 (canonical cross-team id)"
    )
    name: str
    email: str | None = None
    is_active: bool
    last_modified_at: datetime | None = None


class ContactDetails(BaseModel):
    """Complete contact info from Salesforce, with account link and key flags."""

    id: str
    crm_id: str | None = Field(
        default=None, description="CRM_ID__c UUID v4 (canonical cross-team id)"
    )
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


class ContactActivitySummary(BaseModel):
    """Org-wide analytics dashboard for all contacts."""

    total: int
    active: int
    inactive: int
    gdpr_consent: int = Field(description="Contacts with GDPR_Consent__c = true")
    paid: int = Field(description="Contacts with Paid_At__c != null")
    by_role: dict[str, int] = Field(description="Counts per Role__c value plus 'UNKNOWN'")
    new_last_7_days: int = Field(description="Contacts created in the last 7 days")
    modified_last_24h: int = Field(description="Contacts modified in the last 24 hours")


# ---- Company-related (Salesforce Account) ----


class CompanyContactSummary(BaseModel):
    """A contact linked to a specific company, with role and payment info."""

    id: str = Field(description="Salesforce 18-character Contact Id")
    name: str
    email: str | None = None
    role: str | None = Field(default=None, description="Role__c picklist value")
    is_active: bool
    paid_at: datetime | None = None


class CompanySummary(BaseModel):
    id: str = Field(description="Salesforce 18-character Account Id")
    name: str
    vat_number: str | None = None
    country: str | None = Field(default=None, description="ISO 3166-1 alpha-2")
    last_modified_at: datetime | None = None


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


class CompanyCount(BaseModel):
    """Aggregated company counts with active/inactive breakdown."""

    total: int
    active: int
    inactive: int


class CompanyProfile(BaseModel):
    """Full company details with linked contact count."""

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
    contact_count: int = Field(description="Number of contacts linked via AccountId")


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
