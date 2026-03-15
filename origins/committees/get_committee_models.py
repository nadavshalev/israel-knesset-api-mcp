"""Pydantic models for committee_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


# ---------------------------------------------------------------------------
# Nested output models
# ---------------------------------------------------------------------------

class CommitteeSession(KNSBaseModel):
    """A committee session."""
    session_id: int = Field(description="Session ID")
    number: int | None = Field(default=None, description="Session number")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    start_time: str | None = Field(default=None, description="Start time (HH:MM)")
    end_time: str | None = Field(default=None, description="End time (HH:MM)")
    type: str | None = Field(default=None, description="Session type")
    status: str | None = Field(default=None, description="Session status")
    location: str | None = Field(default=None, description="Location")
    url: str | None = Field(default=None, description="Session URL")
    broadcast_url: str | None = Field(default=None, description="Broadcast URL")


class CommitteeMember(KNSBaseModel):
    """A member who served on a committee."""
    member_id: int = Field(description="Member person ID")
    name: str = Field(description="Full name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    role: str | None = Field(default=None, description="Position title")
    start: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end: str | None = Field(default=None, description="End date (YYYY-MM-DD)")


class CommitteeBill(KNSBaseModel):
    """A bill discussed in a committee session."""
    bill_id: int = Field(description="Bill ID")
    name: str | None = Field(default=None, description="Bill name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    sub_type: str | None = Field(default=None, description="Bill sub-type")
    status: str | None = Field(default=None, description="Bill status")


class CommitteeDocument(KNSBaseModel):
    """A document from a committee session."""
    document_id: int = Field(description="Document ID")
    type: str | None = Field(default=None, description="Document group type")
    name: str | None = Field(default=None, description="Document name")
    format: str | None = Field(default=None, description="File format")
    file_path: str | None = Field(default=None, description="File URL/path")
    session_id: int = Field(description="Session ID")
    session_date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class CommitteeDetail(KNSBaseModel):
    """Full committee detail returned by get_committee."""
    committee_id: int = Field(description="Unique committee identifier")
    name: str | None = Field(default=None, description="Committee name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Committee type")
    category: str | None = Field(default=None, description="Category description")
    is_current: bool = Field(description="Whether currently active")
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    parent_committee_id: int | None = Field(default=None, description="Parent committee ID")
    parent_committee_name: str | None = Field(default=None, description="Parent committee name")
    email: str | None = Field(default=None, description="Committee email")
    sessions: list[CommitteeSession] | None = Field(default=None, description="Committee sessions (only when include_sessions=True)")
    session_count: int | None = Field(default=None, description="Number of sessions (when include_sessions=True)")
    members: list[CommitteeMember] | None = Field(default=None, description="Committee members (only when include_members=True)")
    member_count: int | None = Field(default=None, description="Number of members (when include_members=True)")
    bills: list[CommitteeBill] | None = Field(default=None, description="Bills discussed (only when include_bills=True)")
    bill_count: int | None = Field(default=None, description="Number of bills (when include_bills=True)")
    documents: list[CommitteeDocument] | None = Field(default=None, description="Session documents (only when include_documents=True)")
    document_count: int | None = Field(default=None, description="Number of documents (when include_documents=True)")
