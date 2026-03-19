"""Pydantic models for get_cmt_session_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


# ---------------------------------------------------------------------------
# Nested output models
# ---------------------------------------------------------------------------

class ItemVote(KNSBaseModel):
    """A plenum vote on a bill that was discussed in this committee session."""
    vote_id: int = Field(description="Vote ID")
    title: str | None = Field(default=None, description="Vote title")
    date: str | None = Field(default=None, description="Vote date (YYYY-MM-DD)")
    is_accepted: bool | None = Field(default=None, description="Whether accepted")
    total_for: int | None = Field(default=None, description="Votes for")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")


class CmtSessionItem(KNSBaseModel):
    """An agenda item in a committee session."""
    item_id: int = Field(description="Item ID")
    name: str | None = Field(default=None, description="Item name")
    item_type_id: int | None = Field(default=None, description="Item type ID (2=bill, 11=other, etc.)")
    ordinal: int | None = Field(default=None, description="Item ordinal position")
    status_id: int | None = Field(default=None, description="Item status ID")
    item_type: str | None = Field(default=None, description="Item type description (e.g. הצעת חוק, פריט ועדה)")
    linked_bill_id: int | None = Field(default=None, description="Linked bill ID (when ItemTypeID=2)")
    linked_bill_name: str | None = Field(default=None, description="Linked bill name (when ItemTypeID=2)")
    votes: list[ItemVote] | None = Field(default=None, description="Plenum votes on this bill (only for bill items, ItemTypeID=2)")


class CmtSessionDocument(KNSBaseModel):
    """A document from a committee session."""
    document_id: int = Field(description="Document ID")
    type: str | None = Field(default=None, description="Document group type")
    name: str | None = Field(default=None, description="Document name")
    format: str | None = Field(default=None, description="File format")
    file_path: str | None = Field(default=None, description="File URL/path")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class CmtSessionDetail(KNSBaseModel):
    """Full committee session detail returned by get_cmt_session."""
    session_id: int = Field(description="Session ID")
    committee_id: int = Field(description="Committee ID")
    committee_name: str | None = Field(default=None, description="Committee name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    number: int | None = Field(default=None, description="Session number")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    start_time: str | None = Field(default=None, description="Start time (HH:MM)")
    end_time: str | None = Field(default=None, description="End time (HH:MM)")
    type: str | None = Field(default=None, description="Session type")
    status: str | None = Field(default=None, description="Session status")
    location: str | None = Field(default=None, description="Location")
    url: str | None = Field(default=None, description="Session URL")
    broadcast_url: str | None = Field(default=None, description="Broadcast URL")
    note: str | None = Field(default=None, description="Session note")
    items: list[CmtSessionItem] = Field(default_factory=list, description="Agenda items in ordinal order")
    documents: list[CmtSessionDocument] = Field(default_factory=list, description="Session documents")
