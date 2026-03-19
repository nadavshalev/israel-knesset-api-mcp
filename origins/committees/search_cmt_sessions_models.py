"""Pydantic models for search_cmt_sessions_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class CmtSessionSummary(KNSBaseModel):
    """Summary of a single committee session in search results."""
    session_id: int = Field(description="Session ID (PK)")
    committee_id: int = Field(description="Committee ID")
    committee_name: str | None = Field(default=None, description="Committee name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    type: str | None = Field(default=None, description="Session type (פתוחה/חסויה)")
    status: str | None = Field(default=None, description="Session status")
    item_count: int = Field(default=0, description="Number of agenda items")


class CmtSessionSearchResults(KNSBaseModel):
    """Results from search_cmt_sessions."""
    items: list[CmtSessionSummary] = Field(description="List of committee session summaries sorted by date DESC, session_id DESC")
