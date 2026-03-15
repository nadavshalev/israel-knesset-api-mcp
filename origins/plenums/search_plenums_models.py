"""Pydantic models for plenum_sessions_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class SessionSummary(KNSBaseModel):
    """Summary of a single plenum session in search results."""
    session_id: int = Field(description="Unique session identifier")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    name: str | None = Field(default=None, description="Session name")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")


class SessionSearchResults(KNSBaseModel):
    """Results from search_sessions."""
    items: list[SessionSummary] = Field(description="List of session summaries sorted by date DESC, session_id DESC")
