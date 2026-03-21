"""Pydantic models for committees_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionItem, SessionDocument


class CmtSessionResultPartial(KNSBaseModel):
    """A committee session search result (summary fields only)."""
    session_id: int = Field(description="Session ID")
    committee_id: int = Field(description="Committee ID")
    committee_name: str | None = Field(default=None, description="Committee name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    item_count: int = Field(default=0, description="Number of agenda items")


class CmtSessionResultFull(CmtSessionResultPartial):
    """A committee session full-detail result (summary + detail fields)."""
    number: int | None = Field(default=None, description="Session number")
    start_time: str | None = Field(default=None, description="Start time (HH:MM)")
    end_time: str | None = Field(default=None, description="End time (HH:MM)")
    type: str | None = Field(default=None, description="Session type")
    status: str | None = Field(default=None, description="Session status")
    location: str | None = Field(default=None, description="Location")
    url: str | None = Field(default=None, description="Session URL")
    broadcast_url: str | None = Field(default=None, description="Broadcast URL")
    note: str | None = Field(default=None, description="Session note")
    items: list[SessionItem] | None = Field(default=None, description="Agenda items")
    documents: list[SessionDocument] | None = Field(default=None, description="Session documents")


# Backward-compat alias
CmtSessionResult = CmtSessionResultFull


class CmtSessionsResults(KNSBaseModel):
    """Results from committees."""
    items: list[CmtSessionResultPartial | CmtSessionResultFull] = Field(description="List of committee session results sorted by date DESC, session_id DESC")
