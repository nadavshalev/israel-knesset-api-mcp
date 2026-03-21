"""Pydantic models for plenums_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionItem, SessionDocument


class PlenumSessionResultPartial(KNSBaseModel):
    """A plenum session search result (summary fields only)."""
    session_id: int = Field(description="Unique session identifier")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    name: str | None = Field(default=None, description="Session name")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    item_count: int = Field(default=0, description="Number of agenda items")


class PlenumSessionResultFull(PlenumSessionResultPartial):
    """A plenum session full-detail result (summary + detail fields)."""
    items: list[SessionItem] | None = Field(default=None, description="Agenda items in ordinal order")
    documents: list[SessionDocument] | None = Field(default=None, description="Session documents")


# Backward-compat alias
PlenumSessionResult = PlenumSessionResultFull


class PlenumSessionsResults(KNSBaseModel):
    """Results from plenums."""
    items: list[PlenumSessionResultPartial | PlenumSessionResultFull] = Field(description="List of plenum session results sorted by date DESC, session_id DESC")
