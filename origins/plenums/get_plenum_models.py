"""Pydantic models for plenum_session_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


# ---------------------------------------------------------------------------
# Nested output models
# ---------------------------------------------------------------------------

class SessionItem(KNSBaseModel):
    """An agenda item in a plenum session."""
    item_id: int | None = Field(default=None, description="Item ID")
    name: str | None = Field(default=None, description="Item name")
    type: str | None = Field(default=None, description="Item type description")
    status: str | None = Field(default=None, description="Item status description")


class SessionDocument(KNSBaseModel):
    """A document attached to a plenum session."""
    group_type: str | None = Field(default=None, description="Document group type")
    application: str | None = Field(default=None, description="File format")
    file_path: str | None = Field(default=None, description="File URL/path")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class SessionDetail(KNSBaseModel):
    """Full plenum session detail returned by get_session."""
    session_id: int = Field(description="Unique session identifier")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    name: str | None = Field(default=None, description="Session name")
    date: str | None = Field(default=None, description="Session start date (YYYY-MM-DD)")
    items: list[SessionItem] = Field(default_factory=list, description="Agenda items in ordinal order")
    documents: list[SessionDocument] = Field(default_factory=list, description="Session documents")
