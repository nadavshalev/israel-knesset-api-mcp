"""Pydantic models for committees_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class CommitteeSummary(KNSBaseModel):
    """Summary of a single committee in search results."""
    committee_id: int = Field(description="Unique committee identifier")
    name: str | None = Field(default=None, description="Committee name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Committee type (main, sub, etc.)")
    category: str | None = Field(default=None, description="Category description")
    is_current: bool = Field(description="Whether the committee is currently active")
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    parent_committee_id: int | None = Field(default=None, description="Parent committee ID (sub-committees)")
    parent_committee_name: str | None = Field(default=None, description="Parent committee name")


class CommitteeSearchResults(KNSBaseModel):
    """Results from search_committees."""
    items: list[CommitteeSummary] = Field(description="List of committee summaries sorted by start_date DESC, committee_id DESC")
