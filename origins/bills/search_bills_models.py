"""Pydantic models for bills_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class BillSummary(KNSBaseModel):
    """Summary of a single bill in search results."""
    bill_id: int = Field(description="Unique bill identifier")
    name: str | None = Field(default=None, description="Bill name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    sub_type: str | None = Field(default=None, description="Bill sub-type (private/government/committee)")
    status: str | None = Field(default=None, description="Current status description")
    committee: str | None = Field(default=None, description="Assigned committee name")
    committee_id: int | None = Field(default=None, description="Assigned committee ID")
    publication_date: str | None = Field(default=None, description="Publication date (YYYY-MM-DD)")
    publication_series: str | None = Field(default=None, description="Publication series description")
    summary: str | None = Field(default=None, description="Summary of the law")
    primary_initiators: list[str] | None = Field(default=None, description="Primary initiator names with party")


class BillSearchResults(KNSBaseModel):
    """Results from search_bills."""
    items: list[BillSummary] = Field(description="List of bill summaries sorted by publication_date DESC, bill_id DESC")
