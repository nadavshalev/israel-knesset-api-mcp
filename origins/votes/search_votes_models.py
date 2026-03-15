"""Pydantic models for votes_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class VoteSummary(KNSBaseModel):
    """Summary of a single vote in search results."""
    vote_id: int = Field(description="Unique vote identifier")
    bill_id: int | None = Field(default=None, description="Linked bill ID (if vote is on a bill)")
    knesset_num: int | None = Field(default=None, description="Knesset number (via session)")
    session_id: int | None = Field(default=None, description="Plenum session ID")
    title: str | None = Field(default=None, description="Vote title")
    subject: str | None = Field(default=None, description="Vote subject/stage")
    date: str | None = Field(default=None, description="Vote date (YYYY-MM-DD)")
    time: str | None = Field(default=None, description="Vote time (HH:MM)")
    is_accepted: bool | None = Field(default=None, description="Whether the vote passed")
    total_for: int | None = Field(default=None, description="Votes in favour")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")
    for_option: str | None = Field(default=None, description="Label for the 'for' option")
    against_option: str | None = Field(default=None, description="Label for the 'against' option")
    vote_method: str | None = Field(default=None, description="Voting method description")


class VoteSearchResults(KNSBaseModel):
    """Results from search_votes."""
    items: list[VoteSummary] = Field(description="List of vote summaries sorted by date DESC, vote_id DESC")
