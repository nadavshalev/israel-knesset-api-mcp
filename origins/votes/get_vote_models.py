"""Pydantic models for vote_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


# ---------------------------------------------------------------------------
# Nested output models
# ---------------------------------------------------------------------------

class VoteMember(KNSBaseModel):
    """A single MK's vote in a vote."""
    member_id: int = Field(description="Member person ID")
    name: str = Field(description="Member full name")
    result: str = Field(description="Vote result description")


class RelatedVote(KNSBaseModel):
    """A related vote from the same session with the same title."""
    vote_id: int = Field(description="Related vote ID")
    subject: str | None = Field(default=None, description="Vote subject")
    for_option: str | None = Field(default=None, description="For-option label")
    date: str | None = Field(default=None, description="Vote date (YYYY-MM-DD)")
    time: str | None = Field(default=None, description="Vote time (HH:MM)")
    is_accepted: bool | None = Field(default=None, description="Whether accepted")
    total_for: int | None = Field(default=None, description="Votes for")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class VoteDetail(KNSBaseModel):
    """Full vote detail returned by get_vote."""
    vote_id: int = Field(description="Unique vote identifier")
    bill_id: int | None = Field(default=None, description="Linked bill ID")
    knesset_num: int | None = Field(default=None, description="Knesset number")
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
    members: list[VoteMember] = Field(default_factory=list, description="Per-MK vote breakdown")
    related_votes: list[RelatedVote] = Field(default_factory=list, description="Other votes with the same title in the same session")
