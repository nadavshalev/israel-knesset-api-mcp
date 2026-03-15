"""Pydantic models for bill_view inputs and outputs.

These models serve two purposes:
1. Input validation — the view function accepts a typed input model.
2. Output schema — FastMCP uses the output model to generate a proper
   JSON Schema ``outputSchema`` instead of the generic ``{result: string}``.
"""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


# ---------------------------------------------------------------------------
# Nested output models
# ---------------------------------------------------------------------------

class StageVote(KNSBaseModel):
    """Final (decisive) vote for a bill stage."""
    vote_id: int = Field(description="Vote ID")
    title: str | None = Field(default=None, description="Vote title")
    date: str | None = Field(default=None, description="Vote date (YYYY-MM-DD)")
    is_accepted: bool | None = Field(default=None, description="Whether accepted")
    total_for: int | None = Field(default=None, description="Votes for")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")


class BillStage(KNSBaseModel):
    """A single plenum stage (reading) for a bill."""
    date: str | None = Field(default=None, description="Stage date (YYYY-MM-DD)")
    status: str | None = Field(default=None, description="Stage status description")
    session_id: int = Field(description="Plenum session ID")
    vote: StageVote | None = Field(default=None, description="Final (decisive) vote for this stage")


class Initiator(KNSBaseModel):
    """A bill initiator (primary, added, or removed)."""
    person_id: int = Field(description="Person ID")
    name: str = Field(description="Full name")
    party: str | None = Field(default=None, description="Faction/party name")


class RemovedInitiator(Initiator):
    """An initiator who was removed from the bill."""
    reason: str | None = Field(default=None, description="Reason for removal")


class BillInitiators(KNSBaseModel):
    """Bill initiators grouped by role."""
    primary: list[Initiator] | None = Field(default=None, description="Primary initiators")
    added: list[Initiator] | None = Field(default=None, description="Added (co-)initiators")
    removed: list[RemovedInitiator] | None = Field(default=None, description="Removed initiators (historical)")


class BillNameHistory(KNSBaseModel):
    """A historical name entry for a bill."""
    name: str | None = Field(default=None, description="Historical name")
    stage_type: str | None = Field(default=None, description="Stage at which name was used")


class BillDocument(KNSBaseModel):
    """A document attached to a bill."""
    type: str | None = Field(default=None, description="Document group type")
    format: str | None = Field(default=None, description="File format")
    url: str | None = Field(default=None, description="File URL/path")


class SplitBill(KNSBaseModel):
    """A bill related through a split."""
    direction: str = Field(description="'child' or 'parent'")
    bill_id: int = Field(description="Related bill ID")
    name: str | None = Field(default=None, description="Bill name")


class MergedBill(KNSBaseModel):
    """A bill merged with this one."""
    bill_id: int = Field(description="Related bill ID")
    name: str | None = Field(default=None, description="Bill name")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class BillDetail(KNSBaseModel):
    """Full bill detail returned by get_bill."""
    bill_id: int = Field(description="Unique bill identifier")
    name: str | None = Field(default=None, description="Bill name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    sub_type: str | None = Field(default=None, description="Bill sub-type")
    status: str | None = Field(default=None, description="Current status description")
    committee: str | None = Field(default=None, description="Assigned committee name")
    committee_id: int | None = Field(default=None, description="Assigned committee ID")
    publication_date: str | None = Field(default=None, description="Publication date (YYYY-MM-DD)")
    publication_series: str | None = Field(default=None, description="Publication series description")
    summary: str | None = Field(default=None, description="Summary of the law")
    stages: list[BillStage] = Field(default_factory=list, description="Plenum stages (readings) in chronological order")
    initiators: BillInitiators | None = Field(default=None, description="Bill initiators (only present if any exist)")
    name_history: list[BillNameHistory] | None = Field(default=None, description="Bill name changes over time")
    documents: list[BillDocument] | None = Field(default=None, description="Bill documents")
    split_bills: list[SplitBill] | None = Field(default=None, description="Related bills from splits")
    merged_bills: list[MergedBill] | None = Field(default=None, description="Bills merged with this one")


# ---------------------------------------------------------------------------
# Input model
# ---------------------------------------------------------------------------

class GetBillInput(KNSBaseModel):
    """Input parameters for get_bill."""
    bill_id: int = Field(description="The bill ID (required)")
