"""Pydantic models for the unified bills tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument, ItemStage


# ---------------------------------------------------------------------------
# Nested models (full detail)
# ---------------------------------------------------------------------------

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
# Main result model (unified: partial + full)
# ---------------------------------------------------------------------------

class BillResultPartial(KNSBaseModel):
    """A bill search result (summary fields only)."""
    bill_id: int = Field(description="Unique bill identifier")
    name: str | None = Field(default=None, description="Bill name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Bill type (private/government/committee)")
    status: str | None = Field(default=None, description="Current status description")
    committee: str | None = Field(default=None, description="Assigned committee name")
    committee_id: int | None = Field(default=None, description="Assigned committee ID")
    publication_date: str | None = Field(default=None, description="Publication date (YYYY-MM-DD)")
    publication_series: str | None = Field(default=None, description="Publication series description")
    summary: str | None = Field(default=None, description="Summary of the law")
    primary_initiators: list[str] | None = Field(default=None, description="Primary initiator names with party")
    last_update_date: str | None = Field(default=None, description="Last updated date (YYYY-MM-DD)")


class BillResultFull(BillResultPartial):
    """A bill full-detail result (summary + detail fields)."""
    stages: list[ItemStage] | None = Field(default=None, description="Session stages (plenum + committee appearances) in chronological order, with votes on plenum stages")
    initiators: BillInitiators | None = Field(default=None, description="Bill initiators grouped by role")
    name_history: list[BillNameHistory] | None = Field(default=None, description="Bill name changes over time")
    documents: list[SessionDocument] | None = Field(default=None, description="Bill documents")
    split_bills: list[SplitBill] | None = Field(default=None, description="Related bills from splits")
    merged_bills: list[MergedBill] | None = Field(default=None, description="Bills merged with this one")
    related_laws: "list[LawResultPartial] | None" = Field(default=None, description="Primary enacted laws this bill amended or originated")


# Backward-compat alias
BillResult = BillResultFull


class BillsResults(KNSBaseModel):
    """Results from bills tool."""
    total_count: int = Field(description="Total matching results (before pagination)")
    items: list[BillResultPartial | BillResultFull] = Field(description="List of bill results")


# Resolve forward references
from origins.laws.laws_models import LawResultPartial  # noqa: E402
BillResultFull.model_rebuild()
