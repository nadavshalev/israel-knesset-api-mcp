"""Pydantic models for the unified agendas tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument, ItemStage


class AgendaResult(KNSBaseModel):
    """An agenda result (summary or full detail)."""
    # Always present (partial):
    agenda_id: int = Field(description="Unique agenda identifier")
    name: str | None = Field(default=None, description="Agenda name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    classification: str | None = Field(default=None, description="Classification description")
    type: str | None = Field(default=None, description="Sub-type description")
    status: str | None = Field(default=None, description="Status description")
    initiator_name: str | None = Field(default=None, description="Initiator name with party")
    last_update_date: str | None = Field(default=None, description="Last updated date (YYYY-MM-DD)")
    # Full detail only (None when partial):
    stages: list[ItemStage] | None = Field(default=None, description="Session stages (plenum + committee appearances) in chronological order (only when full_details=True)")
    leading_agenda_id: int | None = Field(default=None, description="Leading agenda ID (only when full_details=True)")
    leading_agenda_name: str | None = Field(default=None, description="Leading agenda name (only when full_details=True)")
    gov_recommendation: str | None = Field(default=None, description="Government recommendation (only when full_details=True)")
    postponement_reason: str | None = Field(default=None, description="Postponement reason (only when full_details=True)")
    president_decision_date: str | None = Field(default=None, description="President decision date (only when full_details=True)")
    committee_id: int | None = Field(default=None, description="Committee ID (only when full_details=True)")
    committee_name: str | None = Field(default=None, description="Committee name (only when full_details=True)")
    recommend_committee_id: int | None = Field(default=None, description="Recommended committee ID (only when full_details=True)")
    recommend_committee_name: str | None = Field(default=None, description="Recommended committee name (only when full_details=True)")
    minister_name: str | None = Field(default=None, description="Minister name with party (only when full_details=True)")
    documents: list[SessionDocument] | None = Field(default=None, description="Agenda documents (only when full_details=True)")


class AgendasResults(KNSBaseModel):
    """Results from agendas tool."""
    items: list[AgendaResult] = Field(description="List of agenda results")
