"""Pydantic models for the unified agendas tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument, ItemStage


class AgendaResultPartial(KNSBaseModel):
    """An agenda search result (summary fields only)."""
    agenda_id: int = Field(description="Unique agenda identifier")
    name: str | None = Field(default=None, description="Agenda name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    classification: str | None = Field(default=None, description="Classification description")
    type: str | None = Field(default=None, description="Sub-type description")
    status: str | None = Field(default=None, description="Status description")
    initiator_name: str | None = Field(default=None, description="Initiator name with party")
    last_update_date: str | None = Field(default=None, description="Last updated date (YYYY-MM-DD)")


class AgendaResultFull(AgendaResultPartial):
    """An agenda full-detail result (summary + detail fields)."""
    stages: list[ItemStage] | None = Field(default=None, description="Session stages (plenum + committee appearances) in chronological order")
    leading_agenda_id: int | None = Field(default=None, description="Leading agenda ID")
    leading_agenda_name: str | None = Field(default=None, description="Leading agenda name")
    gov_recommendation: str | None = Field(default=None, description="Government recommendation")
    postponement_reason: str | None = Field(default=None, description="Postponement reason")
    president_decision_date: str | None = Field(default=None, description="President decision date")
    committee_id: int | None = Field(default=None, description="Committee ID")
    committee_name: str | None = Field(default=None, description="Committee name")
    recommend_committee_id: int | None = Field(default=None, description="Recommended committee ID")
    recommend_committee_name: str | None = Field(default=None, description="Recommended committee name")
    minister_name: str | None = Field(default=None, description="Minister name with party")
    documents: list[SessionDocument] | None = Field(default=None, description="Agenda documents")


# Backward-compat alias
AgendaResult = AgendaResultFull


class AgendasResults(KNSBaseModel):
    """Results from agendas tool."""
    items: list[AgendaResultPartial | AgendaResultFull] = Field(description="List of agenda results")
