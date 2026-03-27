"""Pydantic models for the unified secondary_laws tool."""

from __future__ import annotations

from pydantic import Field

from core.models import CountItem, KNSBaseModel
from core.session_models import SessionDocument  # noqa: F401 — re-exported for convenience


# ---------------------------------------------------------------------------
# Nested models (full detail)
# ---------------------------------------------------------------------------

class SecLawRegulator(KNSBaseModel):
    """A regulator (issuing authority) for a secondary law."""
    regulator_type: str | None = Field(default=None, description="Regulator type (e.g. שר, ועדה)")
    regulator_name: str | None = Field(default=None, description="Regulator name/title")


class SecLawBinding(KNSBaseModel):
    """A binding between two secondary laws (from KNS_SecToSecBinding)."""
    related_law: "SecondaryLawResultPartial" = Field(
        description="The other secondary law in this binding"
    )
    related_role: str = Field(
        description="Role of the related law: 'child', 'parent', or 'main'"
    )
    current_role: str = Field(
        description="Role of the current law: 'child', 'parent', or 'main'"
    )
    binding_type: str | None = Field(default=None, description="Binding type description")
    amendment_type: str | None = Field(default=None, description="Amendment type description")
    is_temp_legislation: bool | None = Field(default=None, description="Whether this is temporary legislation")
    is_secondary_amendment: bool | None = Field(default=None, description="Whether this is a secondary amendment")
    correction_number: str | None = Field(default=None, description="Correction number")
    paragraph_number: str | None = Field(default=None, description="Paragraph number")


# ---------------------------------------------------------------------------
# Main result models (unified: partial + full)
# ---------------------------------------------------------------------------

class SecondaryLawResultPartial(KNSBaseModel):
    """A secondary law search result (summary fields only)."""
    secondary_law_id: int = Field(description="Unique secondary law identifier")
    name: str | None = Field(default=None, description="Secondary law name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    type: str | None = Field(default=None, description="Type (e.g. תקנות, צו, כללים)")
    status: str | None = Field(default=None, description="Current status")
    is_current: bool | None = Field(default=None, description="Whether this is the current version")
    publication_date: str | None = Field(default=None, description="Publication date (YYYY-MM-DD)")
    committee_name: str | None = Field(default=None, description="Responsible committee name")
    major_authorizing_law_id: int | None = Field(default=None, description="Primary authorizing law ID")
    major_authorizing_law_name: str | None = Field(default=None, description="Primary authorizing law name")


class SecondaryLawResultFull(SecondaryLawResultPartial):
    """A secondary law full-detail result (summary + detail fields)."""
    classification: str | None = Field(default=None, description="Classification description")
    completion_cause: str | None = Field(default=None, description="Completion cause description")
    postponement_reason: str | None = Field(default=None, description="Postponement reason description")
    knesset_involvement: str | None = Field(default=None, description="Knesset involvement description")
    publication_series: str | None = Field(default=None, description="Publication series description")
    magazine_number: str | None = Field(default=None, description="Magazine/gazette number")
    page_number: str | None = Field(default=None, description="Page number in gazette")
    committee_received_date: str | None = Field(default=None, description="Date received by committee (YYYY-MM-DD)")
    committee_approval_date: str | None = Field(default=None, description="Committee approval date (YYYY-MM-DD)")
    approval_without_discussion_date: str | None = Field(default=None, description="Approval date without discussion (YYYY-MM-DD)")
    secretary_received_date: str | None = Field(default=None, description="Secretary received date (YYYY-MM-DD)")
    plenum_approval_date: str | None = Field(default=None, description="Plenum approval date (YYYY-MM-DD)")
    is_amending_law_original: bool | None = Field(default=None, description="Whether this is an original amending law")
    is_emergency: bool | None = Field(default=None, description="Whether this is emergency legislation")
    committee_id: int | None = Field(default=None, description="Responsible committee ID")
    regulators: list[SecLawRegulator] | None = Field(default=None, description="Issuing authorities/regulators")
    authorizing_laws: list["LawResultPartial"] | None = Field(default=None, description="All primary laws authorizing this secondary law")
    bindings: list[SecLawBinding] | None = Field(default=None, description="Relationships to other secondary laws")
    documents: list[SessionDocument] | None = Field(default=None, description="Attached documents")


class SecondaryLawsResults(KNSBaseModel):
    """Results from secondary_laws tool."""
    total_count: int = Field(description="Total matching results (before pagination)")
    items: list[SecondaryLawResultPartial | SecondaryLawResultFull] = Field(description="List of secondary law results")
    counts: list[CountItem] | None = Field(default=None, description="Grouped counts (when count_by is set)")


# Resolve forward references
from origins.laws.laws_models import LawResultPartial  # noqa: E402
SecLawBinding.model_rebuild()
SecondaryLawResultFull.model_rebuild()
