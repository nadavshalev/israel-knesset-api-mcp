"""Pydantic models for the unified laws tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.session_models import SessionDocument  # noqa: F401 — re-exported for convenience


# ---------------------------------------------------------------------------
# Nested models (full detail)
# ---------------------------------------------------------------------------

class ReplacedLaw(KNSBaseModel):
    """A replaced-law entry from KNS_IsraelLawBinding — shows the law that
    was replaced and the bill that performed the replacement."""
    replaced_law: "LawResultPartial | None" = Field(default=None, description="Partial info of the replaced law")
    bill: "BillResultPartial | None" = Field(default=None, description="Bill that replaced the law")


class LawAmendment(KNSBaseModel):
    """A single amendment entry from KNS_LawBinding (without bill info — bill is on LawChange)."""
    binding_type: str | None = Field(default=None, description="Binding type description")
    amendment_type: str | None = Field(default=None, description="Amendment type description")
    date: str | None = Field(default=None, description="Binding date (YYYY-MM-DD)")
    page_number: str | None = Field(default=None, description="Page number")
    paragraph_number: str | None = Field(default=None, description="Paragraph number")
    amendment_number: str | None = Field(default=None, description="Amendment number")


class LawCorrection(KNSBaseModel):
    """A law correction record (KNS_LawCorrections via junction — without bill info)."""
    correction_type: str | None = Field(default=None, description="Correction type")
    status: str | None = Field(default=None, description="Correction status")
    publication_date: str | None = Field(default=None, description="Publication date")
    magazine_number: str | None = Field(default=None, description="Magazine number")
    page_number: str | None = Field(default=None, description="Page number")
    is_knesset_involvement: bool | None = Field(default=None, description="Whether Knesset was involved")


class LawChange(KNSBaseModel):
    """A change record grouping a bill with its amendments and corrections."""
    bill: "BillResultPartial" = Field(description="The bill that caused this change")
    amendments: list[LawAmendment] | None = Field(default=None, description="Amendment bindings from this bill")
    corrections: list[LawCorrection] | None = Field(default=None, description="Corrections from this bill")


# ---------------------------------------------------------------------------
# Main result models (unified: partial + full)
# ---------------------------------------------------------------------------

class LawResultPartial(KNSBaseModel):
    """A law search result (summary fields only)."""
    law_id: int = Field(description="Unique law identifier")
    name: str | None = Field(default=None, description="Law name")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    law_types: list[str] | None = Field(default=None, description="Law type tags (e.g. חוק יסוד, חוק תקציב, חוק מועדף)")
    publication_date: str | None = Field(default=None, description="Publication date (YYYY-MM-DD)")
    latest_publication_date: str | None = Field(default=None, description="Latest publication date (YYYY-MM-DD)")
    law_validity: str | None = Field(default=None, description="Law validity status")


class LawResultFull(LawResultPartial):
    """A law full-detail result (summary + detail fields)."""
    validity_start_date: str | None = Field(default=None, description="Validity start date")
    validity_start_date_notes: str | None = Field(default=None, description="Validity start date notes")
    validity_finish_date: str | None = Field(default=None, description="Validity finish date")
    validity_finish_date_notes: str | None = Field(default=None, description="Validity finish date notes")
    classifications: list[str] | None = Field(default=None, description="Classification categories")
    ministries: list[str] | None = Field(default=None, description="Associated ministry names")
    alternative_names: list[str] | None = Field(default=None, description="Alternative/historical names (excluding current name)")
    replaced_laws: list[ReplacedLaw] | None = Field(default=None, description="Laws replaced by this law, with the replacing bill")
    original_bill: "BillResultPartial | None" = Field(default=None, description="Original bill this law is based on. Resolved from a binding with type 'החוק המקורי' if present; otherwise from the common ParentLawID when all bindings share the same one.")
    changes: list[LawChange] | None = Field(default=None, description="Bills that changed this law, each with their amendments and corrections")
    secondary_laws: "list[SecondaryLawResultPartial] | None" = Field(default=None, description="Secondary legislation (regulations, orders, rules) derived from this law")
    documents: list[SessionDocument] | None = Field(default=None, description="Attached documents")


class LawsResults(KNSBaseModel):
    """Results from laws tool."""
    items: list[LawResultPartial | LawResultFull] = Field(description="List of law results")


# Resolve forward references
from origins.bills.bills_models import BillResultPartial  # noqa: E402
from origins.secondary_laws.secondary_laws_models import SecondaryLawResultPartial  # noqa: E402
ReplacedLaw.model_rebuild()
LawChange.model_rebuild()
LawResultFull.model_rebuild()
