"""Pydantic models for knesset_dates_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class KnessetPeriod(KNSBaseModel):
    """An assembly/plenum period within a Knesset term."""
    id: int = Field(description="Period row ID")
    assembly: int | None = Field(default=None, description="Assembly number")
    plenum: int | None = Field(default=None, description="Plenum number")
    start_date: str | None = Field(default=None, description="Period start (YYYY-MM-DD)")
    finish_date: str | None = Field(default=None, description="Period end (YYYY-MM-DD)")
    is_current: bool = Field(description="Whether this period is current")


class KnessetTerm(KNSBaseModel):
    """A Knesset term with its nested periods."""
    knesset_num: int = Field(description="Knesset number")
    name: str | None = Field(default=None, description="Knesset term name")
    is_current: bool = Field(description="Whether this is the current Knesset")
    periods: list[KnessetPeriod] = Field(default_factory=list, description="Assembly/plenum periods sorted by (assembly, plenum)")


class KnessetDatesResults(KNSBaseModel):
    """Results from get_knesset_dates."""
    items: list[KnessetTerm] = Field(description="Knesset terms sorted by newest first, each with nested periods")
