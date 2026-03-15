"""Pydantic models for members_view outputs."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel


class MemberSummary(KNSBaseModel):
    """Summary of a single member in search results."""
    member_id: int = Field(description="Member person ID")
    name: str = Field(description="Full name")
    gender: str | None = Field(default=None, description="Gender description")
    knesset_num: int = Field(description="Knesset number")
    faction: list[str] = Field(default_factory=list, description="Faction/party names")
    role_types: list[str] = Field(default_factory=list, description="Distinct position titles held")


class MemberSearchResults(KNSBaseModel):
    """Results from search_members."""
    items: list[MemberSummary] = Field(description="List of member summaries sorted by knesset_num DESC, member_id")
