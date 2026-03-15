"""Pydantic models for member_view outputs.

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

class GovernmentRole(KNSBaseModel):
    """A government/ministerial role held by a member."""
    title: str | None = Field(default=None, description="Role title")
    ministry: str | None = Field(default=None, description="Ministry name")
    is_transition: bool = Field(description="Whether from a transition government")
    start: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end: str | None = Field(default=None, description="End date (YYYY-MM-DD)")


class CommitteeRole(KNSBaseModel):
    """A committee membership held by a member."""
    id: int | None = Field(default=None, description="Committee ID")
    name: str | None = Field(default=None, description="Committee name")
    role: str | None = Field(default=None, description="Position title on committee")
    start: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end: str | None = Field(default=None, description="End date (YYYY-MM-DD)")


class ParliamentaryRole(KNSBaseModel):
    """A parliamentary role held by a member (e.g. Knesset member, Speaker)."""
    name: str | None = Field(default=None, description="Role display title")
    role: str | None = Field(default=None, description="Official position title")
    start: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end: str | None = Field(default=None, description="End date (YYYY-MM-DD)")


class MemberRoles(KNSBaseModel):
    """Roles grouped by category."""
    government: list[GovernmentRole] = Field(default_factory=list, description="Government/ministerial roles")
    committees: list[CommitteeRole] = Field(default_factory=list, description="Committee memberships")
    parliamentary: list[ParliamentaryRole] = Field(default_factory=list, description="Parliamentary roles (e.g. Knesset member, Speaker)")


# ---------------------------------------------------------------------------
# Main output model
# ---------------------------------------------------------------------------

class MemberDetail(KNSBaseModel):
    """Full member detail for one Knesset term, returned by get_member."""
    member_id: int = Field(description="Member person ID")
    name: str = Field(description="Full name")
    gender: str | None = Field(default=None, description="Gender description")
    knesset_num: int = Field(description="Knesset number for this term")
    faction: list[str] = Field(default_factory=list, description="Faction/party names during this term")
    roles: MemberRoles = Field(default_factory=MemberRoles, description="Roles grouped by category")


class MemberDetailList(KNSBaseModel):
    """Wrapper for get_member when returning all terms (no knesset_num filter)."""
    items: list[MemberDetail] = Field(description="List of member details, one per Knesset term")
