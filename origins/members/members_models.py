"""Pydantic models for the unified members tool."""

from __future__ import annotations

from pydantic import Field

from core.models import CountItem, KNSBaseModel


# ---------------------------------------------------------------------------
# Nested models (full detail)
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
# Main result model (unified: partial + full)
# ---------------------------------------------------------------------------

class MemberResultPartial(KNSBaseModel):
    """A member search result (summary fields only)."""
    member_id: int = Field(description="Member person ID")
    name: str = Field(description="Full name")
    gender: str | None = Field(default=None, description="Gender description")
    knesset_num: int = Field(description="Knesset number for this term")
    faction: list[str] = Field(default_factory=list, description="Faction/party names during this term")
    role_types: list[str] = Field(default_factory=list, description="Distinct position titles held")


class MemberResultFull(MemberResultPartial):
    """A member full-detail result (summary + detail fields)."""
    roles: MemberRoles | None = Field(default=None, description="Roles grouped by category (government, committees, parliamentary)")


# Backward-compat alias
MemberResult = MemberResultFull


class MembersResults(KNSBaseModel):
    """Results from members tool."""
    total_count: int = Field(description="Total matching results (before pagination)")
    items: list[MemberResultPartial | MemberResultFull] = Field(description="List of member results")
    counts: list[CountItem] | None = Field(default=None, description="Grouped counts (when count_by is set)")
