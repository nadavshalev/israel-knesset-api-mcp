"""Pydantic models for the metadata tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel

class KnessetAssembly(KNSBaseModel):
    """Assembly/plenum period within a Knesset term."""
    assembly_number: int | None = Field(default=None, description="Assembly number")
    plenum_year: int | None = Field(default=None, description="Plenum year within that assembly")
    start_date: str | None = Field(default=None, description="Period start (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="Period end (YYYY-MM-DD)")


class CommitteeMeta(KNSBaseModel):
    """Committee metadata."""
    committee_id: int = Field(description="Committee ID")
    name: str = Field(description="Committee name")
    type: str | None = Field(default=None, description="Committee type")
    parent_committee: str | None = Field(default=None, description="Parent committee name")
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    heads: list[str] | None = Field(
        default=None,
        description="Committee heads. Format: '{id}: {name} ({party}) - {position} [from {start}] [to {end}]'",
    )


class GovMinistryMeta(KNSBaseModel):
    """Government ministry metadata."""
    ministry_id: int = Field(description="Ministry ID")
    name: str = Field(description="Ministry name")
    minister: list[str] | None = Field(
        default=None,
        description="Ministers. Format: '{id}: {name} ({party}) [from {start}] [to {end}]'",
    )
    deputy_ministers: list[str] | None = Field(
        default=None,
        description="Deputy ministers. Format: '{id}: {name} ({party}) [from {start}] [to {end}]'",
    )
    members: list[str] | None = Field(
        default=None,
        description="Other ministry members not classified as minister/deputy. Format: '{id}: {name} ({party}) [from {start}] [to {end}]'",
    )


class FactionMeta(KNSBaseModel):
    """Parliamentary faction metadata."""
    faction_id: int = Field(description="Faction ID")
    name: str = Field(description="Faction name")
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    members: list[str] | None = Field(
        default=None,
        description="Faction members. Format: '{id}: {name} [from {start}] [to {end}]'",
    )


class GeneralRoleMeta(KNSBaseModel):
    """A parliamentary role not linked to any committee, ministry, or faction."""
    position: str = Field(description="Role title (e.g. 'ראש הממשלה', 'יו\"ר הכנסת')")
    holders: list[str] = Field(
        description="People who held this role. Format: '{id}: {name} ({party}) [from {start}] [to {end}]'"
    )


class MetadataResult(KNSBaseModel):
    """Full metadata for a single Knesset term. Sections omitted when their include_ flag is False."""
    knesset_num: int = Field(description="Knesset number")
    knesset_assemblies: list[KnessetAssembly] | None = Field(default=None, description="Assembly/plenum periods")
    committees: list[CommitteeMeta] | None = Field(default=None, description="Committees with heads")
    gov_ministries: list[GovMinistryMeta] | None = Field(default=None, description="Government ministries with members")
    factions: list[FactionMeta] | None = Field(default=None, description="Parliamentary factions with members")
    general_roles: list[GeneralRoleMeta] | None = Field(
        default=None,
        description=(
            "Parliamentary roles not linked to committee/ministry/faction "
            "(e.g. Prime Minister, Knesset Speaker). "
            "Excludes the generic 'חבר כנסת'/'ח\"כ' role held by all members."
        ),
    )
