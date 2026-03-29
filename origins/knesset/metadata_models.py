"""Pydantic models for the metadata tool."""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel

class KnessetAssembly(KNSBaseModel):
    """One convening period (כינוס) within a Knesset annual session (מושב).

    The Israeli Knesset organises its calendar into annual sessions (מושבים,
    roughly one per year of the term) and within each session there can be
    multiple separate convening periods (כינוסים) separated by recess breaks.
    Each row here is one such period: a contiguous block of sitting days.

    Example: Knesset 25, session 3 (מושב ג׳) might contain two convening
    periods — assembly 1 running autumn→winter and assembly 2 running
    spring→summer, with a recess in between.
    """
    assembly_number: int | None = Field(
        default=None,
        description=(
            "Convening number (כינוס) within the session — "
            "resets to 1 at the start of each new annual session (מושב). "
            "Typically 1 or 2 per session."
        ),
    )
    plenum_year: int | None = Field(
        default=None,
        description=(
            "Annual session number (מושב) within the Knesset term — "
            "increments each year (1 = first year of term, 2 = second year, …). "
            "Corresponds roughly to the calendar year of that session."
        ),
    )
    start_date: str | None = Field(default=None, description="First sitting day of this convening period (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="Last sitting day of this convening period (YYYY-MM-DD); null if still ongoing")


class CommitteeMeta(KNSBaseModel):
    """Committee active during the Knesset term."""
    committee_id: int = Field(description="Committee ID")
    name: str = Field(description="Committee name (Hebrew)")
    type: str | None = Field(
        default=None,
        description="Committee type category (e.g. 'ועדה קבועה' permanent, 'ועדה מיוחדת' special, 'ועדת משנה' sub-committee)",
    )
    parent_committee: str | None = Field(default=None, description="Parent committee name for sub-committees; null for top-level committees")
    start_date: str | None = Field(default=None, description="Date committee was established (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="Date committee was dissolved (YYYY-MM-DD); null if still active")
    heads: list[str] | None = Field(
        default=None,
        description=(
            "Committee chairs (יו\"ר) who served during the term. "
            "Each entry: '{member_id}: {name} ({party}) [from {start}] [to {end}]'. "
            "Dates are omitted when they match the committee's own start/end dates."
        ),
    )


class GovMinistryMeta(KNSBaseModel):
    """Government ministry active during the Knesset term."""
    ministry_id: int = Field(description="Ministry ID")
    name: str = Field(description="Ministry name (Hebrew)")
    minister: list[str] | None = Field(
        default=None,
        description=(
            "People who served as minister (שר/שרה) of this ministry. "
            "Each entry: '{member_id}: {name} ({party}) [from {start}] [to {end}]'. "
            "Dates are omitted when they span the full Knesset term."
        ),
    )
    deputy_ministers: list[str] | None = Field(
        default=None,
        description=(
            "People who served as deputy minister (סגן שר/סגנית שר). "
            "Same format as minister."
        ),
    )
    members: list[str] | None = Field(
        default=None,
        description=(
            "Other people with a ministry role not classified as minister or deputy "
            "(e.g. prime minister's office staff, advisors with official positions). "
            "Same format as minister."
        ),
    )


class FactionMeta(KNSBaseModel):
    """Parliamentary faction (סיעה) active during the Knesset term.

    Factions can split, merge, or be formed mid-term, so start/end dates
    may not span the full term.
    """
    faction_id: int = Field(description="Faction ID")
    name: str = Field(description="Faction name (Hebrew)")
    start_date: str | None = Field(default=None, description="Date faction was established or joined the Knesset (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="Date faction was dissolved or left the Knesset (YYYY-MM-DD); null if active through end of term")
    members: list[str] | None = Field(
        default=None,
        description=(
            "Members who belonged to this faction at any point during the term. "
            "Each entry: '{member_id}: {name} [from {start}] [to {end}]'. "
            "Dates are omitted when they match the faction's own start/end dates. "
            "A member appears here for each faction they belonged to (members can switch factions)."
        ),
    )


class GeneralRoleMeta(KNSBaseModel):
    """A named parliamentary role not linked to any committee, ministry, or faction.

    Covers top-level Knesset and government positions such as Prime Minister
    (ראש הממשלה), Knesset Speaker (יו\"ר הכנסת), Deputy Speakers, and similar.
    The generic 'חבר כנסת'/'ח\"כ' role held by all members is excluded.
    """
    position: str = Field(description="Role title in Hebrew (e.g. 'ראש הממשלה', 'יו\"ר הכנסת', 'סגן יו\"ר הכנסת')")
    holders: list[str] = Field(
        description=(
            "People who held this role during the term. "
            "Each entry: '{member_id}: {name} ({party}) [from {start}] [to {end}]'. "
            "Dates are omitted when they span the full Knesset term."
        ),
    )


class MetadataResult(KNSBaseModel):
    """Full metadata for a single Knesset term.

    Only sections whose include_* flag was set to True are present;
    all others are null (absent from the JSON response).
    """
    knesset_num: int = Field(description="Knesset term number")
    knesset_assemblies: list[KnessetAssembly] | None = Field(
        default=None,
        description=(
            "Calendar of sitting periods: each entry is one convening period (כינוס) "
            "within an annual session (מושב). Sorted newest first (session DESC, assembly DESC). "
            "Present only when include_assemblies=True."
        ),
    )
    committees: list[CommitteeMeta] | None = Field(
        default=None,
        description="All committees active during the term, sorted by name. Present only when include_committees=True.",
    )
    gov_ministries: list[GovMinistryMeta] | None = Field(
        default=None,
        description="Government ministries with their ministers and members. Present only when include_ministries=True.",
    )
    factions: list[FactionMeta] | None = Field(
        default=None,
        description="Parliamentary factions (סיעות) with their members. Present only when include_factions=True.",
    )
    general_roles: list[GeneralRoleMeta] | None = Field(
        default=None,
        description=(
            "Top-level parliamentary and government roles (e.g. Prime Minister, Knesset Speaker) "
            "not linked to any specific committee, ministry, or faction. "
            "Present only when include_roles=True."
        ),
    )
