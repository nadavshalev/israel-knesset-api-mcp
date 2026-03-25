"""Knesset metadata view — returns structured term metadata in a single call.

Includes assembly/plenum dates, committees (with heads), government ministries,
and parliamentary factions. Use the granular include_* flags to add member lists
per section.

Section fetchers are public so they can be reused by MCP resource handlers.
"""

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly
from core.helpers import simple_date, format_person_name, normalize_inputs
from core.mcp_meta import mcp_tool
from origins.knesset.metadata_models import (
    KnessetAssembly,
    CommitteeMeta,
    GovMinistryMeta,
    FactionMeta,
    GeneralRoleMeta,
    MetadataResult,
)


def _fmt_member(
    member_id: int,
    name: str,
    party: str | None,
    position: str | None,
    start: str | None,
    end: str | None,
    parent_start: str | None,
    parent_end: str | None,
) -> str:
    """Build a compact member string, eliding dates that match the parent entity."""
    parts = [f"{member_id}: {name}"]
    if party:
        parts[0] += f" ({party})"
    if position:
        parts[0] += f" - {position}"
    # Elide start if it matches parent start
    if start and start != parent_start:
        parts.append(f"from {start}")
    # Elide end if it matches parent end, or if both are None (still ongoing)
    if end is not None and end != parent_end:
        parts.append(f"to {end}")
    return " ".join(parts) if len(parts) == 1 else parts[0] + " " + " ".join(parts[1:])


_MINISTER_ROLES = {"שר", "שרה"}
_DEPUTY_ROLES = {"סגן שר", "סגנית שר"}


# ---------------------------------------------------------------------------
# Section fetchers (reusable by MCP resource handlers)
# ---------------------------------------------------------------------------

def fetch_knesset_span(cursor, knesset_num: int) -> tuple[str | None, str | None]:
    """Return (start_date, end_date) for the knesset term."""
    cursor.execute(
        """
        SELECT MIN(PlenumStart) AS kstart, MAX(PlenumFinish) AS kend
        FROM knesset_dates_raw WHERE KnessetNum = %s
        """,
        [knesset_num],
    )
    row = cursor.fetchone()
    return (simple_date(row["kstart"]) if row else None,
            simple_date(row["kend"]) if row else None)


def fetch_assemblies(cursor, knesset_num: int) -> list[KnessetAssembly]:
    cursor.execute(
        """
        SELECT Id, Assembly, Plenum, PlenumStart, PlenumFinish, IsCurrent
        FROM knesset_dates_raw WHERE KnessetNum = %s
        ORDER BY Assembly, Plenum
        """,
        [knesset_num],
    )
    return [
        KnessetAssembly(
            assembly_year=row["assembly"],
            plenum_number=row["plenum"],
            start_date=simple_date(row["plenumstart"]),
            end_date=simple_date(row["plenumfinish"]),
        )
        for row in cursor.fetchall()
    ]


def fetch_committees(
    cursor, knesset_num: int, include_heads: bool,
    knesset_start: str | None, knesset_end: str | None,
) -> list[CommitteeMeta]:
    cursor.execute(
        """
        SELECT Id, Name, CommitteeTypeDesc, CommitteeParentName, StartDate, FinishDate
        FROM committee_raw WHERE KnessetNum = %s
        ORDER BY Name
        """,
        [knesset_num],
    )
    committee_rows = cursor.fetchall()

    heads_by_committee: dict[int, list[str]] = defaultdict(list)
    if include_heads:
        cursor.execute(
            """
            SELECT ptp.CommitteeID, ptp.PersonID, p.FirstName, p.LastName,
                   ptp.FactionName, pos.Description, ptp.StartDate, ptp.FinishDate
            FROM person_to_position_raw ptp
            JOIN person_raw p ON p.PersonID = ptp.PersonID
            LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
            WHERE ptp.KnessetNum = %s
              AND ptp.CommitteeID IS NOT NULL
              AND pos.Description LIKE '%%יו"ר%%'
            ORDER BY ptp.CommitteeID, ptp.StartDate
            """,
            [knesset_num],
        )
        committee_dates: dict[int, tuple[str | None, str | None]] = {
            row["id"]: (simple_date(row["startdate"]), simple_date(row["finishdate"]))
            for row in committee_rows
        }
        for row in cursor.fetchall():
            cid = row["committeeid"]
            parent_start, parent_end = committee_dates.get(cid, (None, None))
            heads_by_committee[cid].append(
                _fmt_member(
                    member_id=row["personid"],
                    name=format_person_name(row["firstname"], row["lastname"]),
                    party=row["factionname"],
                    position=None,
                    start=simple_date(row["startdate"]),
                    end=simple_date(row["finishdate"]),
                    parent_start=parent_start,
                    parent_end=parent_end,
                )
            )

    return [
        CommitteeMeta(
            committee_id=row["id"],
            name=row["name"],
            type=row["committeetypedesc"],
            parent_committee=row["committeeparentname"],
            start_date=simple_date(row["startdate"]),
            end_date=simple_date(row["finishdate"]),
            heads=heads_by_committee.get(row["id"]) if include_heads else None,
        )
        for row in committee_rows
    ]


def fetch_ministries(
    cursor, knesset_num: int, include_members: bool,
    knesset_start: str | None, knesset_end: str | None,
) -> list[GovMinistryMeta]:
    cursor.execute(
        """
        SELECT DISTINCT ptp.GovMinistryID, gm.Name
        FROM person_to_position_raw ptp
        JOIN gov_ministry_raw gm ON ptp.GovMinistryID = gm.Id
        WHERE ptp.KnessetNum = %s AND ptp.GovMinistryID IS NOT NULL
        ORDER BY gm.Name
        """,
        [knesset_num],
    )
    ministry_rows = cursor.fetchall()

    minister_by_ministry: dict[int, list[str]] = defaultdict(list)
    deputy_by_ministry: dict[int, list[str]] = defaultdict(list)
    members_by_ministry: dict[int, list[str]] = defaultdict(list)
    if include_members:
        cursor.execute(
            """
            SELECT ptp.GovMinistryID, ptp.PersonID, p.FirstName, p.LastName,
                   ptp.FactionName, pos.Description,
                   ptp.StartDate, ptp.FinishDate
            FROM person_to_position_raw ptp
            JOIN person_raw p ON p.PersonID = ptp.PersonID
            LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
            WHERE ptp.KnessetNum = %s AND ptp.GovMinistryID IS NOT NULL
            ORDER BY ptp.GovMinistryID, ptp.StartDate
            """,
            [knesset_num],
        )
        for row in cursor.fetchall():
            mid = row["govministryid"]
            role = row["description"] or ""
            entry = _fmt_member(
                member_id=row["personid"],
                name=format_person_name(row["firstname"], row["lastname"]),
                party=row["factionname"],
                position=None,
                start=simple_date(row["startdate"]),
                end=simple_date(row["finishdate"]),
                parent_start=knesset_start,
                parent_end=knesset_end,
            )
            if role in _MINISTER_ROLES:
                minister_by_ministry[mid].append(entry)
            elif role in _DEPUTY_ROLES:
                deputy_by_ministry[mid].append(entry)
            else:
                members_by_ministry[mid].append(entry)

    def _nonempty(lst: list) -> list | None:
        return lst if lst else None

    return [
        GovMinistryMeta(
            ministry_id=row["govministryid"],
            name=row["name"],
            minister=_nonempty(minister_by_ministry.get(row["govministryid"], [])) if include_members else None,
            deputy_ministers=_nonempty(deputy_by_ministry.get(row["govministryid"], [])) if include_members else None,
            members=_nonempty(members_by_ministry.get(row["govministryid"], [])) if include_members else None,
        )
        for row in ministry_rows
    ]


def fetch_factions(
    cursor, knesset_num: int, include_members: bool,
    knesset_start: str | None, knesset_end: str | None,
) -> list[FactionMeta]:
    cursor.execute(
        """
        SELECT Id, Name, StartDate, FinishDate
        FROM faction_raw WHERE KnessetNum = %s
        ORDER BY Name
        """,
        [knesset_num],
    )
    faction_rows = cursor.fetchall()

    members_by_faction: dict[int, list[str]] = defaultdict(list)
    if include_members:
        cursor.execute(
            """
            SELECT ptp.FactionID, ptp.PersonID, p.FirstName, p.LastName,
                   ptp.StartDate, ptp.FinishDate
            FROM person_to_position_raw ptp
            JOIN person_raw p ON p.PersonID = ptp.PersonID
            WHERE ptp.KnessetNum = %s AND ptp.FactionID IS NOT NULL
              AND ptp.FactionName IS NOT NULL
            ORDER BY ptp.FactionID, p.LastName, p.FirstName
            """,
            [knesset_num],
        )
        # Deduplicate (PersonID, FactionID) pairs keeping widest date range
        seen: dict[tuple[int, int], dict] = {}
        for row in cursor.fetchall():
            key = (row["factionid"], row["personid"])
            sd = simple_date(row["startdate"])
            ed = simple_date(row["finishdate"])
            if key in seen:
                existing = seen[key]
                if sd and (not existing["start_date"] or sd < existing["start_date"]):
                    existing["start_date"] = sd
                if ed and (not existing["end_date"] or ed > existing["end_date"]):
                    existing["end_date"] = ed
                elif not ed:
                    existing["end_date"] = None
            else:
                seen[key] = {
                    "faction_id": row["factionid"],
                    "person_id": row["personid"],
                    "name": format_person_name(row["firstname"], row["lastname"]),
                    "start_date": sd,
                    "end_date": ed,
                }

        # Build a lookup from faction_id → (start, end) for elision
        faction_dates: dict[int, tuple[str | None, str | None]] = {
            row["id"]: (simple_date(row["startdate"]), simple_date(row["finishdate"]))
            for row in faction_rows
        }
        for (faction_id, _), member in seen.items():
            parent_start, parent_end = faction_dates.get(faction_id, (None, None))
            members_by_faction[faction_id].append(
                _fmt_member(
                    member_id=member["person_id"],
                    name=member["name"],
                    party=None,
                    position=None,
                    start=member["start_date"],
                    end=member["end_date"],
                    parent_start=parent_start,
                    parent_end=parent_end,
                )
            )

    return [
        FactionMeta(
            faction_id=row["id"],
            name=row["name"],
            start_date=simple_date(row["startdate"]),
            end_date=simple_date(row["finishdate"]),
            members=members_by_faction.get(row["id"]) if include_members else None,
        )
        for row in faction_rows
    ]


def fetch_general_roles(
    cursor, knesset_num: int,
    knesset_start: str | None, knesset_end: str | None,
) -> list[GeneralRoleMeta]:
    cursor.execute(
        """
        SELECT ptp.PersonID, p.FirstName, p.LastName, ptp.FactionName,
               pos.Description AS position_desc,
               ptp.StartDate, ptp.FinishDate
        FROM person_to_position_raw ptp
        JOIN person_raw p ON p.PersonID = ptp.PersonID
        LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
        WHERE ptp.KnessetNum = %s
          AND ptp.CommitteeID IS NULL
          AND ptp.GovMinistryID IS NULL
          AND ptp.FactionID IS NULL
          AND pos.Description IS NOT NULL
          AND pos.Description NOT LIKE '%%חבר כנסת%%'
          AND pos.Description NOT LIKE '%%חבר הכנסת%%'
          AND pos.Description NOT LIKE '%%חברת הכנסת%%'
          AND pos.Description NOT LIKE '%%ח"כ%%'
        ORDER BY pos.Description, p.LastName, p.FirstName
        """,
        [knesset_num],
    )
    # Deduplicate (PersonID, position_desc) keeping widest date range
    general_seen: dict[tuple[int, str], dict] = {}
    for row in cursor.fetchall():
        pos_desc = row["position_desc"]
        key = (row["personid"], pos_desc)
        sd = simple_date(row["startdate"])
        ed = simple_date(row["finishdate"])
        if key in general_seen:
            existing = general_seen[key]
            if sd and (not existing["start_date"] or sd < existing["start_date"]):
                existing["start_date"] = sd
            if ed and (not existing["end_date"] or ed > existing["end_date"]):
                existing["end_date"] = ed
            elif not ed:
                existing["end_date"] = None
        else:
            general_seen[key] = {
                "person_id": row["personid"],
                "name": format_person_name(row["firstname"], row["lastname"]),
                "party": row["factionname"],
                "position_desc": pos_desc,
                "start_date": sd,
                "end_date": ed,
            }

    # Group by position_desc
    holders_by_position: dict[str, list[str]] = defaultdict(list)
    for (_, pos_desc), member in sorted(general_seen.items(), key=lambda x: (x[0][1], x[1]["name"])):
        holders_by_position[pos_desc].append(
            _fmt_member(
                member_id=member["person_id"],
                name=member["name"],
                party=member["party"],
                position=None,
                start=member["start_date"],
                end=member["end_date"],
                parent_start=knesset_start,
                parent_end=knesset_end,
            )
        )

    return [
        GeneralRoleMeta(position=pos_desc, holders=holders)
        for pos_desc, holders in sorted(holders_by_position.items())
    ]


# ---------------------------------------------------------------------------
# MCP tool (thin wrapper)
# ---------------------------------------------------------------------------

@mcp_tool(
    name="metadata",
    description=(
        "Get Knesset term metadata: assembly/plenum dates, committees, government ministries, "
        "and parliamentary factions. Use include_committee_heads=True, "
        "include_ministry_members=True, and/or include_faction_members=True to add member "
        "lists per section. Flags can be combined. "
        "Always includes general_roles: parliamentary roles not linked to committee/ministry/faction "
        "(e.g. ראש הממשלה, יו\"ר הכנסת, ראש ממשלה חליפי)."
    ),
    entity="Knesset Metadata",
)
def metadata(
    knesset_num: int,
    include_committee_heads: bool = False,
    include_ministry_members: bool = False,
    include_faction_members: bool = False,
) -> MetadataResult:
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    include_committee_heads = normalized["include_committee_heads"]
    include_ministry_members = normalized["include_ministry_members"]
    include_faction_members = normalized["include_faction_members"]

    conn = connect_readonly()
    cursor = conn.cursor()

    knesset_start, knesset_end = fetch_knesset_span(cursor, knesset_num)
    assemblies = fetch_assemblies(cursor, knesset_num)
    committees = fetch_committees(cursor, knesset_num, include_committee_heads, knesset_start, knesset_end)
    gov_ministries = fetch_ministries(cursor, knesset_num, include_ministry_members, knesset_start, knesset_end)
    factions = fetch_factions(cursor, knesset_num, include_faction_members, knesset_start, knesset_end)
    general_roles = fetch_general_roles(cursor, knesset_num, knesset_start, knesset_end)

    conn.close()

    return MetadataResult(
        knesset_num=knesset_num,
        knesset_assemblies=assemblies,
        committees=committees,
        gov_ministries=gov_ministries,
        factions=factions,
        general_roles=general_roles,
    )


metadata.OUTPUT_MODEL = MetadataResult
