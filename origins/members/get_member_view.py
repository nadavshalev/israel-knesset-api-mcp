"""Single member detail view — returns full data for one member by ID.

Includes general info, factions, government roles, committee memberships,
and parliamentary roles.  For searching/filtering multiple members, use
``members_view.search_members()``.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from typing import Annotated
from pydantic import Field

from core.db import connect_readonly
from core.helpers import simple_date, format_person_name, normalize_inputs
from core.mcp_meta import mcp_tool
from origins.members.get_member_models import (
    GovernmentRole, CommitteeRole, ParliamentaryRole, MemberRoles,
    MemberDetail, MemberDetailList,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_category(row) -> str:
    """Classify a person_to_position_raw row by which fields are populated.

    Each row belongs to exactly one category:
      faction       – FactionName is set
      government    – GovMinistryName is set
      committee     – CommitteeID is set
      parliamentary – none of the above (e.g. חבר הכנסת, יו"ר הכנסת)

    Accepts both lowercase keys (from RealDictCursor) and PascalCase keys
    (from manually constructed dicts in tests).
    """
    def _get(key):
        """Look up key case-insensitively."""
        lower = key.lower()
        for k in row:
            if k.lower() == lower:
                return row[k]
        return None

    if _get("FactionName"):
        return "faction"
    if _get("GovMinistryName"):
        return "government"
    if _get("CommitteeID"):
        return "committee"
    return "parliamentary"


def _is_transition_gov(knesset_num: int, gov_num: int) -> bool:
    if not gov_num or gov_num == 0 or knesset_num < 19:
        return False
    primary_gov_mapping = {
        25: 37,  # Current
        24: 36,  # Bennett-Lapid
        23: 35,  # Netanyahu-Gantz
        22: 34,  # Transition period
        21: 34,  # Transition period
        20: 34,  # Netanyahu
        19: 33,  # Netanyahu
    }
    return gov_num < primary_gov_mapping.get(knesset_num, 0)


# ---------------------------------------------------------------------------
# Build — full member object for one (PersonID, KnessetNum)
# ---------------------------------------------------------------------------

def _build_member_detail(cursor, person_id, knesset_num) -> MemberDetail | None:
    """Build a full MemberDetail model for one member in one Knesset term.

    Includes government roles, committee memberships, and parliamentary roles.
    """
    cursor.execute(
        "SELECT FirstName, LastName, GenderDesc FROM person_raw WHERE PersonID = %s",
        (person_id,),
    )
    p_info = cursor.fetchone()
    if not p_info:
        return None

    cursor.execute(
        """
        SELECT ptp.*, pos.Description AS OfficialPositionTitle
        FROM person_to_position_raw ptp
        LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
        WHERE ptp.PersonID = %s AND ptp.KnessetNum = %s
        ORDER BY ptp.StartDate ASC
        """,
        (person_id, knesset_num),
    )
    role_rows = cursor.fetchall()

    factions: list[str] = []
    gov_roles: list[GovernmentRole] = []
    cmt_roles: list[CommitteeRole] = []
    parl_roles: list[ParliamentaryRole] = []

    for row in role_rows:
        cat = _row_category(row)
        display_title = row["dutydesc"] or row["officialpositiontitle"] or ""

        if cat == "faction":
            factions.append(row["factionname"])

        elif cat == "government":
            gov_roles.append(GovernmentRole(
                title=display_title or None,
                ministry=row["govministryname"] or None,
                is_transition=_is_transition_gov(knesset_num, row["governmentnum"]),
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))

        elif cat == "committee":
            cmt_roles.append(CommitteeRole(
                id=row["committeeid"],
                name=row["committeename"] or None,
                role=row["officialpositiontitle"] or None,
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))

        else:  # parliamentary
            parl_roles.append(ParliamentaryRole(
                name=display_title or None,
                role=row["officialpositiontitle"] or None,
                start=simple_date(row["startdate"]) or None,
                end=simple_date(row["finishdate"]) or None,
            ))

    return MemberDetail(
        member_id=person_id,
        name=format_person_name(p_info['firstname'], p_info['lastname']),
        gender=p_info["genderdesc"] or None,
        knesset_num=knesset_num,
        faction=factions,
        roles=MemberRoles(
            government=gov_roles,
            committees=cmt_roles,
            parliamentary=parl_roles,
        ),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_member",
    description=(
        "Get full detail for a single Knesset member by ID. Includes "
        "factions, government roles, committee memberships, and "
        "parliamentary roles. If knesset_num is omitted, returns all terms."
    ),
    entity="Knesset Members",
    is_list=False,
)
def get_member(
    member_id: Annotated[int, Field(description="The member's person ID (required)")],
    knesset_num: Annotated[int | None, Field(description="Knesset number to filter by; omit for all terms")] = None,
) -> MemberDetail | MemberDetailList | None:
    """Return full detail for a single member.

    If ``knesset_num`` is provided, returns a ``MemberDetail`` for that term.
    If omitted, returns a ``MemberDetailList`` — one item per Knesset term
    the member served in.  Returns ``None`` if no data is found.
    """
    normalized = normalize_inputs(locals())
    member_id = normalized["member_id"]
    knesset_num = normalized["knesset_num"]

    conn = connect_readonly()
    cursor = conn.cursor()

    if knesset_num is not None:
        result = _build_member_detail(cursor, member_id, knesset_num)
        conn.close()
        return result

    # No knesset_num — return all terms
    cursor.execute(
        """
        SELECT DISTINCT KnessetNum
        FROM person_to_position_raw
        WHERE PersonID = %s
        ORDER BY KnessetNum ASC
        """,
        (member_id,),
    )
    knesset_nums = [row["knessetnum"] for row in cursor.fetchall()
                    if isinstance(row["knessetnum"], int)]

    if not knesset_nums:
        conn.close()
        return None

    results = []
    for kns in knesset_nums:
        obj = _build_member_detail(cursor, member_id, kns)
        if obj:
            results.append(obj)

    conn.close()
    return MemberDetailList(items=results) if results else None


get_member.OUTPUT_MODEL = MemberDetailList
