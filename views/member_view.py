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

from core.db import connect_readonly


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
    """
    if row["FactionName"]:
        return "faction"
    if row["GovMinistryName"]:
        return "government"
    if row["CommitteeID"]:
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


def _simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string."""
    if not date_str:
        return ""
    return str(date_str).split("T")[0]


# ---------------------------------------------------------------------------
# Build — full member object for one (PersonID, KnessetNum)
# ---------------------------------------------------------------------------

def _build_member_detail(cursor, person_id, knesset_num):
    """Build a full detail dict for one member in one Knesset term.

    Includes government roles, committee memberships, and parliamentary roles.
    """
    cursor.execute(
        "SELECT FirstName, LastName, GenderDesc FROM person_raw WHERE PersonID = ?",
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
        WHERE ptp.PersonID = ? AND ptp.KnessetNum = ?
        ORDER BY ptp.StartDate ASC
        """,
        (person_id, knesset_num),
    )
    role_rows = cursor.fetchall()

    member = {
        "member_id": person_id,
        "name": f"{p_info['FirstName']} {p_info['LastName']}",
        "gender": p_info["GenderDesc"],
        "knesset_num": knesset_num,
        "faction": [],
        "roles": {
            "government": [],
            "committees": [],
            "parliamentary": [],
        },
    }

    for row in role_rows:
        cat = _row_category(row)
        display_title = row["DutyDesc"] or row["OfficialPositionTitle"] or ""

        if cat == "faction":
            member["faction"].append(row["FactionName"])

        elif cat == "government":
            member["roles"]["government"].append({
                "title": display_title,
                "ministry": row["GovMinistryName"],
                "is_transition": _is_transition_gov(knesset_num, row["GovernmentNum"]),
                "start": _simple_date(row["StartDate"]),
                "end": _simple_date(row["FinishDate"]),
            })

        elif cat == "committee":
            member["roles"]["committees"].append({
                "id": row["CommitteeID"],
                "name": row["CommitteeName"],
                "role": row["OfficialPositionTitle"],
                "start": _simple_date(row["StartDate"]),
                "end": _simple_date(row["FinishDate"]),
            })

        else:  # parliamentary
            member["roles"]["parliamentary"].append({
                "name": display_title,
                "role": row["OfficialPositionTitle"],
                "start": _simple_date(row["StartDate"]),
                "end": _simple_date(row["FinishDate"]),
            })

    return member


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_member(member_id: int, knesset_num: int = None) -> dict | list | None:
    """Return full detail for a single member.

    If ``knesset_num`` is provided, returns a single dict for that term.
    If omitted, returns a list of dicts — one per Knesset term the member
    served in.  Returns ``None`` if no data is found.
    """
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
        WHERE PersonID = ?
        ORDER BY KnessetNum ASC
        """,
        (member_id,),
    )
    knesset_nums = [row["KnessetNum"] for row in cursor.fetchall()
                    if isinstance(row["KnessetNum"], int)]

    if not knesset_nums:
        conn.close()
        return None

    results = []
    for kns in knesset_nums:
        obj = _build_member_detail(cursor, member_id, kns)
        if obj:
            results.append(obj)

    conn.close()
    return results if results else None
