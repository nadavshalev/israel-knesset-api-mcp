"""Members list view — returns summary data for multiple Knesset members.

Shows general person info and a list of distinct role types each member held.
For full detail on a single member (committees, government roles, etc.),
use ``member_view.get_member()``.
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
from core.helpers import simple_date, format_person_name, normalize_inputs, _clean
from core.mcp_meta import mcp_tool
from core.search_meta import register_search

register_search({
    "entity_key": "members",
    "count_sql": """
        SELECT COUNT(DISTINCT PersonID) FROM person_raw
        WHERE FirstName LIKE %s OR LastName LIKE %s
    """,
    "search_sql": """
        SELECT DISTINCT PersonID AS id,
               FirstName || ' ' || LastName AS name,
               LastName, FirstName
        FROM person_raw
        WHERE FirstName LIKE %s OR LastName LIKE %s
        ORDER BY LastName, FirstName
        LIMIT %s
    """,
    "param_count": 2,
})


def _resolve_role_ids(cursor, role_type: str) -> list:
    """Map a free-text role_type to matching Position IDs."""
    cursor.execute(
        "SELECT Id, Description FROM position_raw WHERE Description LIKE %s",
        (f"%{role_type}%",),
    )
    rows = cursor.fetchall()
    return [row["id"] for row in rows]


# ---------------------------------------------------------------------------
# Bulk query — fetch all matching members in a single round-trip
# ---------------------------------------------------------------------------

def _fetch_members_bulk(cursor, *, knesset_num=None, first_name=None,
                        last_name=None, role=None, role_ids=None,
                        party=None, person_id=None):
    """Fetch all matching members in one query and group in Python.

    Returns a list of summary dicts sorted by (knesset_num DESC, member_id).
    Uses a single SQL query instead of per-member round-trips.
    """
    sql = """
    SELECT p.PersonID, p.FirstName, p.LastName, p.GenderDesc,
           ptp.KnessetNum, ptp.FactionName,
           pos.Description AS OfficialPositionTitle,
           ptp.StartDate
    FROM person_raw p
    JOIN person_to_position_raw ptp ON p.PersonID = ptp.PersonID
    LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND ptp.KnessetNum = %s"
        params.append(knesset_num)

    if person_id is not None:
        sql += " AND p.PersonID = %s"
        params.append(person_id)

    if first_name:
        sql += " AND p.FirstName LIKE %s"
        params.append(f"%{first_name}%")

    if last_name:
        sql += " AND p.LastName LIKE %s"
        params.append(f"%{last_name}%")

    if role:
        sql += """ AND (
            pos.Description LIKE %s OR
            ptp.DutyDesc LIKE %s OR
            ptp.GovMinistryName LIKE %s OR
            ptp.CommitteeName LIKE %s
        )"""
        params.extend([f"%{role}%"] * 4)

    if role_ids:
        placeholders = ", ".join(["%s"] * len(role_ids))
        sql += f" AND ptp.PositionID IN ({placeholders})"
        params.extend(role_ids)

    if party:
        sql += """
        AND EXISTS (
            SELECT 1 FROM person_to_position_raw f
            WHERE f.PersonID = p.PersonID
              AND f.KnessetNum = ptp.KnessetNum
              AND f.FactionName LIKE %s
        )"""
        params.append(f"%{party}%")

    sql += " ORDER BY ptp.KnessetNum DESC, p.PersonID, ptp.StartDate ASC"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    # Group rows by (PersonID, KnessetNum) preserving order
    members = {}
    order = []
    for row in rows:
        pid = row["personid"]
        kns = row["knessetnum"]
        if not pid or not isinstance(kns, int):
            continue
        key = (pid, kns)
        if key not in members:
            members[key] = {
                "member_id": pid,
                "name": format_person_name(row["firstname"], row["lastname"]),
                "gender": row["genderdesc"],
                "knesset_num": kns,
                "faction": [],
                "role_types": [],
            }
            order.append(key)
        m = members[key]
        fn = row["factionname"]
        if fn and fn not in m["faction"]:
            m["faction"].append(fn)
        title = row["officialpositiontitle"] or ""
        if title and title not in m["role_types"]:
            m["role_types"].append(title)

    return [members[k] for k in order]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_members",
    description=(
        "Search for Knesset members (MKs). Returns summary info: name, "
        "gender, knesset number, factions, and role types. "
        "Use get_member for full detail on a single member."
    ),
    entity="Knesset Members",
    count_sql="SELECT COUNT(DISTINCT PersonID) FROM person_to_position_raw",
    most_recent_date_sql="SELECT MAX(StartDate) FROM person_to_position_raw",
    enum_sql={
        "role_type": "SELECT DISTINCT Description FROM position_raw ORDER BY Description",
    },
    is_list=True,
)
def search_members(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    first_name: Annotated[str | None, Field(description="First name contains text")] = None,
    last_name: Annotated[str | None, Field(description="Last name contains text")] = None,
    role: Annotated[str | None, Field(description="Free text search across roles, ministries, and committees")] = None,
    role_type: Annotated[str | None, Field(description="Position category")] = None,
    party: Annotated[str | None, Field(description="Party/faction name contains text")] = None,
    person_id: Annotated[int | None, Field(description="Filter by specific person ID")] = None,
) -> list:
    """Search for Knesset members with dynamic filtering.

    Filters (all ANDed): knesset_num, first_name, last_name, role
    (free text across roles/ministries/committees), role_type (position
    category), party (party/faction name), person_id.

    Returns a list of summary dicts sorted by (knesset_num DESC, member_id).
    Each dict contains general info and a ``role_types`` list.
    For full detail on a single member, use ``member_view.get_member()``.
    """
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    first_name = normalized["first_name"]
    last_name = normalized["last_name"]
    role = normalized["role"]
    role_type = normalized["role_type"]
    party = normalized["party"]
    person_id = normalized["person_id"]

    conn = connect_readonly()
    cursor = conn.cursor()

    # Resolve role_type -> position IDs
    role_ids = None
    if role_type:
        role_ids = _resolve_role_ids(cursor, role_type)
        if not role_ids:
            conn.close()
            return []

    results = _fetch_members_bulk(
        cursor,
        knesset_num=knesset_num,
        first_name=first_name,
        last_name=last_name,
        role=role,
        role_ids=role_ids,
        party=party,
        person_id=person_id,
    )

    conn.close()
    return _clean(results)


search_members.RESPONSE_SCHEMA = {
    "_type": "list[dict]",
    "_description": "List of member summaries sorted by knesset_num DESC, member_id",
    "member_id": {"type": "int", "optional": False, "description": "Member person ID"},
    "name": {"type": "str", "optional": False, "description": "Full name"},
    "gender": {"type": "str", "optional": True, "description": "Gender description"},
    "knesset_num": {"type": "int", "optional": False, "description": "Knesset number"},
    "faction": {"type": "list[str]", "optional": False, "description": "Faction/party names"},
    "role_types": {"type": "list[str]", "optional": False, "description": "Distinct position titles held"},
}
