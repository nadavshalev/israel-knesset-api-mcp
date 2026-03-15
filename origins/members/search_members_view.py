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
from core.helpers import simple_date, format_person_name, normalize_inputs
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from origins.members.search_members_models import MemberSummary, MemberSearchResults

def _build_members_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity member search.

    Supports: query (name LIKE), knesset_num (via person_to_position),
    date/date_to (members whose position overlaps the date range).
    """
    conditions = []
    params = []

    if query:
        conditions.append("(p.FirstName LIKE %s OR p.LastName LIKE %s)")
        params.extend([f"%{query}%", f"%{query}%"])

    if knesset_num is not None:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID AND ptp.KnessetNum = %s
        )""")
        params.append(knesset_num)

    if date and date_to:
        # Position overlaps the range: started before range end AND
        # not finished before range start (NULL/empty FinishDate = still active)
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID
            AND ptp.StartDate <= %s
            AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' OR ptp.FinishDate >= %s)
        )""")
        params.extend([date_to + "T99", date])
    elif date:
        # Position active on this date
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.PersonID = p.PersonID
            AND ptp.StartDate <= %s
            AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' OR ptp.FinishDate >= %s)
        )""")
        params.extend([date + "T99", date])

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(DISTINCT p.PersonID) FROM person_raw p
        WHERE {where}
    """
    search_sql = f"""
        SELECT DISTINCT p.PersonID AS id,
               p.FirstName || ' ' || p.LastName AS name,
               p.LastName, p.FirstName
        FROM person_raw p
        WHERE {where}
        ORDER BY p.LastName, p.FirstName
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "members",
    "builder": _build_members_search,
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
    members: dict[tuple, dict] = {}
    order: list[tuple] = []
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
) -> MemberSearchResults:
    """Search for Knesset members with dynamic filtering.

    Filters (all ANDed): knesset_num, first_name, last_name, role
    (free text across roles/ministries/committees), role_type (position
    category), party (party/faction name), person_id.

    Returns a ``MemberSearchResults`` with ``items`` sorted by
    (knesset_num DESC, member_id).  Each item contains general info
    and a ``role_types`` list.  For full detail on a single member,
    use ``member_view.get_member()``.
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
            return MemberSearchResults(items=[])

    raw = _fetch_members_bulk(
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

    items = [
        MemberSummary(
            member_id=m["member_id"],
            name=m["name"],
            gender=m["gender"] or None,
            knesset_num=m["knesset_num"],
            faction=m["faction"],
            role_types=m["role_types"],
        )
        for m in raw
    ]
    return MemberSearchResults(items=items)


search_members.OUTPUT_MODEL = MemberSearchResults
