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

from core.db import connect_readonly


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string."""
    if not date_str:
        return ""
    return str(date_str).split("T")[0]


def _resolve_role_ids(cursor, role_type: str) -> list:
    """Map a free-text role_type to matching Position IDs."""
    cursor.execute(
        "SELECT Id, Description FROM position_raw WHERE Description LIKE ?",
        (f"%{role_type}%",),
    )
    rows = cursor.fetchall()
    return [row["Id"] for row in rows]


# ---------------------------------------------------------------------------
# Search — find (PersonID, KnessetNum) tuples matching filters
# ---------------------------------------------------------------------------

def _find_matching_persons(cursor, *, knesset_num=None, first_name=None,
                           last_name=None, role_query=None, role_ids=None,
                           faction_query=None, person_id=None):
    """Return a set of (PersonID, KnessetNum) tuples matching all filters."""
    sql = """
    SELECT DISTINCT p.PersonID, ptp.KnessetNum
    FROM person_raw p
    JOIN person_to_position_raw ptp ON p.PersonID = ptp.PersonID
    LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND ptp.KnessetNum = ?"
        params.append(knesset_num)

    if person_id is not None:
        sql += " AND p.PersonID = ?"
        params.append(person_id)

    if first_name:
        sql += " AND p.FirstName LIKE ?"
        params.append(f"%{first_name}%")

    if last_name:
        sql += " AND p.LastName LIKE ?"
        params.append(f"%{last_name}%")

    if role_query:
        sql += """ AND (
            pos.Description LIKE ? OR
            ptp.DutyDesc LIKE ? OR
            ptp.GovMinistryName LIKE ? OR
            ptp.CommitteeName LIKE ?
        )"""
        params.extend([f"%{role_query}%"] * 4)

    if role_ids:
        placeholders = ", ".join(["?"] * len(role_ids))
        sql += f" AND ptp.PositionID IN ({placeholders})"
        params.extend(role_ids)

    if faction_query:
        sql += """
        AND EXISTS (
            SELECT 1 FROM person_to_position_raw f
            WHERE f.PersonID = p.PersonID
              AND f.KnessetNum = ptp.KnessetNum
              AND f.FactionName LIKE ?
        )"""
        params.append(f"%{faction_query}%")

    cursor.execute(sql, params)
    return {
        (r["PersonID"], r["KnessetNum"])
        for r in cursor.fetchall()
        if r["PersonID"] and isinstance(r["KnessetNum"], int)
    }


# ---------------------------------------------------------------------------
# Build — summary member object for one (PersonID, KnessetNum)
# ---------------------------------------------------------------------------

def _build_member_summary(cursor, person_id, knesset_num):
    """Build a summary dict for one member in one Knesset term.

    Returns general info (name, gender, faction) plus a ``role_types`` list
    of all distinct role types the member held (e.g. חבר כנסת, שר).
    """
    cursor.execute(
        "SELECT FirstName, LastName, GenderDesc FROM person_raw WHERE PersonID = ?",
        (person_id,),
    )
    p_info = cursor.fetchone()
    if not p_info:
        return None

    # Get all rows for this person+knesset to extract faction and role types
    cursor.execute(
        """
        SELECT ptp.FactionName, ptp.GovMinistryName,
               pos.Description AS OfficialPositionTitle
        FROM person_to_position_raw ptp
        LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
        WHERE ptp.PersonID = ? AND ptp.KnessetNum = ?
        ORDER BY ptp.StartDate ASC
        """,
        (person_id, knesset_num),
    )
    role_rows = cursor.fetchall()

    factions = []
    role_types = []

    for row in role_rows:
        if row["FactionName"] and row["FactionName"] not in factions:
            factions.append(row["FactionName"])

        title = row["OfficialPositionTitle"] or ""
        if title and title not in role_types:
            role_types.append(title)

    return {
        "member_id": person_id,
        "name": f"{p_info['FirstName']} {p_info['LastName']}",
        "gender": p_info["GenderDesc"],
        "knesset_num": knesset_num,
        "faction": factions,
        "role_types": role_types,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_members(
    knesset_num=None,
    first_name=None,
    last_name=None,
    role_query=None,
    role_type=None,
    faction_query=None,
    person_id=None,
) -> list:
    """Search for Knesset members with dynamic filtering.

    Filters (all ANDed): knesset_num, first_name, last_name, role_query
    (free text across roles/ministries/committees), role_type (position
    category), faction_query (party name), person_id.

    Returns a list of summary dicts sorted by (knesset_num, member_id).
    Each dict contains general info and a ``role_types`` list.
    For full detail on a single member, use ``member_view.get_member()``.
    """
    conn = connect_readonly()
    cursor = conn.cursor()

    # Resolve role_type -> position IDs
    role_ids = None
    if role_type:
        role_ids = _resolve_role_ids(cursor, role_type)
        if not role_ids:
            conn.close()
            return []

    # Find matching (PersonID, KnessetNum) pairs
    matches = _find_matching_persons(
        cursor,
        knesset_num=knesset_num,
        first_name=first_name,
        last_name=last_name,
        role_query=role_query,
        role_ids=role_ids,
        faction_query=faction_query,
        person_id=person_id,
    )

    # Build summary objects
    results = []
    for p_id, kns in sorted(matches, key=lambda x: (x[1], x[0])):
        obj = _build_member_summary(cursor, p_id, kns)
        if obj:
            results.append(obj)

    conn.close()
    return results
