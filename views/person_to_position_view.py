import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from config import DEFAULT_DB


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
# Search  –  find (PersonID, KnessetNum) tuples matching filters
# ---------------------------------------------------------------------------

def _find_matching_persons(cursor, *, knesset_num=None, first_name=None,
                           last_name=None, role_query=None, role_ids=None,
                           faction_query=None, person_id=None):
    """Return a set of (PersonID, KnessetNum) tuples matching all filters.

    Filters that target different row categories use EXISTS sub-queries so
    they can cross categories correctly (e.g. "ministers from Likud" needs a
    government row AND a faction row for the same person+knesset).
    """
    sql = """
    SELECT DISTINCT p.PersonID, ptp.KnessetNum
    FROM person_raw p
    JOIN person_to_position_raw ptp ON p.PersonID = ptp.PersonID
    LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
    WHERE 1=1
    """
    params = []

    # --- filters on the main row ---

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

    # --- cross-category filters (EXISTS) ---

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
# Build  –  structured member object for one (PersonID, KnessetNum)
# ---------------------------------------------------------------------------

def _build_member_object(cursor, person_id, knesset_num, *, show_committees=False):
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
        "roles": {"government": [], "parliamentary": []},
    }
    if show_committees:
        member["roles"]["committees"] = []

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
            if show_committees:
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

def search_knesset_members(
    knesset_num=None,
    first_name=None,
    last_name=None,
    role_query=None,
    role_type=None,
    faction_query=None,
    person_id=None,
    show_committees=False,
) -> list:
    """Search for Knesset members with dynamic filtering.

    Filters: knesset_num, name, role text, role category (role_type),
    faction/party, person_id.
    """
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
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

    # Build structured objects
    results = []
    for p_id, kns in sorted(matches, key=lambda x: (x[1], x[0])):
        obj = _build_member_object(cursor, p_id, kns, show_committees=show_committees)
        if obj:
            results.append(obj)

    conn.close()
    return results
