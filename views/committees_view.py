"""Committees list view — returns summary data for multiple committees.

Shows general committee info (name, type, knesset, category, parent).
For full detail on a single committee (sessions, members, bills, documents),
use ``committee_view.get_committee()``.
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from config import DEFAULT_DB
from core.db import ensure_indexes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string."""
    if not date_str:
        return ""
    s = str(date_str)
    if "T" in s:
        return s.split("T")[0]
    if " " in s:
        return s.split(" ")[0]
    return s


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_committees(
    knesset_num=None,
    name=None,
    committee_type=None,
    category=None,
    is_current=None,
    parent_committee_id=None,
) -> list:
    """Search for committees and return summary metadata.

    Filters (all ANDed):
      - knesset_num: committee's Knesset number
      - name: committee name contains text
      - committee_type: CommitteeTypeDesc contains text (ועדה ראשית, ועדת משנה, etc.)
      - category: CategoryDesc contains text
      - is_current: True for current committees, False for inactive
      - parent_committee_id: parent committee ID (for sub-committees)

    Returns a list of committee summary dicts sorted by (knesset_num, name).
    """
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
    ensure_indexes(conn)
    cursor = conn.cursor()

    sql = """
    SELECT c.Id, c.Name, c.KnessetNum, c.CommitteeTypeDesc,
           c.CategoryDesc, c.IsCurrent, c.StartDate, c.FinishDate,
           c.ParentCommitteeID, c.CommitteeParentName, c.Email
    FROM committee_raw c
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND c.KnessetNum = ?"
        params.append(knesset_num)

    if name:
        sql += " AND c.Name LIKE ?"
        params.append(f"%{name}%")

    if committee_type:
        sql += " AND c.CommitteeTypeDesc LIKE ?"
        params.append(f"%{committee_type}%")

    if category:
        sql += " AND c.CategoryDesc LIKE ?"
        params.append(f"%{category}%")

    if is_current is not None:
        sql += " AND c.IsCurrent = ?"
        params.append(1 if is_current else 0)

    if parent_committee_id is not None:
        sql += " AND c.ParentCommitteeID = ?"
        params.append(parent_committee_id)

    sql += " ORDER BY c.KnessetNum, c.Name"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "committee_id": row["Id"],
            "name": row["Name"],
            "knesset_num": row["KnessetNum"],
            "type": row["CommitteeTypeDesc"],
            "category": row["CategoryDesc"],
            "is_current": bool(row["IsCurrent"]),
            "start_date": _simple_date(row["StartDate"]),
            "end_date": _simple_date(row["FinishDate"]),
            "parent_committee_id": row["ParentCommitteeID"],
            "parent_committee_name": row["CommitteeParentName"],
        })

    conn.close()
    return results
