"""Plenum sessions list view — returns summary data for multiple sessions.

No items or documents are included.  For full detail on a single session
(including items and documents), use ``plenum_session_view.get_session()``.
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_sessions(
    knesset_num=None,
    from_date=None,
    to_date=None,
    date=None,
    name=None,
    item_type=None,
) -> list:
    """Search for plenum sessions and return summary data (no items/docs).

    Filters (all ANDed):
      - knesset_num: Knesset number
      - from_date / to_date / date: session date range
      - name: session name or item name contains text
      - item_type: session has items of this type

    Returns a list of session summary dicts sorted by (knesset_num, date).
    """
    conn = connect_readonly()
    cursor = conn.cursor()

    sql = """
    SELECT DISTINCT s.Id, s.KnessetNum, s.Name, s.StartDate
    FROM plenum_session_raw s
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND s.KnessetNum = ?"
        params.append(knesset_num)

    if from_date:
        sql += " AND s.StartDate >= ?"
        params.append(from_date)

    if to_date:
        sql += " AND s.StartDate <= ?"
        params.append(to_date)

    if date:
        sql += " AND s.StartDate LIKE ?"
        params.append(f"{date}%")

    if name:
        sql += """
        AND (
            s.Name LIKE ?
            OR EXISTS (
                SELECT 1 FROM plm_session_item_raw i
                WHERE i.PlenumSessionID = s.Id
                  AND i.Name LIKE ?
            )
        )"""
        params.extend([f"%{name}%", f"%{name}%"])

    if item_type:
        sql += """
        AND EXISTS (
            SELECT 1 FROM plm_session_item_raw i
            WHERE i.PlenumSessionID = s.Id
              AND i.ItemTypeDesc LIKE ?
        )"""
        params.append(f"%{item_type}%")

    sql += " ORDER BY s.KnessetNum, s.StartDate"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "session_id": row["Id"],
            "knesset_num": row["KnessetNum"],
            "name": row["Name"],
            "date": _simple_date(row["StartDate"]),
        })

    conn.close()
    return results
