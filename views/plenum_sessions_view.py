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
from core.helpers import simple_date
from core.mcp_meta import mcp_tool
from core.search_meta import register_search

register_search({
    "entity_key": "plenums",
    "count_sql": """
        SELECT COUNT(DISTINCT ps.Id)
        FROM plenum_session_raw ps
        LEFT JOIN plm_session_item_raw psi
               ON ps.Id = psi.PlenumSessionID
        WHERE ps.Name LIKE ? OR psi.Name LIKE ?
    """,
    "search_sql": """
        SELECT DISTINCT ps.Id AS id,
               ps.Name AS name,
               ps.KnessetNum AS knesset_num,
               ps.StartDate AS date
        FROM plenum_session_raw ps
        LEFT JOIN plm_session_item_raw psi
               ON ps.Id = psi.PlenumSessionID
        WHERE ps.Name LIKE ? OR psi.Name LIKE ?
        ORDER BY ps.Id DESC
        LIMIT ?
    """,
    "param_count": 2,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_plenums",
    description=(
        "Search for Knesset plenum sessions. Returns summary info: "
        "session ID, knesset number, name, date. "
        "Use get_plenum for full detail including agenda items and documents."
    ),
    entity="Plenum Sessions",
    count_sql="SELECT COUNT(*) FROM plenum_session_raw",
    is_list=True,
)
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
            "date": simple_date(row["StartDate"]),
        })

    conn.close()
    return results
