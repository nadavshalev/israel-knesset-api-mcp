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

from typing import Annotated
from pydantic import Field

from core.db import connect_readonly
from core.helpers import simple_date, normalize_inputs
from core.mcp_meta import mcp_tool
from core.search_meta import register_search

register_search({
    "entity_key": "plenums",
    "count_sql": """
        SELECT COUNT(DISTINCT ps.Id)
        FROM plenum_session_raw ps
        LEFT JOIN plm_session_item_raw psi
               ON ps.Id = psi.PlenumSessionID
        WHERE ps.Name LIKE %s OR psi.Name LIKE %s
    """,
    "search_sql": """
        SELECT DISTINCT ps.Id AS id,
               ps.Name AS name,
               ps.KnessetNum AS knesset_num,
               ps.StartDate AS date
        FROM plenum_session_raw ps
        LEFT JOIN plm_session_item_raw psi
               ON ps.Id = psi.PlenumSessionID
        WHERE ps.Name LIKE %s OR psi.Name LIKE %s
        ORDER BY ps.Id DESC
        LIMIT %s
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
    most_recent_date_sql="SELECT MAX(StartDate) FROM plenum_session_raw",
    enum_sql={
        "item_type": "SELECT DISTINCT ItemTypeDesc FROM plm_session_item_raw WHERE ItemTypeDesc IS NOT NULL ORDER BY ItemTypeDesc",
    },
    is_list=True,
)
def search_sessions(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD)")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD)")] = None,
    date: Annotated[str | None, Field(description="Exact date (YYYY-MM-DD)")] = None,
    name: Annotated[str | None, Field(description="Session name or agenda item name contains text")] = None,
    item_type: Annotated[str | None, Field(description="Filter to sessions with items of this type")] = None,
) -> list:
    """Search for plenum sessions and return summary data (no items/docs).

    Filters (all ANDed):
      - knesset_num: Knesset number
      - from_date / to_date / date: session date range
      - name: session name or item name contains text
      - item_type: session has items of this type

    Returns a list of session summary dicts sorted by (knesset_num, date).
    """
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    date = normalized["date"]
    name = normalized["name"]
    item_type = normalized["item_type"]

    conn = connect_readonly()
    cursor = conn.cursor()

    sql = """
    SELECT DISTINCT s.Id, s.KnessetNum, s.Name, s.StartDate
    FROM plenum_session_raw s
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND s.KnessetNum = %s"
        params.append(knesset_num)

    if from_date:
        sql += " AND s.StartDate >= %s"
        params.append(from_date)

    if to_date:
        sql += " AND s.StartDate <= %s"
        params.append(to_date)

    if date:
        sql += " AND s.StartDate LIKE %s"
        params.append(f"{date}%")

    if name:
        sql += """
        AND (
            s.Name LIKE %s
            OR EXISTS (
                SELECT 1 FROM plm_session_item_raw i
                WHERE i.PlenumSessionID = s.Id
                  AND i.Name LIKE %s
            )
        )"""
        params.extend([f"%{name}%", f"%{name}%"])

    if item_type:
        sql += """
        AND EXISTS (
            SELECT 1 FROM plm_session_item_raw i
            WHERE i.PlenumSessionID = s.Id
              AND i.ItemTypeDesc LIKE %s
        )"""
        params.append(f"%{item_type}%")

    sql += " ORDER BY s.KnessetNum, s.StartDate"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "session_id": row["id"],
            "knesset_num": row["knessetnum"],
            "name": row["name"],
            "date": simple_date(row["startdate"]),
        })

    conn.close()
    return results
