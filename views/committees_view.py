"""Committees list view — returns summary data for multiple committees.

Shows general committee info (name, type, knesset, category, parent).
For full detail on a single committee (sessions, members, bills, documents),
use ``committee_view.get_committee()``.
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
    "entity_key": "committees",
    "count_sql": """
        SELECT COUNT(*) FROM committee_raw
        WHERE Name LIKE %s
    """,
    "search_sql": """
        SELECT Id AS id, Name AS name, KnessetNum AS knesset_num,
               CategoryDesc AS category
        FROM committee_raw
        WHERE Name LIKE %s
        ORDER BY Id DESC
        LIMIT %s
    """,
    "param_count": 1,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_committees",
    description=(
        "Search for Knesset committees. Returns summary info: name, type, "
        "category, knesset number, current status. "
        "Use get_committee for full detail on a single committee."
    ),
    entity="Committees",
    count_sql="SELECT COUNT(*) FROM committee_raw",
    is_list=True,
)
def search_committees(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name: Annotated[str | None, Field(description="Committee name contains text")] = None,
    committee_type: Annotated[str | None, Field(description="Committee type (e.g. ועדה ראשית, ועדת משנה)")] = None,
    category: Annotated[str | None, Field(description="Category description contains text")] = None,
    is_current: Annotated[bool | None, Field(description="True for current committees, False for inactive")] = None,
    parent_committee_id: Annotated[int | None, Field(description="Parent committee ID (for sub-committees)")] = None,
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
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    committee_type = normalized["committee_type"]
    category = normalized["category"]
    is_current = normalized["is_current"]
    parent_committee_id = normalized["parent_committee_id"]

    conn = connect_readonly()
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
        sql += " AND c.KnessetNum = %s"
        params.append(knesset_num)

    if name:
        sql += " AND c.Name LIKE %s"
        params.append(f"%{name}%")

    if committee_type:
        sql += " AND c.CommitteeTypeDesc LIKE %s"
        params.append(f"%{committee_type}%")

    if category:
        sql += " AND c.CategoryDesc LIKE %s"
        params.append(f"%{category}%")

    if is_current is not None:
        sql += " AND c.IsCurrent = %s"
        params.append(1 if is_current else 0)

    if parent_committee_id is not None:
        sql += " AND c.ParentCommitteeID = %s"
        params.append(parent_committee_id)

    sql += ' ORDER BY c.KnessetNum, c.Name COLLATE "C"'

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "committee_id": row["id"],
            "name": row["name"],
            "knesset_num": row["knessetnum"],
            "type": row["committeetypedesc"],
            "category": row["categorydesc"],
            "is_current": bool(row["iscurrent"]),
            "start_date": simple_date(row["startdate"]),
            "end_date": simple_date(row["finishdate"]),
            "parent_committee_id": row["parentcommitteeid"],
            "parent_committee_name": row["committeeparentname"],
        })

    conn.close()
    return results
