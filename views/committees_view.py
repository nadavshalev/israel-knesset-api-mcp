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

from core.db import connect_readonly
from core.helpers import simple_date
from core.mcp_meta import mcp_tool
from core.search_meta import register_search

register_search({
    "entity_key": "committees",
    "count_sql": """
        SELECT COUNT(*) FROM committee_raw
        WHERE Name LIKE ?
    """,
    "search_sql": """
        SELECT Id AS id, Name AS name, KnessetNum AS knesset_num,
               CategoryDesc AS category
        FROM committee_raw
        WHERE Name LIKE ?
        ORDER BY Id DESC
        LIMIT ?
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
            "start_date": simple_date(row["StartDate"]),
            "end_date": simple_date(row["FinishDate"]),
            "parent_committee_id": row["ParentCommitteeID"],
            "parent_committee_name": row["CommitteeParentName"],
        })

    conn.close()
    return results
