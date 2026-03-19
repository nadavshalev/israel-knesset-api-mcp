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
from core.helpers import simple_date, normalize_inputs, check_search_count
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from origins.committees.search_committees_models import CommitteeSummary, CommitteeSearchResults

def _build_committees_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity committee search.

    Supports: query (name LIKE), knesset_num,
    date/date_to (committees that had sessions in the date range).
    """
    conditions = []
    params = []

    if query:
        conditions.append("c.Name LIKE %s")
        params.append(f"%{query}%")

    if knesset_num is not None:
        conditions.append("c.KnessetNum = %s")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("""EXISTS (
            SELECT 1 FROM committee_session_raw cs
            WHERE cs.CommitteeID = c.Id
            AND cs.StartDate >= %s AND cs.StartDate <= %s
        )""")
        params.extend([date, date_to + "T99"])
    elif date:
        conditions.append("""EXISTS (
            SELECT 1 FROM committee_session_raw cs
            WHERE cs.CommitteeID = c.Id
            AND cs.StartDate >= %s AND cs.StartDate <= %s
        )""")
        params.extend([date, date + "T99"])

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*) FROM committee_raw c
        WHERE {where}
    """
    search_sql = f"""
        SELECT c.Id AS id, c.Name AS name, c.KnessetNum AS knesset_num,
               c.CategoryDesc AS category
        FROM committee_raw c
        WHERE {where}
        ORDER BY c.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "committees",
    "builder": _build_committees_search,
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
    most_recent_date_sql="SELECT MAX(StartDate) FROM committee_raw",
    enum_sql={
        "committee_type": "SELECT DISTINCT CommitteeTypeDesc FROM committee_raw ORDER BY CommitteeTypeDesc",
    },
    is_list=True,
)
def search_committees(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name: Annotated[str | None, Field(description="Committee name contains text")] = None,
    committee_type: Annotated[str | None, Field(description="Committee type")] = None,
    category: Annotated[str | None, Field(description="Category description contains text")] = None,
    is_current: Annotated[bool | None, Field(description="True for current committees, False for inactive")] = None,
    parent_committee_id: Annotated[int | None, Field(description="Parent committee ID (for sub-committees)")] = None,
    date: Annotated[str | None, Field(description="Filter to committees that had sessions on or after this date (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="Filter to committees that had sessions on or before this date (YYYY-MM-DD). Use with date for a range.")] = None,
) -> CommitteeSearchResults:
    """Search for committees and return summary metadata.

    Filters (all ANDed):
      - knesset_num: committee's Knesset number
      - name: committee name contains text
      - committee_type: CommitteeTypeDesc contains text (ועדה ראשית, ועדת משנה, etc.)
      - category: CategoryDesc contains text
      - is_current: True for current committees, False for inactive
      - parent_committee_id: parent committee ID (for sub-committees)
      - date: committees that had sessions starting on or after this date
      - date_to: committees that had sessions starting on or before this date

    Returns a list of committee summary dicts sorted by (start_date DESC, committee_id DESC).
    """
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    committee_type = normalized["committee_type"]
    category = normalized["category"]
    is_current = normalized["is_current"]
    parent_committee_id = normalized["parent_committee_id"]
    date = normalized["date"]
    date_to = normalized["date_to"]

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if knesset_num is not None:
        conditions.append("c.KnessetNum = %s")
        params.append(knesset_num)

    if name:
        conditions.append("c.Name LIKE %s")
        params.append(f"%{name}%")

    if committee_type:
        conditions.append("c.CommitteeTypeDesc LIKE %s")
        params.append(f"%{committee_type}%")

    if category:
        conditions.append("c.CategoryDesc LIKE %s")
        params.append(f"%{category}%")

    if is_current is not None:
        conditions.append("c.IsCurrent = %s")
        params.append(1 if is_current else 0)

    if parent_committee_id is not None:
        conditions.append("c.ParentCommitteeID = %s")
        params.append(parent_committee_id)

    if date and date_to:
        conditions.append("""EXISTS (
            SELECT 1 FROM committee_session_raw cs
            WHERE cs.CommitteeID = c.Id
            AND cs.StartDate >= %s AND cs.StartDate <= %s
        )""")
        params.extend([date, date_to + "T99"])
    elif date:
        conditions.append("""EXISTS (
            SELECT 1 FROM committee_session_raw cs
            WHERE cs.CommitteeID = c.Id
            AND cs.StartDate >= %s
        )""")
        params.append(date)
    elif date_to:
        conditions.append("""EXISTS (
            SELECT 1 FROM committee_session_raw cs
            WHERE cs.CommitteeID = c.Id
            AND cs.StartDate <= %s
        )""")
        params.append(date_to + "T99")

    where = " AND ".join(conditions) if conditions else "1=1"

    check_search_count(
        cursor,
        f"SELECT COUNT(*) FROM committee_raw c WHERE {where}",
        params,
        entity_name="committees",
    )

    cursor.execute(
        f"""SELECT c.Id, c.Name, c.KnessetNum, c.CommitteeTypeDesc,
               c.CategoryDesc, c.IsCurrent, c.StartDate, c.FinishDate,
               c.ParentCommitteeID, c.CommitteeParentName, c.Email
        FROM committee_raw c
        WHERE {where}
        ORDER BY c.StartDate DESC, c.Id DESC""",
        params,
    )
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append(CommitteeSummary(
            committee_id=row["id"],
            name=row["name"],
            knesset_num=row["knessetnum"],
            type=row["committeetypedesc"],
            category=row["categorydesc"],
            is_current=bool(row["iscurrent"]),
            start_date=simple_date(row["startdate"]) or None,
            end_date=simple_date(row["finishdate"]) or None,
            parent_committee_id=row["parentcommitteeid"],
            parent_committee_name=row["committeeparentname"],
        ))

    conn.close()
    return CommitteeSearchResults(items=results)


search_committees.OUTPUT_MODEL = CommitteeSearchResults
