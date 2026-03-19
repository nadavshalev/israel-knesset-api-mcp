"""Committee sessions search view — returns summary data for multiple sessions.

Session-centric search: the session is the primary entity.  Supports
filtering by committee, knesset number, date range, query text, session
type, and status.

For full detail on a single session (items, documents), use
``get_cmt_session_view.get_cmt_session()``.
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
from origins.committees.search_cmt_sessions_models import CmtSessionSummary, CmtSessionSearchResults


def _build_cmt_sessions_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity committee session search.

    Supports: query (committee name LIKE or item name LIKE), knesset_num,
    date/date_to (StartDate).
    """
    conditions = []
    params = []
    need_item_join = False

    if query:
        conditions.append("(c.Name LIKE %s OR csi.Name LIKE %s)")
        params.extend([f"%{query}%", f"%{query}%"])
        need_item_join = True

    if knesset_num is not None:
        conditions.append("cs.KnessetNum = %s")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("cs.StartDate >= %s")
        params.append(date)
        conditions.append("cs.StartDate <= %s")
        params.append(date_to + "T99")
    elif date:
        conditions.append("cs.StartDate LIKE %s")
        params.append(f"{date}%")

    where = " AND ".join(conditions) if conditions else "1=1"
    item_join = """
        LEFT JOIN cmt_session_item_raw csi
               ON cs.Id = csi.CommitteeSessionID
    """ if need_item_join else ""

    count_sql = f"""
        SELECT COUNT(DISTINCT cs.Id)
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        {item_join}
        WHERE {where}
    """
    search_sql = f"""
        SELECT DISTINCT cs.Id AS id,
               cs.CommitteeID AS committee_id,
               c.Name AS committee_name,
               cs.KnessetNum AS knesset_num,
               cs.StartDate AS date
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        {item_join}
        WHERE {where}
        ORDER BY cs.StartDate DESC, cs.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "committee_sessions",
    "builder": _build_cmt_sessions_search,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_cmt_sessions",
    description=(
        "Search for Knesset committee sessions. Returns summary info: "
        "session ID, committee, date, type, status, item count. "
        "Use get_cmt_session for full detail including agenda items and documents."
    ),
    entity="Committee Sessions",
    count_sql="SELECT COUNT(*) FROM committee_session_raw",
    most_recent_date_sql="SELECT MAX(StartDate) FROM committee_session_raw",
    enum_sql={
        "session_type": "SELECT DISTINCT TypeDesc FROM committee_session_raw WHERE TypeDesc IS NOT NULL ORDER BY TypeDesc",
        "status": "SELECT DISTINCT StatusDesc FROM committee_session_raw WHERE StatusDesc IS NOT NULL ORDER BY StatusDesc",
    },
    is_list=True,
)
def search_cmt_sessions(
    committee_id: Annotated[int | None, Field(description="Filter by committee ID")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    date: Annotated[str | None, Field(description="Single date or start of range (YYYY-MM-DD)")] = None,
    date_to: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD); use with date for a range")] = None,
    query: Annotated[str | None, Field(description="Committee name or agenda item name contains text")] = None,
    session_type: Annotated[str | None, Field(description="Session type (e.g. פתוחה, חסויה)")] = None,
    status: Annotated[str | None, Field(description="Session status")] = None,
) -> CmtSessionSearchResults:
    """Search for committee sessions and return summary data.

    Filters (all ANDed):
      - committee_id: specific committee
      - knesset_num: Knesset number
      - date / date_to: session date filter (single day or range)
      - query: committee name or item name contains text
      - session_type: TypeDesc filter
      - status: StatusDesc filter

    Returns a list of session summaries sorted by (date DESC, session_id DESC).
    """
    normalized = normalize_inputs(locals())
    committee_id = normalized["committee_id"]
    knesset_num = normalized["knesset_num"]
    date = normalized["date"]
    date_to = normalized["date_to"]
    query = normalized["query"]
    session_type = normalized["session_type"]
    status = normalized["status"]

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if committee_id is not None:
        conditions.append("cs.CommitteeID = %s")
        params.append(committee_id)

    if knesset_num is not None:
        conditions.append("cs.KnessetNum = %s")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("cs.StartDate >= %s AND cs.StartDate <= %s")
        params.extend([date, date_to + "T99"])
    elif date:
        conditions.append("cs.StartDate LIKE %s")
        params.append(f"{date}%")

    if query:
        conditions.append("""(
            c.Name LIKE %s
            OR EXISTS (
                SELECT 1 FROM cmt_session_item_raw csi
                WHERE csi.CommitteeSessionID = cs.Id AND csi.Name LIKE %s
            )
        )""")
        params.extend([f"%{query}%", f"%{query}%"])

    if session_type:
        conditions.append("cs.TypeDesc LIKE %s")
        params.append(f"%{session_type}%")

    if status:
        conditions.append("cs.StatusDesc LIKE %s")
        params.append(f"%{status}%")

    where = " AND ".join(conditions) if conditions else "1=1"

    check_search_count(
        cursor,
        f"SELECT COUNT(*) FROM committee_session_raw cs"
        f" JOIN committee_raw c ON c.Id = cs.CommitteeID WHERE {where}",
        params,
        entity_name="committee sessions",
    )

    cursor.execute(
        f"""SELECT DISTINCT cs.Id, cs.CommitteeID, c.Name AS CommitteeName,
               cs.KnessetNum, cs.StartDate, cs.TypeDesc, cs.StatusDesc,
               (SELECT COUNT(*) FROM cmt_session_item_raw
                WHERE CommitteeSessionID = cs.Id) AS item_count
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        WHERE {where}
        ORDER BY cs.StartDate DESC, cs.Id DESC""",
        params,
    )
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append(CmtSessionSummary(
            session_id=row["id"],
            committee_id=row["committeeid"],
            committee_name=row["committeename"] or None,
            knesset_num=row["knessetnum"],
            date=simple_date(row["startdate"]) or None,
            type=row["typedesc"] or None,
            status=row["statusdesc"] or None,
            item_count=row["item_count"] or 0,
        ))

    conn.close()
    return CmtSessionSearchResults(items=results)


search_cmt_sessions.OUTPUT_MODEL = CmtSessionSearchResults
