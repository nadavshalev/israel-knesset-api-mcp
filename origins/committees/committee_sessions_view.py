"""Unified committees tool — search and detail via ``full_details`` flag.

Required scoping: either ``session_id`` or ``from_date`` must be provided.
When ``session_id`` is given, ``full_details`` is auto-enabled.
"""

import sys
from datetime import date as date_today
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from typing import Annotated
from pydantic import Field

from core.db import connect_readonly
from core.helpers import simple_date, simple_time, normalize_inputs, check_search_count, resolve_pagination
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import SessionItem, SessionDocument, get_item_votes
from origins.committees.committee_sessions_models import CmtSessionResultPartial, CmtSessionResultFull, CmtSessionsResults


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_cmt_sessions_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity committee session search."""
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
    "entity_key": "committees",
    "builder": _build_cmt_sessions_search,
    "mapper": lambda row: CmtSessionResultPartial(
        session_id=row["id"],
        committee_id=row["committee_id"],
        committee_name=row["committee_name"],
        knesset_num=row["knesset_num"],
        date=simple_date(row["date"]),
    ),
})


# ---------------------------------------------------------------------------
# Full-details helpers
# ---------------------------------------------------------------------------

def _fetch_items(cursor, session_id):
    """Fetch agenda items for a committee session, with bill links and votes."""
    # Build ItemTypeID -> description lookup from plm_session_item_raw
    cursor.execute(
        "SELECT DISTINCT ItemTypeID, ItemTypeDesc FROM plm_session_item_raw "
        "WHERE ItemTypeID IS NOT NULL AND ItemTypeDesc IS NOT NULL"
    )
    item_type_map = {row["itemtypeid"]: row["itemtypedesc"] for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT csi.Id, csi.Name, csi.ItemTypeID, csi.Ordinal,
               csi.StatusID, csi.ItemID
        FROM cmt_session_item_raw csi
        WHERE csi.CommitteeSessionID = %s
        ORDER BY csi.Ordinal ASC
        """,
        (session_id,),
    )
    item_rows = cursor.fetchall()

    # Fetch status descriptions
    cursor.execute(
        'SELECT Id, "Desc" AS StatusDesc FROM status_raw'
    )
    status_map = {row["id"]: row["statusdesc"] for row in cursor.fetchall()}

    items = []
    for item in item_rows:
        item_id = item["itemid"]
        item_type_id = item["itemtypeid"]
        bill_id = item_id if item_type_id == 2 and item_id else None
        votes = get_item_votes(cursor, item_id) if bill_id else None

        items.append(SessionItem(
            item_id=item["id"],
            item_type=item_type_map.get(item_type_id),
            item_name=item["name"],
            item_status=status_map.get(item["statusid"]),
            bill_id=bill_id,
            votes=votes,
        ))
    return items


def _fetch_documents(cursor, session_id):
    """Fetch documents for a committee session."""
    cursor.execute(
        """
        SELECT GroupTypeDesc, ApplicationDesc, FilePath
        FROM document_committee_session_raw
        WHERE CommitteeSessionID = %s
        ORDER BY GroupTypeDesc, Id
        """,
        (session_id,),
    )
    return [
        SessionDocument(
            name=doc["grouptypedesc"],
            type=doc["applicationdesc"],
            path=doc["filepath"],
        )
        for doc in cursor.fetchall()
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="committees",
    description=(
        "Search for Knesset committee sessions. Returns summary info by default; "
        "set full_details=True for agenda items and documents. "
        "Provide session_id for a single session (auto-enables full_details) "
        "or from_date to search by date range."
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
def committees(
    session_id: Annotated[int | None, Field(description="Get a specific session by ID (auto-enables full_details)")] = None,
    committee_id: Annotated[int | None, Field(description="Filter by committee ID")] = None,
    committee_name_query: Annotated[str | None, Field(description="Partial match on committee name")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD). Required unless session_id is provided. to_date defaults to today if omitted.")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD). Requires from_date.")] = None,
    query_items: Annotated[str | None, Field(description="Committee name or agenda item name contains text")] = None,
    item_type: Annotated[str | None, Field(description="Filter to sessions with items of this type")] = None,
    member_id: Annotated[int | None, Field(description="Filter to sessions where this member served on the committee")] = None,
    session_type: Annotated[str | None, Field(description="Session type (e.g. פתוחה, חסויה)")] = None,
    status: Annotated[str | None, Field(description="Session status")] = None,
    full_details: Annotated[bool, Field(description="Include agenda items and documents (auto-True when session_id is set)")] = False,
    top: Annotated[int | None, Field(description="Max results to return (default 50, max 200)")] = None,
    offset: Annotated[int | None, Field(description="Number of results to skip for pagination")] = None,
) -> CmtSessionsResults:
    """Search for committee sessions with optional full detail.

    Required scoping: either ``session_id`` or ``from_date`` must be provided.
    """
    normalized = normalize_inputs(locals())
    session_id = normalized["session_id"]
    committee_id = normalized["committee_id"]
    committee_name_query = normalized["committee_name_query"]
    knesset_num = normalized["knesset_num"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    query_items = normalized["query_items"]
    item_type = normalized["item_type"]
    member_id = normalized["member_id"]
    session_type = normalized["session_type"]
    status = normalized["status"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    # --- Validation ---
    if to_date and not from_date:
        raise ValueError("to_date requires from_date. Provide from_date or use session_id instead.")
    if not session_id and not from_date:
        raise ValueError("Provide session_id or from_date to scope the query.")

    # Auto-enable full_details when session_id is given
    if session_id:
        full_details = True

    # Default to_date to today when from_date is provided alone
    if from_date and not to_date:
        to_date = str(date_today.today())

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if session_id is not None:
        conditions.append("cs.Id = %s")
        params.append(session_id)

    if committee_id is not None:
        conditions.append("cs.CommitteeID = %s")
        params.append(committee_id)

    if committee_name_query:
        conditions.append("c.Name LIKE %s")
        params.append(f"%{committee_name_query}%")

    if knesset_num is not None:
        conditions.append("cs.KnessetNum = %s")
        params.append(knesset_num)

    if from_date and to_date:
        conditions.append("cs.StartDate >= %s AND cs.StartDate <= %s")
        params.extend([from_date, to_date + "T99"])

    if query_items:
        conditions.append("""(
            c.Name LIKE %s
            OR EXISTS (
                SELECT 1 FROM cmt_session_item_raw csi
                WHERE csi.CommitteeSessionID = cs.Id AND csi.Name LIKE %s
            )
        )""")
        params.extend([f"%{query_items}%", f"%{query_items}%"])

    if item_type:
        conditions.append("""EXISTS (
            SELECT 1 FROM cmt_session_item_raw csi
            WHERE csi.CommitteeSessionID = cs.Id
              AND csi.ItemTypeID IN (
                  SELECT DISTINCT ItemTypeID FROM plm_session_item_raw
                  WHERE ItemTypeDesc LIKE %s
              )
        )""")
        params.append(f"%{item_type}%")

    if member_id is not None:
        conditions.append("""EXISTS (
            SELECT 1 FROM person_to_position_raw ptp
            WHERE ptp.CommitteeID = cs.CommitteeID
              AND ptp.PersonID = %s
              AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' OR ptp.FinishDate >= cs.StartDate)
              AND (ptp.StartDate IS NULL OR ptp.StartDate = '' OR ptp.StartDate <= cs.StartDate)
        )""")
        params.append(member_id)

    if session_type:
        conditions.append("cs.TypeDesc LIKE %s")
        params.append(f"%{session_type}%")

    if status:
        conditions.append("cs.StatusDesc LIKE %s")
        params.append(f"%{status}%")

    where = " AND ".join(conditions) if conditions else "1=1"

    if not session_id:
        total_count = check_search_count(
            cursor,
            f"SELECT COUNT(*) FROM committee_session_raw cs"
            f" JOIN committee_raw c ON c.Id = cs.CommitteeID WHERE {where}",
            params,
            entity_name="committee sessions",
            paginated=True,
        )
    else:
        total_count = None

    # Partial query: summary fields + item_count
    cursor.execute(
        f"""SELECT DISTINCT cs.Id, cs.CommitteeID, c.Name AS CommitteeName,
               cs.KnessetNum, cs.StartDate, cs.FinishDate,
               cs.Number, cs.TypeDesc, cs.StatusDesc,
               cs.Location, cs.SessionUrl, cs.BroadcastUrl, cs.Note,
               (SELECT COUNT(*) FROM cmt_session_item_raw
                WHERE CommitteeSessionID = cs.Id) AS item_count
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        WHERE {where}
        ORDER BY cs.StartDate DESC, cs.Id DESC
        LIMIT %s OFFSET %s""",
        params + [top, offset],
    )
    rows = cursor.fetchall()

    results = []
    for row in rows:
        sid = row["id"]
        partial_kwargs = dict(
            session_id=sid,
            committee_id=row["committeeid"],
            committee_name=row["committeename"] or None,
            knesset_num=row["knessetnum"],
            date=simple_date(row["startdate"]) or None,
            item_count=row["item_count"] or 0,
        )
        if full_details:
            result = CmtSessionResultFull(
                **partial_kwargs,
                number=row["number"],
                start_time=simple_time(row["startdate"]) or None,
                end_time=simple_time(row["finishdate"]) or None,
                type=row["typedesc"] or None,
                status=row["statusdesc"] or None,
                location=row["location"] or None,
                url=row["sessionurl"] or None,
                broadcast_url=row["broadcasturl"] or None,
                note=row["note"] or None,
                items=_fetch_items(cursor, sid),
                documents=_fetch_documents(cursor, sid),
            )
        else:
            result = CmtSessionResultPartial(**partial_kwargs)
        results.append(result)

    conn.close()
    if total_count is None:
        total_count = len(results)
    return CmtSessionsResults(total_count=total_count, items=results)


committees.OUTPUT_MODEL = CmtSessionsResults
