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

from typing import Annotated, Literal
from pydantic import Field

from core.db import connect_readonly
from core.helpers import (
    simple_date, simple_time, normalize_inputs, check_search_count, resolve_pagination,
    CountByConfig, build_count_by_query, fuzzy_condition, fuzzy_params, fts_condition, fts_params,
)
from core.models import CountItem
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

    if query:
        conditions.append(f"""(
            {fuzzy_condition("c.Name")}
            OR EXISTS (SELECT 1 FROM cmt_session_item_raw csi
                       WHERE csi.CommitteeSessionID = cs.Id AND {fts_condition("csi.Name")}))""")
        params.extend(fuzzy_params(query) + fts_params(query))

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

    count_sql = f"""
        SELECT COUNT(*)
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        WHERE {where}
    """
    search_sql = f"""
        SELECT cs.Id AS id,
               cs.CommitteeID AS committee_id,
               c.Name AS committee_name,
               cs.KnessetNum AS knesset_num,
               cs.StartDate AS date
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
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
# count_by configuration
# ---------------------------------------------------------------------------

_CB_BASE_FROM = "committee_session_raw cs"
_CB_BASE_JOINS = "JOIN committee_raw c ON c.Id = cs.CommitteeID"

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "committee": CountByConfig(
        group_by="cs.CommitteeID, c.Name",
        id_select="cs.CommitteeID",
        value_select="c.Name",
        extra_where="cs.CommitteeID IS NOT NULL",
    ),
    "knesset_num": CountByConfig(
        group_by="cs.KnessetNum",
        id_select=None,
        value_select="cs.KnessetNum::text",
        extra_where="cs.KnessetNum IS NOT NULL",
    ),
    "type": CountByConfig(
        group_by="cs.TypeDesc",
        id_select=None,
        value_select="cs.TypeDesc",
        extra_where="cs.TypeDesc IS NOT NULL AND cs.TypeDesc != ''",
    ),
    "status": CountByConfig(
        group_by="cs.StatusDesc",
        id_select=None,
        value_select="cs.StatusDesc",
        extra_where="cs.StatusDesc IS NOT NULL AND cs.StatusDesc != ''",
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="committees",
    description=(
        "Search for Knesset committee sessions. Returns summary info by default; "
        "set full_details=True for agenda items and documents. "
        "Use session_id to filter to a specific session, from_date to search by date range, "
        "or bill_id to find all sessions where a specific bill was discussed."
    ),
    entity="Committee Sessions",
    count_sql="SELECT COUNT(*) FROM committee_session_raw",
    most_recent_date_sql="SELECT MAX(StartDate) FROM committee_session_raw",
    enum_sql={
        "session_type": "SELECT DISTINCT TypeDesc FROM committee_session_raw WHERE TypeDesc IS NOT NULL ORDER BY TypeDesc",
        "item_type": "SELECT DISTINCT itemtypedesc FROM plm_session_item_raw WHERE itemtypeid IN (SELECT DISTINCT itemtypeid FROM cmt_session_item_raw);"
    },
    is_list=True,
)
def committees(
    session_id: Annotated[int | None, Field(description="Filter by session ID")] = None,
    committee_id: Annotated[int | None, Field(description="Filter by committee ID")] = None,
    committee_name: Annotated[str | None, Field(description="Partial match on committee name")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD). Required unless session_id is provided. to_date defaults to today if omitted.")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD). Requires from_date.")] = None,
    query_items: Annotated[str | None, Field(description="Committee name or agenda item name contains text")] = None,
    item_type: Annotated[str | None, Field(description="Filter to sessions with items of this type")] = None,
    bill_id: Annotated[int | None, Field(description="Filter to sessions where this bill was on the agenda")] = None,
    member_id: Annotated[int | None, Field(description="Filter to sessions where this member served on the committee")] = None,
    session_type: Annotated[str | None, Field(description="Session type (e.g. פתוחה, חסויה)")] = None,
    full_details: Annotated[bool, Field(description="Include agenda items and documents. Adds significant data per result — use conservatively. Preferred pattern: search first (full_details=False), then re-call with session_id for only the specific sessions you need detail on.")] = False,
    top: Annotated[int | None, Field(description="Max results (default 50, max 200). Results are sorted newest-first (date DESC) or by count DESC for count_by — so top=N gives the N most recent or highest.")] = None,
    offset: Annotated[int | None, Field(description="Results to skip for pagination. To get the oldest/smallest N: use offset=total_count-N (total_count is in every response).")] = None,
    count_by: Annotated[Literal["all", "committee", "knesset_num", "type", "status"] | None, Field(description='Group and count results. "all" returns only total_count (no items). Other values group by field (sorted by count DESC).')] = None,
) -> CmtSessionsResults:
    """Search for committee sessions with optional full detail.

    Required scoping: either ``session_id``, ``from_date``, or ``bill_id`` must be provided.
    """
    normalized = normalize_inputs(locals())
    session_id = normalized["session_id"]
    committee_id = normalized["committee_id"]
    committee_name = normalized["committee_name"]
    knesset_num = normalized["knesset_num"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    query_items = normalized["query_items"]
    item_type = normalized["item_type"]
    bill_id = normalized["bill_id"]
    member_id = normalized["member_id"]
    session_type = normalized["session_type"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    # --- Validation ---
    if to_date and not from_date:
        raise ValueError("to_date requires from_date. Provide from_date or use session_id instead.")

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

    if committee_name:
        conditions.append(fuzzy_condition("c.Name"))
        params.extend(fuzzy_params(committee_name))

    if knesset_num is not None:
        conditions.append("cs.KnessetNum = %s")
        params.append(knesset_num)

    if from_date and to_date:
        conditions.append("cs.StartDate >= %s AND cs.StartDate <= %s")
        params.extend([from_date, to_date + "T99"])

    if query_items:
        conditions.append(f"""(
            {fuzzy_condition("c.Name")}
            OR EXISTS (
                SELECT 1 FROM cmt_session_item_raw csi
                WHERE csi.CommitteeSessionID = cs.Id AND {fts_condition("csi.Name")}
            )
        )""")
        params.extend(fuzzy_params(query_items) + fts_params(query_items))

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

    if bill_id is not None:
        conditions.append("""EXISTS (
            SELECT 1 FROM cmt_session_item_raw csi
            WHERE csi.CommitteeSessionID = cs.Id AND csi.ItemTypeID = 2 AND csi.ItemID = %s
        )""")
        params.append(bill_id)

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

    where = " AND ".join(conditions) if conditions else "1=1"
    count_sql = f"SELECT COUNT(*) FROM {_CB_BASE_FROM} {_CB_BASE_JOINS} WHERE {where}"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if count_by_val == "all":
            total_count = check_search_count(cursor, count_sql, params, paginated=True)
            conn.close()
            return CmtSessionsResults(total_count=total_count, items=[], counts=[])
        config = _COUNT_BY_OPTIONS.get(count_by_val)
        if config is None:
            raise ValueError(f"count_by must be one of: {', '.join(_COUNT_BY_OPTIONS)}")
        groups_count_sql, group_sql = build_count_by_query(
            base_from=_CB_BASE_FROM, base_joins=_CB_BASE_JOINS, where=where, config=config,
        )
        total_count = check_search_count(cursor, groups_count_sql, params, paginated=True)
        cursor.execute(group_sql, params + [top, offset])
        counts = [CountItem(id=row.get("id"), value=row.get("value"), count=row["count"])
                  for row in cursor.fetchall()]
        conn.close()
        return CmtSessionsResults(total_count=total_count, items=[], counts=counts)

    total_count = check_search_count(cursor, count_sql, params, entity_name="committee sessions", paginated=True)

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
    return CmtSessionsResults(total_count=total_count, items=results)


committees.OUTPUT_MODEL = CmtSessionsResults
