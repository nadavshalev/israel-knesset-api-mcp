"""Unified plenums tool — search and detail via ``full_details`` flag.

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
    simple_date, normalize_inputs, check_search_count, resolve_pagination,
    CountByConfig, build_count_by_query, fuzzy_condition, fuzzy_params,
)
from core.models import CountItem
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import SessionItem, SessionDocument, get_item_votes
from origins.plenums.plenum_sessions_models import PlenumSessionResultPartial, PlenumSessionResultFull, PlenumSessionsResults


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_plenums_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity plenum session search."""
    conditions = []
    params = []

    if query:
        conditions.append(f"""(
            {fuzzy_condition("ps.Name")}
            OR EXISTS (SELECT 1 FROM plm_session_item_raw psi
                       WHERE psi.PlenumSessionID = ps.Id AND {fuzzy_condition("psi.Name")})
        )""")
        params.extend(fuzzy_params(query) + fuzzy_params(query))

    if knesset_num is not None:
        conditions.append("ps.KnessetNum = %s")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("ps.StartDate >= %s")
        params.append(date)
        conditions.append("ps.StartDate <= %s")
        params.append(date_to + "T99")
    elif date:
        conditions.append("ps.StartDate LIKE %s")
        params.append(f"{date}%")

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*)
        FROM plenum_session_raw ps
        WHERE {where}
    """
    search_sql = f"""
        SELECT ps.Id AS id,
               ps.Name AS name,
               ps.KnessetNum AS knesset_num,
               ps.StartDate AS date
        FROM plenum_session_raw ps
        WHERE {where}
        ORDER BY ps.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "plenums",
    "builder": _build_plenums_search,
    "mapper": lambda row: PlenumSessionResultPartial(
        session_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"],
        date=simple_date(row["date"]),
    ),
})


# ---------------------------------------------------------------------------
# Full-details helpers
# ---------------------------------------------------------------------------

def _fetch_items(cursor, session_id):
    """Fetch agenda items for a plenum session, with status and votes."""
    cursor.execute(
        """
        SELECT i.ItemID, i.Name, i.ItemTypeID, i.ItemTypeDesc,
               i.Ordinal, st."Desc" AS StatusDesc
        FROM plm_session_item_raw i
        LEFT JOIN status_raw st ON i.StatusID = st.Id
        WHERE i.PlenumSessionID = %s
        ORDER BY i.Ordinal ASC
        """,
        (session_id,),
    )
    item_rows = cursor.fetchall()

    items = []
    for item in item_rows:
        item_id = item["itemid"]
        item_type_id = item["itemtypeid"]
        bill_id = item_id if item_type_id == 2 and item_id else None
        votes = get_item_votes(cursor, item_id) if bill_id else None

        items.append(SessionItem(
            item_id=item_id,
            item_type=item["itemtypedesc"],
            item_name=item["name"],
            item_status=item["statusdesc"],
            bill_id=bill_id,
            votes=votes,
        ))
    return items


def _fetch_documents(cursor, session_id):
    """Fetch documents for a plenum session."""
    cursor.execute(
        """
        SELECT GroupTypeDesc, ApplicationDesc, FilePath
        FROM document_plenum_session_raw
        WHERE PlenumSessionID = %s
        ORDER BY GroupTypeDesc, ApplicationDesc
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

_CB_BASE_FROM = "plenum_session_raw s"
_CB_BASE_JOINS = ""

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "knesset_num": CountByConfig(
        group_by="s.KnessetNum",
        id_select=None,
        value_select="s.KnessetNum::text",
        extra_where="s.KnessetNum IS NOT NULL",
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="plenums",
    description=(
        "Search for Knesset plenum sessions. Returns summary info by default; "
        "set full_details=True for agenda items and documents. "
        "Use session_id to filter to a specific session, or from_date to search by date range."
    ),
    entity="Plenum Sessions",
    count_sql="SELECT COUNT(*) FROM plenum_session_raw",
    most_recent_date_sql="SELECT MAX(StartDate) FROM plenum_session_raw",
    enum_sql={
        "item_type": "SELECT DISTINCT ItemTypeDesc FROM plm_session_item_raw WHERE ItemTypeDesc IS NOT NULL ORDER BY ItemTypeDesc",
    },
    is_list=True,
)
def plenums(
    session_id: Annotated[int | None, Field(description="Filter by session ID")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD). Required unless session_id is provided. to_date defaults to today if omitted.")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD). Requires from_date.")] = None,
    query_items: Annotated[str | None, Field(description="Session name or agenda item name contains text")] = None,
    item_type: Annotated[str | None, Field(description="Filter to sessions with items of this type")] = None,
    full_details: Annotated[bool, Field(description="Include agenda items and documents")] = False,
    top: Annotated[int | None, Field(description="Max results (default 50, max 200). Results are sorted newest-first (date DESC) or by count DESC for count_by — so top=N gives the N most recent or highest.")] = None,
    offset: Annotated[int | None, Field(description="Results to skip for pagination. To get the oldest/smallest N: use offset=total_count-N (total_count is in every response).")] = None,
    count_by: Annotated[Literal["all", "knesset_num"] | None, Field(description='Group and count results. "all" returns only total_count (no items). Other values group by field (sorted by count DESC).')] = None,
) -> PlenumSessionsResults:
    """Search for plenum sessions with optional full detail.

    Required scoping: either ``session_id`` or ``from_date`` must be provided.
    """
    normalized = normalize_inputs(locals())
    session_id = normalized["session_id"]
    knesset_num = normalized["knesset_num"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    query_items = normalized["query_items"]
    item_type = normalized["item_type"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    # --- Validation ---
    if to_date and not from_date:
        raise ValueError("to_date requires from_date. Provide from_date or use session_id instead.")
    if not session_id and not from_date:
        raise ValueError("Provide session_id or from_date to scope the query.")

    # Default to_date to today when from_date is provided alone
    if from_date and not to_date:
        to_date = str(date_today.today())

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if session_id is not None:
        conditions.append("s.Id = %s")
        params.append(session_id)

    if knesset_num is not None:
        conditions.append("s.KnessetNum = %s")
        params.append(knesset_num)

    if from_date and to_date:
        conditions.append("s.StartDate >= %s")
        params.append(from_date)
        conditions.append("s.StartDate <= %s")
        params.append(to_date + "T99")

    if query_items:
        conditions.append(f"""(
            {fuzzy_condition("s.Name")}
            OR EXISTS (
                SELECT 1 FROM plm_session_item_raw i
                WHERE i.PlenumSessionID = s.Id AND {fuzzy_condition("i.Name")}
            )
        )""")
        params.extend(fuzzy_params(query_items) + fuzzy_params(query_items))

    if item_type:
        conditions.append("""EXISTS (
            SELECT 1 FROM plm_session_item_raw i
            WHERE i.PlenumSessionID = s.Id AND i.ItemTypeDesc LIKE %s
        )""")
        params.append(f"%{item_type}%")

    where = " AND ".join(conditions) if conditions else "1=1"
    count_sql = f"SELECT COUNT(*) FROM {_CB_BASE_FROM} {_CB_BASE_JOINS} WHERE {where}"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if count_by_val == "all":
            total_count = check_search_count(cursor, count_sql, params, paginated=True)
            conn.close()
            return PlenumSessionsResults(total_count=total_count, items=[], counts=[])
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
        return PlenumSessionsResults(total_count=total_count, items=[], counts=counts)

    total_count = check_search_count(cursor, count_sql, params, entity_name="plenum sessions", paginated=True)

    cursor.execute(
        f"""SELECT DISTINCT s.Id, s.KnessetNum, s.Name, s.StartDate,
               (SELECT COUNT(*) FROM plm_session_item_raw
                WHERE PlenumSessionID = s.Id) AS item_count
        FROM plenum_session_raw s
        WHERE {where}
        ORDER BY s.StartDate DESC, s.Id DESC
        LIMIT %s OFFSET %s""",
        params + [top, offset],
    )
    rows = cursor.fetchall()

    results = []
    for row in rows:
        sid = row["id"]
        partial_kwargs = dict(
            session_id=sid,
            knesset_num=row["knessetnum"],
            name=row["name"],
            date=simple_date(row["startdate"]) or None,
            item_count=row["item_count"] or 0,
        )
        if full_details:
            result = PlenumSessionResultFull(
                **partial_kwargs,
                items=_fetch_items(cursor, sid),
                documents=_fetch_documents(cursor, sid),
            )
        else:
            result = PlenumSessionResultPartial(**partial_kwargs)
        results.append(result)

    conn.close()
    return PlenumSessionsResults(total_count=total_count, items=results)


plenums.OUTPUT_MODEL = PlenumSessionsResults
