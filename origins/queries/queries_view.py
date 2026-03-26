"""Unified queries tool — search and detail via ``full_details`` flag.

Covers KNS_Query data (parliamentary queries to ministers).
Search mode returns summaries; ``full_details=True`` or ``query_id``
returns full detail including documents, ministry info, and session stages.
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
from core.helpers import simple_date, normalize_inputs, check_search_count, resolve_pagination
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import (
    SessionDocument, build_session_date_exists, fetch_item_stages,
)
from origins.queries.queries_models import QueryResultPartial, QueryResultFull, QueriesResults

# ItemTypeID for queries in session_item tables (1=שאילתה, 950=שאילתה כוללת)
_QUERY_TYPE_IDS = [1, 950]


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_queries_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity query search."""
    conditions = []
    params = []

    if query:
        conditions.append("q.Name LIKE %s")
        params.append(f"%{query}%")

    if knesset_num is not None:
        conditions.append("q.KnessetNum = %s")
        params.append(knesset_num)

    if date:
        session_sql, session_params = build_session_date_exists(
            "q", "q.QueryID", _QUERY_TYPE_IDS, date, date_to,
        )
        if date_to:
            date_cond = f"""(
                (q.SubmitDate >= %s AND q.SubmitDate <= %s)
                OR (q.ReplyMinisterDate >= %s AND q.ReplyMinisterDate <= %s)
                OR {session_sql}
            )"""
            conditions.append(date_cond)
            params.extend([date, date_to + "T99", date, date_to + "T99"])
            params.extend(session_params)
        else:
            date_cond = f"""(
                q.SubmitDate >= %s
                OR q.ReplyMinisterDate >= %s
                OR {session_sql}
            )"""
            conditions.append(date_cond)
            params.extend([date, date])
            params.extend(session_params)

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*) FROM query_raw q
        WHERE {where}
    """
    search_sql = f"""
        SELECT q.QueryID AS id, q.Name AS name, q.KnessetNum AS knesset_num,
               q.TypeDesc AS type,
               st."Desc" AS status
        FROM query_raw q
        LEFT JOIN status_raw st ON q.StatusID = st.Id
        WHERE {where}
        ORDER BY q.QueryID DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "queries",
    "builder": _build_queries_search,
    "mapper": lambda row: QueryResultPartial(
        query_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"],
        type=row["type"],
        status=row["status"],
    ),
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _person_name_with_party(cursor, person_id, knesset_num):
    """Fetch a person's full name with party for a given Knesset term."""
    if not person_id:
        return None
    cursor.execute(
        """
        SELECT p.FirstName || ' ' || p.LastName AS full_name,
               ptp.FactionName
        FROM person_raw p
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = p.PersonID
              AND ptp2.KnessetNum = %s
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE p.PersonID = %s
        """,
        (knesset_num, person_id),
    )
    row = cursor.fetchone()
    if not row:
        return None
    name = row["full_name"]
    if row["factionname"]:
        name = f"{name} ({row['factionname']})"
    return name


def _batch_person_names(cursor, person_ids, knesset_nums_by_id):
    """Batch-fetch person names with party for multiple person IDs."""
    if not person_ids:
        return {}
    unique_ids = list(set(person_ids))
    placeholders = ",".join(["%s"] * len(unique_ids))
    cursor.execute(
        f"""
        SELECT p.PersonID,
               p.FirstName || ' ' || p.LastName AS full_name
        FROM person_raw p
        WHERE p.PersonID IN ({placeholders})
        """,
        unique_ids,
    )
    names = {}
    for row in cursor.fetchall():
        pid = row["personid"]
        kn = knesset_nums_by_id.get(pid)
        name = row["full_name"]
        if kn:
            cursor.execute(
                """
                SELECT FactionName
                FROM person_to_position_raw
                WHERE PersonID = %s AND KnessetNum = %s
                  AND FactionName IS NOT NULL AND FactionName != ''
                ORDER BY IsCurrent DESC, PersonToPositionID DESC
                LIMIT 1
                """,
                (pid, kn),
            )
            faction_row = cursor.fetchone()
            if faction_row and faction_row["factionname"]:
                name = f"{name} ({faction_row['factionname']})"
        names[pid] = name
    return names


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="queries",
    description=(
        "Search for Knesset parliamentary queries (שאילתות) to ministers. "
        "Returns summary info by default; set full_details=True for documents, "
        "ministry info, reply dates, and session stages. "
        "Provide query_id for a single query (auto-enables full_details)."
    ),
    entity="Queries",
    count_sql="SELECT COUNT(*) FROM query_raw",
    most_recent_date_sql="SELECT MAX(SubmitDate) FROM query_raw",
    enum_sql={
        "status": 'SELECT DISTINCT s."Desc" FROM query_raw q JOIN status_raw s ON q.StatusID = s.Id WHERE s."Desc" IS NOT NULL ORDER BY s."Desc"',
        "type": "SELECT DISTINCT TypeDesc FROM query_raw WHERE TypeDesc IS NOT NULL ORDER BY TypeDesc",
    },
    is_list=True,
)
def queries(
    query_id: Annotated[int | None, Field(description="Get a specific query by ID (auto-enables full_details)")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name_query: Annotated[str | None, Field(description="Query name/subject contains text")] = None,
    status: Annotated[str | None, Field(description="Query status")] = None,
    type: Annotated[str | None, Field(description="Query type")] = None,
    initiator_id: Annotated[int | None, Field(description="Filter by submitter person ID")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD) — filters by session date (plenum)")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) — use with from_date")] = None,
    full_details: Annotated[bool, Field(description="Include documents, ministry info, reply dates, stages (auto-True when query_id is set)")] = False,
    top: Annotated[int | None, Field(description="Max results to return (default 50, max 200)")] = None,
    offset: Annotated[int | None, Field(description="Number of results to skip for pagination")] = None,
) -> QueriesResults:
    """Search for parliamentary queries or get full detail for a single query.

    Filters (all ANDed):
      - knesset_num: query's Knesset number
      - name_query: query name/subject contains text
      - status: status description contains text
      - type: query type description contains text
      - initiator_id: submitter person ID
      - from_date / to_date: discussed in a session in date range
    """
    normalized = normalize_inputs(locals())
    query_id = normalized["query_id"]
    knesset_num = normalized["knesset_num"]
    name_query = normalized["name_query"]
    status = normalized["status"]
    type_ = normalized["type"]
    initiator_id = normalized["initiator_id"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    if query_id is not None:
        full_details = True

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if query_id is not None:
        conditions.append("q.QueryID = %s")
        params.append(query_id)

    if knesset_num is not None:
        conditions.append("q.KnessetNum = %s")
        params.append(knesset_num)

    if name_query:
        conditions.append("q.Name LIKE %s")
        params.append(f"%{name_query}%")

    if type_:
        conditions.append("q.TypeDesc LIKE %s")
        params.append(f"%{type_}%")

    if status:
        conditions.append('st."Desc" LIKE %s')
        params.append(f"%{status}%")

    if initiator_id is not None:
        conditions.append("q.PersonID = %s")
        params.append(initiator_id)

    # Date filters: OR across submit_date, reply_minister_date, and session date
    if from_date:
        session_sql, session_params = build_session_date_exists(
            "q", "q.QueryID", _QUERY_TYPE_IDS, from_date, to_date,
        )
        if to_date:
            date_cond = f"""(
                (q.SubmitDate >= %s AND q.SubmitDate <= %s)
                OR (q.ReplyMinisterDate >= %s AND q.ReplyMinisterDate <= %s)
                OR {session_sql}
            )"""
            conditions.append(date_cond)
            params.extend([from_date, to_date + "T99", from_date, to_date + "T99"])
            params.extend(session_params)
        else:
            # No upper bound — from_date alone means "from this date onwards"
            date_cond = f"""(
                q.SubmitDate >= %s
                OR q.ReplyMinisterDate >= %s
                OR {session_sql}
            )"""
            conditions.append(date_cond)
            params.extend([from_date, from_date])
            params.extend(session_params)

    where = " AND ".join(conditions) if conditions else "1=1"

    if not query_id:
        total_count = check_search_count(
            cursor,
            f"SELECT COUNT(*) FROM query_raw q"
            f" LEFT JOIN status_raw st ON q.StatusID = st.Id"
            f" WHERE {where}",
            params,
            entity_name="queries",
            paginated=True,
        )
    else:
        total_count = None

    cursor.execute(
        f"""SELECT q.QueryID, q.Name, q.KnessetNum, q.TypeDesc,
               st."Desc" AS StatusDesc,
               q.PersonID, q.GovMinistryID,
               gm.Name AS GovMinistryName,
               q.SubmitDate, q.ReplyMinisterDate, q.ReplyDatePlanned,
               q.LastUpdatedDate,
               last_session.PlenumSessionID AS LastSessionID
        FROM query_raw q
        LEFT JOIN status_raw st ON q.StatusID = st.Id
        LEFT JOIN gov_ministry_raw gm ON q.GovMinistryID = gm.Id
        LEFT JOIN LATERAL (
            SELECT i.PlenumSessionID
            FROM plm_session_item_raw i
            WHERE i.ItemID = q.QueryID AND i.ItemTypeID IN ({",".join(["%s"] * len(_QUERY_TYPE_IDS))})
            ORDER BY i.Id DESC
            LIMIT 1
        ) last_session ON true
        WHERE {where}
        ORDER BY q.QueryID DESC
        LIMIT %s OFFSET %s""",
        list(_QUERY_TYPE_IDS) + params + [top, offset],
    )
    rows = cursor.fetchall()

    if not full_details:
        # Batch fetch submitter names
        person_ids = [row["personid"] for row in rows if row["personid"]]
        kn_by_pid = {}
        for row in rows:
            if row["personid"]:
                kn_by_pid[row["personid"]] = row["knessetnum"]
        names = _batch_person_names(cursor, person_ids, kn_by_pid)

        results = []
        for row in rows:
            pid = row["personid"]
            results.append(QueryResultPartial(
                query_id=row["queryid"],
                name=row["name"],
                knesset_num=row["knessetnum"],
                type=row["typedesc"],
                status=row["statusdesc"],
                submitter_name=names.get(pid) if pid else None,
                gov_ministry_name=row["govministryname"],
                session_id=row["lastsessionid"],
                last_update_date=simple_date(row["lastupdateddate"]) or None,
            ))
    else:
        results = []
        for row in rows:
            kn = row["knessetnum"]

            # Submitter name
            submitter_name = _person_name_with_party(cursor, row["personid"], kn)

            # Session stages
            stages = fetch_item_stages(cursor, row["queryid"], _QUERY_TYPE_IDS)

            # Documents
            cursor.execute(
                """
                SELECT GroupTypeDesc, ApplicationDesc, FilePath
                FROM document_query_raw
                WHERE QueryID = %s
                ORDER BY Id ASC
                """,
                (row["queryid"],),
            )
            documents = [
                SessionDocument(
                    name=dr["grouptypedesc"],
                    type=dr["applicationdesc"],
                    path=dr["filepath"],
                )
                for dr in cursor.fetchall()
            ]

            results.append(QueryResultFull(
                query_id=row["queryid"],
                name=row["name"],
                knesset_num=kn,
                type=row["typedesc"],
                status=row["statusdesc"],
                submitter_name=submitter_name,
                gov_ministry_name=row["govministryname"],
                session_id=row["lastsessionid"],
                last_update_date=simple_date(row["lastupdateddate"]) or None,
                stages=stages,
                submit_date=simple_date(row["submitdate"]) or None,
                gov_ministry_id=row["govministryid"],
                reply_minister_date=simple_date(row["replyministerdate"]) or None,
                reply_date_planned=simple_date(row["replydateplanned"]) or None,
                documents=documents or None,
            ))

    conn.close()
    if total_count is None:
        total_count = len(results)
    return QueriesResults(total_count=total_count, items=results)


queries.OUTPUT_MODEL = QueriesResults
