"""Unified bills tool — search and detail via ``full_details`` flag.

Replaces the old ``search_bills`` + ``get_bill`` pair with a single
``bills`` tool.

Search mode returns summaries; ``full_details=True`` or ``bill_id``
returns full detail including plenum stages, votes, initiators,
documents, splits, and merges.
"""

import sys
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
    CountByConfig, build_count_by_query,
)
from core.models import CountItem
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from core.session_models import (
    SessionDocument, StageVote, build_session_date_exists, fetch_item_stages,
)
from origins.bills.bills_models import (
    BillResultPartial, BillResultFull, BillsResults,
    BillInitiators, Initiator, RemovedInitiator,
    BillNameHistory, SplitBill, MergedBill,
)

# ItemTypeID for bills in session_item tables
_BILL_TYPE_IDS = [2]


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_bills_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity bill search."""
    conditions = []
    params = []

    if query:
        conditions.append("b.Name LIKE %s")
        params.append(f"%{query}%")

    if knesset_num is not None:
        conditions.append("b.KnessetNum = %s")
        params.append(knesset_num)

    date_sql, date_params = build_session_date_exists(
        "b", "b.Id", _BILL_TYPE_IDS, date, date_to,
    )
    if date_sql:
        conditions.append(date_sql)
        params.extend(date_params)

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*) FROM bill_raw b
        WHERE {where}
    """
    search_sql = f"""
        SELECT b.Id AS id, b.Name AS name, b.KnessetNum AS knesset_num,
               b.SubTypeDesc AS sub_type,
               st."Desc" AS status
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE {where}
        ORDER BY b.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "bills",
    "builder": _build_bills_search,
    "mapper": lambda row: BillResultPartial(
        bill_id=row["id"],
        name=row["name"],
        knesset_num=row["knesset_num"],
        type=row["sub_type"],
        status=row["status"],
    ),
})


# ---------------------------------------------------------------------------
# Detail helpers
# ---------------------------------------------------------------------------

def _get_stage_vote(cursor, bill_id, session_id):
    """Return the final (decisive) vote for a bill within one session, or None."""
    cursor.execute(
        """
        SELECT v.Id, v.VoteTitle, v.VoteDateTime, v.IsAccepted,
               v.TotalFor, v.TotalAgainst, v.TotalAbstain
        FROM plenum_vote_raw v
        WHERE v.ItemID = %s AND v.SessionID = %s
        ORDER BY v.VoteDateTime DESC, v.Id DESC
        LIMIT 1
        """,
        (bill_id, session_id),
    )
    row = cursor.fetchone()
    if not row:
        return None

    total_for = row["totalfor"]
    total_against = row["totalagainst"]
    total_abstain = row["totalabstain"]

    if total_for is None:
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS total_for,
                SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS total_against,
                SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS total_abstain
            FROM plenum_vote_result_raw
            WHERE VoteID = %s
            """,
            (row["id"],),
        )
        counts = cursor.fetchone()
        if counts and counts["total_for"] is not None:
            total_for = counts["total_for"]
            total_against = counts["total_against"]
            total_abstain = counts["total_abstain"]

    is_accepted = row["isaccepted"]
    if is_accepted is None and total_for is not None and total_against is not None:
        is_accepted = 1 if total_for > total_against else 0

    return StageVote(
        vote_id=row["id"],
        title=row["votetitle"] or None,
        date=simple_date(row["votedatetime"]) or None,
        is_accepted=bool(is_accepted) if is_accepted is not None else None,
        total_for=total_for,
        total_against=total_against,
        total_abstain=total_abstain,
    )


def _fetch_full_detail(cursor, bill, bill_id):
    """Fetch all full-detail fields for a single bill.

    Returns (stages, initiators, name_history, documents, split_bills, merged_bills).
    """
    knesset_num = bill["knessetnum"]

    # ----- Unified stages (plenum + committee), with votes on plenum stages -----
    stages = fetch_item_stages(cursor, bill_id, _BILL_TYPE_IDS)
    if stages:
        for stage in stages:
            if stage.plenum_session:
                vote = _get_stage_vote(cursor, bill_id, stage.plenum_session.session_id)
                if vote:
                    stage.plenum_session.vote = vote

    # ----- Initiators (primary + added) -----
    cursor.execute(
        """
        SELECT bi.PersonID, bi.IsInitiator, bi.Ordinal,
               p.FirstName || ' ' || p.LastName AS full_name,
               ptp.FactionName
        FROM bill_initiator_raw bi
        JOIN person_raw p ON bi.PersonID = p.PersonID
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = bi.PersonID
              AND ptp2.KnessetNum = %s
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE bi.BillID = %s
        ORDER BY bi.Ordinal ASC
        """,
        (knesset_num, bill_id),
    )
    init_rows = cursor.fetchall()
    primary_initiators = []
    added_initiators = []
    for irow in init_rows:
        entry = Initiator(
            person_id=irow["personid"],
            name=irow["full_name"],
            party=irow["factionname"] or None,
        )
        if irow["isinitiator"]:
            primary_initiators.append(entry)
        else:
            added_initiators.append(entry)

    # ----- Removed initiators -----
    cursor.execute(
        """
        SELECT bhi.PersonID, bhi.ReasonDesc,
               p.FirstName || ' ' || p.LastName AS full_name,
               ptp.FactionName
        FROM bill_history_initiator_raw bhi
        JOIN person_raw p ON bhi.PersonID = p.PersonID
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = bhi.PersonID
              AND ptp2.KnessetNum = %s
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE bhi.BillID = %s
        ORDER BY bhi.Id ASC
        """,
        (knesset_num, bill_id),
    )
    removed_initiators = [
        RemovedInitiator(
            person_id=hrow["personid"],
            name=hrow["full_name"],
            party=hrow["factionname"] or None,
            reason=hrow["reasondesc"] or None,
        )
        for hrow in cursor.fetchall()
    ]

    initiators = None
    if primary_initiators or added_initiators or removed_initiators:
        initiators = BillInitiators(
            primary=primary_initiators or None,
            added=added_initiators or None,
            removed=removed_initiators or None,
        )

    # ----- Name history -----
    cursor.execute(
        """
        SELECT Name, NameHistoryTypeDesc
        FROM bill_name_raw
        WHERE BillID = %s
        ORDER BY Id ASC
        """,
        (bill_id,),
    )
    name_history = [
        BillNameHistory(name=nr["name"], stage_type=nr["namehistorytypedesc"])
        for nr in cursor.fetchall()
    ]

    # ----- Documents -----
    cursor.execute(
        """
        SELECT GroupTypeDesc, ApplicationDesc, FilePath
        FROM document_bill_raw
        WHERE BillID = %s
        ORDER BY Id ASC
        """,
        (bill_id,),
    )
    documents = [
        SessionDocument(
            name=dr["grouptypedesc"],
            type=dr["applicationdesc"],
            path=dr["filepath"],
        )
        for dr in cursor.fetchall()
    ]

    # ----- Splits -----
    cursor.execute(
        """
        SELECT 'child' AS direction, bs.SplitBillID AS related_bill_id,
               bs.Name AS split_name, b2.Name AS bill_name
        FROM bill_split_raw bs
        LEFT JOIN bill_raw b2 ON bs.SplitBillID = b2.Id
        WHERE bs.MainBillID = %s
        UNION ALL
        SELECT 'parent' AS direction, bs.MainBillID AS related_bill_id,
               bs.Name AS split_name, b2.Name AS bill_name
        FROM bill_split_raw bs
        LEFT JOIN bill_raw b2 ON bs.MainBillID = b2.Id
        WHERE bs.SplitBillID = %s
        ORDER BY related_bill_id
        """,
        (bill_id, bill_id),
    )
    split_bills = [
        SplitBill(
            direction=sr["direction"],
            bill_id=sr["related_bill_id"],
            name=sr["bill_name"] or sr["split_name"],
        )
        for sr in cursor.fetchall()
    ]

    # ----- Merges -----
    cursor.execute(
        """
        SELECT bu.UnionBillID AS related_bill_id, b2.Name AS bill_name
        FROM bill_union_raw bu
        LEFT JOIN bill_raw b2 ON bu.UnionBillID = b2.Id
        WHERE bu.MainBillID = %s
        UNION ALL
        SELECT bu.MainBillID AS related_bill_id, b2.Name AS bill_name
        FROM bill_union_raw bu
        LEFT JOIN bill_raw b2 ON bu.MainBillID = b2.Id
        WHERE bu.UnionBillID = %s
        ORDER BY related_bill_id
        """,
        (bill_id, bill_id),
    )
    merged_bills = [
        MergedBill(bill_id=ur["related_bill_id"], name=ur["bill_name"])
        for ur in cursor.fetchall()
    ]

    # ----- Related primary laws -----
    from origins.laws.laws_models import LawResultPartial
    from origins.laws.laws_view import _law_types, _build_partial as _law_build_partial
    cursor.execute(
        """SELECT DISTINCT il.Id, il.Name, il.KnessetNum,
                  il.IsBasicLaw, il.IsBudgetLaw, il.IsFavoriteLaw,
                  il.PublicationDate, il.LatestPublicationDate,
                  il.LawValidityDesc
        FROM law_binding_raw lb
        JOIN israel_law_raw il ON lb.IsraelLawID = il.Id
        WHERE lb.LawID = %s
        ORDER BY il.PublicationDate DESC, il.Id DESC""",
        (bill_id,),
    )
    related_laws = [_law_build_partial(r) for r in cursor.fetchall()] or None

    return (
        stages, initiators,
        name_history or None, documents or None,
        split_bills or None, merged_bills or None,
        related_laws,
    )


# ---------------------------------------------------------------------------
# Batch initiator fetch (for search mode)
# ---------------------------------------------------------------------------

def _fetch_primary_initiators_batch(cursor, bill_ids):
    """Batch-fetch primary initiator names for a list of bill IDs."""
    if not bill_ids:
        return {}
    placeholders = ",".join(["%s"] * len(bill_ids))
    cursor.execute(
        f"""
        SELECT bi.BillID, bi.PersonID,
               p.FirstName || ' ' || p.LastName AS full_name,
               b.KnessetNum,
               ptp.FactionName
        FROM bill_initiator_raw bi
        JOIN person_raw p ON bi.PersonID = p.PersonID
        JOIN bill_raw b ON bi.BillID = b.Id
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = bi.PersonID
              AND ptp2.KnessetNum = b.KnessetNum
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE bi.IsInitiator = 1 AND bi.BillID IN ({placeholders})
        ORDER BY bi.Ordinal ASC
        """,
        bill_ids,
    )
    initiators_by_bill: dict = {}
    for irow in cursor.fetchall():
        name = irow["full_name"]
        if irow["factionname"]:
            name = f"{name} ({irow['factionname']})"
        initiators_by_bill.setdefault(irow["billid"], []).append(name)
    return initiators_by_bill


# ---------------------------------------------------------------------------
# count_by configuration
# ---------------------------------------------------------------------------

_CB_BASE_FROM = "bill_raw b"
_CB_BASE_JOINS = (
    "LEFT JOIN status_raw st ON b.StatusID = st.Id\n"
    "    LEFT JOIN committee_raw c ON b.CommitteeID = c.Id"
)

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "initiator": CountByConfig(
        group_by="bi.PersonID, p.FirstName, p.LastName",
        id_select="bi.PersonID",
        value_select="p.FirstName || ' ' || p.LastName",
        extra_joins="JOIN bill_initiator_raw bi ON bi.BillID = b.Id JOIN person_raw p ON p.PersonID = bi.PersonID",
        extra_where="bi.PersonID IS NOT NULL",
    ),
    "status": CountByConfig(
        group_by='st."Desc"',
        id_select=None,
        value_select='st."Desc"',
        extra_where='st."Desc" IS NOT NULL',
    ),
    "type": CountByConfig(
        group_by="b.SubTypeDesc",
        id_select=None,
        value_select="b.SubTypeDesc",
        extra_where="b.SubTypeDesc IS NOT NULL",
    ),
    "committee": CountByConfig(
        group_by="c.Id, c.Name",
        id_select="c.Id",
        value_select="c.Name",
        extra_where="c.Id IS NOT NULL",
    ),
    "knesset_num": CountByConfig(
        group_by="b.KnessetNum",
        id_select=None,
        value_select="b.KnessetNum::text",
        extra_where="b.KnessetNum IS NOT NULL",
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="bills",
    description=(
        "Search for Knesset bills (legislation). Returns summary info by default; "
        "set full_details=True for plenum stages, votes, initiators, documents. "
        "Provide bill_id for a single bill (auto-enables full_details)."
    ),
    entity="Bills",
    count_sql="SELECT COUNT(*) FROM bill_raw",
    most_recent_date_sql="SELECT MAX(PublicationDate) FROM bill_raw",
    enum_sql={
        "status": 'SELECT DISTINCT s."Desc" FROM bill_raw b JOIN status_raw s ON b.StatusID = s.Id WHERE s."Desc" IS NOT NULL ORDER BY s."Desc"',
        "type": "SELECT DISTINCT SubTypeDesc FROM bill_raw WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc",
    },
    is_list=True,
)
def bills(
    bill_id: Annotated[int | None, Field(description="Get a specific bill by ID (auto-enables full_details)")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name_query: Annotated[str | None, Field(description="Bill name contains text")] = None,
    status: Annotated[str | None, Field(description="Bill status")] = None,
    type: Annotated[str | None, Field(description="Bill type (private/government/committee)")] = None,
    initiator_id: Annotated[int | None, Field(description="Filter by initiator's member/person ID")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD) — filters by session date (plenum or committee)")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) — use with from_date")] = None,
    full_details: Annotated[bool, Field(description="Include stages, votes, initiators, documents (auto-True when bill_id is set)")] = False,
    top: Annotated[int | None, Field(description="Max results to return (default 50, max 200)")] = None,
    offset: Annotated[int | None, Field(description="Number of results to skip for pagination")] = None,
    count_by: Annotated[Literal["initiator", "status", "type", "committee", "knesset_num"] | None, Field(description="Group and count results by field. Returns counts instead of items.")] = None,
) -> BillsResults:
    """Search for bills or get full detail for a single bill.

    Filters (all ANDed):
      - knesset_num: bill's Knesset number
      - name_query: bill name contains text
      - status: bill's current status description contains text
      - type: bill type (פרטית/ממשלתית/ועדה)
      - initiator_id: member/person ID who initiated the bill
      - from_date / to_date: discussed in a session (plenum or committee) in date range
    """
    normalized = normalize_inputs(locals())
    bill_id = normalized["bill_id"]
    knesset_num = normalized["knesset_num"]
    name_query = normalized["name_query"]
    status = normalized["status"]
    type_ = normalized["type"]
    initiator_id = normalized["initiator_id"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    # Auto-enable full_details when bill_id is given
    if bill_id is not None:
        full_details = True

    conn = connect_readonly()
    cursor = conn.cursor()

    conditions = []
    params = []

    if bill_id is not None:
        conditions.append("b.Id = %s")
        params.append(bill_id)

    if knesset_num is not None:
        conditions.append("b.KnessetNum = %s")
        params.append(knesset_num)

    if name_query:
        conditions.append("b.Name LIKE %s")
        params.append(f"%{name_query}%")

    if type_:
        conditions.append("b.SubTypeDesc LIKE %s")
        params.append(f"%{type_}%")

    if status:
        conditions.append('st."Desc" LIKE %s')
        params.append(f"%{status}%")

    if initiator_id is not None:
        conditions.append("""EXISTS (
            SELECT 1 FROM bill_initiator_raw bi
            WHERE bi.BillID = b.Id AND bi.PersonID = %s AND bi.IsInitiator = 1
        )""")
        params.append(initiator_id)

    # Session date filters (plenum + committee)
    date_sql, date_params = build_session_date_exists(
        "b", "b.Id", _BILL_TYPE_IDS, from_date, to_date,
    )
    if date_sql:
        conditions.append(date_sql)
        params.extend(date_params)

    where = " AND ".join(conditions) if conditions else "1=1"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if bill_id is not None:
            raise ValueError("count_by cannot be used with single-entity lookup (bill_id)")
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
        return BillsResults(total_count=total_count, items=[], counts=counts)

    if not bill_id:
        total_count = check_search_count(
            cursor,
            f"SELECT COUNT(*) FROM bill_raw b"
            f" LEFT JOIN status_raw st ON b.StatusID = st.Id"
            f" WHERE {where}",
            params,
            entity_name="bills",
            paginated=True,
        )
    else:
        total_count = None

    cursor.execute(
        f"""SELECT b.Id, b.Name, b.KnessetNum, b.SubTypeDesc,
               st."Desc" AS StatusDesc, b.CommitteeID, c.Name AS CommitteeName,
               b.PublicationDate, b.PublicationSeriesDesc, b.SummaryLaw,
               b.LastUpdatedDate
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        LEFT JOIN committee_raw c ON b.CommitteeID = c.Id
        WHERE {where}
        ORDER BY b.PublicationDate DESC, b.Id DESC
        LIMIT %s OFFSET %s""",
        params + [top, offset],
    )
    rows = cursor.fetchall()

    if not full_details:
        # Batch-fetch primary initiators for search mode
        bill_ids = [row["id"] for row in rows]
        initiators_by_bill = _fetch_primary_initiators_batch(cursor, bill_ids)

        results = []
        for row in rows:
            initiators = initiators_by_bill.get(row["id"], [])
            results.append(BillResultPartial(
                bill_id=row["id"],
                name=row["name"],
                knesset_num=row["knessetnum"],
                type=row["subtypedesc"],
                status=row["statusdesc"],
                committee=row["committeename"],
                committee_id=row["committeeid"],
                publication_date=simple_date(row["publicationdate"]) or None,
                publication_series=row["publicationseriesdesc"],
                summary=row["summarylaw"],
                primary_initiators=initiators or None,
                last_update_date=simple_date(row["lastupdateddate"]) or None,
            ))
    else:
        results = []
        for row in rows:
            bid = row["id"]
            stages, initiators_obj, name_history, documents, split_bills, merged_bills, \
                related_laws = \
                _fetch_full_detail(cursor, row, bid)

            # Also get primary initiator names for summary field
            initiators_batch = _fetch_primary_initiators_batch(cursor, [bid])
            primary_names = initiators_batch.get(bid, [])

            results.append(BillResultFull(
                bill_id=bid,
                name=row["name"],
                knesset_num=row["knessetnum"],
                type=row["subtypedesc"],
                status=row["statusdesc"],
                committee=row["committeename"],
                committee_id=row["committeeid"],
                publication_date=simple_date(row["publicationdate"]) or None,
                publication_series=row["publicationseriesdesc"],
                summary=row["summarylaw"],
                primary_initiators=primary_names or None,
                last_update_date=simple_date(row["lastupdateddate"]) or None,
                stages=stages,
                initiators=initiators_obj,
                name_history=name_history,
                documents=documents,
                split_bills=split_bills,
                merged_bills=merged_bills,
                related_laws=related_laws,
            ))

    conn.close()
    if total_count is None:
        total_count = len(results)
    return BillsResults(total_count=total_count, items=results)


bills.OUTPUT_MODEL = BillsResults
