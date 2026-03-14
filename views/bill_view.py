"""Single bill detail view — returns full data for one bill by ID.

Includes bill metadata, plenum stages, and vote summaries per stage.
For searching/filtering multiple bills, use ``bills_view.search_bills()``.
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


# ---------------------------------------------------------------------------
# Vote summary for a bill in a given session
# ---------------------------------------------------------------------------

def _get_stage_vote(cursor, bill_id, session_id):
    """Return the final (decisive) vote for a bill within one session, or None.

    A bill's session may contain many votes — section approvals (סעיפים),
    opposition reservations (הסתייגויות), and the final passage/rejection
    vote.  Only the last vote chronologically matters for determining the
    stage outcome, so we return just that one.

    Links via ``plenum_vote_raw.ItemID = bill_id`` within the same session.
    """
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

    # If stored totals are missing, try computing from per-MK results
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

    return {
        "vote_id": row["id"],
        "title": row["votetitle"] or "",
        "date": simple_date(row["votedatetime"]),
        "is_accepted": bool(is_accepted) if is_accepted is not None else None,
        "total_for": total_for,
        "total_against": total_against,
        "total_abstain": total_abstain,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_bill",
    description=(
        "Get full detail for a single bill by ID. Includes bill metadata, "
        "plenum stages (readings), and vote results per stage."
    ),
    entity="Bills",
    is_list=False,
)
def get_bill(
    bill_id: Annotated[int, Field(description="The bill ID (required)")],
) -> dict | None:
    """Return full detail for a single bill, or None if not found.

    Includes metadata from KNS_Bill, plenum stages from
    plm_session_item_raw, and the final (decisive) vote per stage.
    """
    normalized = normalize_inputs(locals())
    bill_id = normalized["bill_id"]

    conn = connect_readonly()
    cursor = conn.cursor()

    # Bill metadata
    cursor.execute(
        """
        SELECT b.*, st."Desc" AS StatusDesc, c.Name AS CommitteeName
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        LEFT JOIN committee_raw c ON b.CommitteeID = c.Id
        WHERE b.Id = %s
        """,
        (bill_id,),
    )
    bill = cursor.fetchone()
    if not bill:
        conn.close()
        return None

    # Plenum stages
    cursor.execute(
        """
        SELECT i.Id AS item_id,
               ist."Desc" AS StageStatusDesc,
               s.StartDate, s.Id AS session_id
        FROM plm_session_item_raw i
        JOIN plenum_session_raw s ON i.PlenumSessionID = s.Id
        LEFT JOIN status_raw ist ON i.StatusID = ist.Id
        WHERE i.ItemID = %s
        ORDER BY s.StartDate ASC, i.Id ASC
        """,
        (bill_id,),
    )
    stage_rows = cursor.fetchall()

    # Find the last item Id per session — only that stage gets the vote.
    last_item_per_session = {}
    for row in stage_rows:
        sid = row["session_id"]
        iid = row["item_id"]
        if sid not in last_item_per_session or iid > last_item_per_session[sid]:
            last_item_per_session[sid] = iid

    stages = []
    for row in stage_rows:
        stage = {
            "date": simple_date(row["startdate"]),
            "status": row["stagestatusdesc"],
            "session_id": row["session_id"],
        }
        # Attach the final (decisive) vote only to the last sub-stage
        if row["item_id"] == last_item_per_session[row["session_id"]]:
            vote = _get_stage_vote(cursor, bill_id, row["session_id"])
            if vote:
                stage["vote"] = vote
        stages.append(stage)

    # ----- Initiators (primary + added) from KNS_BillInitiator -----
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
        (bill["knessetnum"], bill_id),
    )
    init_rows = cursor.fetchall()
    primary_initiators = []
    added_initiators = []
    for irow in init_rows:
        entry = {
            "person_id": irow["personid"],
            "name": irow["full_name"],
        }
        if irow["factionname"]:
            entry["party"] = irow["factionname"]
        if irow["isinitiator"]:
            primary_initiators.append(entry)
        else:
            added_initiators.append(entry)

    # ----- Removed initiators from KNS_BillHistoryInitiator -----
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
        (bill["knessetnum"], bill_id),
    )
    removed_initiators = []
    for hrow in cursor.fetchall():
        entry = {
            "person_id": hrow["personid"],
            "name": hrow["full_name"],
        }
        if hrow["factionname"]:
            entry["party"] = hrow["factionname"]
        if hrow["reasondesc"]:
            entry["reason"] = hrow["reasondesc"]
        removed_initiators.append(entry)

    # ----- Name history from KNS_BillName -----
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
        {"name": nr["name"], "stage_type": nr["namehistorytypedesc"]}
        for nr in cursor.fetchall()
    ]

    # ----- Documents from KNS_DocumentBill -----
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
        {
            "type": dr["grouptypedesc"],
            "format": dr["applicationdesc"],
            "url": dr["filepath"],
        }
        for dr in cursor.fetchall()
    ]

    # ----- Bill splits (both directions) from KNS_BillSplit -----
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
    split_rows = cursor.fetchall()
    split_bills = [
        {
            "direction": sr["direction"],
            "bill_id": sr["related_bill_id"],
            "name": sr["bill_name"] or sr["split_name"],
        }
        for sr in split_rows
    ]

    # ----- Merged bills from KNS_BillUnion -----
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
    union_rows = cursor.fetchall()
    merged_bills = [
        {"bill_id": ur["related_bill_id"], "name": ur["bill_name"]}
        for ur in union_rows
    ]

    # ----- Build result -----
    obj = {
        "bill_id": bill["id"],
        "name": bill["name"],
        "knesset_num": bill["knessetnum"],
        "sub_type": bill["subtypedesc"],
        "status": bill["statusdesc"],
        "committee": bill["committeename"],
        "committee_id": bill["committeeid"],
        "publication_date": simple_date(bill["publicationdate"]),
        "publication_series": bill["publicationseriesdesc"],
        "summary": bill["summarylaw"],
        "stages": stages,
    }

    # Only include initiators dict if there is data, and only sub-lists
    # that are non-empty.
    initiators = {}
    if primary_initiators:
        initiators["primary"] = primary_initiators
    if added_initiators:
        initiators["added"] = added_initiators
    if removed_initiators:
        initiators["removed"] = removed_initiators
    if initiators:
        obj["initiators"] = initiators

    if name_history:
        obj["name_history"] = name_history
    if documents:
        obj["documents"] = documents
    if split_bills:
        obj["split_bills"] = split_bills
    if merged_bills:
        obj["merged_bills"] = merged_bills

    conn.close()
    return obj
