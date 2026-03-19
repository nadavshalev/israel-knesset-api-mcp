"""Single committee session detail view — returns full data for one session by ID.

Includes session metadata, all agenda items (with linked bill names),
and documents.

For searching/filtering multiple sessions, use
``search_cmt_sessions_view.search_cmt_sessions()``.
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
from core.helpers import simple_date, simple_time, normalize_inputs
from core.mcp_meta import mcp_tool
from origins.committees.get_cmt_session_models import (
    CmtSessionItem, CmtSessionDocument, CmtSessionDetail, ItemVote,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_bill_votes(cursor, bill_id):
    """Return all plenum votes for a bill, newest first.

    Links via ``plenum_vote_raw.ItemID = bill_id``.  When stored totals
    are missing, computes them from per-MK results.
    """
    cursor.execute(
        """
        SELECT v.Id, v.VoteTitle, v.VoteDateTime, v.IsAccepted,
               v.TotalFor, v.TotalAgainst, v.TotalAbstain
        FROM plenum_vote_raw v
        WHERE v.ItemID = %s
        ORDER BY v.VoteDateTime DESC, v.Id DESC
        """,
        (bill_id,),
    )
    rows = cursor.fetchall()
    votes = []
    for row in rows:
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

        votes.append(ItemVote(
            vote_id=row["id"],
            title=row["votetitle"] or None,
            date=simple_date(row["votedatetime"]) or None,
            is_accepted=bool(is_accepted) if is_accepted is not None else None,
            total_for=total_for,
            total_against=total_against,
            total_abstain=total_abstain,
        ))
    return votes or None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_cmt_session",
    description=(
        "Get full detail for a single committee session by ID. Includes "
        "session metadata, all agenda items (with linked bill names), "
        "and documents."
    ),
    entity="Committee Sessions",
    is_list=False,
)
def get_cmt_session(
    session_id: Annotated[int, Field(description="The committee session ID (required)")],
) -> CmtSessionDetail | None:
    """Return full detail for a single committee session, or None if not found.

    Always includes agenda items and documents.

    Args:
        session_id: The committee session ID (required).
    """
    normalized = normalize_inputs(locals())
    session_id = normalized["session_id"]

    conn = connect_readonly()
    cursor = conn.cursor()

    # Session metadata with committee name
    cursor.execute(
        """
        SELECT cs.*, c.Name AS CommitteeName
        FROM committee_session_raw cs
        JOIN committee_raw c ON c.Id = cs.CommitteeID
        WHERE cs.Id = %s
        """,
        (session_id,),
    )
    session = cursor.fetchone()
    if not session:
        conn.close()
        return None

    # Build ItemTypeID -> description lookup from plm_session_item_raw
    cursor.execute(
        "SELECT DISTINCT ItemTypeID, ItemTypeDesc FROM plm_session_item_raw "
        "WHERE ItemTypeID IS NOT NULL AND ItemTypeDesc IS NOT NULL"
    )
    item_type_map = {row["itemtypeid"]: row["itemtypedesc"] for row in cursor.fetchall()}

    # Fetch items with linked bill names (ItemTypeID=2 -> bill_raw)
    cursor.execute(
        """
        SELECT csi.Id, csi.Name, csi.ItemTypeID, csi.Ordinal,
               csi.StatusID, csi.ItemID,
               b.Name AS BillName
        FROM cmt_session_item_raw csi
        LEFT JOIN bill_raw b ON csi.ItemTypeID = 2 AND csi.ItemID = b.Id
        WHERE csi.CommitteeSessionID = %s
        ORDER BY csi.Ordinal ASC
        """,
        (session_id,),
    )
    item_rows = cursor.fetchall()

    # Fetch documents
    cursor.execute(
        """
        SELECT Id, GroupTypeDesc, DocumentName, ApplicationDesc, FilePath
        FROM document_committee_session_raw
        WHERE CommitteeSessionID = %s
        ORDER BY GroupTypeDesc, Id
        """,
        (session_id,),
    )
    doc_rows = cursor.fetchall()

    result = CmtSessionDetail(
        session_id=session["id"],
        committee_id=session["committeeid"],
        committee_name=session["committeename"] or None,
        knesset_num=session["knessetnum"],
        number=session["number"],
        date=simple_date(session["startdate"]) or None,
        start_time=simple_time(session["startdate"]) or None,
        end_time=simple_time(session["finishdate"]) or None,
        type=session["typedesc"] or None,
        status=session["statusdesc"] or None,
        location=session["location"] or None,
        url=session["sessionurl"] or None,
        broadcast_url=session["broadcasturl"] or None,
        note=session["note"] or None,
        items=[
            CmtSessionItem(
                item_id=item["id"],
                name=item["name"] or None,
                item_type_id=item["itemtypeid"],
                item_type=item_type_map.get(item["itemtypeid"]) or None,
                ordinal=item["ordinal"],
                status_id=item["statusid"],
                linked_bill_id=item["itemid"] if item["itemtypeid"] == 2 else None,
                linked_bill_name=item["billname"] or None,
                votes=_get_bill_votes(cursor, item["itemid"]) if item["itemtypeid"] == 2 and item["itemid"] else None,
            )
            for item in item_rows
        ],
        documents=[
            CmtSessionDocument(
                document_id=doc["id"],
                type=doc["grouptypedesc"] or None,
                name=doc["documentname"] or None,
                format=doc["applicationdesc"] or None,
                file_path=doc["filepath"] or None,
            )
            for doc in doc_rows
        ],
    )

    conn.close()
    return result


get_cmt_session.OUTPUT_MODEL = CmtSessionDetail
