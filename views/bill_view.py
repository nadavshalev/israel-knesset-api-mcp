"""Single bill detail view — returns full data for one bill by ID.

Includes bill metadata, plenum stages, and vote summaries per stage.
For searching/filtering multiple bills, use ``bills_view.search_bills()``.
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from config import DEFAULT_DB
from core.db import ensure_indexes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string."""
    if not date_str:
        return ""
    return str(date_str).split("T")[0]


def _simple_time(datetime_str) -> str:
    """Extract HH:MM time from an ISO datetime string."""
    if not datetime_str:
        return ""
    s = str(datetime_str)
    if "T" in s:
        time_part = s.split("T")[1]
        if "+" in time_part:
            time_part = time_part.split("+")[0]
        return time_part[:5]
    return ""


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
        WHERE v.ItemID = ? AND v.SessionID = ?
        ORDER BY v.VoteDateTime DESC, v.Id DESC
        LIMIT 1
        """,
        (bill_id, session_id),
    )
    row = cursor.fetchone()
    if not row:
        return None

    total_for = row["TotalFor"]
    total_against = row["TotalAgainst"]
    total_abstain = row["TotalAbstain"]

    # If stored totals are missing, try computing from per-MK results
    if total_for is None:
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS total_for,
                SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS total_against,
                SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS total_abstain
            FROM plenum_vote_result_raw
            WHERE VoteID = ?
            """,
            (row["Id"],),
        )
        counts = cursor.fetchone()
        if counts and counts["total_for"] is not None:
            total_for = counts["total_for"]
            total_against = counts["total_against"]
            total_abstain = counts["total_abstain"]

    is_accepted = row["IsAccepted"]
    if is_accepted is None and total_for is not None and total_against is not None:
        is_accepted = 1 if total_for > total_against else 0

    return {
        "vote_id": row["Id"],
        "title": row["VoteTitle"] or "",
        "date": _simple_date(row["VoteDateTime"]),
        "is_accepted": bool(is_accepted) if is_accepted is not None else None,
        "total_for": total_for,
        "total_against": total_against,
        "total_abstain": total_abstain,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_bill(bill_id: int) -> dict | None:
    """Return full detail for a single bill, or None if not found.

    Includes metadata from KNS_Bill, plenum stages from
    plm_session_item_raw, and the final (decisive) vote per stage.
    """
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
    ensure_indexes(conn)
    cursor = conn.cursor()

    # Bill metadata
    cursor.execute(
        """
        SELECT b.*, st.[Desc] AS StatusDesc, c.Name AS CommitteeName
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        LEFT JOIN committee_raw c ON b.CommitteeID = c.Id
        WHERE b.Id = ?
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
               ist.[Desc] AS StageStatusDesc,
               s.StartDate, s.Id AS session_id
        FROM plm_session_item_raw i
        JOIN plenum_session_raw s ON i.PlenumSessionID = s.Id
        LEFT JOIN status_raw ist ON i.StatusID = ist.Id
        WHERE i.ItemID = ?
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
            "date": _simple_date(row["StartDate"]),
            "status": row["StageStatusDesc"],
            "session_id": row["session_id"],
        }
        # Attach the final (decisive) vote only to the last sub-stage
        if row["item_id"] == last_item_per_session[row["session_id"]]:
            vote = _get_stage_vote(cursor, bill_id, row["session_id"])
            if vote:
                stage["vote"] = vote
        stages.append(stage)

    obj = {
        "bill_id": bill["Id"],
        "name": bill["Name"],
        "knesset_num": bill["KnessetNum"],
        "sub_type": bill["SubTypeDesc"],
        "status": bill["StatusDesc"],
        "committee": bill["CommitteeName"],
        "publication_date": _simple_date(bill["PublicationDate"]),
        "publication_series": bill["PublicationSeriesDesc"],
        "summary": bill["SummaryLaw"],
        "stages": stages,
    }

    conn.close()
    return obj
