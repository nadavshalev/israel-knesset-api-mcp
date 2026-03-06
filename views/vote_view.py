"""Single vote detail view — returns full data for one vote by ID.

Includes vote metadata, per-MK member breakdown (who participated),
and related votes (same VoteTitle + SessionID, each with its own stage).
For searching/filtering multiple votes, use ``votes_view.search_votes()``.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly
from core.helpers import simple_date, simple_time
from core.mcp_meta import mcp_tool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_vote",
    description=(
        "Get full detail for a single plenum vote by ID. Includes vote "
        "metadata, per-member breakdown (who voted for/against/abstained), "
        "and related votes from the same session."
    ),
    entity="Plenum Votes",
    is_list=False,
)
def get_vote(vote_id: int) -> dict | None:
    """Return full detail for a single vote, or None if not found.

    Includes:
      - Vote metadata (title, subject, date, totals, etc.)
      - ``members``: per-MK breakdown of all who participated in this vote
      - ``related_votes``: other votes with the same VoteTitle + SessionID,
        each with its own subject (stage) and for_option label

    Args:
        vote_id: The vote ID (required).
    """
    conn = connect_readonly()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT v.*, s.KnessetNum,
               b.Id AS _bill_id
        FROM plenum_vote_raw v
        LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
        LEFT JOIN bill_raw b ON v.ItemID = b.Id
        WHERE v.Id = %s
        """,
        (vote_id,),
    )
    vote = cursor.fetchone()
    if not vote:
        conn.close()
        return None

    total_for = vote["totalfor"]
    total_against = vote["totalagainst"]
    total_abstain = vote["totalabstain"]
    is_accepted = vote["isaccepted"]

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
            (vote_id,),
        )
        counts = cursor.fetchone()
        if counts and counts["total_for"] is not None:
            total_for = counts["total_for"]
            total_against = counts["total_against"]
            total_abstain = counts["total_abstain"]

    if is_accepted is None and total_for is not None and total_against is not None:
        is_accepted = 1 if total_for > total_against else 0

    obj = {
        "vote_id": vote["id"],
        "bill_id": vote["_bill_id"],
        "knesset_num": vote["knessetnum"],
        "session_id": vote["sessionid"],
        "title": vote["votetitle"],
        "subject": vote["votesubject"],
        "date": simple_date(vote["votedatetime"]),
        "time": simple_time(vote["votedatetime"]),
        "is_accepted": bool(is_accepted) if is_accepted is not None else None,
        "total_for": total_for,
        "total_against": total_against,
        "total_abstain": total_abstain,
        "for_option": vote["foroptiondesc"],
        "against_option": vote["againstoptiondesc"],
        "vote_method": vote["votemethoddesc"],
    }

    # --- Members: per-MK breakdown ---
    cursor.execute(
        """
        SELECT MkId, ResultCode, ResultDesc, FirstName, LastName
        FROM plenum_vote_result_raw
        WHERE VoteID = %s
        ORDER BY LastName, FirstName
        """,
        (vote_id,),
    )
    members = []
    for row in cursor.fetchall():
        first = row["firstname"] or ""
        last = row["lastname"] or ""
        name = f"{first} {last}".strip() or f"MK {row['mkid']}"
        members.append({
            "member_id": row["mkid"],
            "name": name,
            "result": row["resultdesc"] or str(row["resultcode"]),
        })
    obj["members"] = members

    # --- Related votes: same VoteTitle + SessionID, different vote IDs ---
    vote_title = vote["votetitle"]
    session_id = vote["sessionid"]

    if vote_title and session_id:
        cursor.execute(
            """
            SELECT Id, VoteDateTime, Ordinal, VoteSubject,
                   ForOptionDesc, AgainstOptionDesc,
                   IsAccepted, TotalFor, TotalAgainst, TotalAbstain
            FROM plenum_vote_raw
            WHERE VoteTitle = %s AND SessionID = %s AND Id != %s
            ORDER BY Ordinal ASC
            """,
            (vote_title, session_id, vote_id),
        )
        related = []
        for r in cursor.fetchall():
            r_accepted = r["isaccepted"]
            r_total_for = r["totalfor"]
            r_total_against = r["totalagainst"]
            r_total_abstain = r["totalabstain"]

            # Compute totals from results if missing
            if r_total_for is None:
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS total_for,
                        SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS total_against,
                        SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS total_abstain
                    FROM plenum_vote_result_raw
                    WHERE VoteID = %s
                    """,
                    (r["id"],),
                )
                rc = cursor.fetchone()
                if rc and rc["total_for"] is not None:
                    r_total_for = rc["total_for"]
                    r_total_against = rc["total_against"]
                    r_total_abstain = rc["total_abstain"]

            if r_accepted is None and r_total_for is not None and r_total_against is not None:
                r_accepted = 1 if r_total_for > r_total_against else 0

            related.append({
                "vote_id": r["id"],
                "subject": r["votesubject"],
                "for_option": r["foroptiondesc"],
                "date": simple_date(r["votedatetime"]),
                "time": simple_time(r["votedatetime"]),
                "is_accepted": bool(r_accepted) if r_accepted is not None else None,
                "total_for": r_total_for,
                "total_against": r_total_against,
                "total_abstain": r_total_abstain,
            })
        obj["related_votes"] = related
    else:
        obj["related_votes"] = []

    conn.close()
    return obj
