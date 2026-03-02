"""Single vote detail view — returns full data for one vote by ID.

Includes vote metadata, per-MK member breakdown (who participated),
and related votes (same VoteTitle + SessionID, each with its own stage).
For searching/filtering multiple votes, use ``votes_view.search_votes()``.
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
# Public API
# ---------------------------------------------------------------------------

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
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
    ensure_indexes(conn)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT v.*, s.KnessetNum,
               b.Id AS _bill_id
        FROM plenum_vote_raw v
        LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
        LEFT JOIN bill_raw b ON v.ItemID = b.Id
        WHERE v.Id = ?
        """,
        (vote_id,),
    )
    vote = cursor.fetchone()
    if not vote:
        conn.close()
        return None

    total_for = vote["TotalFor"]
    total_against = vote["TotalAgainst"]
    total_abstain = vote["TotalAbstain"]
    is_accepted = vote["IsAccepted"]

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
        "vote_id": vote["Id"],
        "bill_id": vote["_bill_id"],
        "knesset_num": vote["KnessetNum"],
        "session_id": vote["SessionID"],
        "title": vote["VoteTitle"],
        "subject": vote["VoteSubject"],
        "date": _simple_date(vote["VoteDateTime"]),
        "time": _simple_time(vote["VoteDateTime"]),
        "is_accepted": bool(is_accepted) if is_accepted is not None else None,
        "total_for": total_for,
        "total_against": total_against,
        "total_abstain": total_abstain,
        "for_option": vote["ForOptionDesc"],
        "against_option": vote["AgainstOptionDesc"],
        "vote_method": vote["VoteMethodDesc"],
    }

    # --- Members: per-MK breakdown ---
    cursor.execute(
        """
        SELECT MkId, ResultCode, ResultDesc, FirstName, LastName
        FROM plenum_vote_result_raw
        WHERE VoteID = ?
        ORDER BY LastName, FirstName
        """,
        (vote_id,),
    )
    members = []
    for row in cursor.fetchall():
        first = row["FirstName"] or ""
        last = row["LastName"] or ""
        name = f"{first} {last}".strip() or f"MK {row['MkId']}"
        members.append({
            "member_id": row["MkId"],
            "name": name,
            "result": row["ResultDesc"] or str(row["ResultCode"]),
        })
    obj["members"] = members

    # --- Related votes: same VoteTitle + SessionID, different vote IDs ---
    vote_title = vote["VoteTitle"]
    session_id = vote["SessionID"]

    if vote_title and session_id:
        cursor.execute(
            """
            SELECT Id, VoteDateTime, Ordinal, VoteSubject,
                   ForOptionDesc, AgainstOptionDesc,
                   IsAccepted, TotalFor, TotalAgainst, TotalAbstain
            FROM plenum_vote_raw
            WHERE VoteTitle = ? AND SessionID = ? AND Id != ?
            ORDER BY Ordinal ASC
            """,
            (vote_title, session_id, vote_id),
        )
        related = []
        for r in cursor.fetchall():
            r_accepted = r["IsAccepted"]
            r_total_for = r["TotalFor"]
            r_total_against = r["TotalAgainst"]
            r_total_abstain = r["TotalAbstain"]

            # Compute totals from results if missing
            if r_total_for is None:
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS total_for,
                        SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS total_against,
                        SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS total_abstain
                    FROM plenum_vote_result_raw
                    WHERE VoteID = ?
                    """,
                    (r["Id"],),
                )
                rc = cursor.fetchone()
                if rc and rc["total_for"] is not None:
                    r_total_for = rc["total_for"]
                    r_total_against = rc["total_against"]
                    r_total_abstain = rc["total_abstain"]

            if r_accepted is None and r_total_for is not None and r_total_against is not None:
                r_accepted = 1 if r_total_for > r_total_against else 0

            related.append({
                "vote_id": r["Id"],
                "subject": r["VoteSubject"],
                "for_option": r["ForOptionDesc"],
                "date": _simple_date(r["VoteDateTime"]),
                "time": _simple_time(r["VoteDateTime"]),
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
