"""Votes list view — search/filter plenum votes, summary only.

For full detail on a single vote (members, related votes), use
``vote_view.get_vote()``.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly


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

def search_votes(
    knesset_num=None,
    name=None,
    from_date=None,
    to_date=None,
    date=None,
    accepted=None,
    bill_id=None,
) -> list:
    """Search for plenum votes and return summary results.

    Filters (all ANDed):
      - knesset_num: Knesset number (derived via session join)
      - name: vote title or subject contains text
      - from_date / to_date / date: vote date range
      - accepted: True=accepted only, False=rejected only, None=both
      - bill_id: filter to votes linked to a specific bill (via ItemID)

    Returns a list of vote summary dicts sorted by (date, time, vote_id).
    No members or related votes — use ``vote_view.get_vote()`` for that.

    When ``IsAccepted`` is NULL (OData-origin votes without stored totals),
    totals are computed from per-MK results and ``is_accepted`` is inferred
    as ``total_for > total_against``.
    """
    conn = connect_readonly()
    cursor = conn.cursor()

    # LEFT JOIN with computed totals from per-MK results for votes that
    # lack stored totals (OData-origin).  COALESCE picks stored values
    # first, falling back to computed values.
    sql = """
    SELECT v.Id, v.VoteTitle, v.VoteSubject, v.VoteDateTime,
           v.IsAccepted,
           COALESCE(v.TotalFor, r.comp_for) AS TotalFor,
           COALESCE(v.TotalAgainst, r.comp_against) AS TotalAgainst,
           COALESCE(v.TotalAbstain, r.comp_abstain) AS TotalAbstain,
           v.ForOptionDesc, v.AgainstOptionDesc, v.VoteMethodDesc,
           v.SessionID,
           s.KnessetNum,
           b.Id AS _bill_id
    FROM plenum_vote_raw v
    LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
    LEFT JOIN bill_raw b ON v.ItemID = b.Id
    LEFT JOIN (
        SELECT VoteID,
               SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS comp_for,
               SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS comp_against,
               SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS comp_abstain
        FROM plenum_vote_result_raw
        GROUP BY VoteID
    ) r ON r.VoteID = v.Id AND v.TotalFor IS NULL
    WHERE 1=1
    """
    params = []

    if bill_id is not None:
        sql += " AND v.ItemID = ?"
        params.append(bill_id)

    if knesset_num is not None:
        sql += " AND s.KnessetNum = ?"
        params.append(knesset_num)

    if name:
        sql += " AND (v.VoteTitle LIKE ? OR v.VoteSubject LIKE ?)"
        params.extend([f"%{name}%", f"%{name}%"])

    if accepted is not None:
        # Filter on effective is_accepted: use stored value when available,
        # otherwise infer from (possibly computed) totals.
        if accepted:
            sql += """
            AND (v.IsAccepted = 1
                 OR (v.IsAccepted IS NULL
                     AND COALESCE(v.TotalFor, r.comp_for) >
                         COALESCE(v.TotalAgainst, r.comp_against)))
            """
        else:
            sql += """
            AND (v.IsAccepted = 0
                 OR (v.IsAccepted IS NULL
                     AND COALESCE(v.TotalFor, r.comp_for) <=
                         COALESCE(v.TotalAgainst, r.comp_against)))
            """

    if from_date:
        sql += " AND v.VoteDateTime >= ?"
        params.append(from_date)

    if to_date:
        sql += " AND v.VoteDateTime <= ?"
        params.append(to_date + "T99")

    if date:
        sql += " AND v.VoteDateTime LIKE ?"
        params.append(f"{date}%")

    sql += " ORDER BY v.VoteDateTime ASC, v.Id ASC"
    cursor.execute(sql, params)

    results = []
    for vote in cursor.fetchall():
        total_for = vote["TotalFor"]
        total_against = vote["TotalAgainst"]

        is_accepted = vote["IsAccepted"]
        if is_accepted is None and total_for is not None and total_against is not None:
            is_accepted = 1 if total_for > total_against else 0

        results.append({
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
            "total_against": vote["TotalAgainst"],
            "total_abstain": vote["TotalAbstain"],
            "for_option": vote["ForOptionDesc"],
            "against_option": vote["AgainstOptionDesc"],
            "vote_method": vote["VoteMethodDesc"],
        })

    conn.close()
    return results
