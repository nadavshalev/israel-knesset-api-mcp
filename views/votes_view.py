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

from typing import Annotated
from pydantic import Field

from core.db import connect_readonly
from core.helpers import simple_date, simple_time, normalize_inputs
from core.mcp_meta import mcp_tool
from core.search_meta import register_search

register_search({
    "entity_key": "votes",
    "count_sql": """
        SELECT COUNT(*) FROM plenum_vote_raw
        WHERE VoteTitle LIKE %s OR VoteSubject LIKE %s
    """,
    "search_sql": """
        SELECT v.Id AS id, v.VoteTitle AS name,
               s.KnessetNum AS knesset_num,
               v.VoteDateTime AS date
        FROM plenum_vote_raw v
        LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
        WHERE v.VoteTitle LIKE %s OR v.VoteSubject LIKE %s
        ORDER BY v.Id DESC
        LIMIT %s
    """,
    "param_count": 2,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_votes",
    description=(
        "Search for Knesset plenum votes. Returns summary info: title, "
        "subject, date, totals, accepted/rejected. "
        "Use get_vote for full detail including per-member breakdown."
    ),
    entity="Plenum Votes",
    count_sql="SELECT COUNT(*) FROM plenum_vote_raw",
    is_list=True,
)
def search_votes(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number (via session join)")] = None,
    name: Annotated[str | None, Field(description="Vote title or subject contains text")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD)")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD)")] = None,
    date: Annotated[str | None, Field(description="Exact date (YYYY-MM-DD)")] = None,
    accepted: Annotated[bool | None, Field(description="True=accepted only, False=rejected only, null=both")] = None,
    bill_id: Annotated[int | None, Field(description="Filter to votes linked to a specific bill ID")] = None,
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
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    date = normalized["date"]
    accepted = normalized["accepted"]
    bill_id = normalized["bill_id"]

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
        sql += " AND v.ItemID = %s"
        params.append(bill_id)

    if knesset_num is not None:
        sql += " AND s.KnessetNum = %s"
        params.append(knesset_num)

    if name:
        sql += " AND (v.VoteTitle LIKE %s OR v.VoteSubject LIKE %s)"
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
        sql += " AND v.VoteDateTime >= %s"
        params.append(from_date)

    if to_date:
        sql += " AND v.VoteDateTime <= %s"
        params.append(to_date + "T99")

    if date:
        sql += " AND v.VoteDateTime LIKE %s"
        params.append(f"{date}%")

    sql += " ORDER BY v.VoteDateTime ASC, v.Id ASC"
    cursor.execute(sql, params)

    results = []
    for vote in cursor.fetchall():
        total_for = vote["totalfor"]
        total_against = vote["totalagainst"]

        is_accepted = vote["isaccepted"]
        if is_accepted is None and total_for is not None and total_against is not None:
            is_accepted = 1 if total_for > total_against else 0

        results.append({
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
            "total_against": vote["totalagainst"],
            "total_abstain": vote["totalabstain"],
            "for_option": vote["foroptiondesc"],
            "against_option": vote["againstoptiondesc"],
            "vote_method": vote["votemethoddesc"],
        })

    conn.close()
    return results
