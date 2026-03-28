"""Unified votes tool — search and detail via ``full_details`` flag.

Replaces the old ``search_votes`` + ``get_vote`` pair with a single
``votes`` tool.

Search mode returns summaries (title, subject, date, totals, accepted/
rejected). ``full_details=True`` or ``vote_id`` returns full detail
including per-member breakdown with party and related votes.
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
    simple_date, simple_time, normalize_inputs, check_search_count, resolve_pagination,
    CountByConfig, build_count_by_query, fuzzy_condition_or, fuzzy_params_or,
)
from core.models import CountItem
from core.mcp_meta import mcp_tool
from core.search_meta import register_search
from origins.votes.votes_models import VoteResultPartial, VoteResultFull, VotesResults, VoteMember, RelatedVote


# ---------------------------------------------------------------------------
# Cross-entity search builder (for search_across)
# ---------------------------------------------------------------------------

def _build_votes_search(*, query, knesset_num, date, date_to, top_n):
    """Build SQL for cross-entity vote search."""
    conditions = []
    params = []

    if query:
        conditions.append(fuzzy_condition_or("v.VoteTitle", "v.VoteSubject"))
        params.extend(fuzzy_params_or(query))

    if knesset_num is not None:
        conditions.append("s.KnessetNum = %s")
        params.append(knesset_num)

    if date and date_to:
        conditions.append("v.VoteDateTime >= %s AND v.VoteDateTime <= %s")
        params.extend([date, date_to + "T99"])
    elif date:
        conditions.append("v.VoteDateTime >= %s AND v.VoteDateTime <= %s")
        params.extend([date, date + "T99"])

    where = " AND ".join(conditions) if conditions else "1=1"

    count_sql = f"""
        SELECT COUNT(*) FROM plenum_vote_raw v
        LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
        WHERE {where}
    """
    search_sql = f"""
        SELECT v.Id AS id, v.VoteTitle AS name,
               s.KnessetNum AS knesset_num,
               v.VoteDateTime AS date
        FROM plenum_vote_raw v
        LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
        WHERE {where}
        ORDER BY v.Id DESC
        LIMIT %s
    """
    return count_sql, list(params), search_sql, list(params) + [top_n]


register_search({
    "entity_key": "votes",
    "builder": _build_votes_search,
    "mapper": lambda row: VoteResultPartial(
        vote_id=row["id"],
        title=row["name"],
        knesset_num=row["knesset_num"],
        date=simple_date(row["date"]),
    ),
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_vote_result(vote_row) -> tuple:
    """Extract and compute totals/is_accepted from a vote row dict.

    Returns (total_for, total_against, total_abstain, is_accepted).
    """
    total_for = vote_row["totalfor"]
    total_against = vote_row["totalagainst"]
    total_abstain = vote_row["totalabstain"]
    is_accepted = vote_row["isaccepted"]
    if is_accepted is None and total_for is not None and total_against is not None:
        is_accepted = 1 if total_for > total_against else 0
    return total_for, total_against, total_abstain, is_accepted


def _fetch_members(cursor, vote_id, knesset_num) -> list[VoteMember]:
    """Fetch per-MK breakdown with party for one vote."""
    cursor.execute(
        """
        SELECT r.MkId, r.ResultCode, r.ResultDesc, r.FirstName, r.LastName,
               ptp.FactionName
        FROM plenum_vote_result_raw r
        LEFT JOIN LATERAL (
            SELECT ptp2.FactionName
            FROM person_to_position_raw ptp2
            WHERE ptp2.PersonID = r.MkId
              AND ptp2.KnessetNum = %s
              AND ptp2.FactionName IS NOT NULL
              AND ptp2.FactionName != ''
            ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
            LIMIT 1
        ) ptp ON true
        WHERE r.VoteID = %s
        ORDER BY r.LastName, r.FirstName
        """,
        (knesset_num, vote_id),
    )
    members = []
    for row in cursor.fetchall():
        first = row["firstname"] or ""
        last = row["lastname"] or ""
        name = f"{first} {last}".strip() or f"MK {row['mkid']}"
        members.append(VoteMember(
            member_id=row["mkid"],
            name=name,
            party=row["factionname"] or None,
            result=row["resultdesc"] or str(row["resultcode"]),
        ))
    return members


def _fetch_related_votes(cursor, vote_id, vote_title, session_id) -> list[RelatedVote]:
    """Fetch related votes (same VoteTitle + SessionID) for one vote."""
    if not vote_title or not session_id:
        return []
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
        r_total_for = r["totalfor"]
        r_total_against = r["totalagainst"]
        r_total_abstain = r["totalabstain"]
        r_accepted = r["isaccepted"]

        if r_total_for is None:
            cursor.execute(
                """
                SELECT
                    SUM(CASE WHEN ResultCode = 7 THEN 1 ELSE 0 END) AS total_for,
                    SUM(CASE WHEN ResultCode = 8 THEN 1 ELSE 0 END) AS total_against,
                    SUM(CASE WHEN ResultCode = 9 THEN 1 ELSE 0 END) AS total_abstain
                FROM plenum_vote_result_raw WHERE VoteID = %s
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

        related.append(RelatedVote(
            vote_id=r["id"],
            subject=r["votesubject"],
            for_option=r["foroptiondesc"],
            date=simple_date(r["votedatetime"]) or None,
            time=simple_time(r["votedatetime"]) or None,
            is_accepted=bool(r_accepted) if r_accepted is not None else None,
            total_for=r_total_for,
            total_against=r_total_against,
            total_abstain=r_total_abstain,
        ))
    return related


# ---------------------------------------------------------------------------
# count_by configuration
# ---------------------------------------------------------------------------

_CB_BASE_FROM = "plenum_vote_raw v"
_CB_BASE_JOINS = (
    "LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id\n"
    "    LEFT JOIN bill_raw b ON v.ItemID = b.Id"
)

_COUNT_BY_OPTIONS: dict[str, CountByConfig] = {
    "bill": CountByConfig(
        group_by="v.ItemID, b.Name",
        id_select="v.ItemID",
        value_select="b.Name",
        extra_where="v.ItemID IS NOT NULL",
    ),
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
    name="votes",
    description=(
        "Search for Knesset plenum votes. "
        "Returns summary info by default (title, subject, date, totals, accepted/rejected). "
        "Set full_details=True for per-member breakdown with party and related votes from the same session."
    ),
    entity="Plenum Votes",
    count_sql="SELECT COUNT(*) FROM plenum_vote_raw",
    most_recent_date_sql="SELECT MAX(VoteDateTime) FROM plenum_vote_raw",
    is_list=True,
)
def votes(
    vote_id: Annotated[int | None, Field(description="Filter by vote ID")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number (via session join)")] = None,
    name: Annotated[str | None, Field(description="Vote title or subject contains text")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD). If to_date is omitted, filters to this single day.")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD). Requires from_date.")] = None,
    accepted: Annotated[bool | None, Field(description="True=accepted only, False=rejected only, null=both")] = None,
    bill_id: Annotated[int | None, Field(description="Filter to votes linked to a specific bill ID")] = None,
    full_details: Annotated[bool, Field(description="Include per-member breakdown with party and related votes")] = False,
    top: Annotated[int | None, Field(description="Max results (default 50, max 200). Results are sorted newest-first (date DESC) or by count DESC for count_by — so top=N gives the N most recent or highest.")] = None,
    offset: Annotated[int | None, Field(description="Results to skip for pagination. To get the oldest/smallest N: use offset=total_count-N (total_count is in every response).")] = None,
    count_by: Annotated[Literal["all", "bill", "knesset_num"] | None, Field(description='Group and count results. "all" returns only total_count (no items). Other values group by field (sorted by count DESC).')] = None,
) -> VotesResults:
    """Search for plenum votes or get full detail for a single vote.

    Filters (all ANDed):
      - knesset_num: Knesset number (derived via session join)
      - name: vote title or subject contains text
      - from_date / to_date: vote date filter (single day or range)
      - accepted: True=accepted only, False=rejected only, None=both
      - bill_id: filter to votes linked to a specific bill (via ItemID)

    Returns results sorted by (date DESC, time DESC, vote_id DESC).
    """
    normalized = normalize_inputs(locals())
    vote_id = normalized["vote_id"]
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    accepted = normalized["accepted"]
    bill_id = normalized["bill_id"]
    full_details = normalized["full_details"]
    top, offset = resolve_pagination(normalized["top"], normalized["offset"])

    conn = connect_readonly()
    cursor = conn.cursor()

    # Build simple WHERE conditions (used for count, count_by, and base of main query)
    count_conditions = []
    count_params = []
    if vote_id is not None:
        count_conditions.append("v.Id = %s")
        count_params.append(vote_id)
    if bill_id is not None:
        count_conditions.append("v.ItemID = %s")
        count_params.append(bill_id)
    if knesset_num is not None:
        count_conditions.append("s.KnessetNum = %s")
        count_params.append(knesset_num)
    if name:
        count_conditions.append(fuzzy_condition_or("v.VoteTitle", "v.VoteSubject"))
        count_params.extend(fuzzy_params_or(name))
    if from_date and to_date:
        count_conditions.append("v.VoteDateTime >= %s AND v.VoteDateTime <= %s")
        count_params.extend([from_date, to_date + "T99"])
    elif from_date:
        count_conditions.append("v.VoteDateTime >= %s AND v.VoteDateTime <= %s")
        count_params.extend([from_date, from_date + "T99"])
    count_where = " AND ".join(count_conditions) if count_conditions else "1=1"
    count_sql = f"SELECT COUNT(*) FROM {_CB_BASE_FROM} {_CB_BASE_JOINS} WHERE {count_where}"

    count_by_val = normalized.get("count_by")
    if count_by_val:
        if count_by_val == "all":
            total_count = check_search_count(cursor, count_sql, count_params, paginated=True)
            conn.close()
            return VotesResults(total_count=total_count, items=[], counts=[])
        config = _COUNT_BY_OPTIONS.get(count_by_val)
        if config is None:
            raise ValueError(f"count_by must be one of: {', '.join(_COUNT_BY_OPTIONS)}")
        groups_count_sql, group_sql = build_count_by_query(
            base_from=_CB_BASE_FROM, base_joins=_CB_BASE_JOINS, where=count_where, config=config,
        )
        total_count = check_search_count(cursor, groups_count_sql, count_params, paginated=True)
        cursor.execute(group_sql, count_params + [top, offset])
        counts = [CountItem(id=row.get("id"), value=row.get("value"), count=row["count"])
                  for row in cursor.fetchall()]
        conn.close()
        return VotesResults(total_count=total_count, items=[], counts=counts)

    total_count = check_search_count(cursor, count_sql, count_params, entity_name="votes", paginated=True)

    # Main query with computed totals for OData-origin votes
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

    if vote_id is not None:
        sql += " AND v.Id = %s"
        params.append(vote_id)

    if bill_id is not None:
        sql += " AND v.ItemID = %s"
        params.append(bill_id)

    if knesset_num is not None:
        sql += " AND s.KnessetNum = %s"
        params.append(knesset_num)

    if name:
        sql += f" AND {fuzzy_condition_or('v.VoteTitle', 'v.VoteSubject')}"
        params.extend(fuzzy_params_or(name))

    if accepted is not None:
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

    if from_date and to_date:
        sql += " AND v.VoteDateTime >= %s AND v.VoteDateTime <= %s"
        params.extend([from_date, to_date + "T99"])
    elif from_date:
        sql += " AND v.VoteDateTime >= %s AND v.VoteDateTime <= %s"
        params.extend([from_date, from_date + "T99"])

    sql += " ORDER BY v.VoteDateTime DESC, v.Id DESC"
    sql += " LIMIT %s OFFSET %s"
    params.extend([top, offset])
    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for vote in rows:
        total_for, total_against, total_abstain, is_accepted = _build_vote_result(vote)

        partial_kwargs = dict(
            vote_id=vote["id"],
            bill_id=vote["_bill_id"],
            knesset_num=vote["knessetnum"],
            session_id=vote["sessionid"],
            title=vote["votetitle"],
            subject=vote["votesubject"],
            date=simple_date(vote["votedatetime"]) or None,
            time=simple_time(vote["votedatetime"]) or None,
            is_accepted=bool(is_accepted) if is_accepted is not None else None,
            total_for=total_for,
            total_against=total_against,
            total_abstain=total_abstain,
            for_option=vote["foroptiondesc"],
            against_option=vote["againstoptiondesc"],
            vote_method=vote["votemethoddesc"],
        )

        if full_details:
            members = _fetch_members(cursor, vote["id"], vote["knessetnum"])
            related_votes = _fetch_related_votes(
                cursor, vote["id"], vote["votetitle"], vote["sessionid"],
            )
            results.append(VoteResultFull(
                **partial_kwargs,
                members=members,
                related_votes=related_votes,
            ))
        else:
            results.append(VoteResultPartial(**partial_kwargs))

    conn.close()
    return VotesResults(total_count=total_count, items=results)


votes.OUTPUT_MODEL = VotesResults
