"""Shared Pydantic models and helpers for session items and documents.

Used by both plenums and committees views.
"""

from __future__ import annotations

from pydantic import Field

from core.models import KNSBaseModel
from core.helpers import simple_date


class ItemVote(KNSBaseModel):
    """A plenum vote linked to a session item."""
    vote_id: int = Field(description="Vote ID")
    is_accepted: bool | None = Field(default=None, description="Whether accepted")
    total_for: int | None = Field(default=None, description="Votes for")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")


class SessionItem(KNSBaseModel):
    """An agenda item discussed in a session (plenum or committee)."""
    item_id: int | None = Field(default=None, description="Item ID")
    item_type: str | None = Field(default=None, description="Item type description")
    item_name: str | None = Field(default=None, description="Item name")
    item_status: str | None = Field(default=None, description="Item status description")
    bill_id: int | None = Field(default=None, description="Linked bill ID (for bill items)")
    votes: list[ItemVote] | None = Field(default=None, description="Plenum votes on this item (when available)")


class SessionDocument(KNSBaseModel):
    """A document attached to a session (plenum or committee)."""
    name: str | None = Field(default=None, description="Document group type")
    type: str | None = Field(default=None, description="File format")
    path: str | None = Field(default=None, description="File URL/path")


class StageVote(KNSBaseModel):
    """Final (decisive) vote for an item stage."""
    vote_id: int = Field(description="Vote ID")
    title: str | None = Field(default=None, description="Vote title")
    date: str | None = Field(default=None, description="Vote date (YYYY-MM-DD)")
    is_accepted: bool | None = Field(default=None, description="Whether accepted")
    total_for: int | None = Field(default=None, description="Votes for")
    total_against: int | None = Field(default=None, description="Votes against")
    total_abstain: int | None = Field(default=None, description="Abstentions")


class ItemStagePlenumSession(KNSBaseModel):
    """Partial plenum session info attached to an item stage."""
    session_id: int = Field(description="Plenum session ID")
    name: str | None = Field(default=None, description="Session name")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    knesset_num: int | None = Field(default=None, description="Knesset number")
    vote: StageVote | None = Field(default=None, description="Decisive vote at this stage (bills only)")


class ItemStageCmtSession(KNSBaseModel):
    """Partial committee session info attached to an item stage."""
    session_id: int = Field(description="Committee session ID")
    committee_id: int | None = Field(default=None, description="Committee ID")
    committee_name: str | None = Field(default=None, description="Committee name")
    date: str | None = Field(default=None, description="Session date (YYYY-MM-DD)")
    knesset_num: int | None = Field(default=None, description="Knesset number")


class ItemStage(KNSBaseModel):
    """A stage (appearance) of an item in a session."""
    status: str | None = Field(default=None, description="Stage status description")
    plenum_session: ItemStagePlenumSession | None = Field(default=None, description="Plenum session (if discussed in plenum)")
    committee_session: ItemStageCmtSession | None = Field(default=None, description="Committee session (if discussed in committee)")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_item_votes(cursor, bill_id):
    """Return all plenum votes for a bill, newest first.

    Links via ``plenum_vote_raw.ItemID = bill_id``.  When stored totals
    are missing, computes them from per-MK results.
    """
    cursor.execute(
        """
        SELECT v.Id, v.VoteDateTime, v.IsAccepted,
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
            is_accepted=bool(is_accepted) if is_accepted is not None else None,
            total_for=total_for,
            total_against=total_against,
            total_abstain=total_abstain,
        ))
    return votes or None


def build_session_date_exists(item_table_alias, item_id_col, item_type_ids,
                              from_date, to_date):
    """Build an EXISTS clause filtering items by session date (plenum + committee).

    Returns (condition_sql, params) or (None, []) if no date filter needed.
    """
    if not from_date:
        return None, []

    type_placeholders = ",".join(["%s"] * len(item_type_ids))

    if to_date:
        date_cond = "s.StartDate >= %s AND s.StartDate <= %s"
        date_params = [from_date, to_date + "T99"]
    else:
        date_cond = "s.StartDate LIKE %s"
        date_params = [f"{from_date}%"]

    sql = f"""(
        EXISTS (
            SELECT 1 FROM plm_session_item_raw i
            JOIN plenum_session_raw s ON s.Id = i.PlenumSessionID
            WHERE i.ItemID = {item_id_col}
              AND i.ItemTypeID IN ({type_placeholders})
              AND {date_cond}
        )
        OR EXISTS (
            SELECT 1 FROM cmt_session_item_raw ci
            JOIN committee_session_raw cs ON cs.Id = ci.CommitteeSessionID
            WHERE ci.ItemID = {item_id_col}
              AND ci.ItemTypeID IN ({type_placeholders})
              AND {date_cond.replace('s.', 'cs.')}
        )
    )"""
    params = list(item_type_ids) + date_params + list(item_type_ids) + date_params
    return sql, params


def fetch_item_stages(cursor, item_id, item_type_ids):
    """Fetch all stages (plenum + committee appearances) for an item, deduplicated.

    Uses DISTINCT ON to deduplicate duplicate rows in session_item tables for
    the same (item, session) pair, keeping the row with the highest item_pk.

    Returns a list of ``ItemStage`` objects sorted by date ASC, or None.
    """
    type_placeholders = ",".join(["%s"] * len(item_type_ids))

    # Plenum stages — DISTINCT ON (session_id) keeps highest item_pk per session
    cursor.execute(
        f"""
        SELECT DISTINCT ON (s.Id)
               i.Id AS item_pk, st."Desc" AS status_desc,
               s.Id AS session_id, s.Name AS session_name,
               s.StartDate, s.KnessetNum
        FROM plm_session_item_raw i
        JOIN plenum_session_raw s ON i.PlenumSessionID = s.Id
        LEFT JOIN status_raw st ON i.StatusID = st.Id
        WHERE i.ItemID = %s AND i.ItemTypeID IN ({type_placeholders})
        ORDER BY s.Id, i.Id DESC
        """,
        [item_id] + list(item_type_ids),
    )
    plm_rows = cursor.fetchall()

    # Committee stages — DISTINCT ON (cs.Id) keeps highest item_pk per session
    cursor.execute(
        f"""
        SELECT DISTINCT ON (cs.Id)
               ci.Id AS item_pk, st."Desc" AS status_desc,
               cs.Id AS session_id, cs.CommitteeID,
               c.Name AS committee_name,
               cs.StartDate, cs.KnessetNum
        FROM cmt_session_item_raw ci
        JOIN committee_session_raw cs ON ci.CommitteeSessionID = cs.Id
        LEFT JOIN committee_raw c ON cs.CommitteeID = c.Id
        LEFT JOIN status_raw st ON ci.StatusID = st.Id
        WHERE ci.ItemID = %s AND ci.ItemTypeID IN ({type_placeholders})
        ORDER BY cs.Id, ci.Id DESC
        """,
        [item_id] + list(item_type_ids),
    )
    cmt_rows = cursor.fetchall()

    stages = []
    for row in plm_rows:
        stages.append((
            simple_date(row["startdate"]) or "",
            row["item_pk"],
            ItemStage(
                status=row["status_desc"],
                plenum_session=ItemStagePlenumSession(
                    session_id=row["session_id"],
                    name=row["session_name"],
                    date=simple_date(row["startdate"]) or None,
                    knesset_num=row["knessetnum"],
                ),
            ),
        ))

    for row in cmt_rows:
        stages.append((
            simple_date(row["startdate"]) or "",
            row["item_pk"],
            ItemStage(
                status=row["status_desc"],
                committee_session=ItemStageCmtSession(
                    session_id=row["session_id"],
                    committee_id=row["committeeid"],
                    committee_name=row["committee_name"],
                    date=simple_date(row["startdate"]) or None,
                    knesset_num=row["knessetnum"],
                ),
            ),
        ))

    stages.sort(key=lambda t: (t[0], t[1]))
    return [s[2] for s in stages] or None
