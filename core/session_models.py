"""Shared Pydantic models and helpers for session items and documents.

Used by both plenum_sessions and committee_sessions views.
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
