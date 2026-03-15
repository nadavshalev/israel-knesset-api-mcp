"""Single committee detail view — returns metadata for one committee by ID.

Opt-in flags control which related data is included:
  - ``include_sessions``  — committee sessions (filtered by date range)
  - ``include_members``   — members who served on the committee
  - ``include_bills``     — bills discussed in committee sessions
  - ``include_documents`` — documents from committee sessions

Date filtering via ``date`` (or ``date``/``date_to`` range) narrows
sessions, bills, documents, and member overlap to the given window.

For searching/filtering multiple committees, use
``committees_view.search_committees()``.
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
from core.helpers import simple_date, simple_time, format_person_name, normalize_inputs
from core.mcp_meta import mcp_tool
from origins.committees.get_committee_models import (
    CommitteeSession, CommitteeMember, CommitteeBill,
    CommitteeDocument, CommitteeDetail,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _date_clauses(from_date, to_date, date_column="cs.StartDate"):
    """Build WHERE fragments and params for date filtering.

    Returns ``(fragments, params)`` where *fragments* is a list of SQL
    strings (each starting with ``AND``) and *params* is a list of values.
    """
    frags: list[str] = []
    params: list[str] = []
    if from_date:
        frags.append(f"AND {date_column} >= %s")
        params.append(from_date)
    if to_date:
        frags.append(f"AND {date_column} < (%s::date + INTERVAL '1 day')::text")
        params.append(to_date)
    return frags, params


# ---------------------------------------------------------------------------
# Detail builders
# ---------------------------------------------------------------------------

def _get_sessions(cursor, committee_id, from_date=None, to_date=None):
    """Return committee sessions, newest first, optionally filtered by date."""
    date_frags, date_params = _date_clauses(from_date, to_date,
                                            date_column="StartDate")
    sql = f"""
        SELECT Id, Number, StartDate, FinishDate, TypeDesc, StatusDesc,
               Location, SessionUrl, BroadcastUrl
        FROM committee_session_raw
        WHERE CommitteeID = %s
        {' '.join(date_frags)}
        ORDER BY StartDate DESC, Id DESC
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        CommitteeSession(
            session_id=row["id"],
            number=row["number"],
            date=simple_date(row["startdate"]) or None,
            start_time=simple_time(row["startdate"]) or None,
            end_time=simple_time(row["finishdate"]) or None,
            type=row["typedesc"] or None,
            status=row["statusdesc"] or None,
            location=row["location"] or None,
            url=row["sessionurl"] or None,
            broadcast_url=row["broadcasturl"] or None,
        )
        for row in rows
    ]


def _get_members(cursor, committee_id, from_date=None, to_date=None):
    """Return members who served on this committee, optionally filtered.

    When date filters are active, only members whose tenure overlaps with
    the [from_date, to_date] window are returned.
    """
    overlap_frags: list[str] = []
    overlap_params: list[str] = []
    if from_date:
        # Member must not have finished before the window starts
        overlap_frags.append(
            "AND (ptp.FinishDate IS NULL OR ptp.FinishDate = '' "
            "OR ptp.FinishDate >= %s)"
        )
        overlap_params.append(from_date)
    if to_date:
        # Member must have started before the window ends
        overlap_frags.append(
            "AND (ptp.StartDate IS NULL OR ptp.StartDate = '' "
            "OR ptp.StartDate <= %s)"
        )
        overlap_params.append(to_date)

    sql = f"""
        SELECT DISTINCT ptp.PersonID, p.FirstName, p.LastName,
               ptp.KnessetNum,
               pos.Description AS PositionTitle,
               ptp.StartDate, ptp.FinishDate
        FROM person_to_position_raw ptp
        JOIN person_raw p ON ptp.PersonID = p.PersonID
        LEFT JOIN position_raw pos ON ptp.PositionID = pos.Id
        WHERE ptp.CommitteeID = %s
        {' '.join(overlap_frags)}
        ORDER BY ptp.KnessetNum, p.LastName, p.FirstName, ptp.StartDate
    """
    cursor.execute(sql, [committee_id] + overlap_params)
    rows = cursor.fetchall()
    return [
        CommitteeMember(
            member_id=row["personid"],
            name=format_person_name(row['firstname'], row['lastname']),
            knesset_num=row["knessetnum"],
            role=row["positiontitle"] or None,
            start=simple_date(row["startdate"]) or None,
            end=simple_date(row["finishdate"]) or None,
        )
        for row in rows
    ]


def _get_bills(cursor, committee_id, from_date=None, to_date=None):
    """Return bills discussed in this committee's sessions.

    Links via cmt_session_item_raw (ItemTypeID=2) -> bill_raw.
    When date filters are active, only bills from sessions within the
    date range are returned.
    """
    date_frags, date_params = _date_clauses(from_date, to_date)
    sql = f"""
        SELECT DISTINCT b.Id, b.Name, b.KnessetNum, b.SubTypeDesc,
               st."Desc" AS StatusDesc
        FROM cmt_session_item_raw csi
        JOIN committee_session_raw cs ON csi.CommitteeSessionID = cs.Id
        JOIN bill_raw b ON csi.ItemID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE cs.CommitteeID = %s AND csi.ItemTypeID = 2
        {' '.join(date_frags)}
        ORDER BY b.Name
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        CommitteeBill(
            bill_id=row["id"],
            name=row["name"] or None,
            knesset_num=row["knessetnum"],
            sub_type=row["subtypedesc"] or None,
            status=row["statusdesc"] or None,
        )
        for row in rows
    ]


def _get_documents(cursor, committee_id, from_date=None, to_date=None):
    """Return documents from this committee's sessions, newest first.

    When date filters are active, only documents from sessions within
    the date range are returned.
    """
    date_frags, date_params = _date_clauses(from_date, to_date)
    sql = f"""
        SELECT d.Id, d.GroupTypeDesc, d.DocumentName, d.ApplicationDesc,
               d.FilePath, cs.Id AS session_id, cs.StartDate AS SessionDate
        FROM document_committee_session_raw d
        JOIN committee_session_raw cs ON d.CommitteeSessionID = cs.Id
        WHERE cs.CommitteeID = %s
        {' '.join(date_frags)}
        ORDER BY cs.StartDate DESC, d.Id DESC
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        CommitteeDocument(
            document_id=row["id"],
            type=row["grouptypedesc"] or None,
            name=row["documentname"] or None,
            format=row["applicationdesc"] or None,
            file_path=row["filepath"] or None,
            session_id=row["session_id"],
            session_date=simple_date(row["sessiondate"]) or None,
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_committee",
    description=(
        "Get full detail for a single committee by ID. Always returns "
        "committee metadata. Use opt-in flags to include sessions, "
        "members, bills, or documents. Date filters narrow the included "
        "data to a time window."
    ),
    entity="Committees",
    is_list=False,
)
def get_committee(
    committee_id: Annotated[int, Field(description="The committee ID (required)")],
    knesset_num: Annotated[int | None, Field(description="Knesset number (informational context)")] = None,
    date: Annotated[str | None, Field(description="Single date or start of range (YYYY-MM-DD) for sessions/bills/documents")] = None,
    date_to: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) for sessions/bills/documents; use with date for a range")] = None,
    include_sessions: Annotated[bool, Field(description="Include committee sessions")] = False,
    include_members: Annotated[bool, Field(description="Include members who served on the committee")] = False,
    include_bills: Annotated[bool, Field(description="Include bills discussed in committee sessions")] = False,
    include_documents: Annotated[bool, Field(description="Include documents from committee sessions")] = False,
) -> CommitteeDetail | None:
    """Return detail for a single committee, or ``None`` if not found.

    Always returns committee metadata.  Related data is opt-in:

    ``include_sessions``
        Include committee sessions (newest first).
    ``include_members``
        Include members who served on the committee.
    ``include_bills``
        Include bills discussed in committee sessions.
    ``include_documents``
        Include documents from committee sessions.

    Date filtering (applied to sessions, bills, documents, member overlap):

    ``date``
        Single date or start of date range (YYYY-MM-DD).
    ``date_to``
        End of date range (YYYY-MM-DD).  When provided together with
        ``date``, forms an inclusive range.
    ``knesset_num``
        Passed through for informational context (not used for filtering).
    """
    normalized = normalize_inputs(locals())
    committee_id = normalized["committee_id"]
    knesset_num = normalized["knesset_num"]
    date = normalized["date"]
    date_to = normalized["date_to"]
    include_sessions = normalized["include_sessions"]
    include_members = normalized["include_members"]
    include_bills = normalized["include_bills"]
    include_documents = normalized["include_documents"]

    # Resolve effective from/to for internal date filtering
    if date and date_to:
        eff_from = date
        eff_to = date_to
    elif date:
        eff_from = date
        eff_to = date
    else:
        eff_from = None
        eff_to = None

    conn = connect_readonly()
    cursor = conn.cursor()

    # Committee metadata
    cursor.execute("SELECT * FROM committee_raw WHERE Id = %s", (committee_id,))
    committee = cursor.fetchone()
    if not committee:
        conn.close()
        return None

    obj = CommitteeDetail(
        committee_id=committee["id"],
        name=committee["name"] or None,
        knesset_num=committee["knessetnum"],
        type=committee["committeetypedesc"] or None,
        category=committee["categorydesc"] or None,
        is_current=bool(committee["iscurrent"]),
        start_date=simple_date(committee["startdate"]) or None,
        end_date=simple_date(committee["finishdate"]) or None,
        parent_committee_id=committee["parentcommitteeid"],
        parent_committee_name=committee["committeeparentname"] or None,
        email=committee["email"] or None,
    )

    if include_sessions:
        sessions = _get_sessions(cursor, committee_id, eff_from, eff_to)
        obj.sessions = sessions
        obj.session_count = len(sessions)

    if include_members:
        members = _get_members(cursor, committee_id, eff_from, eff_to)
        obj.members = members
        obj.member_count = len(members)

    if include_bills:
        bills = _get_bills(cursor, committee_id, eff_from, eff_to)
        obj.bills = bills
        obj.bill_count = len(bills)

    if include_documents:
        documents = _get_documents(cursor, committee_id, eff_from, eff_to)
        obj.documents = documents
        obj.document_count = len(documents)

    conn.close()
    return obj


get_committee.OUTPUT_MODEL = CommitteeDetail
