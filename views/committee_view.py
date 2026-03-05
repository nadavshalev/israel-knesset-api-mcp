"""Single committee detail view — returns metadata for one committee by ID.

Opt-in flags control which related data is included:
  - ``include_sessions``  — committee sessions (filtered by date range)
  - ``include_members``   — members who served on the committee
  - ``include_bills``     — bills discussed in committee sessions
  - ``include_documents`` — documents from committee sessions

Date filtering via ``from_date``/``to_date`` (or ``date`` shortcut) narrows
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

from core.db import connect_readonly


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string."""
    if not date_str:
        return ""
    s = str(date_str)
    if "T" in s:
        return s.split("T")[0]
    if " " in s:
        return s.split(" ")[0]
    return s


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
    if " " in s:
        time_part = s.split(" ")[1]
        return time_part[:5]
    return ""


def _date_clauses(from_date, to_date, date_column="cs.StartDate"):
    """Build WHERE fragments and params for date filtering.

    Returns ``(fragments, params)`` where *fragments* is a list of SQL
    strings (each starting with ``AND``) and *params* is a list of values.
    """
    frags: list[str] = []
    params: list[str] = []
    if from_date:
        frags.append(f"AND {date_column} >= ?")
        params.append(from_date)
    if to_date:
        frags.append(f"AND {date_column} < date(?, '+1 day')")
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
        WHERE CommitteeID = ?
        {' '.join(date_frags)}
        ORDER BY StartDate DESC, Id DESC
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        {
            "session_id": row["Id"],
            "number": row["Number"],
            "date": _simple_date(row["StartDate"]),
            "start_time": _simple_time(row["StartDate"]),
            "end_time": _simple_time(row["FinishDate"]),
            "type": row["TypeDesc"],
            "status": row["StatusDesc"],
            "location": row["Location"],
            "url": row["SessionUrl"],
            "broadcast_url": row["BroadcastUrl"],
        }
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
            "OR ptp.FinishDate >= ?)"
        )
        overlap_params.append(from_date)
    if to_date:
        # Member must have started before the window ends
        overlap_frags.append(
            "AND (ptp.StartDate IS NULL OR ptp.StartDate = '' "
            "OR ptp.StartDate <= ?)"
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
        WHERE ptp.CommitteeID = ?
        {' '.join(overlap_frags)}
        ORDER BY ptp.KnessetNum, p.LastName, p.FirstName, ptp.StartDate
    """
    cursor.execute(sql, [committee_id] + overlap_params)
    rows = cursor.fetchall()
    return [
        {
            "member_id": row["PersonID"],
            "name": f"{row['FirstName']} {row['LastName']}",
            "knesset_num": row["KnessetNum"],
            "role": row["PositionTitle"],
            "start": _simple_date(row["StartDate"]),
            "end": _simple_date(row["FinishDate"]),
        }
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
               st.[Desc] AS StatusDesc
        FROM cmt_session_item_raw csi
        JOIN committee_session_raw cs ON csi.CommitteeSessionID = cs.Id
        JOIN bill_raw b ON csi.ItemID = b.Id
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE cs.CommitteeID = ? AND csi.ItemTypeID = 2
        {' '.join(date_frags)}
        ORDER BY b.Name
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        {
            "bill_id": row["Id"],
            "name": row["Name"],
            "knesset_num": row["KnessetNum"],
            "sub_type": row["SubTypeDesc"],
            "status": row["StatusDesc"],
        }
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
        WHERE cs.CommitteeID = ?
        {' '.join(date_frags)}
        ORDER BY cs.StartDate DESC, d.Id DESC
    """
    cursor.execute(sql, [committee_id] + date_params)
    rows = cursor.fetchall()
    return [
        {
            "document_id": row["Id"],
            "type": row["GroupTypeDesc"],
            "name": row["DocumentName"],
            "format": row["ApplicationDesc"],
            "file_path": row["FilePath"],
            "session_id": row["session_id"],
            "session_date": _simple_date(row["SessionDate"]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_committee(
    committee_id: int,
    knesset_num: int | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    include_sessions: bool = False,
    include_members: bool = False,
    include_bills: bool = False,
    include_documents: bool = False,
) -> dict | None:
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
        Single-date shortcut — sets both *from_date* and *to_date*.
    ``from_date`` / ``to_date``
        Inclusive date range (YYYY-MM-DD).
    ``knesset_num``
        Passed through for informational context (not used for filtering).
    """
    # Normalise date shortcut
    if date:
        from_date = from_date or date
        to_date = to_date or date

    conn = connect_readonly()
    cursor = conn.cursor()

    # Committee metadata
    cursor.execute("SELECT * FROM committee_raw WHERE Id = ?", (committee_id,))
    committee = cursor.fetchone()
    if not committee:
        conn.close()
        return None

    obj: dict = {
        "committee_id": committee["Id"],
        "name": committee["Name"],
        "knesset_num": committee["KnessetNum"],
        "type": committee["CommitteeTypeDesc"],
        "category": committee["CategoryDesc"],
        "is_current": bool(committee["IsCurrent"]),
        "start_date": _simple_date(committee["StartDate"]),
        "end_date": _simple_date(committee["FinishDate"]),
        "parent_committee_id": committee["ParentCommitteeID"],
        "parent_committee_name": committee["CommitteeParentName"],
        "email": committee["Email"],
    }

    if include_sessions:
        sessions = _get_sessions(cursor, committee_id, from_date, to_date)
        obj["sessions"] = sessions
        obj["session_count"] = len(sessions)

    if include_members:
        members = _get_members(cursor, committee_id, from_date, to_date)
        obj["members"] = members
        obj["member_count"] = len(members)

    if include_bills:
        bills = _get_bills(cursor, committee_id, from_date, to_date)
        obj["bills"] = bills
        obj["bill_count"] = len(bills)

    if include_documents:
        documents = _get_documents(cursor, committee_id, from_date, to_date)
        obj["documents"] = documents
        obj["document_count"] = len(documents)

    conn.close()
    return obj
