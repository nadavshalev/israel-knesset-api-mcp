"""Single plenum session detail view — returns full data for one session by ID.

Includes session metadata, all items, and documents.
For searching/filtering multiple sessions, use
``plenum_sessions_view.search_sessions()``.
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_session(session_id: int) -> dict | None:
    """Return full detail for a single plenum session, or None if not found.

    Includes session metadata, all items, and documents.

    Args:
        session_id: The session ID (required).
    """
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
    ensure_indexes(conn)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM plenum_session_raw WHERE Id = ?",
        (session_id,),
    )
    session = cursor.fetchone()
    if not session:
        conn.close()
        return None

    # Fetch items sorted by Ordinal, with status description
    item_sql = """
        SELECT i.*, st.[Desc] AS StatusDesc
        FROM plm_session_item_raw i
        LEFT JOIN status_raw st ON i.StatusID = st.Id
        WHERE i.PlenumSessionID = ?
        ORDER BY i.Ordinal ASC
    """
    item_params = [session_id]

    cursor.execute(item_sql, item_params)
    item_rows = cursor.fetchall()

    # Always include documents for single-session detail
    cursor.execute(
        """
        SELECT * FROM document_plenum_session_raw
        WHERE PlenumSessionID = ?
        ORDER BY GroupTypeDesc, ApplicationDesc
        """,
        (session_id,),
    )
    doc_rows = cursor.fetchall()

    obj = {
        "session_id": session["Id"],
        "knesset_num": session["KnessetNum"],
        "name": session["Name"],
        "date": _simple_date(session["StartDate"]),
        "items": [
            {
                "item_id": item["ItemID"],
                "name": item["Name"],
                "type": item["ItemTypeDesc"],
                "status": item["StatusDesc"],
            }
            for item in item_rows
        ],
        "documents": [
            {
                "group_type": doc["GroupTypeDesc"],
                "application": doc["ApplicationDesc"],
                "file_path": doc["FilePath"],
            }
            for doc in doc_rows
        ],
    }

    conn.close()
    return obj
