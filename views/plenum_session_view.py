"""Single plenum session detail view — returns full data for one session by ID.

Includes session metadata, all items, and documents.
For searching/filtering multiple sessions, use
``plenum_sessions_view.search_sessions()``.
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
from core.helpers import simple_date, normalize_inputs
from core.mcp_meta import mcp_tool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_plenum",
    description=(
        "Get full detail for a single plenum session by ID. Includes "
        "session metadata, all agenda items, and documents."
    ),
    entity="Plenum Sessions",
    is_list=False,
)
def get_session(
    session_id: Annotated[int, Field(description="The plenum session ID (required)")],
) -> dict | None:
    """Return full detail for a single plenum session, or None if not found.

    Includes session metadata, all items, and documents.

    Args:
        session_id: The session ID (required).
    """
    normalized = normalize_inputs(locals())
    session_id = normalized["session_id"]

    conn = connect_readonly()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM plenum_session_raw WHERE Id = %s",
        (session_id,),
    )
    session = cursor.fetchone()
    if not session:
        conn.close()
        return None

    # Fetch items sorted by Ordinal, with status description
    item_sql = """
        SELECT i.*, st."Desc" AS StatusDesc
        FROM plm_session_item_raw i
        LEFT JOIN status_raw st ON i.StatusID = st.Id
        WHERE i.PlenumSessionID = %s
        ORDER BY i.Ordinal ASC
    """
    item_params = [session_id]

    cursor.execute(item_sql, item_params)
    item_rows = cursor.fetchall()

    # Always include documents for single-session detail
    cursor.execute(
        """
        SELECT * FROM document_plenum_session_raw
        WHERE PlenumSessionID = %s
        ORDER BY GroupTypeDesc, ApplicationDesc
        """,
        (session_id,),
    )
    doc_rows = cursor.fetchall()

    obj = {
        "session_id": session["id"],
        "knesset_num": session["knessetnum"],
        "name": session["name"],
        "date": simple_date(session["startdate"]),
        "items": [
            {
                "item_id": item["itemid"],
                "name": item["name"],
                "type": item["itemtypedesc"],
                "status": item["statusdesc"],
            }
            for item in item_rows
        ],
        "documents": [
            {
                "group_type": doc["grouptypedesc"],
                "application": doc["applicationdesc"],
                "file_path": doc["filepath"],
            }
            for doc in doc_rows
        ],
    }

    conn.close()
    return obj
