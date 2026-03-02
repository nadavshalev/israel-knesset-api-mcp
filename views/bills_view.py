"""Bills list view — returns summary data for multiple bills (no stages/votes).

Use this view when searching/filtering bills.  For full detail on a single
bill (including plenum stages and votes), use ``bill_view.get_bill()``.
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

def search_bills(
    knesset_num=None,
    name=None,
    status=None,
    sub_type=None,
    from_date=None,
    to_date=None,
    date=None,
) -> list:
    """Search for bills and return summary metadata (no stages/votes).

    Filters (all ANDed):
      - knesset_num: bill's Knesset number
      - name: bill name contains text
      - status: bill's current status description contains text
      - sub_type: bill sub-type (פרטית/ממשלתית/ועדה)
      - from_date / to_date / date: appeared in a plenum session in date range

    Returns a list of bill summary dicts sorted by (knesset_num, name).
    """
    conn = sqlite3.connect(DEFAULT_DB)
    conn.row_factory = sqlite3.Row
    ensure_indexes(conn)
    cursor = conn.cursor()

    sql = """
    SELECT b.Id, b.Name, b.KnessetNum, b.SubTypeDesc,
           st.[Desc] AS StatusDesc, c.Name AS CommitteeName,
           b.PublicationDate, b.PublicationSeriesDesc, b.SummaryLaw
    FROM bill_raw b
    LEFT JOIN status_raw st ON b.StatusID = st.Id
    LEFT JOIN committee_raw c ON b.CommitteeID = c.Id
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND b.KnessetNum = ?"
        params.append(knesset_num)

    if name:
        sql += " AND b.Name LIKE ?"
        params.append(f"%{name}%")

    if sub_type:
        sql += " AND b.SubTypeDesc LIKE ?"
        params.append(f"%{sub_type}%")

    if status:
        sql += " AND st.[Desc] LIKE ?"
        params.append(f"%{status}%")

    # Plenum-stage date filters
    stage_conditions = []
    stage_params = []

    if from_date:
        stage_conditions.append("s.StartDate >= ?")
        stage_params.append(from_date)

    if to_date:
        stage_conditions.append("s.StartDate <= ?")
        stage_params.append(to_date)

    if date:
        stage_conditions.append("s.StartDate LIKE ?")
        stage_params.append(f"{date}%")

    if stage_conditions:
        cond_str = " AND ".join(stage_conditions)
        sql += f"""
        AND EXISTS (
            SELECT 1 FROM plm_session_item_raw i
            JOIN plenum_session_raw s ON s.Id = i.PlenumSessionID
            WHERE i.ItemID = b.Id
              AND {cond_str}
        )"""
        params.extend(stage_params)

    sql += " ORDER BY b.KnessetNum, b.Name"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "bill_id": row["Id"],
            "name": row["Name"],
            "knesset_num": row["KnessetNum"],
            "sub_type": row["SubTypeDesc"],
            "status": row["StatusDesc"],
            "committee": row["CommitteeName"],
            "publication_date": _simple_date(row["PublicationDate"]),
            "publication_series": row["PublicationSeriesDesc"],
            "summary": row["SummaryLaw"],
        })

    conn.close()
    return results
