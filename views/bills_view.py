"""Bills list view — returns summary data for multiple bills (no stages/votes).

Use this view when searching/filtering bills.  For full detail on a single
bill (including plenum stages and votes), use ``bill_view.get_bill()``.
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
from core.search_meta import register_search

register_search({
    "entity_key": "bills",
    "count_sql": """
        SELECT COUNT(*) FROM bill_raw
        WHERE Name LIKE %s
    """,
    "search_sql": """
        SELECT b.Id AS id, b.Name AS name, b.KnessetNum AS knesset_num,
               b.SubTypeDesc AS sub_type,
               st."Desc" AS status
        FROM bill_raw b
        LEFT JOIN status_raw st ON b.StatusID = st.Id
        WHERE b.Name LIKE %s
        ORDER BY b.Id DESC
        LIMIT %s
    """,
    "param_count": 1,
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_bills",
    description=(
        "Search for Knesset bills (legislation). Returns summary info: "
        "name, knesset number, type, status, committee, publication. "
        "Use get_bill for full detail including plenum stages and votes."
    ),
    entity="Bills",
    count_sql="SELECT COUNT(*) FROM bill_raw",
    is_list=True,
)
def search_bills(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name: Annotated[str | None, Field(description="Bill name contains text")] = None,
    status: Annotated[str | None, Field(description="Bill status description contains text")] = None,
    sub_type: Annotated[str | None, Field(description="Bill sub-type: פרטית (private), ממשלתית (government), ועדה (committee)")] = None,
    from_date: Annotated[str | None, Field(description="Start of date range (YYYY-MM-DD) for plenum appearance")] = None,
    to_date: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) for plenum appearance")] = None,
    date: Annotated[str | None, Field(description="Exact date (YYYY-MM-DD) for plenum appearance")] = None,
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
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    status = normalized["status"]
    sub_type = normalized["sub_type"]
    from_date = normalized["from_date"]
    to_date = normalized["to_date"]
    date = normalized["date"]

    conn = connect_readonly()
    cursor = conn.cursor()

    sql = """
    SELECT b.Id, b.Name, b.KnessetNum, b.SubTypeDesc,
           st."Desc" AS StatusDesc, b.CommitteeID, c.Name AS CommitteeName,
           b.PublicationDate, b.PublicationSeriesDesc, b.SummaryLaw
    FROM bill_raw b
    LEFT JOIN status_raw st ON b.StatusID = st.Id
    LEFT JOIN committee_raw c ON b.CommitteeID = c.Id
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND b.KnessetNum = %s"
        params.append(knesset_num)

    if name:
        sql += " AND b.Name LIKE %s"
        params.append(f"%{name}%")

    if sub_type:
        sql += " AND b.SubTypeDesc LIKE %s"
        params.append(f"%{sub_type}%")

    if status:
        sql += " AND st.\"Desc\" LIKE %s"
        params.append(f"%{status}%")

    # Plenum-stage date filters
    stage_conditions = []
    stage_params = []

    if from_date:
        stage_conditions.append("s.StartDate >= %s")
        stage_params.append(from_date)

    if to_date:
        stage_conditions.append("s.StartDate <= %s")
        stage_params.append(to_date)

    if date:
        stage_conditions.append("s.StartDate LIKE %s")
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

    sql += ' ORDER BY b.KnessetNum, b.Name COLLATE "C", b.Id'

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append({
            "bill_id": row["id"],
            "name": row["name"],
            "knesset_num": row["knessetnum"],
            "sub_type": row["subtypedesc"],
            "status": row["statusdesc"],
            "committee": row["committeename"],
            "committee_id": row["committeeid"],
            "publication_date": simple_date(row["publicationdate"]),
            "publication_series": row["publicationseriesdesc"],
            "summary": row["summarylaw"],
        })

    conn.close()
    return results
