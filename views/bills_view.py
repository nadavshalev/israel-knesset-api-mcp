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
from core.helpers import simple_date, normalize_inputs, _clean
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
    most_recent_date_sql="SELECT MAX(PublicationDate) FROM bill_raw",
    enum_sql={
        "status": 'SELECT DISTINCT "Desc" FROM status_raw ORDER BY "Desc"',
        "sub_type": "SELECT DISTINCT SubTypeDesc FROM bill_raw WHERE SubTypeDesc IS NOT NULL ORDER BY SubTypeDesc",
    },
    is_list=True,
)
def search_bills(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
    name: Annotated[str | None, Field(description="Bill name contains text")] = None,
    status: Annotated[str | None, Field(description="Bill status")] = None,
    sub_type: Annotated[str | None, Field(description="Bill sub-type")] = None,
    initiator_id: Annotated[int | None, Field(description="Filter by initiator's member/person ID")] = None,
    date: Annotated[str | None, Field(description="Single date or start of range (YYYY-MM-DD) for plenum appearance")] = None,
    date_to: Annotated[str | None, Field(description="End of date range (YYYY-MM-DD) for plenum appearance; use with date for a range")] = None,
) -> list:
    """Search for bills and return summary metadata (no stages/votes).

    Filters (all ANDed):
      - knesset_num: bill's Knesset number
      - name: bill name contains text
      - status: bill's current status description contains text
      - sub_type: bill sub-type (פרטית/ממשלתית/ועדה)
      - initiator_id: member/person ID who initiated the bill
      - date / date_to: appeared in a plenum session in date range

    Returns a list of bill summary dicts sorted by (publication_date DESC, bill_id DESC).
    """
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]
    name = normalized["name"]
    status = normalized["status"]
    sub_type = normalized["sub_type"]
    initiator_id = normalized["initiator_id"]
    date = normalized["date"]
    date_to = normalized["date_to"]

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

    if initiator_id is not None:
        sql += """
        AND EXISTS (
            SELECT 1 FROM bill_initiator_raw bi
            WHERE bi.BillID = b.Id AND bi.PersonID = %s AND bi.IsInitiator = 1
        )"""
        params.append(initiator_id)

    # Plenum-stage date filters
    stage_conditions = []
    stage_params = []

    if date and date_to:
        # Date range
        stage_conditions.append("s.StartDate >= %s")
        stage_params.append(date)
        stage_conditions.append("s.StartDate <= %s")
        stage_params.append(date_to)
    elif date:
        # Single day
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

    sql += ' ORDER BY b.PublicationDate DESC, b.Id DESC'

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    # Collect bill IDs and fetch primary initiators in batch
    bill_ids = [row["id"] for row in rows]
    # Map knesset_num per bill for faction lookup
    knesset_by_bill = {row["id"]: row["knessetnum"] for row in rows}
    initiators_by_bill: dict = {}
    if bill_ids:
        placeholders = ",".join(["%s"] * len(bill_ids))
        cursor.execute(
            f"""
            SELECT bi.BillID, bi.PersonID,
                   p.FirstName || ' ' || p.LastName AS full_name,
                   b.KnessetNum,
                   ptp.FactionName
            FROM bill_initiator_raw bi
            JOIN person_raw p ON bi.PersonID = p.PersonID
            JOIN bill_raw b ON bi.BillID = b.Id
            LEFT JOIN LATERAL (
                SELECT ptp2.FactionName
                FROM person_to_position_raw ptp2
                WHERE ptp2.PersonID = bi.PersonID
                  AND ptp2.KnessetNum = b.KnessetNum
                  AND ptp2.FactionName IS NOT NULL
                  AND ptp2.FactionName != ''
                ORDER BY ptp2.IsCurrent DESC, ptp2.PersonToPositionID DESC
                LIMIT 1
            ) ptp ON true
            WHERE bi.IsInitiator = 1 AND bi.BillID IN ({placeholders})
            ORDER BY bi.Ordinal ASC
            """,
            bill_ids,
        )
        for irow in cursor.fetchall():
            name = irow["full_name"]
            if irow["factionname"]:
                name = f"{name} ({irow['factionname']})"
            initiators_by_bill.setdefault(irow["billid"], []).append(name)

    results = []
    for row in rows:
        result = {
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
        }
        initiators = initiators_by_bill.get(row["id"], [])
        if initiators:
            result["primary_initiators"] = initiators
        results.append(result)

    conn.close()
    return _clean(results)


search_bills.RESPONSE_SCHEMA = {
    "_type": "list[dict]",
    "_description": "List of bill summaries sorted by publication_date DESC, bill_id DESC",
    "bill_id": {"type": "int", "optional": False, "description": "Unique bill identifier"},
    "name": {"type": "str", "optional": True, "description": "Bill name"},
    "knesset_num": {"type": "int", "optional": True, "description": "Knesset number"},
    "sub_type": {"type": "str", "optional": True, "description": "Bill sub-type (private/government/committee)"},
    "status": {"type": "str", "optional": True, "description": "Current status description"},
    "committee": {"type": "str", "optional": True, "description": "Assigned committee name"},
    "committee_id": {"type": "int", "optional": True, "description": "Assigned committee ID"},
    "publication_date": {"type": "str", "optional": True, "description": "Publication date (YYYY-MM-DD)"},
    "publication_series": {"type": "str", "optional": True, "description": "Publication series description"},
    "summary": {"type": "str", "optional": True, "description": "Summary of the law"},
    "primary_initiators": {"type": "list[str]", "optional": True, "description": "Primary initiator names with party (only present if initiators exist)"},
}
