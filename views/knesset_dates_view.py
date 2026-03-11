"""Knesset dates view — returns knesset terms, assemblies, and plenum periods.

Each knesset term has multiple rows (one per assembly/plenum combination).
Results are grouped by knesset number, with a nested list of periods per
knesset.
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
    name="get_knesset_dates",
    description=(
        "Look up Knesset terms and their assembly/plenum session periods. "
        "Returns data grouped by Knesset number, each with a nested list "
        "of periods (assembly + plenum combinations with start/finish dates). "
        "Use this to find when a specific Knesset term started or ended, "
        "which plenum session is current, or to list all historical terms."
    ),
    entity="Knesset Dates",
    count_sql="SELECT COUNT(DISTINCT KnessetNum) FROM knesset_dates_raw",
    is_list=True,
)
def get_knesset_dates(
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number")] = None,
) -> list:
    """Look up Knesset terms and their plenum periods.

    Optionally filter by ``knesset_num`` to get periods for a single term.
    Returns a list of dicts, one per knesset number, sorted ascending.
    Each dict contains knesset metadata and a ``periods`` list with
    assembly/plenum entries sorted by (assembly, plenum).
    """
    normalized = normalize_inputs(locals())
    knesset_num = normalized["knesset_num"]

    conn = connect_readonly()
    cursor = conn.cursor()

    sql = """
    SELECT Id, KnessetNum, Name, Assembly, Plenum,
           PlenumStart, PlenumFinish, IsCurrent
    FROM knesset_dates_raw
    WHERE 1=1
    """
    params = []

    if knesset_num is not None:
        sql += " AND KnessetNum = %s"
        params.append(knesset_num)

    sql += " ORDER BY KnessetNum, Assembly, Plenum"

    cursor.execute(sql, params)
    rows = cursor.fetchall()

    # Group by KnessetNum, preserving order
    grouped: dict[int, dict] = {}
    order: list[int] = []
    for row in rows:
        knum = row["knessetnum"]
        if knum not in grouped:
            grouped[knum] = {
                "knesset_num": knum,
                "name": row["name"],
                "is_current": False,
                "periods": [],
            }
            order.append(knum)

        is_current = bool(row["iscurrent"])
        if is_current:
            grouped[knum]["is_current"] = True

        grouped[knum]["periods"].append({
            "id": row["id"],
            "assembly": row["assembly"],
            "plenum": row["plenum"],
            "start_date": simple_date(row["plenumstart"]),
            "finish_date": simple_date(row["plenumfinish"]),
            "is_current": is_current,
        })

    conn.close()
    return [grouped[k] for k in order]
