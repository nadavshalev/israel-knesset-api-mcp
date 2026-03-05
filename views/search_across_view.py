"""Cross-entity search view — triage tool for finding items across all entities.

Searches members, bills, committees, votes, and plenum sessions by a free-text
query.  Returns match counts per entity type and the top N results for each,
allowing the caller to decide which entity to drill into.

Designed to be fast: uses LIKE on indexed name/title columns with LIMIT per
entity.  Not meant for exhaustive results — use the per-entity search tools
for that.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from config import SEARCH_ACROSS_TOP_N
from core.db import connect_readonly


# ---------------------------------------------------------------------------
# Per-entity search definitions
# ---------------------------------------------------------------------------

_ENTITY_QUERIES = [
    {
        "entity": "members",
        "count_sql": """
            SELECT COUNT(DISTINCT PersonID) FROM person_raw
            WHERE FirstName LIKE ? OR LastName LIKE ?
        """,
        "search_sql": """
            SELECT DISTINCT PersonID AS id,
                   FirstName || ' ' || LastName AS name
            FROM person_raw
            WHERE FirstName LIKE ? OR LastName LIKE ?
            ORDER BY LastName, FirstName
            LIMIT ?
        """,
        "param_count": 2,
    },
    {
        "entity": "bills",
        "count_sql": """
            SELECT COUNT(*) FROM bill_raw
            WHERE Name LIKE ?
        """,
        "search_sql": """
            SELECT b.Id AS id, b.Name AS name, b.KnessetNum AS knesset_num,
                   b.SubTypeDesc AS sub_type,
                   st.[Desc] AS status
            FROM bill_raw b
            LEFT JOIN status_raw st ON b.StatusID = st.Id
            WHERE b.Name LIKE ?
            ORDER BY b.Id DESC
            LIMIT ?
        """,
        "param_count": 1,
    },
    {
        "entity": "committees",
        "count_sql": """
            SELECT COUNT(*) FROM committee_raw
            WHERE Name LIKE ?
        """,
        "search_sql": """
            SELECT Id AS id, Name AS name, KnessetNum AS knesset_num,
                   CategoryDesc AS category
            FROM committee_raw
            WHERE Name LIKE ?
            ORDER BY Id DESC
            LIMIT ?
        """,
        "param_count": 1,
    },
    {
        "entity": "votes",
        "count_sql": """
            SELECT COUNT(*) FROM plenum_vote_raw
            WHERE VoteTitle LIKE ? OR VoteSubject LIKE ?
        """,
        "search_sql": """
            SELECT v.Id AS id, v.VoteTitle AS name,
                   s.KnessetNum AS knesset_num,
                   v.VoteDateTime AS date
            FROM plenum_vote_raw v
            LEFT JOIN plenum_session_raw s ON v.SessionID = s.Id
            WHERE v.VoteTitle LIKE ? OR v.VoteSubject LIKE ?
            ORDER BY v.Id DESC
            LIMIT ?
        """,
        "param_count": 2,
    },
    {
        "entity": "plenums",
        "count_sql": """
            SELECT COUNT(DISTINCT ps.Id)
            FROM plenum_session_raw ps
            LEFT JOIN plm_session_item_raw psi
                   ON ps.Id = psi.PlenumSessionID
            WHERE ps.Name LIKE ? OR psi.Name LIKE ?
        """,
        "search_sql": """
            SELECT DISTINCT ps.Id AS id,
                   ps.Name AS name,
                   ps.KnessetNum AS knesset_num,
                   ps.StartDate AS date
            FROM plenum_session_raw ps
            LEFT JOIN plm_session_item_raw psi
                   ON ps.Id = psi.PlenumSessionID
            WHERE ps.Name LIKE ? OR psi.Name LIKE ?
            ORDER BY ps.Id DESC
            LIMIT ?
        """,
        "param_count": 2,
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_across(query: str, top_n: int | None = None) -> dict:
    """Search across all entity types for *query*.

    Parameters
    ----------
    query : str
        Free-text search term.
    top_n : int, optional
        Max results per entity type.  Defaults to ``SEARCH_ACROSS_TOP_N``
        from config.

    Returns
    -------
    dict
        ``{"query": ..., "results": {entity: {"count": N, "top": [...]}}}``
    """
    if not query or not query.strip():
        return {"query": query, "results": {}}

    if top_n is None:
        top_n = SEARCH_ACROSS_TOP_N

    pattern = f"%{query.strip()}%"
    conn = connect_readonly()
    cursor = conn.cursor()

    results = {}
    for eq in _ENTITY_QUERIES:
        entity = eq["entity"]
        n_params = eq["param_count"]

        # Count
        try:
            cursor.execute(eq["count_sql"], [pattern] * n_params)
            count = cursor.fetchone()[0]
        except Exception:
            count = 0

        # Top N results
        try:
            cursor.execute(eq["search_sql"], [pattern] * n_params + [top_n])
            rows = cursor.fetchall()
            top = [dict(row) for row in rows]
        except Exception:
            top = []

        results[entity] = {"count": count, "top": top}

    conn.close()

    return {"query": query, "results": results}
