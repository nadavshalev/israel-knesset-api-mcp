"""Cross-entity search view — triage tool for finding items across all entities.

Searches members, bills, committees, votes, and plenum sessions by a free-text
query.  Returns match counts per entity type and the top N results for each,
allowing the caller to decide which entity to drill into.

Designed to be fast: uses LIKE on indexed name/title columns with LIMIT per
entity.  Not meant for exhaustive results — use the per-entity search tools
for that.

Search SQL for each entity is defined in the respective view module via
``register_search()`` from ``core.search_meta``, keeping all view-specific
code in the view.
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
from core.mcp_meta import mcp_tool


def _get_entity_queries() -> list[dict]:
    """Discover search entries from view modules.

    Import is deferred to avoid circular imports (views/__init__.py imports
    this module, and the view modules that call register_search() are
    siblings).
    """
    from core.search_meta import get_search_entries
    return get_search_entries()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="search_across",
    description=(
        "Search across all entity types (members, bills, committees, votes, "
        "plenums) with a single query. Returns match counts and top results "
        "per entity type. Use this as a triage tool to find which entity "
        "type has relevant data, then drill down with specific search tools."
    ),
    entity="Cross-Entity Search",
)
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

    entity_queries = _get_entity_queries()

    results = {}
    for eq in entity_queries:
        entity = eq["entity_key"]
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
