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

from typing import Annotated
from pydantic import Field

from config import SEARCH_ACROSS_TOP_N
from core.db import connect_readonly
from core.helpers import normalize_inputs
from core.mcp_meta import mcp_tool
from origins.search.search_across_models import EntityResult, SearchAcrossResults


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
def search_across(
    query: Annotated[str, Field(description="Free-text search term")],
    top_n: Annotated[int | None, Field(description="Max results per entity type (default from server config)")] = None,
) -> SearchAcrossResults:
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
    normalized = normalize_inputs(locals())
    query = normalized["query"]
    top_n = normalized["top_n"]

    if not query or not query.strip():
        return SearchAcrossResults(query=query or "", results={})

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
            row = cursor.fetchone()
            count = list(row.values())[0] if row else 0
        except Exception:
            conn.rollback()
            count = 0

        # Top N results
        try:
            cursor.execute(eq["search_sql"], [pattern] * n_params + [top_n])
            rows = cursor.fetchall()
            top = [dict(row) for row in rows]
        except Exception:
            conn.rollback()
            top = []

        results[entity] = EntityResult(count=count, top=top)

    conn.close()

    return SearchAcrossResults(query=query, results=results)


search_across.OUTPUT_MODEL = SearchAcrossResults
