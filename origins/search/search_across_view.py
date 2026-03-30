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
        "plenums). Returns match counts and top results per entity type. "
        "Use this as a triage tool to find which entity type has relevant "
        "data, then drill down with specific search tools.\n\n"
        "At least one filter must be provided (query, knesset_num, or date).\n\n"
        "Tip: For structural questions about a Knesset term (committees, factions, assembly date ranges), "
        "use the metadata tool directly rather than searching across entities."
    ),
    entity="Cross-Entity Search",
)
def search_across(
    query: Annotated[str | None, Field(description="Free-text search term (optional if other filters given)")] = None,
    knesset_num: Annotated[int | None, Field(description="Filter by Knesset number (e.g. 25)")] = None,
    date: Annotated[str | None, Field(description="Filter by date (YYYY-MM-DD). Exact date, or range start if date_to given")] = None,
    date_to: Annotated[str | None, Field(description="End date for range filter (YYYY-MM-DD). Requires date")] = None,
    top_n: Annotated[int | None, Field(description="Max results per entity type (default from server config)")] = None,
) -> SearchAcrossResults:
    """Search across all entity types.

    Parameters
    ----------
    query : str, optional
        Free-text search term.
    knesset_num : int, optional
        Filter by Knesset number.
    date : str, optional
        Filter by date (YYYY-MM-DD).  Exact match, or range start when
        combined with *date_to*.
    date_to : str, optional
        End of date range (YYYY-MM-DD).  Requires *date*.
    top_n : int, optional
        Max results per entity type.  Defaults to ``SEARCH_ACROSS_TOP_N``
        from config.

    Returns
    -------
    SearchAcrossResults
    """
    normalized = normalize_inputs(locals())
    query = normalized["query"]
    knesset_num = normalized["knesset_num"]
    date = normalized["date"]
    date_to = normalized["date_to"]
    top_n = normalized["top_n"]

    # Normalize query: treat whitespace-only as None
    if query and not query.strip():
        query = None

    # Must have at least one filter
    if not query and knesset_num is None and not date:
        return SearchAcrossResults(query=query, results={})

    if top_n is None:
        top_n = SEARCH_ACROSS_TOP_N

    conn = connect_readonly()
    cursor = conn.cursor()

    entity_queries = _get_entity_queries()

    results = {}
    for eq in entity_queries:
        entity = eq["entity_key"]
        builder = eq["builder"]

        count_sql, count_params, search_sql, search_params = builder(
            query=query, knesset_num=knesset_num, date=date,
            date_to=date_to, top_n=top_n,
        )

        # Count
        try:
            cursor.execute(count_sql, count_params)
            row = cursor.fetchone()
            count = list(row.values())[0] if row else 0
        except Exception:
            conn.rollback()
            count = 0

        # Top N results
        try:
            cursor.execute(search_sql, search_params)
            rows = cursor.fetchall()
            mapper = eq.get("mapper")
            if mapper:
                top = [mapper(dict(row)).model_dump(exclude_none=True) for row in rows]
            else:
                top = [dict(row) for row in rows]
        except Exception:
            conn.rollback()
            top = []

        results[entity] = EntityResult(count=count, top=top)

    conn.close()

    return SearchAcrossResults(
        query=query, knesset_num=knesset_num, date=date,
        date_to=date_to, results=results,
    )


search_across.OUTPUT_MODEL = SearchAcrossResults
