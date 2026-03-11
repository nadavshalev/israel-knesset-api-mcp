"""Database status view — reports entity counts, available tools, and last sync.

Discovers tools from ``@mcp_tool``-decorated view functions via
``core.mcp_meta``.  Never exposes raw table names to the caller.
"""

import inspect
import sys
import types
from pathlib import Path
from typing import Annotated, get_args, get_origin, Union

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly
from core.mcp_meta import mcp_tool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Map Python types to simple type names for the status report
_TYPE_NAMES = {
    int: "integer",
    str: "string",
    bool: "boolean",
    float: "number",
}


def _param_type_name(annotation) -> str:
    """Convert a type annotation to a simple type name string."""
    # Unwrap Annotated[X, ...] -> X
    origin = get_origin(annotation)
    if origin is Annotated:
        annotation = get_args(annotation)[0]
        origin = get_origin(annotation)
    # Handle Optional[X] (which is Union[X, None])
    if origin in (Union, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if args:
            return _TYPE_NAMES.get(args[0], "string")
    return _TYPE_NAMES.get(annotation, "string")


def _param_description(annotation) -> str | None:
    """Extract a Field description from an Annotated type hint, if present."""
    origin = get_origin(annotation)
    if origin is Annotated:
        for meta in get_args(annotation)[1:]:
            # pydantic FieldInfo has a .description attribute
            desc = getattr(meta, "description", None)
            if desc:
                return desc
    return None


def _build_filters(fn) -> list[dict]:
    """Extract parameter info from a function's signature."""
    sig = inspect.signature(fn)
    filters = []
    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        ann = param.annotation
        required = param.default is inspect.Parameter.empty
        entry = {
            "name": name,
            "type": _param_type_name(ann if ann is not inspect.Parameter.empty else str),
            "required": required,
        }
        desc = _param_description(ann)
        if desc:
            entry["description"] = desc
        filters.append(entry)
    return filters


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@mcp_tool(
    name="get_database_status",
    description=(
        "Get database status: entity counts, available tools with their "
        "filters, and last sync time. Call this first to understand what "
        "data is available."
    ),
    entity="Database Status",
)
def get_database_status() -> dict:
    """Return a status report: entity counts, available tools, last sync time.

    Entity counts are derived from search tools that define ``count_sql``.

    The mcp_meta import is deferred to avoid circular imports
    (views/__init__.py -> database_status_view -> core.mcp_meta -> views).
    """
    from core.mcp_meta import get_all_tools

    all_tools = get_all_tools()

    conn = connect_readonly()
    cursor = conn.cursor()

    # --- Entity counts and most recent dates ---
    entities = {}
    for fn in all_tools:
        meta = fn._mcp_tool
        count_sql = meta.get("count_sql")
        if not count_sql:
            continue
        entity = meta["entity"]
        if entity in entities:
            continue  # already counted via another tool for same entity

        entry = {"count": None, "most_recent_date": None}

        try:
            cursor.execute(count_sql)
            row = cursor.fetchone()
            entry["count"] = list(row.values())[0] if row else 0
        except Exception:
            pass  # table might not exist yet

        date_sql = meta.get("most_recent_date_sql")
        if date_sql:
            try:
                cursor.execute(date_sql)
                row = cursor.fetchone()
                val = list(row.values())[0] if row else None
                # Normalise to YYYY-MM-DD (values may be full timestamps)
                if val and isinstance(val, str) and len(val) >= 10:
                    val = val[:10]
                entry["most_recent_date"] = val
            except Exception:
                pass  # column/table might not exist yet

        entities[entity] = entry

    # --- Available tools ---
    tools_info = []
    for fn in all_tools:
        meta = fn._mcp_tool
        entry = {
            "name": meta["name"],
            "entity": meta["entity"],
            "description": meta["description"],
            "type": "search" if meta["is_list"] else "detail",
            "filters": _build_filters(fn),
        }
        tools_info.append(entry)

    # --- Last sync time ---
    last_sync = None
    try:
        cursor.execute(
            "SELECT MAX(last_sync_completed_at) FROM metadata"
        )
        row = cursor.fetchone()
        if row:
            last_sync = list(row.values())[0]
    except Exception:
        pass  # metadata table may not exist

    conn.close()

    return {
        "entity_counts": entities,
        "tools": tools_info,
        "last_sync": last_sync,
    }
