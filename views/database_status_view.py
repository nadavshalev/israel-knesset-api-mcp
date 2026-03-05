"""Database status view — reports entity counts, available tools, and last sync.

Uses ``core/registry.py`` as the single source of truth for what entities
and tools exist.  Never exposes raw table names to the caller.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_readonly


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_database_status() -> dict:
    """Return a status report: entity counts, available tools, last sync time.

    Entity counts are derived from registry ``count_sql`` entries (search
    tools only — detail tools have ``count_sql=None``).

    The registry import is deferred to avoid circular imports
    (views/__init__.py -> database_status_view -> core.registry -> views).
    """
    from core.registry import TOOLS

    conn = connect_readonly()
    cursor = conn.cursor()

    # --- Entity counts ---
    entities = {}
    for tool in TOOLS:
        if tool["count_sql"] is None:
            continue
        entity = tool["entity"]
        if entity in entities:
            continue  # already counted via another tool for same entity
        try:
            cursor.execute(tool["count_sql"])
            row = cursor.fetchone()
            entities[entity] = row[0] if row else 0
        except Exception:
            entities[entity] = None  # table might not exist yet

    # --- Available tools ---
    tools_info = []
    for tool in TOOLS:
        entry = {
            "name": tool["tool_name"],
            "entity": tool["entity"],
            "description": tool["description"],
            "type": "search" if tool["is_list"] else "detail",
            "filters": [
                {
                    "name": f["name"],
                    "type": f["type"],
                    "description": f["description"],
                    "required": f.get("required", False),
                }
                for f in tool["filters"]
            ],
        }
        tools_info.append(entry)

    # --- Last sync time ---
    last_sync = None
    try:
        cursor.execute(
            "SELECT MAX(last_sync_completed_at) FROM metadata"
        )
        row = cursor.fetchone()
        if row and row[0]:
            last_sync = row[0]
    except Exception:
        pass  # metadata table may not exist

    conn.close()

    return {
        "entity_counts": entities,
        "tools": tools_info,
        "last_sync": last_sync,
    }
