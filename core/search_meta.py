"""Registry for cross-entity search configuration.

Each view module that participates in ``search_across`` calls
``register_search()`` at module level with a dict containing the SQL
needed to count and retrieve matching rows.

``search_across_view.py`` calls ``get_search_entries()`` to discover all
registered entries — no explicit imports of individual views required
(as long as ``views/__init__.py`` has already been imported to trigger
module-level registration).

Usage in a view module::

    from core.search_meta import register_search

    register_search({
        "entity_key": "members",
        "count_sql": "SELECT COUNT(...) FROM ... WHERE ... LIKE ?",
        "search_sql": "SELECT ... FROM ... WHERE ... LIKE ? LIMIT ?",
        "param_count": 2,
    })
"""

from __future__ import annotations

from typing import Any

# Global list of search entries, populated at import time.
_SEARCH_ENTRIES: list[dict[str, Any]] = []


def register_search(entry: dict[str, Any]) -> None:
    """Register a cross-entity search entry.

    Parameters
    ----------
    entry : dict
        Must contain keys: ``entity_key``, ``count_sql``, ``search_sql``,
        ``param_count``.
    """
    _SEARCH_ENTRIES.append(entry)


def get_search_entries() -> list[dict[str, Any]]:
    """Return all registered search entries (in registration order)."""
    return list(_SEARCH_ENTRIES)
