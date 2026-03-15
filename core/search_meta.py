"""Registry for cross-entity search configuration.

Each view module that participates in ``search_across`` calls
``register_search()`` at module level with a dict containing the SQL
builder needed to count and retrieve matching rows.

``search_across_view.py`` calls ``get_search_entries()`` to discover all
registered entries — no explicit imports of individual views required
(as long as ``origins/__init__.py`` has already been imported to trigger
module-level registration).

Usage in a view module::

    from core.search_meta import register_search

    def _build_bills_search(*, query, knesset_num, date, date_to, top_n):
        conditions, params = [], []
        if query:
            conditions.append("Name LIKE %s")
            params.append(f"%{query}%")
        ...
        where = " AND ".join(conditions) if conditions else "1=1"
        count_sql = f"SELECT COUNT(*) FROM bill_raw WHERE {where}"
        search_sql = f"SELECT ... FROM bill_raw WHERE {where} LIMIT %s"
        return count_sql, params, search_sql, params + [top_n]

    register_search({"entity_key": "bills", "builder": _build_bills_search})
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
        Must contain keys: ``entity_key`` and ``builder``.
        The ``builder`` is a callable with signature::

            (*, query, knesset_num, date, date_to, top_n)
            -> (count_sql, count_params, search_sql, search_params)
    """
    _SEARCH_ENTRIES.append(entry)


def get_search_entries() -> list[dict[str, Any]]:
    """Return all registered search entries (in registration order)."""
    return list(_SEARCH_ENTRIES)
