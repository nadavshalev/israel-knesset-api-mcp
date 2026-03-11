"""Decorator for marking view functions as MCP tools.

Attaches metadata as function attributes so that ``mcp_server.py`` can
discover tools by inspecting the decorated functions — no central
registry needed.

Usage::

    @mcp_tool(
        name="search_members",
        description="Search for Knesset members...",
        entity="Knesset Members",
        count_sql="SELECT COUNT(DISTINCT PersonID) FROM person_to_position_raw",
        most_recent_date_sql="SELECT MAX(StartDate) FROM person_to_position_raw",
        enum_sql={"role_type": "SELECT DISTINCT Description FROM position_raw ORDER BY Description"},
        is_list=True,
    )
    def search_members(knesset_num=None, ...):
        ...
"""

from __future__ import annotations

from typing import Callable

# Global list that collects all decorated functions in import order.
_ALL_TOOLS: list[Callable] = []


def mcp_tool(
    *,
    name: str,
    description: str,
    entity: str,
    count_sql: str | None = None,
    most_recent_date_sql: str | None = None,
    enum_sql: dict[str, str] | None = None,
    is_list: bool = False,
) -> Callable:
    """Decorator that attaches MCP metadata to a view function.

    Parameters
    ----------
    name : str
        MCP tool name (e.g. ``"search_members"``).
    description : str
        Human-readable description shown in MCP Inspector.
    entity : str
        Entity category (e.g. ``"Knesset Members"``).
    count_sql : str, optional
        SQL to count entities (search/list tools only).
    most_recent_date_sql : str, optional
        SQL returning the most recent content date (search/list tools only).
    enum_sql : dict[str, str], optional
        Mapping of parameter name to SQL that returns its valid values.
        At server startup the SQL is executed and the parameter's type
        annotation is replaced with a ``Literal[...]`` constraint.
    is_list : bool
        True for search/list tools, False for detail tools.
    """
    def decorator(fn: Callable) -> Callable:
        fn._mcp_tool = {
            "name": name,
            "description": description,
            "entity": entity,
            "count_sql": count_sql,
            "most_recent_date_sql": most_recent_date_sql,
            "enum_sql": enum_sql or {},
            "is_list": is_list,
        }
        _ALL_TOOLS.append(fn)
        return fn
    return decorator


def get_all_tools() -> list[Callable]:
    """Return all decorated view functions (in import order)."""
    return list(_ALL_TOOLS)


def get_search_tools() -> list[Callable]:
    """Return only search/list tools (those with count_sql)."""
    return [fn for fn in _ALL_TOOLS if fn._mcp_tool.get("count_sql")]
