"""MCP server for Knesset data API.

Exposes all view functions as MCP tools over Streamable HTTP transport
(stateless mode, JSON responses).  Includes per-IP rate limiting and
response size validation.

Usage::

    python mcp_server.py          # uses .env for config
    # or via uvicorn:
    uvicorn mcp_server:app --host 0.0.0.0 --port 8000
"""

import inspect
import json
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mcp.server.fastmcp import FastMCP

from config import (
    MAX_RESULTS_SIZE,
    MCP_ENDPOINT,
    MCP_HOST,
    MCP_PORT,
    RATE_LIMIT_PER_MINUTE,
    SEARCH_ACROSS_TOP_N,
)
from core.db import connect_db, ensure_indexes
from core.rate_limit import RateLimitMiddleware
from core.registry import TOOLS
from views.database_status_view import get_database_status
from views.search_across_view import search_across


# ---------------------------------------------------------------------------
# Startup — ensure indexes (write access, once)
# ---------------------------------------------------------------------------

def _ensure_indexes_at_startup() -> None:
    """Create/verify indexes using a writable connection, then close it."""
    conn = connect_db()
    ensure_indexes(conn)
    conn.close()


_ensure_indexes_at_startup()

# ---------------------------------------------------------------------------
# MCP server instance
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Knesset Data API",
    instructions=(
        "Israeli Knesset (parliament) data API. Search and retrieve information "
        "about Knesset members, committees, bills, plenum sessions, and votes. "
        "Start with get_database_status to see available data, or use "
        "search_across to find items across all entity types."
    ),
    stateless_http=True,
    json_response=True,
    host=MCP_HOST,
    port=MCP_PORT,
    streamable_http_path=MCP_ENDPOINT,
)


# ---------------------------------------------------------------------------
# Response size validation
# ---------------------------------------------------------------------------

def _validate_size(result, max_size: int | None = None) -> str:
    """Serialize result to JSON and check size.

    Returns the JSON string if within limits, or an error message if too large.
    """
    limit = max_size if max_size is not None else MAX_RESULTS_SIZE
    text = json.dumps(result, ensure_ascii=False, default=str)
    if len(text) > limit:
        return json.dumps({
            "error": "Response too large",
            "size": len(text),
            "limit": limit,
            "hint": "Add more filters to narrow results, or reduce max_results_size.",
        }, ensure_ascii=False)
    return text


# ---------------------------------------------------------------------------
# Register tools from registry
# ---------------------------------------------------------------------------

# Type map from registry filter types to Python types
_TYPE_MAP = {
    "integer": int,
    "boolean": bool,
    "string": str,
}


def _make_handler(tool_entry: dict):
    """Create an MCP tool handler with explicit typed parameters.

    FastMCP infers the input schema from function signatures, so we
    dynamically build a function with properly typed/named parameters
    instead of using generic **kwargs.
    """
    handler = tool_entry["handler"]
    param_map = tool_entry["handler_param_map"]
    filters = tool_entry["filters"]

    # Build parameter list for the dynamic function
    params = []
    for f in filters:
        py_type = _TYPE_MAP.get(f["type"], str)
        if f.get("required"):
            # Required param — no default
            params.append(
                inspect.Parameter(
                    f["name"],
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=py_type,
                )
            )
        else:
            # Optional param — default None
            params.append(
                inspect.Parameter(
                    f["name"],
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    default=None,
                    annotation=Optional[py_type],
                )
            )

    # Add max_results_size as optional int
    params.append(
        inspect.Parameter(
            "max_results_size",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            default=None,
            annotation=Optional[int],
        )
    )

    # Build the actual handler that calls the view function
    async def _impl(**kwargs) -> str:
        max_size = kwargs.pop("max_results_size", None)
        view_kwargs = {}
        for mcp_name, view_name in param_map.items():
            if mcp_name in kwargs and kwargs[mcp_name] is not None:
                view_kwargs[view_name] = kwargs[mcp_name]
        result = handler(**view_kwargs)
        return _validate_size(result, max_size)

    # Create a new signature and attach it to the function
    sig = inspect.Signature(params, return_annotation=str)
    _impl.__signature__ = sig
    _impl.__name__ = tool_entry["tool_name"]
    _impl.__qualname__ = tool_entry["tool_name"]

    # Build annotations dict for FastMCP introspection
    _impl.__annotations__ = {p.name: p.annotation for p in params}
    _impl.__annotations__["return"] = str

    # Build docstring from filter descriptions
    doc_lines = [tool_entry["description"], ""]
    for f in filters:
        req = " (required)" if f.get("required") else ""
        doc_lines.append(f"  {f['name']}: {f['description']}{req}")
    doc_lines.append(
        f"  max_results_size: Max response size in characters "
        f"(default: {MAX_RESULTS_SIZE})"
    )
    _impl.__doc__ = "\n".join(doc_lines)

    return _impl


# Register all registry tools
for _tool_entry in TOOLS:
    _handler = _make_handler(_tool_entry)
    mcp.tool(
        name=_tool_entry["tool_name"],
        description=_tool_entry["description"],
    )(_handler)


# ---------------------------------------------------------------------------
# Additional tools: database_status and search_across
# ---------------------------------------------------------------------------

@mcp.tool(
    name="get_database_status",
    description=(
        "Get database status: entity counts, available tools with their "
        "filters, and last sync time. Call this first to understand what "
        "data is available."
    ),
)
async def tool_database_status() -> str:
    result = get_database_status()
    return _validate_size(result)


@mcp.tool(
    name="search_across",
    description=(
        "Search across all entity types (members, bills, committees, votes, "
        "plenums) with a single query. Returns match counts and top results "
        "per entity type. Use this as a triage tool to find which entity "
        "type has relevant data, then drill down with specific search tools."
    ),
)
async def tool_search_across(
    query: str,
    top_n: int | None = None,
    max_results_size: int | None = None,
) -> str:
    """Search across all entities."""
    result = search_across(query, top_n=top_n)
    return _validate_size(result, max_results_size)


# ---------------------------------------------------------------------------
# ASGI app with rate limiting
# ---------------------------------------------------------------------------

# Get the Starlette ASGI app from MCP
_mcp_app = mcp.streamable_http_app()

# Wrap with rate limiting middleware
app = RateLimitMiddleware(_mcp_app, max_per_minute=RATE_LIMIT_PER_MINUTE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    print(f"Starting Knesset MCP server on {MCP_HOST}:{MCP_PORT}{MCP_ENDPOINT}")
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
