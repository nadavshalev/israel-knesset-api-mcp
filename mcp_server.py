"""MCP server for Knesset data API.

Exposes all @mcp_tool-decorated view functions as MCP tools over Streamable
HTTP transport (stateless mode, JSON responses).  Includes per-IP rate
limiting and response size validation.

Usage::

    python mcp_server.py          # uses .env for config
    # or via uvicorn:
    uvicorn mcp_server:app --host 0.0.0.0 --port 8000
"""

import inspect
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mcp.server.fastmcp import FastMCP

from config import (
    MAX_OUTPUT_TOKENS,
    MCP_ENDPOINT,
    MCP_HOST,
    MCP_PORT,
    RATE_LIMIT_PER_MINUTE,
)
from core.db import connect_db, ensure_indexes
from core.rate_limit import RateLimitMiddleware

# Import all view modules so @mcp_tool decorators and register_search()
# calls run and populate their global registries.
import views  # noqa: F401 — triggers views/__init__.py which imports all view modules

from core.mcp_meta import get_all_tools


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

def _validate_size(result) -> str:
    """Serialize result to JSON and check against MAX_OUTPUT_TOKENS.

    Returns the JSON string if within the limit, or an error message
    instructing the client to use better filters.
    """
    text = json.dumps(result, ensure_ascii=False, default=str)
    if len(text) > MAX_OUTPUT_TOKENS:
        return json.dumps({
            "error": "Response too large",
            "size": len(text),
            "limit": MAX_OUTPUT_TOKENS,
            "hint": "Add more filters to narrow results.",
        }, ensure_ascii=False)
    return text


# ---------------------------------------------------------------------------
# Register all tools from decorated views
# ---------------------------------------------------------------------------

def _make_handler(view_fn):
    """Create an async MCP handler that wraps a view function.

    The handler has the same typed parameters as the view function.
    FastMCP infers the MCP input schema from the handler's signature.
    """
    view_sig = inspect.signature(view_fn)
    params = list(view_sig.parameters.values())

    async def handler(**kwargs) -> str:
        # Remove None-valued optional params so the view uses its defaults
        view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        result = view_fn(**view_kwargs)
        return _validate_size(result)

    # Attach the view's signature so FastMCP can introspect it
    handler.__signature__ = inspect.Signature(
        params, return_annotation=str
    )
    handler.__annotations__ = {p.name: p.annotation for p in params}
    handler.__annotations__["return"] = str

    return handler


for _fn in get_all_tools():
    _meta = _fn._mcp_tool
    _handler = _make_handler(_fn)
    mcp.tool(
        name=_meta["name"],
        description=_meta["description"],
    )(_handler)


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
