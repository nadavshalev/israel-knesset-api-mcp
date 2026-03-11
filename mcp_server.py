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
import logging
import sys
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

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
from core.db import connect_db, connect_readonly, ensure_indexes
from core.rate_limit import RateLimitMiddleware

# Import all view modules so @mcp_tool decorators and register_search()
# calls run and populate their global registries.
import views  # noqa: F401 — triggers views/__init__.py which imports all view modules

from core.mcp_meta import get_all_tools

logger = logging.getLogger(__name__)


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
# Startup — query dynamic metadata from the database
# ---------------------------------------------------------------------------

def _query_startup_metadata() -> dict:
    """Run all startup SQL queries and return aggregated metadata.

    Returns a dict with:
      - ``enum_values``: {tool_name: {param_name: [val, ...]}}
      - ``counts``:      {tool_name: int}
      - ``recent_dates``:{tool_name: str|None}
      - ``last_sync``:   str|None
    """
    conn = connect_readonly()
    cursor = conn.cursor()

    enum_values: dict[str, dict[str, list[str]]] = {}
    counts: dict[str, int] = {}
    recent_dates: dict[str, str | None] = {}

    for fn in get_all_tools():
        meta = fn._mcp_tool
        tool_name = meta["name"]

        # --- enum_sql ---
        esql = meta.get("enum_sql") or {}
        if esql:
            enum_values[tool_name] = {}
        for param_name, sql in esql.items():
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                # Each row is a RealDictRow with one column
                vals = [list(r.values())[0] for r in rows if list(r.values())[0]]
                enum_values[tool_name][param_name] = vals
            except Exception:
                logger.warning("enum_sql failed for %s.%s", tool_name, param_name, exc_info=True)

        # --- count_sql ---
        count_sql = meta.get("count_sql")
        if count_sql:
            try:
                cursor.execute(count_sql)
                row = cursor.fetchone()
                counts[tool_name] = list(row.values())[0] if row else 0
            except Exception:
                logger.warning("count_sql failed for %s", tool_name, exc_info=True)

        # --- most_recent_date_sql ---
        date_sql = meta.get("most_recent_date_sql")
        if date_sql:
            try:
                cursor.execute(date_sql)
                row = cursor.fetchone()
                val = list(row.values())[0] if row else None
                if val and isinstance(val, str) and len(val) >= 10:
                    val = val[:10]
                recent_dates[tool_name] = val
            except Exception:
                logger.warning("most_recent_date_sql failed for %s", tool_name, exc_info=True)

    # --- last_sync ---
    last_sync = None
    try:
        cursor.execute("SELECT MAX(last_sync_completed_at) FROM metadata")
        row = cursor.fetchone()
        if row:
            last_sync = list(row.values())[0]
    except Exception:
        logger.warning("last_sync query failed", exc_info=True)

    conn.close()
    return {
        "enum_values": enum_values,
        "counts": counts,
        "recent_dates": recent_dates,
        "last_sync": last_sync,
    }


_startup_meta = _query_startup_metadata()

# ---------------------------------------------------------------------------
# MCP server instance
# ---------------------------------------------------------------------------

def _build_instructions() -> str:
    """Build the server-level instructions string.

    Includes the last-sync timestamp queried at startup so that LLM
    clients know how fresh the data is without a separate tool call.
    """
    last_sync = _startup_meta.get("last_sync")
    sync_line = ""
    if last_sync:
        sync_line = f"\n\n**Last data sync:** {last_sync}"

    return f"""\
Israeli Knesset (parliament) data API — members, committees, bills, \
plenum sessions, and votes.{sync_line}

## Getting Started
1. Use `search_across` for broad discovery — it searches all entity types \
at once and returns the top matches per type.
2. Each search tool's description includes the number of records and data \
freshness date. Parameter schemas include the exact allowed values where \
applicable — use those values verbatim.

## Search → Detail Workflow
- **Search tools** (`search_members`, `search_bills`, `search_votes`, \
`search_committees`, `search_plenums`) return compact summaries. Use them \
to find IDs.
- **Detail tools** (`get_member`, `get_bill`, `get_vote`, `get_committee`, \
`get_plenum`) return the full record for a single entity by ID.
- Always search first to find the ID, then call the detail tool. \
Do not guess IDs.

## Always Filter — Responses Are Size-Capped
Responses that exceed the server limit are rejected with an error. \
The more filters you provide, the smaller and faster the response.
- **`knesset_num`** is the single most important filter — always provide it \
when you know which Knesset term you need.
- Combine `knesset_num` with name, type, status, or date filters to narrow \
results further.
- If you get a "Response too large" error, add more filters — do not retry \
the same query.

## Date Filtering — Use Ranges, Not Single Days
Several search tools accept `from_date`, `to_date`, and `date` \
(all in `YYYY-MM-DD` format).
- **Use `from_date` + `to_date` for date ranges.** \
For example, to get all votes in March 2020: \
`from_date="2020-03-01", to_date="2020-03-31"`. \
Do NOT send a separate request for each day — a single range query is \
faster and uses one response.
- **`date` is a shortcut for a single day** — equivalent to setting both \
`from_date` and `to_date` to the same value.
- **Always provide `to_date` when using `from_date`**, otherwise you get \
everything from that date to the present, which is usually too large.
- Date-filterable search tools: `search_votes`, `search_bills`, \
`search_plenums`. The detail tool `get_committee` also accepts date \
params to scope its sessions, members, bills, and documents.

## Common Patterns
- **Find a member's activity**: `search_members` by name/party → \
`get_member` with `knesset_num` for full roles and committees.
- **Find a bill and its votes**: `search_bills` by name → `get_bill` \
by ID (includes plenum stages with vote summaries).
- **Explore a committee**: `search_committees` by name/type → \
`get_committee` by ID with `include_sessions=True` (and date filters \
to scope the time window).
- **Votes on a specific bill**: `search_votes` with `bill_id` filter.

## Parameter Types
- IDs (`vote_id`, `bill_id`, `member_id`, etc.) are integers.
- `knesset_num` is an integer.
- Boolean flags (`accepted`, `is_current`, `include_sessions`, etc.) \
accept `true`/`false`.
- All text filters (names, types, statuses) are Hebrew strings \
with case-insensitive substring matching.
- Parameters with enum constraints list the exact allowed values in \
their schema — use those values verbatim (they are in Hebrew).
"""


_INSTRUCTIONS = _build_instructions()

mcp = FastMCP(
    "Knesset Data API",
    instructions=_INSTRUCTIONS,
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

def _make_handler(view_fn, enum_map: dict[str, list[str]]):
    """Create an async MCP handler that wraps a view function.

    The handler has the same typed parameters as the view function,
    with enum-constrained parameters replaced by ``Literal[..] | None``
    types so FastMCP exposes them as JSON-schema ``enum`` constraints.
    """
    view_sig = inspect.signature(view_fn)
    new_params: list[inspect.Parameter] = []

    for p in view_sig.parameters.values():
        if p.name in enum_map and enum_map[p.name]:
            # Build Literal["val1", "val2", ...] | None
            lit_type = Literal[tuple(enum_map[p.name])]  # type: ignore[valid-type]
            new_ann = Annotated[
                lit_type | None,  # type: ignore[operator]
                # Preserve the original Field(...) description
                _extract_field(p.annotation),
            ]
            new_params.append(p.replace(annotation=new_ann))
        else:
            new_params.append(p)

    async def handler(**kwargs) -> str:
        # Remove None-valued optional params so the view uses its defaults
        view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        result = view_fn(**view_kwargs)
        return _validate_size(result)

    # Attach the enriched signature so FastMCP can introspect it
    handler.__signature__ = inspect.Signature(
        new_params, return_annotation=str
    )
    handler.__annotations__ = {p.name: p.annotation for p in new_params}
    handler.__annotations__["return"] = str

    return handler


def _extract_field(annotation) -> Field:
    """Pull the pydantic Field() from an Annotated[..., Field(...)] type.

    Falls back to a bare Field() if no Field metadata is found.
    """
    from typing import get_origin, get_args
    if get_origin(annotation) is Annotated:
        for meta in get_args(annotation)[1:]:
            if hasattr(meta, "description"):  # pydantic FieldInfo
                return meta
    return Field()


def _enrich_description(base: str, tool_name: str) -> str:
    """Append entity count and most-recent date to a tool description."""
    count = _startup_meta["counts"].get(tool_name)
    date = _startup_meta["recent_dates"].get(tool_name)
    if count is not None:
        count_str = f"{count:,}"
        if date:
            return f"{base}\n\nSearches {count_str} records (data through {date})."
        return f"{base}\n\nSearches {count_str} records."
    return base


for _fn in get_all_tools():
    _meta = _fn._mcp_tool
    _tool_name = _meta["name"]
    _enum_map = _startup_meta["enum_values"].get(_tool_name, {})
    _handler = _make_handler(_fn, _enum_map)
    _desc = _enrich_description(_meta["description"], _tool_name)
    mcp.tool(
        name=_tool_name,
        description=_desc,
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
