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
import time
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mcp.server.fastmcp import FastMCP

from config import (
    LOG_FILE,
    LOG_LEVEL,
    MAX_OUTPUT_TOKENS,
    MCP_ENDPOINT,
    MCP_HOST,
    MCP_PORT,
    RATE_LIMIT_PER_MINUTE,
)
from core.db import connect_db, connect_readonly, ensure_indexes
from core.helpers import clean
from core.rate_limit import RateLimitMiddleware

# Import all view modules so @mcp_tool decorators and register_search()
# calls run and populate their global registries.
import origins  # noqa: F401 — triggers origins/__init__.py which imports all view modules

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

    # --- current_knesset_num ---
    current_knesset_num = None
    try:
        cursor.execute(
            "SELECT KnessetNum FROM knesset_dates_raw WHERE IsCurrent = 1 LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            current_knesset_num = list(row.values())[0]
    except Exception:
        logger.warning("current_knesset_num query failed", exc_info=True)

    conn.close()
    return {
        "enum_values": enum_values,
        "counts": counts,
        "recent_dates": recent_dates,
        "last_sync": last_sync,
        "current_knesset_num": current_knesset_num,
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

    current_knesset = _startup_meta.get("current_knesset_num")
    knesset_line = ""
    if current_knesset is not None:
        knesset_line = f"\n\n**Current Knesset:** {current_knesset}"

    template_path = ROOT / "mcp_description.md"
    template = template_path.read_text(encoding="utf-8")
    return template.format(sync_line=sync_line, knesset_line=knesset_line)


_INSTRUCTIONS = _build_instructions()

mcp = FastMCP(
    "Knesset Data API",
    instructions=_INSTRUCTIONS,
    stateless_http=True,
    json_response=True,
    host=MCP_HOST,
    port=MCP_PORT,
    streamable_http_path=MCP_ENDPOINT,
    log_level=LOG_LEVEL,
)

# --- Optional file logging (useful when stderr is captured by a parent process) ---
if LOG_FILE:
    _fh = logging.FileHandler(LOG_FILE)
    _fh.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _fh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logging.getLogger().addHandler(_fh)


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


def _check_size_dict(result_dict: dict) -> None:
    """Raise ValueError if a dict result exceeds MAX_OUTPUT_TOKENS.

    Unlike _validate_size (which returns a JSON string), this raises so
    that Pydantic-model handlers never return a dict that violates the
    output schema.
    """
    text = json.dumps(result_dict, ensure_ascii=False, default=str)
    if len(text) > MAX_OUTPUT_TOKENS:
        raise ValueError(
            f"Response too large ({len(text)} chars, limit {MAX_OUTPUT_TOKENS}). "
            "Add more filters to narrow results."
        )


# ---------------------------------------------------------------------------
# Register all tools from decorated views
# ---------------------------------------------------------------------------

def _make_handler(view_fn, enum_map: dict[str, list[str]]):
    """Create an async MCP handler that wraps a view function.

    The handler has the same typed parameters as the view function,
    with enum-constrained parameters replaced by ``Literal[..] | None``
    types so FastMCP exposes them as JSON-schema ``enum`` constraints.

    If the view function has an ``OUTPUT_MODEL`` attribute (a Pydantic
    BaseModel class), the handler returns a dict that FastMCP validates
    against that model — producing a proper ``outputSchema`` instead of
    the generic ``{result: string}``.
    """
    tool_name = view_fn._mcp_tool["name"]
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

    output_model = getattr(view_fn, "OUTPUT_MODEL", None)

    if output_model is not None:
        is_list_tool = getattr(view_fn, '_mcp_tool', {}).get('is_list', False)

        if is_list_tool:
            # List/search tools always return a model (never None)
            async def handler(**kwargs):
                view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
                result = view_fn(**view_kwargs)
                result_dict = result.model_dump()
                _check_size_dict(result_dict)
                return result_dict

            handler.__signature__ = inspect.Signature(
                new_params, return_annotation=output_model
            )
            handler.__annotations__ = {p.name: p.annotation for p in new_params}
            handler.__annotations__["return"] = output_model
        else:
            # Single-entity views may return None when the ID is not found
            async def handler(**kwargs):
                view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
                result = view_fn(**view_kwargs)
                if result is None:
                    return {"error": "not_found", "message": "No record found for the given ID."}
                result_dict = result.model_dump()
                _check_size_dict(result_dict)
                return result_dict

            handler.__signature__ = inspect.Signature(
                new_params, return_annotation=output_model
            )
            handler.__annotations__ = {p.name: p.annotation for p in new_params}
            handler.__annotations__["return"] = output_model
    else:
        # Legacy: view returns a plain dict/list — serialize to JSON string
        async def handler(**kwargs) -> str:
            view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            result = view_fn(**view_kwargs)
            return _validate_size(clean(result))

        handler.__signature__ = inspect.Signature(
            new_params, return_annotation=str
        )
        handler.__annotations__ = {p.name: p.annotation for p in new_params}
        handler.__annotations__["return"] = str

    # Wrap with logging: tool name, params, duration, errors
    original_handler = handler

    async def logged_handler(**kwargs):
        view_kwargs = {k: v for k, v in kwargs.items() if v is not None}
        logger.info("tool_call: %s  params=%s", tool_name, view_kwargs or "{}")
        t0 = time.perf_counter()
        try:
            result = await original_handler(**kwargs)
            elapsed = time.perf_counter() - t0
            logger.info("tool_done: %s  %.3fs", tool_name, elapsed)
            return result
        except Exception:
            elapsed = time.perf_counter() - t0
            logger.error("tool_error: %s  %.3fs", tool_name, elapsed, exc_info=True)
            raise

    logged_handler.__signature__ = handler.__signature__
    logged_handler.__annotations__ = handler.__annotations__
    return logged_handler


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


_OMISSION_NOTE = (
    "Note: fields with null/empty values are omitted from the response. "
    "Check for key existence before accessing optional fields."
)


def _enrich_description(base: str, tool_name: str, view_fn) -> str:
    """Append entity count, most-recent date, and omission note."""
    parts = [base]
    count = _startup_meta["counts"].get(tool_name)
    date = _startup_meta["recent_dates"].get(tool_name)
    if count is not None:
        count_str = f"{count:,}"
        if date:
            parts.append(f"Searches {count_str} records (data through {date}).")
        else:
            parts.append(f"Searches {count_str} records.")

    parts.append(_OMISSION_NOTE)
    return "\n\n".join(parts)


for _fn in get_all_tools():
    _meta = _fn._mcp_tool
    _tool_name = _meta["name"]
    _enum_map = _startup_meta["enum_values"].get(_tool_name, {})
    _handler = _make_handler(_fn, _enum_map)
    _desc = _enrich_description(_meta["description"], _tool_name, _fn)
    mcp.tool(
        name=_tool_name,
        description=_desc,
    )(_handler)


# ---------------------------------------------------------------------------
# MCP Resources — Knesset term metadata (slowly-changing reference data)
# ---------------------------------------------------------------------------

from origins.knesset.metadata_view import (
    fetch_knesset_span,
    fetch_assemblies,
    fetch_committees,
    fetch_ministries,
    fetch_factions,
    fetch_general_roles,
)


def _log_resource(resource_name: str, knesset_num: int):
    logger.info("resource_call: %s  knesset_num=%s", resource_name, knesset_num)


def _log_resource_done(resource_name: str, t0: float):
    logger.info("resource_done: %s  %.3fs", resource_name, time.perf_counter() - t0)


def _log_resource_error(resource_name: str, t0: float):
    logger.error("resource_error: %s  %.3fs", resource_name, time.perf_counter() - t0, exc_info=True)


@mcp.resource(
    "knesset://knesset/{knesset_num}/assemblies",
    name="Knesset Assemblies",
    description="Assembly/plenum periods for a Knesset term",
    mime_type="application/json",
)
def resource_assemblies(knesset_num: int) -> list[dict]:
    knesset_num = int(knesset_num)
    _log_resource("assemblies", knesset_num)
    t0 = time.perf_counter()
    try:
        conn = connect_readonly()
        cursor = conn.cursor()
        result = fetch_assemblies(cursor, knesset_num)
        conn.close()
        _log_resource_done("assemblies", t0)
        return [r.model_dump(exclude_none=True) for r in result]
    except Exception:
        _log_resource_error("assemblies", t0)
        raise


@mcp.resource(
    "knesset://knesset/{knesset_num}/committees",
    name="Knesset Committees",
    description="Committees with chairs for a Knesset term",
    mime_type="application/json",
)
def resource_committees(knesset_num: int) -> list[dict]:
    knesset_num = int(knesset_num)
    _log_resource("committees", knesset_num)
    t0 = time.perf_counter()
    try:
        conn = connect_readonly()
        cursor = conn.cursor()
        kstart, kend = fetch_knesset_span(cursor, knesset_num)
        result = fetch_committees(cursor, knesset_num, include_heads=True, knesset_start=kstart, knesset_end=kend)
        conn.close()
        _log_resource_done("committees", t0)
        return [r.model_dump(exclude_none=True) for r in result]
    except Exception:
        _log_resource_error("committees", t0)
        raise


@mcp.resource(
    "knesset://knesset/{knesset_num}/ministries",
    name="Government Ministries",
    description="Government ministries with ministers, deputies, and members for a Knesset term",
    mime_type="application/json",
)
def resource_ministries(knesset_num: int) -> list[dict]:
    knesset_num = int(knesset_num)
    _log_resource("ministries", knesset_num)
    t0 = time.perf_counter()
    try:
        conn = connect_readonly()
        cursor = conn.cursor()
        kstart, kend = fetch_knesset_span(cursor, knesset_num)
        result = fetch_ministries(cursor, knesset_num, include_members=True, knesset_start=kstart, knesset_end=kend)
        conn.close()
        _log_resource_done("ministries", t0)
        return [r.model_dump(exclude_none=True) for r in result]
    except Exception:
        _log_resource_error("ministries", t0)
        raise


@mcp.resource(
    "knesset://knesset/{knesset_num}/factions",
    name="Parliamentary Factions",
    description="Factions with member lists for a Knesset term",
    mime_type="application/json",
)
def resource_factions(knesset_num: int) -> list[dict]:
    knesset_num = int(knesset_num)
    _log_resource("factions", knesset_num)
    t0 = time.perf_counter()
    try:
        conn = connect_readonly()
        cursor = conn.cursor()
        kstart, kend = fetch_knesset_span(cursor, knesset_num)
        result = fetch_factions(cursor, knesset_num, include_members=True, knesset_start=kstart, knesset_end=kend)
        conn.close()
        _log_resource_done("factions", t0)
        return [r.model_dump(exclude_none=True) for r in result]
    except Exception:
        _log_resource_error("factions", t0)
        raise


@mcp.resource(
    "knesset://knesset/{knesset_num}/roles",
    name="General Parliamentary Roles",
    description="Parliamentary roles not linked to committees/ministries/factions (e.g. Prime Minister, Knesset Speaker)",
    mime_type="application/json",
)
def resource_roles(knesset_num: int) -> list[dict]:
    knesset_num = int(knesset_num)
    _log_resource("roles", knesset_num)
    t0 = time.perf_counter()
    try:
        conn = connect_readonly()
        cursor = conn.cursor()
        kstart, kend = fetch_knesset_span(cursor, knesset_num)
        result = fetch_general_roles(cursor, knesset_num, knesset_start=kstart, knesset_end=kend)
        conn.close()
        _log_resource_done("roles", t0)
        return [r.model_dump(exclude_none=True) for r in result]
    except Exception:
        _log_resource_error("roles", t0)
        raise


# ---------------------------------------------------------------------------
# Health check endpoint
# ---------------------------------------------------------------------------

async def _health_check(scope, receive, send):
    """Minimal /health endpoint for Docker/load-balancer health checks."""
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/json"],
        ],
    })
    await send({
        "type": "http.response.body",
        "body": b'{"status": "ok"}',
    })


class _HealthCheckMiddleware:
    """ASGI middleware that handles /health before passing to the inner app."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") == "/health":
            await _health_check(scope, receive, send)
            return
        await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# ASGI app with rate limiting
# ---------------------------------------------------------------------------

# Get the Starlette ASGI app from MCP
_mcp_app = mcp.streamable_http_app()

# Wrap with rate limiting middleware, then health check (outermost)
app = _HealthCheckMiddleware(
    RateLimitMiddleware(_mcp_app, max_per_minute=RATE_LIMIT_PER_MINUTE)
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    print(f"Starting Knesset MCP server on {MCP_HOST}:{MCP_PORT}{MCP_ENDPOINT}")
    uvicorn.run(app, host=MCP_HOST, port=MCP_PORT)
