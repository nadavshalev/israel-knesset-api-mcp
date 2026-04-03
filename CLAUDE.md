# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Always activate the virtualenv before running anything:

```bash
source .venv/bin/activate
```

```bash
# Run tests
pytest tests/test_plenum_sessions_view.py -q   # single file
pytest --tb=short -q                           # full suite

# Start the MCP server
python mcp_server.py

# Query views directly without MCP (useful for manual testing)
python scripts/local_query.py members --member-id 839
python scripts/local_query.py bills --knesset 20 --name "חוק-יסוד"
python scripts/local_query.py search-across "נתניהו"

# DB setup (first time or after schema changes)
python core/db_cli.py init-db
python core/db_cli.py fetch-all
python core/db_cli.py fetch-all --since 2025-01-01T00:00:00
```

Never use bare `pytest` or `python -m pytest` without the venv active — the dependencies won't be available.

## Architecture

This is a **Model Context Protocol (MCP) server** (Streamable HTTP, `/mcp` endpoint) that exposes Israeli Knesset data from a PostgreSQL database as read-only AI tools.

### Request lifecycle

1. HTTP → rate-limit middleware → FastMCP handler (in `mcp_server.py`)
2. Handler normalizes inputs via `normalize_inputs(locals())`, calls the view function
3. View queries Postgres via `connect_readonly()`, builds Pydantic models, returns result
4. `clean()` strips `None`/`""`/`-1` from serialized output; `KNSBaseModel` applies this automatically on `model_dump()`

### Tool registration pipeline

Tools are discovered at import time with zero central registry:

1. **`origins/__init__.py`** walks all sub-packages, imports every `*_view.py`
2. **`@mcp_tool(...)`** decorator (in `core/mcp_meta.py`) attaches `fn._mcp_tool = {...}` and appends to `_ALL_TOOLS`
3. **`mcp_server.py` startup** calls `get_all_tools()`, then for each tool:
   - Executes `enum_sql` queries → replaces parameter types with `Literal[...]` constraints
   - Executes `count_sql` / `most_recent_date_sql` → injects record counts into tool description
   - Wraps the function in a logging/validation handler and registers it with FastMCP

### Cross-entity search

Each view that participates in `search_across` also calls `register_search({"entity_key": ..., "builder": ..., "mapper": ...})`. The builder returns `(count_sql, count_params, search_sql, search_params)`. `search_across_view.py` fans out to all registered builders.

### Origins package structure

Each entity lives in `origins/{entity}/`:
- `{entity}_view.py` — the `@mcp_tool`-decorated function, the `register_search()` call
- `{entity}_models.py` — Pydantic models for partial (search) and full (detail) results
- `tables/{table}.py` — `TABLE_NAME`, `ODATA_NAME`, `ENSURE_INDEXES`, `create_table()`, `fetch_rows()`

### Core utilities

| Module | Purpose |
|---|---|
| `core/db.py` | `connect_readonly()` (RealDictCursor), connection pool, `ensure_indexes()` |
| `core/helpers.py` | `normalize_inputs()`, `fuzzy_condition()`/`fuzzy_params()`, `simple_date()`, `resolve_pagination()`, `check_search_count()`, `build_count_by_query()` |
| `core/models.py` | `KNSBaseModel` — auto-cleans output on `model_dump()`; `CountItem` for `count_by` responses |
| `core/mcp_meta.py` | `@mcp_tool` decorator, `get_all_tools()` |
| `core/search_meta.py` | `register_search()`, `get_search_entries()` |
| `core/session_models.py` | Shared `ItemStage`, `SessionDocument` models and helpers used by multiple tools |

### Key conventions

- All views are **read-only** — always use `connect_readonly()`, never `connect_db()`
- `normalize_inputs(locals())` must be called at the top of every view to coerce string-typed agent inputs (e.g. `"25"` → `25`, `"true"` → `True`, `"none"` → `None`)
- `simple_date()`, `simple_time()`, `format_person_name()` return `None` (not `""`) so `clean()` strips them automatically
- Fuzzy search uses PostgreSQL trigrams + FTS via `fuzzy_condition(col)` / `fuzzy_params(query)` — never use `LIKE %s` directly for user-facing text search
- `check_search_count()` runs the count query and returns the total — call it before executing paginated queries to populate `total_count` in the response
- LSP warnings about `_mcp_tool`, `__signature__`, `OUTPUT_MODEL` on function objects are false positives — Python allows arbitrary attributes on functions
- `full_details=False` is the default; detail mode fetches significantly more data and should only be triggered when the caller has a specific ID
