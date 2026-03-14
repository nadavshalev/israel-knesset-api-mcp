# Agent Instructions

## Project Setup
- activate the virtual environment: `source .venv/bin/activate`

## Running Tests
- Use `.venv/bin/pytest`, never bare `pytest` or `python -m pytest`.
- Run **only the relevant test file(s)** for targeted changes:
  ```
  pytest tests/test_bill_view.py -q
  ```
- Run the full suite only when changes are broad (e.g. something which affect all views):
  ```
  pytest --tb=short -q
  ```

## Project Layout

- `views/` — View functions (one per entity), each decorated with `@mcp_tool`.
- `tables/` — Table modules that load CSV data into DuckDB.
- `core/` — Shared helpers (`helpers.py`), DB CLI (`db_cli.py`), MCP decorator (`mcp_meta.py`).
- `mcp_server.py` — MCP server entry point; registers tools, enriches descriptions.
- `tests/` — Pytest tests, one file per view (e.g., `test_bill_view.py` tests `views/bill_view.py`).
- `scripts/` — Data fetching scripts (OData, vote results).

## Conventions

- Every view function has a `.RESPONSE_SCHEMA` dict attribute documenting its return shape.
- `_clean()` from `core/helpers.py` is applied at the return point of every view to strip `None`, `""`, and `-1` values. It preserves `False`, `0`, `[]`, and `{}`.
- The `@mcp_tool` decorator (in `core/mcp_meta.py`) attaches `._mcp_tool` metadata and appends to `_ALL_TOOLS`.
- LSP warnings about `_mcp_tool`, `__signature__`, `RESPONSE_SCHEMA` on function objects are false positives — Python allows arbitrary attributes on functions.
