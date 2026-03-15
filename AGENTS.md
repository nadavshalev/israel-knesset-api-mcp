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

- `origins/` — Co-located entity packages. Each sub-package (e.g., `origins/bills/`) contains table modules, view files, and Pydantic model files for that entity.
  - Sub-packages: `bills`, `members`, `committees`, `votes`, `plenums`, `knesset`, `search`.
  - View files are named after their MCP tool (e.g., `search_bills_view.py`, `get_bill_view.py`).
  - Model files match their view (e.g., `search_bills_models.py`, `get_bill_models.py`).
  - `origins/__init__.py` auto-discovers all table modules and view modules across sub-packages.
- `core/` — Shared helpers (`helpers.py`), base model (`models.py`), DB CLI (`db_cli.py`), MCP decorator (`mcp_meta.py`).
- `mcp_server.py` — MCP server entry point; registers tools, enriches descriptions.
- `tests/` — Pytest tests, one file per view (e.g., `test_bill_view.py` tests `origins/bills/get_bill_view.py`).
- `scripts/` — Data fetching scripts (OData, vote results).

## Conventions

- Every view function has a `.RESPONSE_SCHEMA` dict attribute documenting its return shape.
- All Pydantic models inherit from `KNSBaseModel` (in `core/models.py`), which uses a `@model_serializer` to apply `clean()` on every `model_dump()` call — stripping `None`, `""`, and `-1` from serialized output. This ensures clean output even when FastMCP re-serializes models internally. Views do **not** call `clean()` themselves.
- `clean()` from `core/helpers.py` is applied centrally in `mcp_server.py`'s handler (inside `_make_handler`) to strip `None`, `""`, and `-1` from all MCP tool responses. It preserves `False`, `0`, `[]`, and `{}`. Views do **not** call `clean()` themselves.
- Helper functions `simple_date()`, `simple_time()`, and `format_person_name()` return `None` (not `""`) for empty/missing inputs, so `clean()` will strip them automatically.
- The `@mcp_tool` decorator (in `core/mcp_meta.py`) attaches `._mcp_tool` metadata and appends to `_ALL_TOOLS`.
- LSP warnings about `_mcp_tool`, `__signature__`, `RESPONSE_SCHEMA` on function objects are false positives — Python allows arbitrary attributes on functions.
