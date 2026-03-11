# Open Knesset Data API

MCP server for Israeli Knesset (parliament) data. Exposes structured search and detail tools over a local SQLite database via the [Model Context Protocol](https://modelcontextprotocol.io/) (Streamable HTTP transport).

Data is sourced from the [Knesset OData V4 API](https://knesset.gov.il/OdataV4/ParliamentInfo/) using a CSV-first strategy via [Open Knesset](https://oknesset.org/), with incremental OData updates.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create and populate the database
python core/db_cli.py init-db
python core/db_cli.py fetch-all

# Copy .env.example and adjust as needed
cp .env.example .env

# Start the MCP server
python mcp_server.py
```

The server starts at `http://0.0.0.0:8000/mcp` by default.

## Docker Deployment (VPS)

This repository includes Docker Compose for running:

- `mcp`: the MCP HTTP server
- `updater`: a background worker that runs `update_all.py` on a day/hour schedule

### 1) Configure environment

```bash
cp .env.example .env
```

Set your external PostgreSQL credentials/host in `.env`:

- `POSTGRES_PATH`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Set updater cycle:

- `UPDATE_CYCLE_DAYS` (for example `1`)
- `UPDATE_HOUR_IN_DAY` (0-23, for example `3`)
- `UPDATE_RUN_ON_START` (`true` or `false`)

### 2) Build and start

```bash
docker compose up -d --build
```

### 3) Verify

```bash
docker compose ps
docker compose logs -f mcp
docker compose logs -f updater
```

MCP endpoint will be available at:

`http://<your-vps-ip>:${MCP_PORT}/mcp`

## MCP Server

The server exposes 12 tools over Streamable HTTP (stateless mode, JSON responses). Any MCP-compatible client can connect to it.

### Connecting

Point your MCP client to the server URL (default `http://<host>:8000/mcp`) using the **Streamable HTTP** transport. The server runs stateless — each request is independent, no session tracking.

Clients must send the `Accept: application/json` header.

#### Testing with MCP Inspector

```bash
npx @modelcontextprotocol/inspector
```

Connect to `http://localhost:8000/mcp` with transport type "Streamable HTTP".

### Available Tools

| Tool | Type | Description |
|------|------|-------------|
| `search_across` | Search | Cross-entity search (members, bills, committees, votes, plenums) |
| `search_members` | Search | Search Knesset members by name, party, role, knesset number |
| `get_member` | Detail | Full member detail: factions, roles, committees |
| `search_committees` | Search | Search committees by name, type, category, status |
| `get_committee` | Detail | Full committee detail with optional sessions, members, bills, documents |
| `search_plenums` | Search | Search plenum sessions by date, name, knesset number |
| `get_plenum` | Detail | Full session detail with agenda items and documents |
| `search_bills` | Search | Search bills by name, status, type, date |
| `get_bill` | Detail | Full bill detail with plenum stages and votes |
| `search_votes` | Search | Search votes by name, date, outcome, linked bill |
| `get_vote` | Detail | Full vote detail with per-member breakdown |

Each search tool's description includes record counts and data freshness. Parameter schemas include enum constraints with exact allowed values where applicable. Use `search_across` to find items across all entity types before drilling down with specific tools.

### Response Size Limits

Responses are capped at `MAX_OUTPUT_TOKENS` characters (configured via `.env`). If a response exceeds this limit, an error is returned instructing the client to add more filters to narrow the results.

### Rate Limiting

Per-IP sliding window rate limiting is applied to all requests. Default: 60 requests/minute, configurable via `RATE_LIMIT_PER_MINUTE` in `.env`.

## Configuration

All configuration is loaded from `.env` (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PATH` | `data.sqlite` | Path to the SQLite database |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8000` | Server port |
| `MCP_ENDPOINT` | `/mcp` | MCP endpoint path |
| `RATE_LIMIT_PER_MINUTE` | `60` | Max requests per IP per minute |
| `MAX_OUTPUT_TOKENS` | `50000` | Max response size in characters |
| `SEARCH_ACROSS_TOP_N` | `5` | Top results per entity in `search_across` |

### Running with uvicorn

```bash
uvicorn mcp_server:app --host 0.0.0.0 --port 8000
```

## Local Query CLI

`local_query.py` is a support tool for querying the same views directly from the command line. All commands output JSON to stdout.

```bash
python local_query.py <command> [options]
```

### Commands

#### `status` -- Database status

```bash
python local_query.py status
```

#### `search-across` -- Cross-entity search

```bash
python local_query.py search-across --query "נתניהו"
python local_query.py search-across --query "חינוך" --top-n 3
```

#### `members` -- Search Knesset members

```bash
python local_query.py members --knesset 20
python local_query.py members --knesset 20 --party "הליכוד"
python local_query.py members --last-name "נתניהו"
python local_query.py members --person-id 839 --committees
```

| Flag | Description |
|------|-------------|
| `--knesset` | Knesset number |
| `--party` | Party name (Hebrew) |
| `--role` | Role description text search |
| `--role-type` | Role type: שר, ח"כ, ראש ממשלה, סגן שר, יו"ר כנסת |
| `--first-name` | First name |
| `--last-name` | Last name |
| `--person-id` | Person ID |
| `--committees` | Include committee memberships in output |

#### `plenums` -- Search plenum sessions

```bash
python local_query.py plenums --knesset 20
python local_query.py plenums --from-date 2015-03-31 --to-date 2015-04-07
```

| Flag | Description |
|------|-------------|
| `--knesset` | Knesset number |
| `--date` | Exact date (YYYY-MM-DD) |
| `--from-date` | Start date |
| `--to-date` | End date |
| `--name` | Session name text search |

#### `plenum` -- Single plenum session detail

```bash
python local_query.py plenum --session-id 12345
```

#### `bills` -- Search bills

```bash
python local_query.py bills --knesset 20 --name "חוק-יסוד"
python local_query.py bills --knesset 20 --sub-type "ממשלתית"
python local_query.py bills --knesset 20 --status "אושר"
```

| Flag | Description |
|------|-------------|
| `--knesset` | Knesset number |
| `--name` | Bill name text search |
| `--sub-type` | Bill sub-type: פרטית, ממשלתית, ועדה |
| `--status` | Status description text search |
| `--date`, `--from-date`, `--to-date` | Date filters |

#### `bill` -- Single bill detail

```bash
python local_query.py bill --bill-id 565913
```

#### `votes` -- Search plenum votes

```bash
python local_query.py votes --knesset 20 --date 2015-03-31
python local_query.py votes --knesset 20 --accepted
python local_query.py votes --bill-id 565913
```

| Flag | Description |
|------|-------------|
| `--knesset` | Knesset number |
| `--date` | Exact date |
| `--from-date`, `--to-date` | Date range |
| `--name` | Title or subject text search |
| `--accepted` | Show only accepted votes |
| `--rejected` | Show only rejected votes |
| `--bill-id` | Filter to votes for a specific bill |

#### `vote` -- Single vote detail

```bash
python local_query.py vote --vote-id 26916
```

## Fetching Data

Populate the local SQLite database (`data.sqlite`) from remote sources:

```bash
# Create schema
python core/db_cli.py init-db

# Fetch all tables
python core/db_cli.py fetch-all

# Incremental update (only rows updated after the given datetime)
python core/db_cli.py fetch-all --since 2025-01-01T00:00:00

# Fetch individual tables
python core/db_cli.py fetch-persons
python core/db_cli.py fetch-bills
python core/db_cli.py fetch-votes
```

### Available fetch commands

| Command | OData Entity | SQLite Table | Strategy |
|---------|-------------|--------------|----------|
| `fetch-persons` | KNS_Person | `person_raw` | CSV+OData |
| `fetch-positions` | KNS_Position | `position_raw` | CSV+OData |
| `fetch-person-to-position` | KNS_PersonToPosition | `person_to_position_raw` | CSV+OData |
| `fetch-plenum-sessions` | KNS_PlenumSession | `plenum_session_raw` | CSV+OData |
| `fetch-plm-session-items` | KNS_PlmSessionItem | `plm_session_item_raw` | CSV+OData |
| `fetch-document-plenum-sessions` | KNS_DocumentPlenumSession | `document_plenum_session_raw` | CSV+OData |
| `fetch-status` | KNS_Status | `status_raw` | OData only |
| `fetch-bills` | KNS_Bill | `bill_raw` | CSV+OData |
| `fetch-committees` | KNS_Committee | `committee_raw` | CSV+OData |
| `fetch-votes` | KNS_PlenumVote | `plenum_vote_raw` | CSV+OData |
| `fetch-vote-results` | KNS_PlenumVoteResult | `plenum_vote_result_raw` | OData only |

## Architecture

```
mcp_server.py             MCP server entry point (12 tools, rate limiting)
local_query.py            CLI for querying views (12 subcommands, JSON output)
config.py                 Configuration (loaded from .env)
core/
  db.py                   SQLite connection (read-only + writable), indexes, metadata
  mcp_meta.py             @mcp_tool decorator — attaches metadata to view functions
  rate_limit.py           Per-IP ASGI rate limiting middleware
  odata_client.py         CSV+OData fetching with pagination
  db_cli.py               Fetch CLI (13 subcommands)
views/                    Query views (12 views, structured search/detail layers)
tables/                   Raw table modules (mirror OData fields exactly)
tests/                    Integration tests against real data.sqlite
```

### Data flow

1. **Fetch**: CSV bulk download + OData incremental updates -> raw SQLite tables
2. **Index**: `ensure_indexes()` creates performance indexes at startup (write access)
3. **Serve**: MCP tools call view functions -> SQL joins over raw tables -> JSON responses via read-only connections

### Key design decisions

- **Read-only queries**: all view functions use `connect_readonly()` (SQLite URI mode `?mode=ro`). Write access is only used once at startup for index creation.
- **Decorator-driven tools**: each view function is decorated with `@mcp_tool(...)` from `core/mcp_meta.py`, which attaches metadata (name, description, entity, count SQL, enum SQL). The MCP server and `search_across` discover tools by inspecting decorated functions -- no central registry file needed.
- **Dynamic schema enrichment**: at startup the MCP server queries the database for enum values, entity counts, and data freshness dates. Enum-constrained parameters are exposed as `Literal[...]` types in the JSON schema, and tool descriptions include record counts and last-data dates.
- **Explicit parameter schemas**: MCP tool handlers have dynamically constructed typed signatures so the MCP Inspector shows proper input fields (not generic kwargs).
- Raw tables mirror OData fields exactly; views provide the structured query layer.
- CSV-first fetching: bulk CSV download, then OData for rows newer than the CSV's max `LastUpdatedDate`.
- List views return summaries; detail views return full nested data.
- When `IsAccepted` is NULL (OData-origin votes), it is inferred from computed totals (`total_for > total_against`).
- Bill stage votes show only the final (decisive) vote per session, excluding section and reservation votes.

## Tests

Integration tests run against the real `data.sqlite` with known historical data from Knessets 19 and 20.

```bash
python -m pytest tests/ -v
```

237 tests across 11 test files covering all 12 views.
