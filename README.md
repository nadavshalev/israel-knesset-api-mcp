# Open Knesset Data API

MCP server exposing Israeli Knesset (parliament) data — members, bills, votes, committee sessions, plenum sessions, agendas, parliamentary queries, and term metadata. Connects AI clients to a PostgreSQL database via the [Model Context Protocol](https://modelcontextprotocol.io/) (Streamable HTTP transport).

Data is sourced from the [Knesset OData V4 API](https://knesset.gov.il/OdataV4/ParliamentInfo/) with a CSV-first bulk-load strategy and incremental OData updates.

---

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env

# Initialize DB schema and fetch data
python core/db_cli.py init-db
python core/db_cli.py fetch-all

# Start the MCP server
python mcp_server.py
```

The server starts at `http://0.0.0.0:8000/mcp` by default.

---

## Docker Deployment

```bash
cp .env.example .env
# Set POSTGRES_* vars and UPDATE_* schedule in .env
docker compose up -d --build

docker compose ps
docker compose logs -f mcp
docker compose logs -f updater
```

Two services run:
- **`mcp`** — the MCP HTTP server
- **`updater`** — background worker running `update_all.py` on a configured day/hour schedule

---

## MCP Tools

9 tools exposed over Streamable HTTP (stateless, JSON responses). Seven are unified tools that combine search and detail in a single call via a `full_details` flag (auto-enabled when an ID is provided). One is a cross-entity triage tool. One provides term-level reference data.

| Tool | Description |
|------|-------------|
| `search_across` | Cross-entity triage — searches members, bills, committees, votes, plenums, agendas, and queries in one call |
| `members` | Search Knesset members or get full detail (factions, government roles, committees) |
| `votes` | Search plenum votes or get full detail (per-member breakdown, related votes) |
| `bills` | Search bills or get full detail (stages, votes, initiators, documents) |
| `agendas` | Search motions for the agenda or get full detail |
| `queries` | Search parliamentary queries or get full detail |
| `plenums` | Search plenum sessions or get full session detail (agenda items, documents) |
| `committees` | Search committee sessions or get full session detail (agenda items, documents) |
| `metadata` | Knesset term metadata: assembly dates, committees, ministries, factions, general roles |

### Connecting

Point any MCP-compatible client to `http://<host>:8000/mcp` using the **Streamable HTTP** transport. The server is stateless — no session tracking.

```bash
# Test with MCP Inspector
npx @modelcontextprotocol/inspector
# Connect to http://localhost:8000/mcp, transport: Streamable HTTP
```

### Response Size Limits

Responses are capped at `MAX_OUTPUT_TOKENS` characters. If exceeded, an error is returned — add more filters and retry.

### Rate Limiting

Per-IP sliding window: 60 requests/minute by default, configurable via `RATE_LIMIT_PER_MINUTE`.

---

## Configuration

All config is in `.env` (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | — | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | — | Database name |
| `POSTGRES_USER` | — | Database user |
| `POSTGRES_PASSWORD` | — | Database password |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8000` | Server port |
| `MCP_ENDPOINT` | `/mcp` | MCP endpoint path |
| `RATE_LIMIT_PER_MINUTE` | `60` | Max requests per IP per minute |
| `MAX_OUTPUT_TOKENS` | `50000` | Max response size in characters |
| `SEARCH_ACROSS_TOP_N` | `5` | Top results per entity in `search_across` |
| `UPDATE_CYCLE_DAYS` | `1` | Updater schedule: days between runs |
| `UPDATE_HOUR_IN_DAY` | `3` | Updater schedule: hour of day (0–23) |
| `UPDATE_RUN_ON_START` | `false` | Run updater immediately on container start |

---

## Local Query CLI

`scripts/local_query.py` queries the same views from the command line. All output is JSON to stdout.

```bash
python scripts/local_query.py <command> [options]
```

### Commands

#### `search-across`
```bash
python scripts/local_query.py search-across "נתניהו"
python scripts/local_query.py search-across "חינוך" --top-n 3
```

#### `members`
```bash
python scripts/local_query.py members --knesset 20
python scripts/local_query.py members --knesset 20 --party "הליכוד"
python scripts/local_query.py members --last-name "נתניהו"
python scripts/local_query.py members --role-type "שר"
```

#### `member`
```bash
python scripts/local_query.py member --member-id 839
python scripts/local_query.py member --member-id 839 --knesset 20
```

#### `plenums`
```bash
python scripts/local_query.py plenums --knesset 20 --from-date 2015-03-31 --to-date 2015-04-07
python scripts/local_query.py plenums --session-id 568294 --full-details
```

#### `committees`
```bash
python scripts/local_query.py committees --knesset 20 --from-date 2016-01-01 --to-date 2016-01-31
python scripts/local_query.py committees --committee-name "כספים" --from-date 2016-01-01 --to-date 2016-03-31
python scripts/local_query.py committees --session-id 2064301 --full-details
```

#### `bills`
```bash
python scripts/local_query.py bills --knesset 20 --name "חוק-יסוד"
python scripts/local_query.py bills --knesset 20 --type "ממשלתית" --status "אושר"
python scripts/local_query.py bills --bill-id 565913
```

#### `agendas`
```bash
python scripts/local_query.py agendas --knesset 20 --name "חינוך"
python scripts/local_query.py agendas --agenda-id 12345
```

#### `queries`
```bash
python scripts/local_query.py queries --knesset 20 --name "תקציב"
python scripts/local_query.py queries --query-id 54321
```

#### `votes`
```bash
python scripts/local_query.py votes --knesset 20 --date 2015-03-31
python scripts/local_query.py votes --knesset 20 --accepted
python scripts/local_query.py votes --bill-id 565913
```

#### `vote`
```bash
python scripts/local_query.py vote --vote-id 26916
```

#### `metadata`
```bash
python scripts/local_query.py metadata --knesset 25
python scripts/local_query.py metadata --knesset 20 --committee-heads --ministry-members
```

---

## Data Fetching

```bash
# Initialize schema
python core/db_cli.py init-db

# Fetch all tables from scratch
python core/db_cli.py fetch-all

# Incremental update (rows updated after a given datetime)
python core/db_cli.py fetch-all --since 2025-01-01T00:00:00
```

### Fetch Commands

| Command | OData Entity | DB Table |
|---------|-------------|----------|
| `fetch-persons` | KNS_Person | `person_raw` |
| `fetch-positions` | KNS_Position | `position_raw` |
| `fetch-person-to-position` | KNS_PersonToPosition | `person_to_position_raw` |
| `fetch-plenum-sessions` | KNS_PlenumSession | `plenum_session_raw` |
| `fetch-plm-session-items` | KNS_PlmSessionItem | `plm_session_item_raw` |
| `fetch-document-plenum-sessions` | KNS_DocumentPlenumSession | `document_plenum_session_raw` |
| `fetch-status` | KNS_Status | `status_raw` |
| `fetch-bills` | KNS_Bill | `bill_raw` |
| `fetch-committees` | KNS_Committee | `committee_raw` |
| `fetch-committee-sessions` | KNS_CommitteeSession | `committee_session_raw` |
| `fetch-votes` | KNS_PlenumVote | `plenum_vote_raw` |
| `fetch-vote-results` | KNS_PlenumVoteResult | `plenum_vote_result_raw` |
| `fetch-knesset-dates` | KNS_KnessetDates | `knesset_dates_raw` |
| `fetch-factions` | KNS_Faction | `faction_raw` |

---

## Architecture

```
mcp_server.py             MCP server entry point — tool registration, rate limiting, size caps
scripts/local_query.py    CLI for querying views directly (JSON output)
config.py                 Environment-based configuration
core/
  db.py                   PostgreSQL connections (read-only + writable), index management
  mcp_meta.py             @mcp_tool decorator — attaches metadata, drives auto-discovery
  search_meta.py          register_search() — plugs entity types into search_across
  session_models.py       Shared models for session items and documents
  rate_limit.py           Per-IP ASGI rate limiting middleware
  odata_client.py         CSV bulk-load + OData incremental fetching
  db_cli.py               Fetch CLI
origins/
  bills/                  bills tool
  members/                search_members + get_member tools
  votes/                  search_votes + get_vote tools
  agendas/                agendas tool
  queries/                queries tool
  plenums/                plenums tool
  committees/             committees tool
  knesset/                metadata tool
  search/                 search_across tool
tests/                    Integration tests against live DB (Knessets 19, 20 — stable data)
```

### How tools are registered

Each view function is decorated with `@mcp_tool(name=..., description=..., ...)` from `core/mcp_meta.py`. The decorator registers the function in a global list. At server startup, `mcp_server.py` iterates that list, enriches each description with live DB stats (record count, last-updated date, enum values), and registers the function as an MCP tool with a typed signature.

`search_across` works the same way: each entity view calls `register_search(...)` at import time, and `search_across` queries all registered builders.

### Key design decisions

- **Stateless / read-only queries**: all view functions use `connect_readonly()`. Write access only happens once at startup for index creation.
- **Unified tools**: `bills`, `agendas`, `queries`, `plenums`, and `committees` combine search and detail in a single tool — pass an ID to get full detail, or `from_date` to search.
- **Dynamic schema enrichment**: enum-constrained parameters are exposed as `Literal[...]` types built from live DB values at startup. Tool descriptions include record counts and freshness dates.
- **CSV-first fetching**: bulk CSV download sets the baseline; OData fetches only rows newer than the CSV's max `LastUpdatedDate`.
- **Compact responses**: `KNSBaseModel` omits null/empty/default fields. Member strings use compact formatting with date elision.

---

## Tests

Integration tests run against the live DB using stable historical data from Knessets 19 and 20.

```bash
.venv/bin/python -m pytest tests/ -v
```
