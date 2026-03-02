# Open Knesset Data API

Local SQLite mirror of Israeli Knesset OData tables with structured query views and a JSON CLI.

Fetches data from the [Knesset OData V4 API](https://knesset.gov.il/OdataV4/ParliamentInfo/) using a CSV-first strategy (bulk download from oknesset.org, then incremental OData updates). Query views layer structured search and detail endpoints on top of the raw tables.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Fetching Data

Populate the local SQLite database (`data.sqlite`) from remote sources:

```bash
# Create schema
python core/db_cli.py init-db

# Fetch all tables
python core/db_cli.py fetch-all

# Fetch individual tables
python core/db_cli.py fetch-persons
python core/db_cli.py fetch-bills
python core/db_cli.py fetch-votes
# ... etc

# Incremental update (only rows updated after the given datetime)
python core/db_cli.py fetch-all --since 2025-01-01T00:00:00
```

Vote results (~1.86M rows) have a dedicated parallel fetcher:

```bash
python fetch_vote_results.py              # fetch or resume
python fetch_vote_results.py --workers 5  # set parallelism
python fetch_vote_results.py --reset      # drop and refetch from scratch
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

## Querying Data

All queries output JSON to stdout.

```bash
python local_query.py <command> [options]
```

### Commands

#### `members` -- Search Knesset members

```bash
python local_query.py members --knesset 20
python local_query.py members --knesset 20 --party "הליכוד"
python local_query.py members --knesset 20 --role-type "שר"
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

#### `plenums` -- Search plenum sessions (list)

```bash
python local_query.py plenums --knesset 20
python local_query.py plenums --date 2015-03-31
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

Returns the session with its agenda items and documents.

```bash
python local_query.py plenum --session-id 12345
```

#### `bills` -- Search bills (list)

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

Returns the bill with all plenum stages and the final (decisive) vote per stage.

```bash
python local_query.py bill --bill-id 565913
```

#### `votes` -- Search plenum votes (list)

```bash
python local_query.py votes --knesset 20 --date 2015-03-31
python local_query.py votes --knesset 20 --accepted
python local_query.py votes --bill-id 565913
python local_query.py votes --name "חוק-יסוד" --from-date 2018-07-01
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

Returns the vote with per-MK member breakdown and related votes (same title + session).

```bash
python local_query.py vote --vote-id 26916
```

## Architecture

```
config.py             Configuration (DB path, OData URL, page size)
core/
  odata_client.py     CSV+OData fetching with pagination
  db.py               SQLite connection, indexes, metadata
  db_cli.py           Fetch CLI (13 subcommands)
tables/               Raw table modules (10 tables, mirror OData fields exactly)
views/                Query views (7 views, structured search/detail layers)
tests/                Integration tests against real data.sqlite
local_query.py        Query CLI (7 subcommands, JSON output)
```

### Data flow

1. **Fetch**: CSV bulk download + OData incremental updates -> raw SQLite tables
2. **Query**: SQL joins + aggregation over raw tables -> structured JSON via views

### Key design decisions

- Raw tables mirror OData fields exactly; views provide the structured query layer.
- CSV-first fetching: bulk CSV download, then OData for rows newer than the CSV's max `LastUpdatedDate`.
- List views return summaries only. Detail views (by ID) return full nested data.
- When `IsAccepted` is NULL (OData-origin votes), it is inferred from computed totals (`total_for > total_against`).
- Bill stage votes show only the final (decisive) vote per session, excluding section and reservation votes.
- Performance indexes are created idempotently on every view connection.

## Tests

Integration tests run against the real `data.sqlite` with known historical data from Knessets 19 and 20.

```bash
python -m pytest tests/ -v
```

131 tests across 6 test files covering all 7 views.
