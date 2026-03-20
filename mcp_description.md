# Israeli Knesset Data MCP

Israeli Knesset (parliament) data API — members, committees, bills, agendas, queries, plenum sessions, votes, and Knesset term dates.{sync_line}{knesset_line}

## Getting Started

1. Use `search_across` for broad discovery — it searches all entity types at once and returns the top matches per type.
2. Each search tool's description includes the number of records and data freshness date. Parameter schemas include the exact allowed values where applicable — use those values verbatim.
3. Use `get_knesset_dates` to find which Knesset term is current or to look up term dates before querying other tools.

## Notes
* **Fields with null, empty, or meaningless values are omitted from responses.** Do not assume every field listed in the schema will be present — check for key existence before accessing optional fields. Fields marked `optional: true` in the schema may be absent when the underlying data is empty.
* Use `get_response_schema(tool_name)` to get the full response schema for any tool — field names, types, optionality, descriptions, and nested structure.
* There is rate limiting in place to prevent abuse. Don't use the tools too aggressively — if you get rate limited, wait a bit before retrying.

## Search → Detail Workflow

- **Unified item tools** (`bills`, `agendas`, `queries`) combine search and detail in one tool — use `full_details=True` or provide an item ID to get full detail.
- **Search tools** (`search_members`, `search_votes`) return compact summaries. Use them to find IDs.
- **Detail tools** (`get_member`, `get_vote`) return the full record for a single entity by ID.
- **Session tools** (`plenum_sessions`, `committee_sessions`) combine search and detail in one tool — use `full_details=True` or `session_id` to get full detail.
- **Lookup tools** (`get_knesset_dates`) return reference data grouped by Knesset number.
- Always search first to find the ID, then call the detail tool. Do not guess IDs.

## Always Filter — Responses Are Size-Capped

Responses that exceed the server limit are rejected with an error. The more filters you provide, the smaller and faster the response.

- **`knesset_num`** is the single most important filter — always provide it when you know which Knesset term you need.
- Combine `knesset_num` with name, type, status, or date filters to narrow results further.
- If you get a "Response too large" error, add more filters — do not retry the same query.

## Date Filtering

Session tools (`plenum_sessions`, `committee_sessions`) use `from_date` and `to_date` (both `YYYY-MM-DD`):

- **`from_date` is required** (unless `session_id` is provided). If `to_date` is omitted, it defaults to today.
- **`to_date` requires `from_date`** — providing `to_date` alone is an error.

Item tools (`bills`, `agendas`, `queries`) also use `from_date` and `to_date`:

- **For `bills`**: filters by plenum session appearance date.
- **For `agendas`**: filters by president decision date.
- **For `queries`**: filters by submission date.
- Use `from_date` + `to_date` for date ranges. `from_date` alone filters to a single day.

Other search tools (`search_votes`) use `date` and `date_to`:

- **Use `date` + `date_to` for date ranges.** For example, to get all votes in March 2020: `date="2020-03-01", date_to="2020-03-31"`.
- **`date` alone is a shortcut for a single day** — filters to that exact date only.

## Parameter Types

- IDs (`vote_id`, `bill_id`, `member_id`, `session_id`, `agenda_id`, `query_id`, etc.) are integers.
- `knesset_num` is an integer.
- Boolean flags (`accepted`, `is_current`, `full_details`, etc.) accept `true`/`false`.
- All text filters (names, types, statuses) are Hebrew strings with case-insensitive substring matching.
- Parameters with enum constraints list the exact allowed values in their schema — use those values verbatim (they are in Hebrew).

## Common Patterns with Examples

### Broad discovery

Use `search_across` when you don't know which entity type to look for:

```
search_across(query="חינוך")
→ Returns top matches across members, bills, agendas, queries, committees, votes, and plenums related to "חינוך" (education)

search_across(query="נתניהו", top_n=3)
→ Returns top 3 matches per entity type for "נתניהו" (Netanyahu)
```

### Find a member's activity

Search by name, party, or role, then drill into full detail:

```
search_members(last_name="לפיד", knesset_num=20)
→ Summary: member_id, factions, role_types

get_member(member_id=839, knesset_num=20)
→ Full detail: factions, government roles, parliamentary roles, committee memberships
```

Search by role type to find all members in a specific position:

```
search_members(knesset_num=25, role_type="שר")
→ All ministers in the current Knesset

search_members(knesset_num=20, party="הליכוד", role_type="שר")
→ Likud ministers in the 20th Knesset
```

Get a member's full career across all Knesset terms:

```
get_member(member_id=839)
→ Full history across all terms (omit knesset_num for all terms)
```

### Find a bill and its votes

```
bills(knesset_num=20, name_query="חוק-יסוד")
→ Basic law bills in the 20th Knesset (bill_id, name, status, type)

bills(knesset_num=20, type="ממשלתית", status="אושר בקריאה שלישית")
→ Government bills that passed third reading

bills(bill_id=565913)
→ Full bill detail with plenum stages and the decisive vote at each stage (auto full_details)

bills(knesset_num=20, from_date="2016-01-01", to_date="2016-06-30")
→ Bills with plenum activity in H1 2016
```

### Search agendas (motions for the agenda)

```
agendas(knesset_num=20, name_query="חינוך")
→ Education-related agendas in the 20th Knesset

agendas(agenda_id=12345)
→ Full agenda detail with documents, committee info, minister info (auto full_details)

agendas(knesset_num=20, initiator_id=839)
→ Agendas initiated by member 839 in the 20th Knesset
```

### Search parliamentary queries

```
queries(knesset_num=20, name_query="תקציב")
→ Budget-related queries in the 20th Knesset

queries(query_id=54321)
→ Full query detail with documents, ministry info, reply dates (auto full_details)

queries(knesset_num=20, initiator_id=839)
→ Queries submitted by member 839 in the 20th Knesset
```

### Search votes

```
search_votes(knesset_num=20, date="2015-03-31")
→ All votes on a specific date

search_votes(knesset_num=20, date="2020-03-01", date_to="2020-03-31", accepted=true)
→ All accepted votes in March 2020

search_votes(bill_id=565913)
→ All votes linked to a specific bill

get_vote(vote_id=26916)
→ Full vote detail with per-member breakdown and related votes
```

### Search plenum sessions

```
plenum_sessions(from_date="2015-03-31", to_date="2015-04-07", knesset_num=20)
→ Plenum session summaries in a date range

plenum_sessions(from_date="2015-03-01", to_date="2015-12-31", query_items="תקציב")
→ Plenum sessions with "תקציב" (budget) in the agenda

plenum_sessions(session_id=568294)
→ Full session detail with all agenda items and documents (auto full_details)
```

### Search committee sessions

```
committee_sessions(from_date="2016-01-01", to_date="2016-01-31", knesset_num=20)
→ Committee session summaries in a date range

committee_sessions(committee_name_query="כספים", from_date="2016-01-01", to_date="2016-03-31")
→ Finance committee sessions in Q1 2016

committee_sessions(session_id=2064301)
→ Full session detail with agenda items, documents, and bill votes (auto full_details)

committee_sessions(from_date="2016-01-01", to_date="2016-01-31", member_id=839)
→ Sessions where member 839 served on the committee
```

### Look up Knesset terms and dates

```
get_knesset_dates()
→ All Knesset terms from the 1st to the current, with assembly/plenum periods

get_knesset_dates(knesset_num=25)
→ The current Knesset's term dates: when each assembly and plenum started/ended
```

### Multi-step research examples

**"What bills did Knesset member X vote on?"**
1. `search_members(last_name="X")` → get `member_id`
2. `get_member(member_id=..., knesset_num=25)` → see their committees and roles
3. `search_votes(knesset_num=25, name="...")` → find relevant votes
4. `get_vote(vote_id=...)` → see the member's vote in the per-member breakdown

**"What happened in the Finance Committee last month?"**
1. `committee_sessions(committee_name_query="כספים", from_date="2026-02-01", to_date="2026-02-28", full_details=True)` → sessions with items and documents

**"What how is the last vote on Bill Y divided by party?"**
1. `bills(name_query="Y")` → get `bill_id`
2. `search_votes(bill_id=..., accepted=true)` → get the decisive vote for the bill
3. `get_vote(vote_id=...)` → see the per-member votes
4. `search_members(knesset_num=25)` → get current members with their parties

**"What agendas did member X submit?"**
1. `search_members(last_name="X")` → get `person_id`
2. `agendas(knesset_num=25, initiator_id=...)` → agendas they initiated
3. `agendas(agenda_id=...)` → full detail for a specific agenda

**"What queries were submitted about topic Y?"**
1. `queries(knesset_num=25, name_query="Y")` → find matching queries
2. `queries(query_id=...)` → full detail with documents and ministry response info
