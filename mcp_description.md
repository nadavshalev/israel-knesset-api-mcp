# Israeli Knesset Data MCP

Israeli Knesset (parliament) data API — members, bills, votes, committee sessions, plenum sessions, agendas, parliamentary queries, enacted laws, secondary legislation, and Knesset term metadata.{sync_line}{knesset_line}

## Getting Started

1. Use `search_across` for broad discovery — it searches all entity types at once and returns the top matches per type.
2. Each tool's description includes the number of records and data freshness date. Parameter schemas include the exact allowed values where applicable — use those values verbatim.
3. For slowly-changing reference data (assembly dates, committees, ministries, factions, roles), use the **MCP resources** below instead of the `metadata` tool — resources can be cached by clients.

## Notes

* **Fields with null, empty, or meaningless values are omitted from responses.** Do not assume every field will be present — check for key existence before accessing optional fields.
* There is rate limiting in place. Don't call tools too aggressively — if you get rate limited, wait before retrying.

## Tool Overview

There are **11 tools**. Nine are unified tools that combine search and detail in a single call. One is a cross-entity triage tool. One provides term-level reference data.

| Tool | Purpose |
|------|---------|
| `search_across` | Triage: search all entity types at once, get match counts and top results per type |
| `members` | Search Knesset members or get full detail for one member (factions, roles, committees) |
| `votes` | Search plenum votes or get full detail for one vote (per-member breakdown, related votes) |
| `bills` | Search bills or get full detail for one bill (stages, votes, initiators, documents) |
| `agendas` | Search motions for the agenda or get full detail (documents, committee info, minister info) |
| `queries` | Search parliamentary queries (שאילתות) or get full detail (documents, ministry, reply dates) |
| `plenums` | Search plenum sessions or get full session detail (agenda items, documents) |
| `committees` | Search committee sessions or get full session detail (agenda items, documents) |
| `laws` | Search enacted Israeli laws or get full detail (classifications, ministries, changes/amendments, documents) |
| `secondary_laws` | Search secondary legislation (חקיקת משנה — regulations, orders, rules) or get full detail (regulators, authorizing laws, bindings, documents) |
| `metadata` | Knesset term metadata: assembly dates, committees, ministries, factions, general roles |

## Resources

Five MCP resources expose per-Knesset-term reference data. Resources are readable and cacheable — prefer them over `metadata` when you need this data repeatedly.

| Resource URI | Content |
|---|---|
| `knesset://knesset/{{knesset_num}}/assemblies` | Assembly/plenum periods with start and end dates |
| `knesset://knesset/{{knesset_num}}/committees` | Committees with type, parent, dates, and chair list |
| `knesset://knesset/{{knesset_num}}/ministries` | Government ministries with minister, deputies, and members |
| `knesset://knesset/{{knesset_num}}/factions` | Parliamentary factions with member lists |
| `knesset://knesset/{{knesset_num}}/roles` | General roles not tied to a committee/ministry/faction (e.g. Prime Minister, Knesset Speaker) |

Replace `{{knesset_num}}` with the Knesset term number (e.g. `25` for the current term).

Member strings in resources use compact format: `{{id}}: {{name}} ({{party}}) [from {{start}}] [to {{end}}]`. Dates matching the parent entity's span are elided for brevity.

## How Unified Tools Work

All unified tools (`members`, `votes`, `bills`, `agendas`, `queries`, `plenums`, `committees`, `laws`, `secondary_laws`) follow the same pattern:

- **Search mode** (default): provide filters like `knesset_num`, `name_query`, date ranges → returns a list of summary results.
- **Detail mode**: provide an ID parameter (`member_id`, `vote_id`, `bill_id`, `agenda_id`, `query_id`, `session_id`) or set `full_details=True` → returns full nested detail.
- Providing an ID **auto-enables** `full_details=True` — no need to set both.

## Filtering and Response Limits

Responses that exceed the server size limit are rejected with an error. The more filters you provide, the smaller and faster the response.

- **`knesset_num`** is the single most important filter — always provide it when you know which Knesset term you need.
- Combine `knesset_num` with name, type, status, or date filters to narrow results further.
- If you get a "Response too large" error, add more filters — do not retry the same query.

## Date Filtering

### Session tools (`plenums`, `committees`)

Use `from_date` and `to_date` (both `YYYY-MM-DD`):

- **`from_date` is required** (unless `session_id` is provided). If `to_date` is omitted, it defaults to today.
- **`to_date` requires `from_date`** — providing `to_date` alone is an error.

### Item tools (`bills`, `agendas`, `queries`)

Also use `from_date` and `to_date`:

- **`bills`**: filters by plenum session appearance date.
- **`agendas`**: filters by president decision date.
- **`queries`**: filters by submission date.
- Use `from_date` + `to_date` for date ranges. `from_date` alone filters to a single day.

### Vote search (`votes`)

Uses `from_date` and `to_date`:

- **Use `from_date` + `to_date` for date ranges.** Example: to get all votes in March 2020: `from_date="2020-03-01", to_date="2020-03-31"`.
- **`from_date` alone** filters to that single day only.

## Parameter Types

- **IDs** (`vote_id`, `bill_id`, `member_id`, `session_id`, `agenda_id`, `query_id`, `law_id`, `secondary_law_id`) are integers.
- **`knesset_num`** is an integer.
- **Boolean flags** (`accepted`, `full_details`, `include_committee_heads`, etc.) accept `true`/`false`.
- **Text filters** (names, types, statuses) are Hebrew strings with case-insensitive substring matching.
- **Enum parameters** list exact allowed values in their schema — use those values verbatim (they are in Hebrew).

## Tool Details and Examples

### `search_across` — Cross-entity triage

Searches all 9 entity types (members, bills, committees, votes, plenums, agendas, queries, laws, secondary_laws) and returns match counts plus top results per type. At least one filter required.

**Parameters:** `query` (text), `knesset_num`, `date`, `date_to`, `top_n`

```
search_across(query="חינוך")
→ Top matches across all entity types for "חינוך" (education)

search_across(query="נתניהו", top_n=3)
→ Top 3 per entity type for "נתניהו"

search_across(knesset_num=25, query="תקציב")
→ Budget-related matches in the current Knesset
```

### `members` — Knesset members

**Search parameters:** `knesset_num`, `first_name`, `last_name`, `role` (free text across all roles), `role_type` (position category), `party`

**Detail parameter:** `member_id` (auto-enables full_details; omit `knesset_num` for all terms)

**Search returns:** member_id, name, gender, knesset_num, factions, role_types

**Detail adds:** government roles (title, ministry, dates), committee memberships (name, role, dates), parliamentary roles (name, role, dates)

```
members(knesset_num=25, role_type="שר")
→ All current ministers

members(last_name="לפיד", knesset_num=20)
→ Members named Lapid in the 20th Knesset

members(member_id=839, knesset_num=20)
→ Full detail for member 839 in the 20th Knesset

members(member_id=839)
→ Full career across all Knesset terms
```

### `votes` — Plenum votes

**Search parameters:** `knesset_num`, `name`, `from_date`, `to_date`, `accepted` (true/false/omit), `bill_id`

**Detail parameter:** `vote_id` (auto-enables full_details)

**Search returns:** vote_id, bill_id, title, subject, date, totals (for/against/abstain), is_accepted

**Detail adds:** per-member breakdown (member_id, name, party, result), related votes from same session

```
votes(knesset_num=20, from_date="2015-03-31")
→ All votes on March 31, 2015

votes(knesset_num=20, from_date="2020-03-01", to_date="2020-03-31", accepted=true)
→ Accepted votes in March 2020

votes(bill_id=565913)
→ All votes linked to bill 565913

votes(vote_id=26916)
→ Full vote detail with per-member breakdown
```

### `bills` — Legislation

**Search parameters:** `knesset_num`, `name_query`, `status`, `type` (פרטית/ממשלתית/ועדה), `initiator_id`, `from_date`, `to_date`

**Detail parameter:** `bill_id` (auto-enables full_details)

**Search returns:** bill_id, name, knesset_num, type, status, committee, publication_date, primary_initiators

**Detail adds:** stages (plenum and committee, with votes), full initiator lists (primary/added/removed), name history, documents, split/merged bills

```
bills(knesset_num=20, name_query="חוק-יסוד")
→ Basic law bills in the 20th Knesset

bills(knesset_num=20, type="ממשלתית", status="אושר בקריאה שלישית")
→ Government bills that passed third reading

bills(bill_id=565913)
→ Full detail with stages, votes, initiators, documents

bills(knesset_num=20, from_date="2016-01-01", to_date="2016-06-30")
→ Bills with plenum activity in H1 2016
```

### `agendas` — Motions for the agenda

**Search parameters:** `knesset_num`, `name_query`, `status`, `type`, `initiator_id`, `from_date`, `to_date`

**Detail parameter:** `agenda_id` (auto-enables full_details)

**Search returns:** agenda_id, name, knesset_num, classification, type, status, initiator_name

**Detail adds:** stages, leading agenda, government recommendation, committee details, minister info, documents

```
agendas(knesset_num=20, name_query="חינוך")
→ Education-related agendas

agendas(knesset_num=20, initiator_id=839)
→ Agendas initiated by member 839

agendas(agenda_id=12345)
→ Full detail with documents, committee info, stages
```

### `queries` — Parliamentary queries (שאילתות)

**Search parameters:** `knesset_num`, `name_query`, `status`, `type`, `initiator_id`, `from_date`, `to_date`

**Detail parameter:** `query_id` (auto-enables full_details)

**Search returns:** query_id, name, knesset_num, type, status, submitter_name, gov_ministry_name

**Detail adds:** stages, submit date, ministry details, reply dates (planned + actual), documents

```
queries(knesset_num=20, name_query="תקציב")
→ Budget-related queries

queries(knesset_num=20, initiator_id=839)
→ Queries submitted by member 839

queries(query_id=54321)
→ Full detail with documents and ministry response info
```

### `plenums` — Plenum sessions

**Search parameters:** `knesset_num`, `from_date` (required unless session_id), `to_date`, `query_items` (text search in session/item names), `item_type`

**Detail parameter:** `session_id` (auto-enables full_details)

**Search returns:** session_id, knesset_num, name, date, item_count

**Detail adds:** agenda items (item_id, type, name, status, bill_id, votes), documents

```
plenums(from_date="2015-03-31", to_date="2015-04-07", knesset_num=20)
→ Plenum session summaries in a date range

plenums(from_date="2015-03-01", to_date="2015-12-31", query_items="תקציב")
→ Sessions with "תקציב" (budget) in the agenda

plenums(session_id=568294)
→ Full session detail with agenda items and documents
```

### `committees` — Committee sessions

**Search parameters:** `knesset_num`, `from_date` (required unless session_id), `to_date`, `committee_id`, `committee_name_query`, `query_items`, `item_type`, `member_id`, `session_type`, `status`

**Detail parameter:** `session_id` (auto-enables full_details)

**Search returns:** session_id, committee_id, committee_name, knesset_num, date, item_count

**Detail adds:** session number, times, type, status, location, URLs, note, agenda items (with votes), documents

```
committees(from_date="2016-01-01", to_date="2016-01-31", knesset_num=20)
→ All committee sessions in January 2016

committees(committee_name_query="כספים", from_date="2016-01-01", to_date="2016-03-31")
→ Finance committee sessions in Q1 2016

committees(session_id=2064301)
→ Full detail with agenda items, documents, and bill votes

committees(from_date="2016-01-01", to_date="2016-01-31", member_id=839)
→ Sessions where member 839 served on the committee
```

### `laws` — Enacted Israeli laws

**Search parameters:** `knesset_num`, `name_query`, `law_type` (חוק יסוד/חוק תקציב/חוק מועדף), `law_validity`, `from_date`, `to_date`

**Detail parameter:** `law_id` (auto-enables full_details)

**Search returns:** law_id, name, knesset_num, law_types, publication_date, latest_publication_date, law_validity

**Detail adds:** validity dates/notes, classifications, ministries, alternative names, replaced laws, original bill, changes (bill + amendments + corrections grouped together), documents

```
laws(knesset_num=25)
→ All laws enacted in the 25th Knesset

laws(law_type="חוק יסוד")
→ All basic laws

laws(knesset_num=20, name_query="חינוך")
→ Education-related laws in the 20th Knesset

laws(law_id=12345)
→ Full detail with classifications, ministries, changes, documents
```

### `secondary_laws` — Secondary legislation (חקיקת משנה)

**Search parameters:** `knesset_num`, `name_query`, `type` (תקנות/צו/כללים/etc.), `status`, `classification`, `is_current` (true/false), `authorizing_law_id`, `from_date`, `to_date`

**Detail parameter:** `secondary_law_id` (auto-enables full_details)

**Search returns:** secondary_law_id, name, knesset_num, type, status, is_current, publication_date, committee_name, major_authorizing_law_id, major_authorizing_law_name

**Detail adds:** classification, publication series/page, committee/secretary/plenum dates, regulators (issuing authorities), authorizing primary laws (full partial), sec-to-sec bindings (child/parent/main relationships), documents

```
secondary_laws(knesset_num=25, type="תקנות")
→ Regulations in the 25th Knesset

secondary_laws(knesset_num=20, is_current=true)
→ Current secondary laws from the 20th Knesset

secondary_laws(authorizing_law_id=2000156, knesset_num=20)
→ Secondary laws authorized by a specific primary law

secondary_laws(secondary_law_id=2067535)
→ Full detail with regulators, authorizing laws, bindings, documents
```

### `metadata` — Knesset term reference data

Returns structured reference data for a single Knesset term in one call. Prefer the individual **MCP resources** (`knesset://knesset/{{knesset_num}}/...`) when you only need one section or want cached data. Use `metadata` when you need everything at once.

**Required parameter:** `knesset_num`

**Optional flags:** `include_committee_heads`, `include_ministry_members`, `include_faction_members`

**Returns:**
- `knesset_assemblies`: assembly/plenum periods with start/end dates
- `committees`: committee_id, name, type, parent, dates. With `include_committee_heads=True`: chair list.
- `gov_ministries`: ministry_id, name. With `include_ministry_members=True`: separate `minister`, `deputy_ministers`, and `members` lists (empty fields omitted).
- `factions`: faction_id, name, dates. With `include_faction_members=True`: member list.
- `general_roles`: always present — parliamentary roles not linked to committees/ministries/factions (e.g. Prime Minister, Knesset Speaker). Each role has a `position` title and `holders` list. Excludes generic "חבר כנסת" role.

Member strings use compact format: `{{id}}: {{name}} ({{party}}) [from {{start}}] [to {{end}}]`. Dates matching the parent entity's span are elided for brevity.

```
metadata(knesset_num=25)
→ Assembly dates, committees, ministries, factions, general roles

metadata(knesset_num=20, include_committee_heads=True)
→ Same, plus committee chairs

metadata(knesset_num=20, include_ministry_members=True)
→ Same, plus ministry members split by minister / deputy / other

metadata(knesset_num=20, include_faction_members=True)
→ Same, plus faction member lists
```

## Multi-step Research Patterns

**"What bills did member X vote on?"**
1. `members(last_name="X")` → get `member_id`
2. `members(member_id=..., knesset_num=25)` → see their committees and roles
3. `votes(knesset_num=25, name="...")` → find relevant votes
4. `votes(vote_id=...)` → see the member's vote in the per-member breakdown

**"What happened in the Finance Committee last month?"**
1. `committees(committee_name_query="כספים", from_date="2026-02-01", to_date="2026-02-28", full_details=True)` → sessions with items and documents

**"How is the last vote on Bill Y divided by party?"**
1. `bills(name_query="Y")` → get `bill_id`
2. `votes(bill_id=..., accepted=True)` → get the decisive vote
3. `votes(vote_id=...)` → see the per-member votes with party affiliation
4. `members(knesset_num=25)` → get current members with their parties

**"What agendas did member X submit?"**
1. `members(last_name="X")` → get `member_id`
2. `agendas(knesset_num=25, initiator_id=...)` → agendas they initiated
3. `agendas(agenda_id=...)` → full detail for a specific agenda

**"What queries were submitted about topic Y?"**
1. `queries(knesset_num=25, name_query="Y")` → find matching queries
2. `queries(query_id=...)` → full detail with documents and ministry response info

**"What law did bill X become?"**
1. `bills(name_query="X")` → get `bill_id`
2. `laws(knesset_num=25, name_query="X")` → find the enacted law
3. `laws(law_id=...)` → full detail with changes and documents

**"What regulations were issued under law X?"**
1. `laws(name_query="X")` → get `law_id`
2. `secondary_laws(authorizing_law_id=..., knesset_num=25)` → regulations authorized by that law
3. `secondary_laws(secondary_law_id=...)` → full detail with regulators and bindings

**"Who are the ministers in the current Knesset?"**
1. `metadata(knesset_num=25, include_ministry_members=True)` → all ministries with minister/deputy/member lists
2. Or: `members(knesset_num=25, role_type="שר")` → all ministers with their factions
