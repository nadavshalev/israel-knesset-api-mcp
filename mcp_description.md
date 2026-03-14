# Israeli Knesset Data MCP

Israeli Knesset (parliament) data API — members, committees, bills, plenum sessions, votes, and Knesset term dates.{sync_line}{knesset_line}

## Getting Started

1. Use `search_across` for broad discovery — it searches all entity types at once and returns the top matches per type.
2. Each search tool's description includes the number of records and data freshness date. Parameter schemas include the exact allowed values where applicable — use those values verbatim.
3. Use `get_knesset_dates` to find which Knesset term is current or to look up term dates before querying other tools.

## Notes
* There is rate limiting in place to prevent abuse. Don't use the tools too aggressively — if you get rate limited, wait a bit before retrying.

## Search → Detail Workflow

- **Search tools** (`search_members`, `search_bills`, `search_votes`, `search_committees`, `search_plenums`) return compact summaries. Use them to find IDs.
- **Detail tools** (`get_member`, `get_bill`, `get_vote`, `get_committee`, `get_plenum`) return the full record for a single entity by ID.
- **Lookup tools** (`get_knesset_dates`) return reference data grouped by Knesset number.
- Always search first to find the ID, then call the detail tool. Do not guess IDs.

## Always Filter — Responses Are Size-Capped

Responses that exceed the server limit are rejected with an error. The more filters you provide, the smaller and faster the response.

- **`knesset_num`** is the single most important filter — always provide it when you know which Knesset term you need.
- Combine `knesset_num` with name, type, status, or date filters to narrow results further.
- If you get a "Response too large" error, add more filters — do not retry the same query.

## Date Filtering — Use Ranges, Not Single Days

Several search tools accept `date` and `date_to` (both in `YYYY-MM-DD` format).

- **Use `date` + `date_to` for date ranges.** For example, to get all votes in March 2020: `date="2020-03-01", date_to="2020-03-31"`. Do NOT send a separate request for each day — a single range query is faster and uses one response.
- **`date` alone is a shortcut for a single day** — filters to that exact date only.
- **Always provide `date_to` when you need a range**, otherwise `date` alone filters to just that one day.
- Date-filterable search tools: `search_votes`, `search_bills`, `search_plenums`. The detail tool `get_committee` also accepts date params to scope its sessions, members, bills, and documents.

## Parameter Types

- IDs (`vote_id`, `bill_id`, `member_id`, etc.) are integers.
- `knesset_num` is an integer.
- Boolean flags (`accepted`, `is_current`, `include_sessions`, etc.) accept `true`/`false`.
- All text filters (names, types, statuses) are Hebrew strings with case-insensitive substring matching.
- Parameters with enum constraints list the exact allowed values in their schema — use those values verbatim (they are in Hebrew).

## Common Patterns with Examples

### Broad discovery

Use `search_across` when you don't know which entity type to look for:

```
search_across(query="חינוך")
→ Returns top matches across members, bills, committees, votes, and plenums related to "חינוך" (education)

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
search_bills(knesset_num=20, name="חוק-יסוד")
→ Basic law bills in the 20th Knesset (bill_id, name, status, sub_type)

search_bills(knesset_num=20, sub_type="ממשלתית", status="אושר בקריאה שלישית")
→ Government bills that passed third reading

get_bill(bill_id=565913)
→ Full bill detail with plenum stages and the decisive vote at each stage
```

Use date filters to find bills discussed in a specific period:

```
search_bills(knesset_num=20, date="2016-01-01", date_to="2016-06-30")
→ Bills with plenum activity in H1 2016
```

### Explore a committee

```
search_committees(knesset_num=25, committee_type="ועדה ראשית")
→ All main committees in the current Knesset

search_committees(knesset_num=20, name="כספים")
→ Finance-related committees in the 20th Knesset

get_committee(committee_id=928, include_sessions=True, date="2016-01-01", date_to="2016-06-30")
→ Committee 928's sessions in H1 2016

get_committee(committee_id=928, include_members=True, include_bills=True)
→ All members who served on the committee and all bills discussed
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
search_plenums(knesset_num=20, date="2015-03-31", date_to="2015-04-07")
→ Plenum sessions in a date range

search_plenums(knesset_num=20, name="תקציב")
→ Plenum sessions with "תקציב" (budget) in the agenda

get_plenum(session_id=12345)
→ Full session detail with all agenda items and documents
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
1. `search_committees(knesset_num=25, name="כספים")` → get `committee_id`
2. `get_committee(committee_id=..., include_sessions=True, include_bills=True, date="2026-02-01", date_to="2026-02-28")` → sessions and bills from February

**"What how is the last vote on Bill Y devided by party?"**
1. `search_bills(name="Y")` → get `bill_id`
2. `search_votes(bill_id=..., accepted=true)` → get the decisive vote for the bill
3. `get_vote(vote_id=...)` → see the per-member votes
4. `search_members(knesset_num=25)` → get current members with their parties