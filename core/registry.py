"""Tool/view registry — single source of truth for all queryable tools.

Used by:
  - ``mcp_server.py`` to register MCP tools and generate descriptions
  - ``views/database_status_view.py`` to report available tools and counts
  - ``local_query.py`` could also use this in the future

Each entry describes one tool (MCP tool = one view function).
"""

from views import (
    members_view,
    member_view,
    committees_view,
    committee_view,
    plenum_sessions_view,
    plenum_session_view,
    bills_view,
    bill_view,
    votes_view,
    vote_view,
)


# ---------------------------------------------------------------------------
# Filter descriptors — reusable building blocks
# ---------------------------------------------------------------------------

def _f(name: str, typ: str, description: str, required: bool = False) -> dict:
    """Shorthand for a filter/parameter descriptor."""
    return {"name": name, "type": typ, "description": description,
            "required": required}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

TOOLS = [
    # ---- Members ----
    {
        "tool_name": "search_members",
        "entity": "Knesset Members",
        "description": (
            "Search for Knesset members (MKs). Returns summary info: name, "
            "gender, knesset number, factions, and role types. "
            "Use get_member for full detail on a single member."
        ),
        "count_sql": "SELECT COUNT(DISTINCT PersonID) FROM person_to_position_raw",
        "filters": [
            _f("knesset_num", "integer", "Knesset number (e.g. 20, 25)"),
            _f("first_name", "string", "First name contains text"),
            _f("last_name", "string", "Last name contains text"),
            _f("role", "string", "Free text search across roles, ministries, committees"),
            _f("role_type", "string", "Position category (e.g. שר, ח\"כ, ראש ממשלה)"),
            _f("party", "string", "Party/faction name contains text"),
            _f("person_id", "integer", "Specific person ID"),
        ],
        "handler": members_view.search_members,
        "handler_param_map": {
            "knesset_num": "knesset_num",
            "first_name": "first_name",
            "last_name": "last_name",
            "role": "role_query",
            "role_type": "role_type",
            "party": "faction_query",
            "person_id": "person_id",
        },
        "is_list": True,
    },
    {
        "tool_name": "get_member",
        "entity": "Knesset Members",
        "description": (
            "Get full detail for a single Knesset member by ID. Includes "
            "factions, government roles, committee memberships, and "
            "parliamentary roles. If knesset_num is omitted, returns all terms."
        ),
        "count_sql": None,
        "filters": [
            _f("member_id", "integer", "Member/Person ID", required=True),
            _f("knesset_num", "integer", "Knesset number (omit for all terms)"),
        ],
        "handler": member_view.get_member,
        "handler_param_map": {
            "member_id": "member_id",
            "knesset_num": "knesset_num",
        },
        "is_list": False,
    },

    # ---- Committees ----
    {
        "tool_name": "search_committees",
        "entity": "Committees",
        "description": (
            "Search for Knesset committees. Returns summary info: name, type, "
            "category, knesset number, current status. "
            "Use get_committee for full detail on a single committee."
        ),
        "count_sql": "SELECT COUNT(*) FROM committee_raw",
        "filters": [
            _f("knesset_num", "integer", "Knesset number"),
            _f("name", "string", "Committee name contains text"),
            _f("committee_type", "string",
               "Committee type (ועדה ראשית, ועדת משנה, ועדה מיוחדת, ועדה משותפת)"),
            _f("category", "string", "Category description contains text"),
            _f("is_current", "boolean", "True for current committees, False for inactive"),
            _f("parent_committee_id", "integer", "Parent committee ID (for sub-committees)"),
        ],
        "handler": committees_view.search_committees,
        "handler_param_map": {
            "knesset_num": "knesset_num",
            "name": "name",
            "committee_type": "committee_type",
            "category": "category",
            "is_current": "is_current",
            "parent_committee_id": "parent_committee_id",
        },
        "is_list": True,
    },
    {
        "tool_name": "get_committee",
        "entity": "Committees",
        "description": (
            "Get full detail for a single committee by ID. Always returns "
            "committee metadata. Use opt-in flags to include sessions, "
            "members, bills, or documents. Date filters narrow the included "
            "data to a time window."
        ),
        "count_sql": None,
        "filters": [
            _f("committee_id", "integer", "Committee ID", required=True),
            _f("knesset_num", "integer", "Knesset number (informational context)"),
            _f("date", "string", "Single date (YYYY-MM-DD) — shortcut for from/to"),
            _f("from_date", "string", "Start of date range (YYYY-MM-DD)"),
            _f("to_date", "string", "End of date range (YYYY-MM-DD)"),
            _f("include_sessions", "boolean", "Include committee sessions"),
            _f("include_members", "boolean", "Include committee members"),
            _f("include_bills", "boolean", "Include bills discussed in committee"),
            _f("include_documents", "boolean", "Include session documents"),
        ],
        "handler": committee_view.get_committee,
        "handler_param_map": {
            "committee_id": "committee_id",
            "knesset_num": "knesset_num",
            "date": "date",
            "from_date": "from_date",
            "to_date": "to_date",
            "include_sessions": "include_sessions",
            "include_members": "include_members",
            "include_bills": "include_bills",
            "include_documents": "include_documents",
        },
        "is_list": False,
    },

    # ---- Plenum Sessions ----
    {
        "tool_name": "search_plenums",
        "entity": "Plenum Sessions",
        "description": (
            "Search for Knesset plenum sessions. Returns summary info: "
            "session ID, knesset number, name, date. "
            "Use get_plenum for full detail including agenda items and documents."
        ),
        "count_sql": "SELECT COUNT(*) FROM plenum_session_raw",
        "filters": [
            _f("knesset_num", "integer", "Knesset number"),
            _f("from_date", "string", "Start of date range (YYYY-MM-DD)"),
            _f("to_date", "string", "End of date range (YYYY-MM-DD)"),
            _f("date", "string", "Exact date (YYYY-MM-DD)"),
            _f("name", "string", "Session or item name contains text"),
            _f("item_type", "string", "Item type contains text"),
        ],
        "handler": plenum_sessions_view.search_sessions,
        "handler_param_map": {
            "knesset_num": "knesset_num",
            "from_date": "from_date",
            "to_date": "to_date",
            "date": "date",
            "name": "name",
            "item_type": "item_type",
        },
        "is_list": True,
    },
    {
        "tool_name": "get_plenum",
        "entity": "Plenum Sessions",
        "description": (
            "Get full detail for a single plenum session by ID. Includes "
            "session metadata, all agenda items, and documents."
        ),
        "count_sql": None,
        "filters": [
            _f("session_id", "integer", "Session ID", required=True),
        ],
        "handler": plenum_session_view.get_session,
        "handler_param_map": {
            "session_id": "session_id",
        },
        "is_list": False,
    },

    # ---- Bills ----
    {
        "tool_name": "search_bills",
        "entity": "Bills",
        "description": (
            "Search for Knesset bills (legislation). Returns summary info: "
            "name, knesset number, type, status, committee, publication. "
            "Use get_bill for full detail including plenum stages and votes."
        ),
        "count_sql": "SELECT COUNT(*) FROM bill_raw",
        "filters": [
            _f("knesset_num", "integer", "Knesset number"),
            _f("name", "string", "Bill name contains text"),
            _f("status", "string", "Current status description contains text"),
            _f("sub_type", "string", "Bill sub-type (פרטית/ממשלתית/ועדה)"),
            _f("from_date", "string", "Plenum session date from (YYYY-MM-DD)"),
            _f("to_date", "string", "Plenum session date to (YYYY-MM-DD)"),
            _f("date", "string", "Plenum session date (YYYY-MM-DD)"),
        ],
        "handler": bills_view.search_bills,
        "handler_param_map": {
            "knesset_num": "knesset_num",
            "name": "name",
            "status": "status",
            "sub_type": "sub_type",
            "from_date": "from_date",
            "to_date": "to_date",
            "date": "date",
        },
        "is_list": True,
    },
    {
        "tool_name": "get_bill",
        "entity": "Bills",
        "description": (
            "Get full detail for a single bill by ID. Includes bill metadata, "
            "plenum stages (readings), and vote results per stage."
        ),
        "count_sql": None,
        "filters": [
            _f("bill_id", "integer", "Bill ID", required=True),
        ],
        "handler": bill_view.get_bill,
        "handler_param_map": {
            "bill_id": "bill_id",
        },
        "is_list": False,
    },

    # ---- Votes ----
    {
        "tool_name": "search_votes",
        "entity": "Plenum Votes",
        "description": (
            "Search for Knesset plenum votes. Returns summary info: title, "
            "subject, date, totals, accepted/rejected. "
            "Use get_vote for full detail including per-member breakdown."
        ),
        "count_sql": "SELECT COUNT(*) FROM plenum_vote_raw",
        "filters": [
            _f("knesset_num", "integer", "Knesset number"),
            _f("name", "string", "Vote title or subject contains text"),
            _f("from_date", "string", "Start of date range (YYYY-MM-DD)"),
            _f("to_date", "string", "End of date range (YYYY-MM-DD)"),
            _f("date", "string", "Exact date (YYYY-MM-DD)"),
            _f("accepted", "boolean", "True=accepted only, False=rejected only"),
            _f("bill_id", "integer", "Filter to votes linked to a specific bill"),
        ],
        "handler": votes_view.search_votes,
        "handler_param_map": {
            "knesset_num": "knesset_num",
            "name": "name",
            "from_date": "from_date",
            "to_date": "to_date",
            "date": "date",
            "accepted": "accepted",
            "bill_id": "bill_id",
        },
        "is_list": True,
    },
    {
        "tool_name": "get_vote",
        "entity": "Plenum Votes",
        "description": (
            "Get full detail for a single plenum vote by ID. Includes vote "
            "metadata, per-member breakdown (who voted for/against/abstained), "
            "and related votes from the same session."
        ),
        "count_sql": None,
        "filters": [
            _f("vote_id", "integer", "Vote ID", required=True),
        ],
        "handler": vote_view.get_vote,
        "handler_param_map": {
            "vote_id": "vote_id",
        },
        "is_list": False,
    },
]


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def get_tool(name: str) -> dict | None:
    """Return a tool entry by name, or None."""
    for t in TOOLS:
        if t["tool_name"] == name:
            return t
    return None


def list_tools() -> list[dict]:
    """Return all tool entries."""
    return list(TOOLS)


def search_tools() -> list[dict]:
    """Return only list/search tools (those with count_sql)."""
    return [t for t in TOOLS if t["count_sql"]]
