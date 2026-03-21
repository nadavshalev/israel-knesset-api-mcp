import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from core.db import connect_db, ensure_indexes
from origins.members import members_view
from origins.plenums import plenum_sessions_view as plenums_view
from origins.committees import committee_sessions_view as committees_view
from origins.bills import bills_view
from origins.agendas import agendas_view
from origins.queries import queries_view
from origins.votes import votes_view
from origins.search import search_across_view
from origins.knesset import metadata_view


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _output(results) -> None:
    json.dump(results, sys.stdout, ensure_ascii=False, indent=2, default=str)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query Knesset PostgreSQL database")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- members (list) ---
    members_p = sub.add_parser("members", help="Search Knesset members (summary, no detailed roles)")
    members_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    members_p.add_argument("--role", type=str, default=None, help="Role description contains text")
    members_p.add_argument("--role-type", dest="role_type", type=str, default=None,
                           help="Role type (שר, ח\"כ, ראש ממשלה, סגן שר, יו\"ר כנסת)")
    members_p.add_argument("--party", type=str, default=None, help="Party/faction name contains text")
    members_p.add_argument("--first-name", dest="first_name", type=str, default=None, help="First name contains")
    members_p.add_argument("--last-name", dest="last_name", type=str, default=None, help="Last name contains")
    members_p.add_argument("--person-id", dest="person_id", type=int, default=None, help="Person ID (auto-enables full detail)")
    members_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include roles, committees, government positions")

    # --- committees ---
    cmt_p = sub.add_parser("committees", help="Search committee sessions")
    cmt_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    cmt_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    cmt_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    cmt_p.add_argument("--committee-id", dest="committee_id", type=int, default=None, help="Filter by committee ID")
    cmt_p.add_argument("--committee-name", dest="committee_name_query", type=str, default=None, help="Committee name contains text")
    cmt_p.add_argument("--query-items", dest="query_items", type=str, default=None, help="Item name contains text")
    cmt_p.add_argument("--session-id", dest="session_id", type=int, default=None, help="Get a specific session by ID")
    cmt_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include items and documents")

    # --- plenums ---
    plenum_p = sub.add_parser("plenums", help="Search plenum sessions")
    plenum_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    plenum_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    plenum_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    plenum_p.add_argument("--query-items", dest="query_items", type=str, default=None, help="Session/item name contains text")
    plenum_p.add_argument("--item-type", dest="item_type", type=str, default=None, help="Item type contains text")
    plenum_p.add_argument("--session-id", dest="session_id", type=int, default=None, help="Get a specific session by ID")
    plenum_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include items and documents")

    # --- bills (unified) ---
    bills_p = sub.add_parser("bills", help="Search bills or get full detail")
    bills_p.add_argument("--bill-id", dest="bill_id", type=int, default=None, help="Bill ID (auto-enables full details)")
    bills_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    bills_p.add_argument("--name", dest="name_query", type=str, default=None, help="Bill name contains text")
    bills_p.add_argument("--status", type=str, default=None, help="Current status contains text")
    bills_p.add_argument("--type", type=str, default=None, help="Bill type (פרטית/ממשלתית/ועדה)")
    bills_p.add_argument("--initiator-id", dest="initiator_id", type=int, default=None, help="Initiator person ID")
    bills_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    bills_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    bills_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include stages, votes, initiators, documents")

    # --- agendas (unified) ---
    agendas_p = sub.add_parser("agendas", help="Search agendas or get full detail")
    agendas_p.add_argument("--agenda-id", dest="agenda_id", type=int, default=None, help="Agenda ID (auto-enables full details)")
    agendas_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    agendas_p.add_argument("--name", dest="name_query", type=str, default=None, help="Agenda name contains text")
    agendas_p.add_argument("--status", type=str, default=None, help="Status contains text")
    agendas_p.add_argument("--type", type=str, default=None, help="Sub-type contains text")
    agendas_p.add_argument("--initiator-id", dest="initiator_id", type=int, default=None, help="Initiator person ID")
    agendas_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    agendas_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    agendas_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include documents, committee details, minister info")

    # --- queries (unified) ---
    queries_p = sub.add_parser("queries", help="Search parliamentary queries or get full detail")
    queries_p.add_argument("--query-id", dest="query_id", type=int, default=None, help="Query ID (auto-enables full details)")
    queries_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    queries_p.add_argument("--name", dest="name_query", type=str, default=None, help="Query name/subject contains text")
    queries_p.add_argument("--status", type=str, default=None, help="Status contains text")
    queries_p.add_argument("--type", type=str, default=None, help="Query type contains text")
    queries_p.add_argument("--initiator-id", dest="initiator_id", type=int, default=None, help="Submitter person ID")
    queries_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    queries_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    queries_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include documents, ministry info, reply dates")

    # --- votes (list) ---
    votes_p = sub.add_parser("votes", help="Search plenum votes (summary)")
    votes_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    votes_p.add_argument("--bill-id", dest="bill_id", type=int, default=None, help="Filter votes by bill ID")
    votes_p.add_argument("--name", type=str, default=None, help="Vote title/subject contains text")
    votes_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Start of date range (YYYY-MM-DD)")
    votes_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="End of date range (YYYY-MM-DD)")
    votes_p.add_argument("--vote-id", dest="vote_id", type=int, default=None, help="Vote ID (auto-enables full detail)")
    votes_p.add_argument("--accepted", dest="accepted", default=None, action="store_true", help="Accepted votes only")
    votes_p.add_argument("--rejected", dest="rejected", default=None, action="store_true", help="Rejected votes only")
    votes_p.add_argument("--full-details", dest="full_details", action="store_true", help="Include per-member breakdown and related votes")

    # --- search-across ---
    sa_p = sub.add_parser("search-across", help="Search across all entity types (triage tool)")
    sa_p.add_argument("query", type=str, help="Free-text search term")
    sa_p.add_argument("--top-n", dest="top_n", type=int, default=None,
                       help="Max results per entity type (default from config)")

    # --- metadata ---
    meta_p = sub.add_parser("metadata", help="Get Knesset term metadata (assemblies, committees, ministries, factions)")
    meta_p.add_argument("--knesset", type=int, required=True, help="Knesset number (required)")
    meta_p.add_argument("--committee-heads", dest="committee_heads", action="store_true", help="Include committee heads")
    meta_p.add_argument("--ministry-members", dest="ministry_members", action="store_true", help="Include ministry members")
    meta_p.add_argument("--faction-members", dest="faction_members", action="store_true", help="Include faction members")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Ensure indexes exist (requires write access, done once at startup)
    conn = connect_db()
    ensure_indexes(conn)
    conn.close()

    if args.command == "members":
        results = members_view.members(
            knesset_num=args.knesset,
            first_name=args.first_name,
            last_name=args.last_name,
            role=args.role,
            role_type=args.role_type,
            party=args.party,
            member_id=args.person_id,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "committees":
        results = committees_view.committees(
            session_id=args.session_id,
            committee_id=args.committee_id,
            committee_name_query=args.committee_name_query,
            knesset_num=args.knesset,
            from_date=args.from_date,
            to_date=args.to_date,
            query_items=args.query_items,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "plenums":
        results = plenums_view.plenums(
            session_id=args.session_id,
            knesset_num=args.knesset,
            from_date=args.from_date,
            to_date=args.to_date,
            query_items=args.query_items,
            item_type=args.item_type,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "bills":
        results = bills_view.bills(
            bill_id=args.bill_id,
            knesset_num=args.knesset,
            name_query=args.name_query,
            status=args.status,
            type=args.type,
            initiator_id=args.initiator_id,
            from_date=args.from_date,
            to_date=args.to_date,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "agendas":
        results = agendas_view.agendas(
            agenda_id=args.agenda_id,
            knesset_num=args.knesset,
            name_query=args.name_query,
            status=args.status,
            type=args.type,
            initiator_id=args.initiator_id,
            from_date=args.from_date,
            to_date=args.to_date,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "queries":
        results = queries_view.queries(
            query_id=args.query_id,
            knesset_num=args.knesset,
            name_query=args.name_query,
            status=args.status,
            type=args.type,
            initiator_id=args.initiator_id,
            from_date=args.from_date,
            to_date=args.to_date,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "votes":
        accepted = None
        if getattr(args, "accepted", None):
            accepted = True
        elif getattr(args, "rejected", None):
            accepted = False

        results = votes_view.votes(
            vote_id=args.vote_id,
            knesset_num=args.knesset,
            bill_id=args.bill_id,
            name=args.name,
            from_date=args.from_date,
            to_date=args.to_date,
            accepted=accepted,
            full_details=args.full_details,
        )
        _output(results)
        return

    if args.command == "search-across":
        result = search_across_view.search_across(args.query, top_n=args.top_n)
        _output(result)
        return

    if args.command == "metadata":
        result = metadata_view.metadata(
            knesset_num=args.knesset,
            include_committee_heads=args.committee_heads,
            include_ministry_members=args.ministry_members,
            include_faction_members=args.faction_members,
        )
        _output(result)
        return



if __name__ == "__main__":
    main()
