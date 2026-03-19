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
from origins.members import search_members_view as members_view
from origins.members import get_member_view as member_view
from origins.plenums import plenum_sessions_view
from origins.committees import committee_sessions_view
from origins.bills import search_bills_view as bills_view
from origins.bills import get_bill_view as bill_view
from origins.votes import search_votes_view as votes_view
from origins.votes import get_vote_view as vote_view
from origins.search import search_across_view
from origins.knesset import get_knesset_dates_view as knesset_dates_view


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
    members_p.add_argument("--person-id", dest="person_id", type=int, default=None, help="Person ID")

    # --- member (single) ---
    member_p = sub.add_parser("member", help="Get full detail for a single member (with committees/government)")
    member_p.add_argument("--member-id", dest="member_id", type=int, required=True, help="Member/Person ID (required)")
    member_p.add_argument("--knesset", type=int, default=None, help="Knesset number (omit for all terms)")

    # --- committee-sessions ---
    cmt_p = sub.add_parser("committee-sessions", help="Search committee sessions")
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

    # --- bills (list) ---
    bills_p = sub.add_parser("bills", help="Search bills (summary, no stages)")
    bills_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    bills_p.add_argument("--name", type=str, default=None, help="Bill name contains text")
    bills_p.add_argument("--status", type=str, default=None, help="Current status contains text")
    bills_p.add_argument("--sub-type", dest="sub_type", type=str, default=None, help="Sub-type (פרטית/ממשלתית/ועדה)")
    bills_p.add_argument("--date", type=str, default=None, help="Single date or start of range (YYYY-MM-DD)")
    bills_p.add_argument("--date-to", dest="date_to", type=str, default=None, help="End of range (YYYY-MM-DD)")

    # --- bill (single) ---
    bill_p = sub.add_parser("bill", help="Get full detail for a single bill (with stages/votes)")
    bill_p.add_argument("--bill-id", dest="bill_id", type=int, required=True, help="Bill ID (required)")

    # --- votes (list) ---
    votes_p = sub.add_parser("votes", help="Search plenum votes (summary)")
    votes_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    votes_p.add_argument("--bill-id", dest="bill_id", type=int, default=None, help="Filter votes by bill ID")
    votes_p.add_argument("--name", type=str, default=None, help="Vote title/subject contains text")
    votes_p.add_argument("--date", type=str, default=None, help="Single date or start of range (YYYY-MM-DD)")
    votes_p.add_argument("--date-to", dest="date_to", type=str, default=None, help="End of range (YYYY-MM-DD)")
    votes_p.add_argument("--accepted", dest="accepted", default=None, action="store_true", help="Accepted votes only")
    votes_p.add_argument("--rejected", dest="rejected", default=None, action="store_true", help="Rejected votes only")

    # --- vote (single) ---
    vote_p = sub.add_parser("vote", help="Get full detail for a single vote (with members/related)")
    vote_p.add_argument("--vote-id", dest="vote_id", type=int, required=True, help="Vote ID (required)")

    # --- search-across ---
    sa_p = sub.add_parser("search-across", help="Search across all entity types (triage tool)")
    sa_p.add_argument("query", type=str, help="Free-text search term")
    sa_p.add_argument("--top-n", dest="top_n", type=int, default=None,
                       help="Max results per entity type (default from config)")

    # --- knesset-dates ---
    kd_p = sub.add_parser("knesset-dates", help="Look up Knesset terms and plenum periods")
    kd_p.add_argument("--knesset", type=int, default=None, help="Knesset number")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Ensure indexes exist (requires write access, done once at startup)
    conn = connect_db()
    ensure_indexes(conn)
    conn.close()

    if args.command == "members":
        results = members_view.search_members(
            knesset_num=args.knesset,
            first_name=args.first_name,
            last_name=args.last_name,
            role=args.role,
            role_type=args.role_type,
            party=args.party,
            person_id=args.person_id,
        )
        _output(results)
        return

    if args.command == "member":
        result = member_view.get_member(args.member_id, knesset_num=args.knesset)
        _output(result)
        return

    if args.command == "committee-sessions":
        results = committee_sessions_view.committee_sessions(
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
        results = plenum_sessions_view.plenum_sessions(
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
        results = bills_view.search_bills(
            knesset_num=args.knesset,
            name=args.name,
            status=args.status,
            sub_type=args.sub_type,
            date=args.date,
            date_to=args.date_to,
        )
        _output(results)
        return

    if args.command == "bill":
        result = bill_view.get_bill(args.bill_id)
        _output(result)
        return

    if args.command == "votes":
        accepted = None
        if getattr(args, "accepted", None):
            accepted = True
        elif getattr(args, "rejected", None):
            accepted = False

        results = votes_view.search_votes(
            knesset_num=args.knesset,
            bill_id=args.bill_id,
            name=args.name,
            date=args.date,
            date_to=args.date_to,
            accepted=accepted,
        )
        _output(results)
        return

    if args.command == "vote":
        result = vote_view.get_vote(args.vote_id)
        _output(result)
        return

    if args.command == "search-across":
        result = search_across_view.search_across(args.query, top_n=args.top_n)
        _output(result)
        return

    if args.command == "knesset-dates":
        result = knesset_dates_view.get_knesset_dates(knesset_num=args.knesset)
        _output(result)
        return



if __name__ == "__main__":
    main()
