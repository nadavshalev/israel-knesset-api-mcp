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
from views import members_view
from views import member_view
from views import committees_view
from views import committee_view
from views import plenum_sessions_view
from views import plenum_session_view
from views import bills_view
from views import bill_view
from views import votes_view
from views import vote_view
from views import search_across_view
from views import knesset_dates_view


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

    # --- committees (list) ---
    committees_p = sub.add_parser("committees", help="Search committees (summary)")
    committees_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    committees_p.add_argument("--name", type=str, default=None, help="Committee name contains text")
    committees_p.add_argument("--type", dest="committee_type", type=str, default=None,
                              help="Committee type (ועדה ראשית, ועדת משנה, ועדה מיוחדת, ועדה משותפת)")
    committees_p.add_argument("--category", type=str, default=None, help="Category description contains text")
    committees_p.add_argument("--current", dest="is_current", default=None, action="store_true",
                              help="Current committees only")
    committees_p.add_argument("--inactive", dest="is_inactive", default=None, action="store_true",
                              help="Inactive committees only")
    committees_p.add_argument("--parent-id", dest="parent_committee_id", type=int, default=None,
                              help="Parent committee ID (for sub-committees)")

    # --- committee (single) ---
    committee_p = sub.add_parser("committee", help="Get full detail for a single committee (metadata + opt-in lists)")
    committee_p.add_argument("--committee-id", dest="committee_id", type=int, required=True, help="Committee ID (required)")
    committee_p.add_argument("--knesset", type=int, default=None, help="Knesset number (informational context)")
    committee_p.add_argument("--date", type=str, default=None, help="Single date or start of range (YYYY-MM-DD)")
    committee_p.add_argument("--date-to", dest="date_to", type=str, default=None, help="End of range (YYYY-MM-DD)")
    committee_p.add_argument("--sessions", dest="include_sessions", action="store_true", help="Include committee sessions")
    committee_p.add_argument("--members", dest="include_members", action="store_true", help="Include committee members")
    committee_p.add_argument("--bills", dest="include_bills", action="store_true", help="Include bills discussed")
    committee_p.add_argument("--documents", dest="include_documents", action="store_true", help="Include session documents")

    # --- plenums (list) ---
    plenum_p = sub.add_parser("plenums", help="Search plenum sessions (summary, no items/docs)")
    plenum_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    plenum_p.add_argument("--date", type=str, default=None, help="Single date or start of range (YYYY-MM-DD)")
    plenum_p.add_argument("--date-to", dest="date_to", type=str, default=None, help="End of range (YYYY-MM-DD)")
    plenum_p.add_argument("--name", type=str, default=None, help="Session/item name contains text")
    plenum_p.add_argument("--item-type", dest="item_type", type=str, default=None, help="Item type contains text")

    # --- plenum (single) ---
    session_p = sub.add_parser("plenum", help="Get full detail for a single plenum session")
    session_p.add_argument("--session-id", dest="session_id", type=int, required=True, help="Session ID (required)")

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

    if args.command == "committees":
        is_current = None
        if getattr(args, "is_current", None):
            is_current = True
        elif getattr(args, "is_inactive", None):
            is_current = False

        results = committees_view.search_committees(
            knesset_num=args.knesset,
            name=args.name,
            committee_type=args.committee_type,
            category=args.category,
            is_current=is_current,
            parent_committee_id=args.parent_committee_id,
        )
        _output(results)
        return

    if args.command == "committee":
        result = committee_view.get_committee(
            args.committee_id,
            knesset_num=args.knesset,
            date=args.date,
            date_to=args.date_to,
            include_sessions=args.include_sessions,
            include_members=args.include_members,
            include_bills=args.include_bills,
            include_documents=args.include_documents,
        )
        _output(result)
        return

    if args.command == "plenums":
        results = plenum_sessions_view.search_sessions(
            knesset_num=args.knesset,
            date=args.date,
            date_to=args.date_to,
            name=args.name,
            item_type=args.item_type,
        )
        _output(results)
        return

    if args.command == "plenum":
        result = plenum_session_view.get_session(args.session_id)
        _output(result)
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
