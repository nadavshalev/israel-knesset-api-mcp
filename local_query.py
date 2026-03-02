import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from views import person_to_position_view as p2p_view
from views import plenum_sessions_view
from views import plenum_session_view
from views import bills_view
from views import bill_view
from views import votes_view
from views import vote_view


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
    parser = argparse.ArgumentParser(description="Query local Knesset SQLite database")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- members ---
    members_p = sub.add_parser("members", help="Search Knesset members")
    members_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    members_p.add_argument("--role", type=str, default=None, help="Role description contains text")
    members_p.add_argument("--role-type", dest="role_type", type=str, default=None,
                           help="Role type (שר, ח\"כ, ראש ממשלה, סגן שר, יו\"ר כנסת)")
    members_p.add_argument("--party", type=str, default=None, help="Party/faction name contains text")
    members_p.add_argument("--first-name", dest="first_name", type=str, default=None, help="First name contains")
    members_p.add_argument("--last-name", dest="last_name", type=str, default=None, help="Last name contains")
    members_p.add_argument("--person-id", dest="person_id", type=int, default=None, help="Person ID")
    members_p.add_argument("--committees", dest="show_committees", action="store_true", help="Include committee data")

    # --- plenums (list) ---
    plenum_p = sub.add_parser("plenums", help="Search plenum sessions (summary, no items/docs)")
    plenum_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    plenum_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="From date (YYYY-MM-DD)")
    plenum_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="To date (YYYY-MM-DD)")
    plenum_p.add_argument("--date", type=str, default=None, help="Exact date (YYYY-MM-DD)")
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
    bills_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="Plenum date from (YYYY-MM-DD)")
    bills_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="Plenum date to (YYYY-MM-DD)")
    bills_p.add_argument("--date", type=str, default=None, help="Plenum date (YYYY-MM-DD)")

    # --- bill (single) ---
    bill_p = sub.add_parser("bill", help="Get full detail for a single bill (with stages/votes)")
    bill_p.add_argument("--bill-id", dest="bill_id", type=int, required=True, help="Bill ID (required)")

    # --- votes (list) ---
    votes_p = sub.add_parser("votes", help="Search plenum votes (summary)")
    votes_p.add_argument("--knesset", type=int, default=None, help="Knesset number")
    votes_p.add_argument("--bill-id", dest="bill_id", type=int, default=None, help="Filter votes by bill ID")
    votes_p.add_argument("--name", type=str, default=None, help="Vote title/subject contains text")
    votes_p.add_argument("--from-date", dest="from_date", type=str, default=None, help="From date (YYYY-MM-DD)")
    votes_p.add_argument("--to-date", dest="to_date", type=str, default=None, help="To date (YYYY-MM-DD)")
    votes_p.add_argument("--date", type=str, default=None, help="Exact date (YYYY-MM-DD)")
    votes_p.add_argument("--accepted", dest="accepted", default=None, action="store_true", help="Accepted votes only")
    votes_p.add_argument("--rejected", dest="rejected", default=None, action="store_true", help="Rejected votes only")

    # --- vote (single) ---
    vote_p = sub.add_parser("vote", help="Get full detail for a single vote (with members/related)")
    vote_p.add_argument("--vote-id", dest="vote_id", type=int, required=True, help="Vote ID (required)")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.command == "members":
        results = p2p_view.search_knesset_members(
            knesset_num=args.knesset,
            first_name=args.first_name,
            last_name=args.last_name,
            role_query=args.role,
            role_type=args.role_type,
            faction_query=args.party,
            person_id=args.person_id,
            show_committees=args.show_committees,
        )
        _output(results)
        return

    if args.command == "plenums":
        results = plenum_sessions_view.search_sessions(
            knesset_num=args.knesset,
            from_date=args.from_date,
            to_date=args.to_date,
            date=args.date,
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
            from_date=args.from_date,
            to_date=args.to_date,
            date=args.date,
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
            from_date=args.from_date,
            to_date=args.to_date,
            date=args.date,
            accepted=accepted,
        )
        _output(results)
        return

    if args.command == "vote":
        result = vote_view.get_vote(args.vote_id)
        _output(result)
        return


if __name__ == "__main__":
    main()
