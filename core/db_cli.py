import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path when run as a script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DEFAULT_DB
from core.db import connect_db
from tables import persons
from tables import person_to_position as p2p_fetch
from tables import positions
from tables import plenum_session
from tables import plm_session_item
from tables import document_plenum_session
from tables import status
from tables import bill
from tables import committee
from tables import committee_session
from tables import document_committee_session
from tables import cmt_session_item
from tables import plenum_vote
from tables import plenum_vote_result
from views import members_view
from views import member_view


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Knesset data tooling")
    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser("init-db", help="Create SQLite schema if missing")

    fetch_persons = sub.add_parser("fetch-persons", help="Fetch KNS_Person rows into person_raw")
    fetch_persons.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_positions = sub.add_parser("fetch-positions", help="Fetch KNS_Position rows into position_raw")
    fetch_positions.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_ptp = sub.add_parser("fetch-person-to-position", help="Fetch KNS_PersonToPosition rows into raw table")
    fetch_ptp.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_all = sub.add_parser("fetch-all", help="Fetch all raw tables")
    fetch_all.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_ps = sub.add_parser("fetch-plenum-sessions", help="Fetch KNS_PlenumSession rows into plenum_session_raw")
    fetch_ps.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_psi = sub.add_parser("fetch-plm-session-items", help="Fetch KNS_PlmSessionItem rows into plm_session_item_raw")
    fetch_psi.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_dps = sub.add_parser("fetch-document-plenum-sessions", help="Fetch KNS_DocumentPlenumSession rows into document_plenum_session_raw")
    fetch_dps.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_st = sub.add_parser("fetch-status", help="Fetch KNS_Status rows into status_raw")
    fetch_st.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_bill = sub.add_parser("fetch-bills", help="Fetch KNS_Bill rows into bill_raw")
    fetch_bill.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_comm = sub.add_parser("fetch-committees", help="Fetch KNS_Committee rows into committee_raw")
    fetch_comm.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_cs = sub.add_parser("fetch-committee-sessions", help="Fetch KNS_CommitteeSession rows into committee_session_raw")
    fetch_cs.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_dcs = sub.add_parser("fetch-document-committee-sessions", help="Fetch KNS_DocumentCommitteeSession rows")
    fetch_dcs.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_csi = sub.add_parser("fetch-cmt-session-items", help="Fetch KNS_CmtSessionItem rows")
    fetch_csi.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_pv = sub.add_parser("fetch-votes", help="Fetch KNS_PlenumVote rows into plenum_vote_raw")
    fetch_pv.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    fetch_pvr = sub.add_parser("fetch-vote-results", help="Fetch KNS_PlenumVoteResult into plenum_vote_result_raw")
    fetch_pvr.add_argument("--since", type=str, default=None, help="ISO datetime for LastUpdatedDate filter (UTC)")

    return parser.parse_args()


def ensure_tables(conn) -> None:
    persons.create_table(conn)
    p2p_fetch.create_table(conn)
    positions.create_table(conn)
    plenum_session.create_table(conn)
    plm_session_item.create_table(conn)
    document_plenum_session.create_table(conn)
    status.create_table(conn)
    bill.create_table(conn)
    committee.create_table(conn)
    committee_session.create_table(conn)
    document_committee_session.create_table(conn)
    cmt_session_item.create_table(conn)
    plenum_vote.create_table(conn)
    plenum_vote_result.create_table(conn)


def main() -> None:
    args = parse_args()
    conn = connect_db(DEFAULT_DB)

    if args.command == "init-db":
        ensure_tables(conn)
        print(f"Initialized schema at {DEFAULT_DB}")
        return

    if args.command == "fetch-persons":
        ensure_tables(conn)
        persons.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-person-to-position":
        ensure_tables(conn)
        p2p_fetch.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-positions":
        ensure_tables(conn)
        positions.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-all":
        ensure_tables(conn)
        persons.fetch_rows(conn, since=args.since)
        p2p_fetch.fetch_rows(conn, since=args.since)
        positions.fetch_rows(conn, since=args.since)
        plenum_session.fetch_rows(conn, since=args.since)
        plm_session_item.fetch_rows(conn, since=args.since)
        document_plenum_session.fetch_rows(conn, since=args.since)
        status.fetch_rows(conn, since=args.since)
        bill.fetch_rows(conn, since=args.since)
        committee.fetch_rows(conn, since=args.since)
        committee_session.fetch_rows(conn, since=args.since)
        document_committee_session.fetch_rows(conn, since=args.since)
        cmt_session_item.fetch_rows(conn, since=args.since)
        plenum_vote.fetch_rows(conn, since=args.since)
        plenum_vote_result.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-plenum-sessions":
        ensure_tables(conn)
        plenum_session.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-plm-session-items":
        ensure_tables(conn)
        plm_session_item.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-document-plenum-sessions":
        ensure_tables(conn)
        document_plenum_session.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-status":
        ensure_tables(conn)
        status.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-bills":
        ensure_tables(conn)
        bill.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-committees":
        ensure_tables(conn)
        committee.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-committee-sessions":
        ensure_tables(conn)
        committee_session.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-document-committee-sessions":
        ensure_tables(conn)
        document_committee_session.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-cmt-session-items":
        ensure_tables(conn)
        cmt_session_item.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-votes":
        ensure_tables(conn)
        plenum_vote.fetch_rows(conn, since=args.since)
        return

    if args.command == "fetch-vote-results":
        ensure_tables(conn)
        plenum_vote_result.fetch_rows(conn, since=args.since)
        return


if __name__ == "__main__":
    main()
