#!/usr/bin/env python3
"""Incremental update script — fetches only new/changed data from OData.

Usage:
    python3 update_all.py            # update all tables (incremental)
    python3 update_all.py --table persons --table bill   # update specific tables
    python3 update_all.py --full     # force full re-fetch (ignore metadata)
    python3 update_all.py --dry-run  # show what would be fetched, don't fetch
    python3 update_all.py --status   # show current sync status for all tables

Each table's last sync timestamp is stored in the ``metadata`` table.
On each run, only rows updated since that timestamp are fetched from OData.

The ``plenum_vote_result`` table uses Id-based cursors internally, so it
always resumes from the highest Id already in the database.
"""

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import connect_db, ensure_indexes

from tables import persons
from tables import positions
from tables import person_to_position
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


# ---------------------------------------------------------------------------
# Table registry — order matters (reference tables first, dependents later)
# ---------------------------------------------------------------------------

TABLES = [
    # Reference / dimension tables
    ("persons",                      persons,                      "person_raw"),
    ("positions",                    positions,                    "position_raw"),
    ("status",                       status,                       "status_raw"),
    # People → positions
    ("person_to_position",           person_to_position,           "person_to_position_raw"),
    # Bills
    ("bill",                         bill,                         "bill_raw"),
    # Committees
    ("committee",                    committee,                    "committee_raw"),
    ("committee_session",            committee_session,            "committee_session_raw"),
    ("document_committee_session",   document_committee_session,   "document_committee_session_raw"),
    ("cmt_session_item",             cmt_session_item,             "cmt_session_item_raw"),
    # Plenum
    ("plenum_session",               plenum_session,               "plenum_session_raw"),
    ("plm_session_item",             plm_session_item,             "plm_session_item_raw"),
    ("document_plenum_session",      document_plenum_session,      "document_plenum_session_raw"),
    # Votes (headers then per-MK results)
    ("plenum_vote",                  plenum_vote,                  "plenum_vote_raw"),
    ("plenum_vote_result",           plenum_vote_result,           "plenum_vote_result_raw"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_cutoff(conn, table_name: str):
    """Read last_updated_cutoff from the metadata table, or None."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_updated_cutoff FROM metadata WHERE table_name = %s",
            (table_name,),
        )
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception:
        conn.rollback()
        # metadata table may not exist yet
        return None


def _get_row_count(conn, table_name: str) -> int:
    """Return number of rows in a table, or 0 if table doesn't exist."""
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        cur.close()
        return count
    except Exception:
        conn.rollback()
        return 0


def _get_all_metadata(conn):
    """Return dict of {table_name: (last_sync, cutoff)} from metadata table."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name, last_sync_completed_at, last_updated_cutoff "
            "FROM metadata ORDER BY table_name"
        )
        rows = cur.fetchall()
        cur.close()
        return {r[0]: (r[1], r[2]) for r in rows}
    except Exception:
        conn.rollback()
        return {}


def _format_duration(seconds: float) -> str:
    """Format seconds into a human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:.0f}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins}m"


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def show_status(conn):
    """Print sync status for all tables."""
    meta = _get_all_metadata(conn)
    valid_names = {raw_name for _, _, raw_name in TABLES}

    print(f"\n{'Table':<40} {'Rows':>10}   {'Last Sync':<22} {'Cutoff':<22}")
    print("-" * 100)
    for label, _module, raw_name in TABLES:
        count = _get_row_count(conn, raw_name)
        sync_at, cutoff = meta.get(raw_name, (None, None))
        sync_str = sync_at[:19] if sync_at else "never"
        cutoff_str = cutoff[:19] if cutoff else "n/a"
        print(f"{raw_name:<40} {count:>10,}   {sync_str:<22} {cutoff_str:<22}")

    # Warn about stale metadata rows
    stale = sorted(k for k in meta if k not in valid_names)
    if stale:
        print(f"\n  WARNING: {len(stale)} stale metadata row(s) "
              f"(run --repair-metadata to clean up):")
        for name in stale:
            print(f"    - {name}")
    print()


def repair_metadata(conn):
    """Remove metadata rows for tables that no longer exist in TABLES."""
    valid_names = {raw_name for _, _, raw_name in TABLES}
    meta = _get_all_metadata(conn)
    stale = sorted(k for k in meta if k not in valid_names)

    if not stale:
        print("Metadata is clean — no stale rows found.")
        return

    print(f"Found {len(stale)} stale metadata row(s):")
    for name in stale:
        sync_at, cutoff = meta[name]
        print(f"  - {name}  (last_sync={sync_at}, cutoff={cutoff})")

    cur = conn.cursor()
    cur.execute(
        f"DELETE FROM metadata WHERE table_name NOT IN "
        f"({','.join('%s' for _ in valid_names)})",
        list(valid_names),
    )
    cur.close()
    conn.commit()
    print(f"Deleted {len(stale)} stale row(s) from metadata.")


def update_tables(conn, table_filter=None, full=False, dry_run=False):
    """Run incremental (or full) updates on selected tables."""
    # Determine which tables to update
    if table_filter:
        selected = []
        valid_labels = {label for label, _, _ in TABLES}
        for name in table_filter:
            matches = [(l, m, r) for l, m, r in TABLES if l == name]
            if not matches:
                print(f"ERROR: Unknown table '{name}'. Valid names: {', '.join(sorted(valid_labels))}")
                sys.exit(1)
            selected.extend(matches)
    else:
        selected = list(TABLES)

    total_start = time.time()
    results = []

    for label, module, raw_name in selected:
        # Determine the since value
        if full:
            since = None
            mode = "full"
        elif raw_name == "plenum_vote_result_raw":
            # This table uses Id-based cursor internally;
            # passing since=None makes it resume from MAX(Id) in the DB.
            since = None
            mode = "incremental (Id-based)"
        else:
            since = _get_cutoff(conn, raw_name)
            if since:
                mode = f"incremental (since {since[:19]})"
            else:
                mode = "initial load (no prior sync)"

        row_count_before = _get_row_count(conn, raw_name)

        print(f"\n--- {label} ({raw_name}) ---")
        print(f"  Mode: {mode}")
        print(f"  Rows before: {row_count_before:,}")

        if dry_run:
            print("  [dry-run] Skipping fetch")
            results.append((label, 0, 0.0))
            continue

        t0 = time.time()
        try:
            module.fetch_rows(conn, since=since)
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((label, 0, time.time() - t0))
            continue
        elapsed = time.time() - t0

        row_count_after = _get_row_count(conn, raw_name)
        delta = row_count_after - row_count_before
        print(f"  Rows after:  {row_count_after:,}  (delta: {delta:+,})")
        print(f"  Time: {_format_duration(elapsed)}")
        results.append((label, delta, elapsed))

    # Rebuild indexes after all fetches
    if not dry_run:
        print("\n--- Ensuring indexes ---")
        ensure_indexes(conn)
        print("  Done")

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"Update complete in {_format_duration(total_elapsed)}")
    print(f"\n{'Table':<35} {'New Rows':>10}   {'Time':>10}")
    print("-" * 60)
    for label, delta, elapsed in results:
        print(f"{label:<35} {delta:>+10,}   {_format_duration(elapsed):>10}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Incremental update of all Knesset data tables from OData",
    )
    parser.add_argument(
        "--table", action="append", dest="tables", default=None,
        help="Update only specific table(s). Can be repeated. "
             "Valid names: " + ", ".join(l for l, _, _ in TABLES),
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Force full re-fetch (ignore stored sync timestamps)",
    )
    parser.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Show what would be fetched without actually fetching",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show current sync status for all tables and exit",
    )
    parser.add_argument(
        "--repair-metadata", dest="repair_metadata", action="store_true",
        help="Remove stale metadata rows for tables that no longer exist",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    conn = connect_db()

    if args.status:
        show_status(conn)
        conn.close()
        return

    if args.repair_metadata:
        repair_metadata(conn)
        conn.close()
        return

    # Ensure all table schemas exist before fetching
    from core.db_cli import ensure_tables
    ensure_tables(conn)

    update_tables(conn, table_filter=args.tables, full=args.full,
                  dry_run=args.dry_run)
    conn.close()


if __name__ == "__main__":
    main()
