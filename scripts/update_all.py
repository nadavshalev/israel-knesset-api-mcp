#!/usr/bin/env python3
"""Incremental update script — fetches only new/changed data from OData.

Usage:
    python3 update_all.py            # update all tables (parallel, incremental)
    python3 update_all.py --sync     # update all tables (sequential, incremental)
    python3 update_all.py --table persons --table bill   # update specific tables
    python3 update_all.py --full     # force full re-fetch (ignore metadata)
    python3 update_all.py --dry-run  # show what would be fetched, don't fetch
    python3 update_all.py --status   # show current sync status for all tables
    python3 update_all.py --proxy    # route OData fetches through ISRAEL_PROXY_URL

Each table's last sync timestamp is stored in the ``metadata`` table.
On each run, only rows updated since that timestamp are fetched from OData.

The ``plenum_vote_result`` table uses Id-based cursors internally, so it
always resumes from the highest Id already in the database.

By default tables are fetched in parallel (one thread per table, up to
POOL_MAX_CONN workers). Pass --sync to revert to sequential behaviour.
"""

import argparse
import sys
import time
import threading
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import POOL_MAX_CONN, ISRAEL_PROXY_URL
from core.db import connect_db, ensure_indexes
from core.api_compatibility import print_latest_report
import core.odata_client as _odata_client

from origins import TableSpec, get_table_spec, get_table_specs

_print_lock = threading.Lock()


def _tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)


# ---------------------------------------------------------------------------
TABLES: tuple[TableSpec, ...] = get_table_specs()


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
    valid_names = {spec.table_name for spec in TABLES}

    print(f"\n{'Table':<40} {'Rows':>10}   {'Last Sync':<22} {'Cutoff':<22}")
    print("-" * 100)
    for spec in TABLES:
        count = _get_row_count(conn, spec.table_name)
        sync_at, cutoff = meta.get(spec.table_name, (None, None))
        sync_str = sync_at[:19] if sync_at else "never"
        cutoff_str = cutoff[:19] if cutoff else "n/a"
        print(f"{spec.table_name:<40} {count:>10,}   {sync_str:<22} {cutoff_str:<22}")

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
    valid_names = {spec.table_name for spec in TABLES}
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


def _resolve_selected(table_filter) -> list[TableSpec]:
    """Return the list of TableSpec objects to process."""
    if not table_filter:
        return list(TABLES)
    selected: list[TableSpec] = []
    for name in table_filter:
        try:
            selected.append(get_table_spec(name))
        except KeyError as exc:
            print(f"ERROR: {exc.args[0]}")
            sys.exit(1)
    return selected


def _since_and_mode(conn, spec: TableSpec, full: bool) -> tuple[str | None, str]:
    """Return (since, mode_description) for a given spec."""
    if full:
        return None, "full"
    if spec.cursor_mode == "id":
        return None, "incremental (Id-based)"
    since = _get_cutoff(conn, spec.table_name)
    if since:
        return since, f"incremental (since {since[:19]})"
    return None, "initial load (no prior sync)"


@dataclass
class FetchResult:
    label: str
    delta: int | None
    elapsed: float
    latest_row: str = "n/a"
    error: str | None = None
    skipped: bool = False


def _get_latest_row_timestamp(conn, table_name: str) -> str:
    """Read the newest source update in UTC, including pre-existing rows.

    Legacy timestamps without an offset are treated as UTC, as in the
    incremental OData cursor. Cast before MAX so different offsets sort
    chronologically rather than lexicographically.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            cur.execute(
                f"SELECT MAX(NULLIF(LastUpdatedDate, '')::timestamp with time zone "
                f"AT TIME ZONE 'UTC') FROM {table_name}"
            )
            latest = cur.fetchone()[0]
        return latest.isoformat(timespec="seconds") + "Z" if latest else "n/a"
    except Exception as exc:
        conn.rollback()
        _tprint(f"  WARNING: Could not read latest row timestamp for {table_name}: {exc}")
        return "unavailable"


def _print_summary(results: list[FetchResult], total_elapsed: float) -> None:
    failed = sum(result.error is not None for result in results)
    print(f"\n{'='*110}")
    outcome = f"Update finished with {failed} failed table(s)" if failed else "Update complete"
    print(f"{outcome} in {_format_duration(total_elapsed)}")
    print(f"\n{'Table':<35} {'New Rows':>10}   {'Time':>10}   {'Latest Row (UTC)':<20}   Status")
    print("-" * 110)
    for result in results:
        delta = f"{result.delta:+,}" if result.delta is not None else "n/a"
        if result.error is not None:
            status = "FAILED: " + " ".join(result.error.split())
        elif result.skipped:
            status = "SKIPPED: dry-run"
        else:
            status = "OK"
        print(f"{result.label:<35} {delta:>10}   {_format_duration(result.elapsed):>10}   "
              f"{result.latest_row:<20}   {status}")
    print()


# ---------------------------------------------------------------------------
# Parallel worker
# ---------------------------------------------------------------------------

def _fetch_one(spec: TableSpec, full: bool, dry_run: bool) -> FetchResult:
    """Fetch a single table in its own DB connection."""
    conn = None
    t0 = time.time()
    try:
        conn = connect_db()
        return _fetch_table(conn, spec, full, dry_run)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _tprint(f"  ERROR in {spec.label}: {error}")
        return FetchResult(spec.label, None, time.time() - t0, "unavailable", error)
    finally:
        if conn is not None:
            conn.close()


def _fetch_table(conn, spec: TableSpec, full: bool, dry_run: bool) -> FetchResult:
    """Collect the same fetch outcome for parallel and sequential runs."""
    t0 = time.time()
    try:
        since, mode = _since_and_mode(conn, spec, full)
        row_count_before = _get_row_count(conn, spec.table_name)

        _tprint(f"\n--- {spec.label} ({spec.table_name}) ---")
        _tprint(f"  Mode: {mode}")
        _tprint(f"  Rows before: {row_count_before:,}")

        if dry_run:
            _tprint("  [dry-run] Skipping fetch")
            return FetchResult(spec.label, 0, 0.0,
                               _get_latest_row_timestamp(conn, spec.table_name), skipped=True)

        t0 = time.time()
        spec.module.fetch_rows(conn, since=since)
        elapsed = time.time() - t0

        row_count_after = _get_row_count(conn, spec.table_name)
        delta = row_count_after - row_count_before
        _tprint(f"  Rows after:  {row_count_after:,}  (delta: {delta:+,})")
        _tprint(f"  Time: {_format_duration(elapsed)}")
        return FetchResult(spec.label, delta, elapsed,
                           _get_latest_row_timestamp(conn, spec.table_name))
    except Exception as e:
        elapsed = time.time() - t0
        error = f"{type(e).__name__}: {e}"
        _tprint(f"  ERROR in {spec.label}: {error}")
        conn.rollback()
        return FetchResult(spec.label, None, elapsed,
                           _get_latest_row_timestamp(conn, spec.table_name), error)


# ---------------------------------------------------------------------------
# Update entry points
# ---------------------------------------------------------------------------

def update_tables_parallel(conn, table_filter=None, full=False, dry_run=False):
    """Run updates on all selected tables in parallel (one thread per table)."""
    selected = _resolve_selected(table_filter)
    # Main conn holds 1 pool slot; workers get the rest.
    max_workers = min(max(POOL_MAX_CONN - 1, 1), len(selected))
    print(f"Running {len(selected)} table(s) in parallel (workers={max_workers})")

    total_start = time.time()
    # Preserve original table order in summary
    label_order = [spec.label for spec in selected]
    results_by_label: dict[str, FetchResult] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_one, spec, full, dry_run): spec
            for spec in selected
        }
        for future in as_completed(futures):
            result = future.result()
            results_by_label[result.label] = result

    if not dry_run:
        print("\n--- Ensuring indexes ---")
        ensure_indexes(conn)
        print("  Done")

    results = [results_by_label[lbl] for lbl in label_order]
    _print_summary(results, time.time() - total_start)
    print_latest_report(conn, cached_only=dry_run)


def update_tables_sync(conn, table_filter=None, full=False, dry_run=False):
    """Run incremental (or full) updates on selected tables sequentially."""
    selected = _resolve_selected(table_filter)
    total_start = time.time()
    results = []

    for spec in selected:
        results.append(_fetch_table(conn, spec, full, dry_run))

    if not dry_run:
        print("\n--- Ensuring indexes ---")
        ensure_indexes(conn)
        print("  Done")

    _print_summary(results, time.time() - total_start)
    print_latest_report(conn, cached_only=dry_run)


def update_tables(conn, table_filter=None, full=False, dry_run=False, sync=False):
    if sync:
        update_tables_sync(conn, table_filter=table_filter, full=full, dry_run=dry_run)
    else:
        update_tables_parallel(conn, table_filter=table_filter, full=full, dry_run=dry_run)


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
             "Valid names: " + ", ".join(spec.label for spec in TABLES),
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
    parser.add_argument(
        "--sync", action="store_true", dest="sync", default=False,
        help="Fetch tables sequentially instead of in parallel",
    )
    parser.add_argument(
        "--proxy", action="store_true", dest="use_proxy", default=False,
        help="Route OData fetches through the Israel proxy (ISRAEL_PROXY_URL)",
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

    if args.use_proxy:
        if not ISRAEL_PROXY_URL:
            print("ERROR: --proxy requires ISRAEL_PROXY_URL to be set")
            sys.exit(1)
        _odata_client.enable_proxy(ISRAEL_PROXY_URL)
        print(f"Proxy enabled: {ISRAEL_PROXY_URL}")

    kwargs = dict(table_filter=args.tables, full=args.full,
                  dry_run=args.dry_run, sync=args.sync)
    update_tables(conn, **kwargs)
    conn.close()


if __name__ == "__main__":
    main()
