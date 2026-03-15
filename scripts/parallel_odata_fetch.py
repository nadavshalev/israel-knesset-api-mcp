#!/usr/bin/env python3
"""General-purpose parallel OData fetcher for tables without usable CSVs.

Uses Id-based pagination with parallel workers for speed.  Each worker
fetches a non-overlapping Id range, inserting to DB page by page.
Supports resume: each chunk checks local max Id and skips already-fetched rows.

Usage:
    .venv/bin/python scripts/parallel_odata_fetch.py document_bill
    .venv/bin/python scripts/parallel_odata_fetch.py document_bill --reset
    .venv/bin/python scripts/parallel_odata_fetch.py document_bill --workers 5
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import BASE_URL
from core.db import connect_db, update_metadata
from core.odata_client import _utc_now_iso, _request_with_retry, _get_json, fetch_odata_max_id
from origins import get_table_spec

DEFAULT_WORKERS = 5
DEFAULT_CHUNK_SIZE = 50_000
PAGE_SIZE = 100

# Shared progress counter
_lock = threading.Lock()
_total_rows = 0
_total_pages = 0


def _progress(rows: int) -> None:
    global _total_rows, _total_pages
    with _lock:
        _total_rows += rows
        _total_pages += 1
        if _total_pages % 20 == 0:
            print(f"  Progress: {_total_rows:,} rows ({_total_pages} pages)")


def _safe_insert(insert_fn, conn, batch) -> int:
    """Call the table's _insert_to_db, handling both 2-tuple and 3-tuple returns."""
    result = insert_fn(conn, batch)
    # result is either (count, max_updated) or (count, max_updated, max_id)
    return result[0]


def _fetch_chunk(
    odata_name: str,
    table_name: str,
    insert_fn,
    range_start: int,
    range_end: int,
    chunk_idx: int,
    total_chunks: int,
) -> Tuple[int, float]:
    """Fetch one Id range page-by-page, inserting each page to DB.

    Returns (total_count, elapsed_seconds).
    """
    t0 = time.time()
    url = f"{BASE_URL}{odata_name}"
    conn = connect_db()
    chunk_label = f"Chunk {chunk_idx}/{total_chunks} (Id {range_start+1}..{range_end})"

    # Check how far we got within this chunk previously
    cur = conn.cursor()
    cur.execute(
        f"SELECT MAX(Id) FROM {table_name} WHERE Id > %s AND Id <= %s",
        (range_start, range_end),
    )
    row = cur.fetchone()
    local_max = row[0] if row and row[0] is not None else None

    if local_max is not None:
        cursor = local_max
        cur.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE Id > %s AND Id <= %s",
            (range_start, range_end),
        )
        existing = cur.fetchone()[0]
        print(f"  {chunk_label}: resuming from Id {local_max} ({existing} rows already)")
    else:
        cursor = range_start
    cur.close()

    chunk_count = 0

    while cursor < range_end:
        params: Dict[str, Any] = {
            "$top": PAGE_SIZE,
            "$orderby": "Id asc",
            "$filter": f"Id gt {cursor} and Id le {range_end}",
        }
        try:
            resp = _request_with_retry(url, params, timeout=120)
        except Exception as exc:
            print(f"  {chunk_label}: Failed at cursor {cursor} after retries: {exc}. Stopping chunk.")
            break

        data = _get_json(resp)
        batch = data.get("value", [])
        if not batch:
            break

        count = _safe_insert(insert_fn, conn, batch)
        chunk_count += count

        last_id = batch[-1].get("Id")
        if last_id is None or last_id <= cursor:
            break
        cursor = last_id

        _progress(count)

    conn.close()
    elapsed = time.time() - t0
    print(f"  {chunk_label}: {chunk_count:,} rows in {elapsed:.0f}s")
    return chunk_count, elapsed


def run_parallel_fetch(
    table_label: str,
    *,
    reset: bool = False,
    workers: int = DEFAULT_WORKERS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """Run a parallel OData fetch for the given table label."""
    global _total_rows, _total_pages

    spec = get_table_spec(table_label)
    module = spec.module
    odata_name = module.ODATA_NAME
    table_name = module.TABLE_NAME
    create_table_fn = module.create_table
    insert_fn = module._insert_to_db

    conn = connect_db()

    if reset:
        print(f"Dropping {table_name}...")
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute("DELETE FROM metadata WHERE table_name = %s", (table_name,))
        cur.close()
        conn.commit()

    create_table_fn(conn)

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    existing = cur.fetchone()[0]
    cur.close()

    print(f"Querying OData for max Id ({odata_name})...")
    max_remote_id = fetch_odata_max_id(odata_name, "Id")
    if max_remote_id is None:
        print("ERROR: Could not determine max Id from OData")
        conn.close()
        return

    print(f"Existing: {existing:,} rows. Remote max Id={max_remote_id:,}.")

    chunks: List[Tuple[int, int]] = []
    cursor = 0
    while cursor < max_remote_id:
        chunk_end = min(cursor + chunk_size, max_remote_id)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end

    total_chunks = len(chunks)
    print(
        f"Split into {total_chunks} chunks of ~{chunk_size:,} Ids, "
        f"using {workers} parallel workers.\n"
    )

    _total_rows = 0
    _total_pages = 0
    t_start = time.time()
    total_inserted = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _fetch_chunk, odata_name, table_name, insert_fn,
                c_start, c_end, idx + 1, total_chunks,
            ): (c_start, c_end)
            for idx, (c_start, c_end) in enumerate(chunks)
        }
        for future in as_completed(futures):
            try:
                count, _ = future.result()
                total_inserted += count
            except Exception as exc:
                c_start, c_end = futures[future]
                print(f"  ERROR chunk Id {c_start+1}..{c_end}: {exc}")

    elapsed_total = time.time() - t_start

    # Update metadata
    cur = conn.cursor()
    cur.execute(
        f"SELECT MAX(LastUpdatedDate) FROM {table_name} "
        "WHERE LastUpdatedDate IS NOT NULL"
    )
    row = cur.fetchone()
    db_max_updated = row[0] if row else None
    cur.close()
    update_metadata(conn, table_name, _utc_now_iso(), db_max_updated)

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    final = cur.fetchone()[0]
    cur.close()
    rate = total_inserted / elapsed_total if elapsed_total > 0 else 0
    print(
        f"\nDone. Inserted {total_inserted:,} rows in {elapsed_total:.0f}s "
        f"({rate:.0f} rows/s). Total {table_name}: {final:,}"
    )
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parallel OData fetcher for tables without usable CSVs"
    )
    parser.add_argument(
        "table",
        help="Table label (e.g. document_bill, plenum_vote_result)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop existing table and fetch from scratch",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Id range per chunk (default: {DEFAULT_CHUNK_SIZE:,})",
    )
    args = parser.parse_args()

    run_parallel_fetch(
        args.table,
        reset=args.reset,
        workers=args.workers,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    main()
