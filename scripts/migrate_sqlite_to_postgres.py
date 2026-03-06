#!/usr/bin/env python3
"""Migrate all data from SQLite (data.sqlite) to PostgreSQL.

Reads every row from each SQLite table and inserts it into the corresponding
PostgreSQL table (which must already exist — run ``ensure_tables()`` first).

Usage:
    .venv/bin/python scripts/migrate_sqlite_to_postgres.py
    .venv/bin/python scripts/migrate_sqlite_to_postgres.py --table person_raw
    .venv/bin/python scripts/migrate_sqlite_to_postgres.py --dry-run
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2.extras
from core.db import connect_db
from core.db_cli import ensure_tables

# ---------------------------------------------------------------------------
# Table definitions: (table_name, primary_key_column)
# Order matters — reference tables first.
# ---------------------------------------------------------------------------

TABLES = [
    "person_raw",
    "position_raw",
    "status_raw",
    "person_to_position_raw",
    "bill_raw",
    "committee_raw",
    "committee_session_raw",
    "document_committee_session_raw",
    "cmt_session_item_raw",
    "plenum_session_raw",
    "plm_session_item_raw",
    "document_plenum_session_raw",
    "plenum_vote_raw",
    "plenum_vote_result_raw",
    "metadata",
]

# status_raw has a reserved-word column "Desc" that needs quoting in PG
RESERVED_COLUMNS = {"Desc"}

BATCH_SIZE = 5000


def _pg_col(col: str) -> str:
    """Quote a column name for PostgreSQL if it is a reserved word."""
    if col in RESERVED_COLUMNS:
        return f'"{col}"'
    return col


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table_name: str,
    dry_run: bool = False,
) -> int:
    """Migrate a single table from SQLite to PostgreSQL.

    Returns the number of rows migrated.
    """
    # Get column names from SQLite
    sqlite_cur = sqlite_conn.execute(f"PRAGMA table_info([{table_name}])")
    columns = [row[1] for row in sqlite_cur.fetchall()]

    if not columns:
        print(f"  WARNING: Table '{table_name}' not found in SQLite. Skipping.")
        return 0

    # Count rows in SQLite
    sqlite_count = sqlite_conn.execute(
        f"SELECT COUNT(*) FROM [{table_name}]"
    ).fetchone()[0]

    # Count existing rows in PostgreSQL
    pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        pg_count = pg_cur.fetchone()[0]
    except Exception:
        pg_conn.rollback()
        pg_count = 0
    pg_cur.close()

    print(f"  SQLite: {sqlite_count:,} rows | PostgreSQL: {pg_count:,} rows")

    if dry_run:
        print(f"  [dry-run] Would migrate {sqlite_count:,} rows")
        return 0

    if sqlite_count == 0:
        print("  No rows to migrate.")
        return 0

    # Determine which columns are non-TEXT in PostgreSQL so we can convert
    # empty strings (allowed by SQLite's loose typing) to NULL.
    pg_cur = pg_conn.cursor()
    pg_cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s "
        "ORDER BY ordinal_position",
        (table_name,),
    )
    pg_types = {row[0]: row[1] for row in pg_cur.fetchall()}
    pg_cur.close()

    # PostgreSQL folds unquoted identifiers to lowercase
    non_text_indices = set()
    for i, col in enumerate(columns):
        pg_type = pg_types.get(col.lower(), "text")
        if pg_type != "text":
            non_text_indices.add(i)

    def _clean_row(row):
        """Convert empty strings and non-castable values to None for non-TEXT columns.

        SQLite is loosely typed and allows text in INTEGER columns.
        PostgreSQL enforces types, so we must sanitise the data.
        """
        if not non_text_indices:
            return tuple(row)
        cleaned = []
        for i, val in enumerate(row):
            if i in non_text_indices and isinstance(val, str):
                # Try to cast to int; if it fails, use None
                try:
                    val = int(val) if val != "" else None
                except (ValueError, TypeError):
                    val = None
            cleaned.append(val)
        return tuple(cleaned)

    # Build INSERT ... ON CONFLICT DO NOTHING SQL for PostgreSQL
    pg_columns = [_pg_col(c) for c in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(pg_columns)
    insert_sql = (
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )

    # Stream rows from SQLite and batch-insert into PostgreSQL
    sqlite_cur = sqlite_conn.execute(f"SELECT * FROM [{table_name}]")
    migrated = 0

    while True:
        batch = sqlite_cur.fetchmany(BATCH_SIZE)
        if not batch:
            break

        # Convert sqlite3.Row objects to clean tuples
        rows = [_clean_row(row) for row in batch]

        pg_cur = pg_conn.cursor()
        psycopg2.extras.execute_batch(
            pg_cur,
            insert_sql,
            rows,
            page_size=1000,
        )
        pg_cur.close()
        pg_conn.commit()

        migrated += len(rows)
        if sqlite_count > BATCH_SIZE:
            pct = (migrated / sqlite_count) * 100
            print(f"    {migrated:,} / {sqlite_count:,} ({pct:.0f}%)")

    return migrated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate data from SQLite to PostgreSQL"
    )
    parser.add_argument(
        "--sqlite-path",
        type=str,
        default=str(ROOT / "data.sqlite"),
        help="Path to SQLite database (default: data.sqlite in project root)",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        default=None,
        help="Migrate only specific table(s). Can be repeated.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without writing",
    )
    args = parser.parse_args()

    # Connect to SQLite
    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        print(f"ERROR: SQLite database not found at {sqlite_path}")
        sys.exit(1)

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = None  # plain tuples

    # Connect to PostgreSQL
    pg_conn = connect_db()

    # Ensure all PostgreSQL tables exist
    print("Ensuring PostgreSQL table schemas exist...")
    ensure_tables(pg_conn)
    print()

    # Determine which tables to migrate
    selected = args.tables if args.tables else TABLES

    total_start = time.time()
    results = []

    for table_name in selected:
        if table_name not in TABLES and not args.tables:
            print(f"WARNING: Unknown table '{table_name}'. Skipping.")
            continue

        print(f"\n--- {table_name} ---")
        t0 = time.time()
        count = migrate_table(sqlite_conn, pg_conn, table_name, dry_run=args.dry_run)
        elapsed = time.time() - t0
        results.append((table_name, count, elapsed))
        if count > 0:
            print(f"  Migrated {count:,} rows in {_format_duration(elapsed)}")

    # Summary
    total_elapsed = time.time() - total_start
    total_rows = sum(c for _, c, _ in results)

    print(f"\n{'='*60}")
    print(f"Migration complete in {_format_duration(total_elapsed)}")
    print(f"Total rows migrated: {total_rows:,}")
    print(f"\n{'Table':<40} {'Rows':>10}   {'Time':>10}")
    print("-" * 65)
    for table_name, count, elapsed in results:
        print(f"{table_name:<40} {count:>10,}   {_format_duration(elapsed):>10}")
    print()

    # Cleanup
    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
