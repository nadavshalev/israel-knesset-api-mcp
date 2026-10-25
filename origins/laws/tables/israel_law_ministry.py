from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_IsraelLawMinistry"
TABLE_NAME = "israel_law_ministry_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_israellawministry_israellawid ON israel_law_ministry_raw (IsraelLawID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS israel_law_ministry_raw (
            Id INTEGER PRIMARY KEY,
            IsraelLawID INTEGER,
            MinistryCategoryID INTEGER,
            MinistryCategoryDesc TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    # Upgrade existing databases to the current upstream fields.
    cur.execute(
        f"ALTER TABLE {TABLE_NAME} "
        "ADD COLUMN IF NOT EXISTS MinistryCategoryID INTEGER, "
        "ADD COLUMN IF NOT EXISTS MinistryCategoryDesc TEXT, "
        "DROP COLUMN IF EXISTS GovMinistryID"
    )
    cur.close()
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    now = _utc_now_iso()
    sql = (
        f"INSERT INTO {TABLE_NAME} "
        "(Id, IsraelLawID, MinistryCategoryID, MinistryCategoryDesc, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(IsraelLawID)s, %(MinistryCategoryID)s, %(MinistryCategoryDesc)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "IsraelLawID=EXCLUDED.IsraelLawID, MinistryCategoryID=EXCLUDED.MinistryCategoryID, "
        "MinistryCategoryDesc=EXCLUDED.MinistryCategoryDesc, "
        "LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        missing = [field for field in ("Id", "IsraelLawID", "MinistryCategoryID")
                   if row.get(field) is None]
        if missing:
            raise ValueError(f"{ODATA_NAME} row is missing required fields: {', '.join(missing)}")
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("Id"),
                "IsraelLawID": row.get("IsraelLawID"),
                "MinistryCategoryID": row.get("MinistryCategoryID"),
                "MinistryCategoryDesc": row.get("MinistryCategoryDesc") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    with conn.cursor() as cur:
        if payload:
            psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    if since is not None:
        # A timestamp-only update cannot repair older rows whose new columns
        # were just added. Backfill them even when upstream dates did not change.
        with conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {TABLE_NAME} WHERE MinistryCategoryID IS NULL)")
            needs_backfill = cur.fetchone()[0]
        if needs_backfill:
            print("Backfilling law ministry categories from all upstream records")
            since = None
    if since is None:
        # IDs are unique: a full backfill must not skip rows sharing a timestamp
        # at the page boundary.
        rows = fetch_odata_table(table=ODATA_NAME, since_field="Id", numeric=True)
    else:
        rows = fetch_odata_table(table=ODATA_NAME, since=since)
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
