from typing import Any, Dict, Iterable, Optional, Tuple
from datetime import datetime, timezone

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_PlenumVoteResult"
TABLE_NAME = "plenum_vote_result_raw"


def _normalize_dt(value: Optional[str]) -> Optional[str]:
    """Normalize a datetime string to naive-UTC ``YYYY-MM-DDTHH:MM:SS``."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return value


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plenum_vote_result_raw (
            Id INTEGER PRIMARY KEY,
            VoteID INTEGER NOT NULL,
            MkId INTEGER NOT NULL,
            VoteDate TEXT,
            ResultCode INTEGER,
            ResultDesc TEXT,
            LastUpdatedDate TEXT,
            LastName TEXT,
            FirstName TEXT,
            SessionID INTEGER,
            ItemID INTEGER,
            fetched_at TEXT
        )
        """
    )
    # Index for the most common query pattern: all MKs for a given vote
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_pvr_vote_id ON plenum_vote_result_raw (VoteID)"
    )
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str], Optional[int]]:
    """Insert rows into the table.

    Returns (count_inserted, max_LastUpdatedDate, max_Id).
    """
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT OR REPLACE INTO {TABLE_NAME} "
        "(Id, VoteID, MkId, VoteDate, ResultCode, ResultDesc, "
        "LastUpdatedDate, LastName, FirstName, SessionID, ItemID, fetched_at) "
        "VALUES (:Id, :VoteID, :MkId, :VoteDate, :ResultCode, :ResultDesc, "
        ":LastUpdatedDate, :LastName, :FirstName, :SessionID, :ItemID, :fetched_at)"
    )
    payload = []
    max_updated: Optional[str] = None
    max_id: Optional[int] = None
    count = 0
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        row_id = row.get("Id")
        if row_id is not None and (max_id is None or row_id > max_id):
            max_id = row_id
        payload.append(
            {
                "Id": row_id,
                "VoteID": row.get("VoteID"),
                "MkId": row.get("MkId"),
                "VoteDate": _normalize_dt(row.get("VoteDate")),
                "ResultCode": row.get("ResultCode"),
                "ResultDesc": row.get("ResultDesc"),
                "LastUpdatedDate": _normalize_dt(last_updated),
                "LastName": row.get("LastName"),
                "FirstName": row.get("FirstName"),
                "SessionID": row.get("SessionID"),
                "ItemID": row.get("ItemID"),
                "fetched_at": now,
            }
        )
        # Batch insert every 10,000 rows for memory efficiency
        if len(payload) >= 10_000:
            cur.executemany(sql, payload)
            count += len(payload)
            payload.clear()
    if payload:
        cur.executemany(sql, payload)
        count += len(payload)
    conn.commit()
    return count, max_updated, max_id


def fetch_rows(conn, since: Optional[str] = None) -> None:
    """Fetch per-MK vote results from OData using Id-based pagination.

    Uses ``Id`` as cursor (unique per row, ascending) to avoid data loss
    from duplicate ``LastUpdatedDate`` values.

    When ``since`` is None (initial load):
      Fetch all rows ordered by Id ascending.

    When ``since`` is provided (incremental update):
      Fetch rows with Id > since (numeric).
    """
    # Determine starting cursor
    if since is not None:
        cursor_val = since
    else:
        # Check if we have existing data to resume from
        row = conn.execute(
            f"SELECT MAX(Id) FROM {TABLE_NAME}"
        ).fetchone()
        max_existing = row[0] if row and row[0] is not None else None
        cursor_val = str(max_existing) if max_existing else None

    odata_rows = fetch_odata_table(
        table=ODATA_NAME,
        since=cursor_val,
        since_field="Id",
        orderby="Id asc",
        numeric=True,
    )
    count, max_updated, max_id = _insert_to_db(conn, odata_rows)

    # Store max LastUpdatedDate for informational purposes,
    # but we always use Id-based cursor for this table.
    if count > 0:
        cur = conn.cursor()
        cur.execute(
            f"SELECT MAX(LastUpdatedDate) FROM {TABLE_NAME} "
            "WHERE LastUpdatedDate IS NOT NULL"
        )
        row = cur.fetchone()
        db_max_updated = row[0] if row else None
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), db_max_updated)

    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Fetched {count} rows. Total {TABLE_NAME}: {total}")
