from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_csv_table, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_PlenumVote"
TABLE_NAME = "plenum_vote_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_vote_title ON plenum_vote_raw (VoteTitle)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/votes/view_vote_rslts_hdr_approved/view_vote_rslts_hdr_approved.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plenum_vote_raw (
            Id INTEGER PRIMARY KEY,
            VoteDateTime TEXT,
            SessionID INTEGER,
            ItemID INTEGER,
            Ordinal INTEGER,
            VoteMethodID INTEGER,
            VoteMethodDesc TEXT,
            VoteStatusCode INTEGER,
            VoteStatusDesc TEXT,
            VoteTitle TEXT,
            VoteSubject TEXT,
            IsNoConfidenceInGov INTEGER,
            ForOptionID INTEGER,
            ForOptionDesc TEXT,
            AgainstOptionID INTEGER,
            AgainstOptionDesc TEXT,
            IsAccepted INTEGER,
            TotalFor INTEGER,
            TotalAgainst INTEGER,
            TotalAbstain INTEGER,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT INTO {TABLE_NAME} "
        "(Id, VoteDateTime, SessionID, ItemID, Ordinal, "
        "VoteMethodID, VoteMethodDesc, VoteStatusCode, VoteStatusDesc, "
        "VoteTitle, VoteSubject, IsNoConfidenceInGov, "
        "ForOptionID, ForOptionDesc, AgainstOptionID, AgainstOptionDesc, "
        "IsAccepted, TotalFor, TotalAgainst, TotalAbstain, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(VoteDateTime)s, %(SessionID)s, %(ItemID)s, %(Ordinal)s, "
        "%(VoteMethodID)s, %(VoteMethodDesc)s, %(VoteStatusCode)s, %(VoteStatusDesc)s, "
        "%(VoteTitle)s, %(VoteSubject)s, %(IsNoConfidenceInGov)s, "
        "%(ForOptionID)s, %(ForOptionDesc)s, %(AgainstOptionID)s, %(AgainstOptionDesc)s, "
        "%(IsAccepted)s, %(TotalFor)s, %(TotalAgainst)s, %(TotalAbstain)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "VoteDateTime=EXCLUDED.VoteDateTime, SessionID=EXCLUDED.SessionID, "
        "ItemID=EXCLUDED.ItemID, Ordinal=EXCLUDED.Ordinal, "
        "VoteMethodID=EXCLUDED.VoteMethodID, VoteMethodDesc=EXCLUDED.VoteMethodDesc, "
        "VoteStatusCode=EXCLUDED.VoteStatusCode, VoteStatusDesc=EXCLUDED.VoteStatusDesc, "
        "VoteTitle=EXCLUDED.VoteTitle, VoteSubject=EXCLUDED.VoteSubject, "
        "IsNoConfidenceInGov=EXCLUDED.IsNoConfidenceInGov, "
        "ForOptionID=EXCLUDED.ForOptionID, ForOptionDesc=EXCLUDED.ForOptionDesc, "
        "AgainstOptionID=EXCLUDED.AgainstOptionID, AgainstOptionDesc=EXCLUDED.AgainstOptionDesc, "
        "IsAccepted=EXCLUDED.IsAccepted, TotalFor=EXCLUDED.TotalFor, "
        "TotalAgainst=EXCLUDED.TotalAgainst, TotalAbstain=EXCLUDED.TotalAbstain, "
        "LastUpdatedDate=EXCLUDED.LastUpdatedDate, fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        payload.append(
            {
                "Id": row.get("Id"),
                "VoteDateTime": row.get("VoteDateTime"),
                "SessionID": row.get("SessionID"),
                "ItemID": row.get("ItemID"),
                "Ordinal": row.get("Ordinal"),
                "VoteMethodID": row.get("VoteMethodID"),
                "VoteMethodDesc": row.get("VoteMethodDesc"),
                "VoteStatusCode": row.get("VoteStatusCode"),
                "VoteStatusDesc": row.get("VoteStatusDesc"),
                "VoteTitle": row.get("VoteTitle"),
                "VoteSubject": row.get("VoteSubject"),
                "IsNoConfidenceInGov": row.get("IsNoConfidenceInGov"),
                "ForOptionID": row.get("ForOptionID"),
                "ForOptionDesc": row.get("ForOptionDesc"),
                "AgainstOptionID": row.get("AgainstOptionID"),
                "AgainstOptionDesc": row.get("AgainstOptionDesc"),
                "IsAccepted": row.get("IsAccepted"),
                "TotalFor": row.get("TotalFor"),
                "TotalAgainst": row.get("TotalAgainst"),
                "TotalAbstain": row.get("TotalAbstain"),
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def _map_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    """Map an old V3 CSV row to V4-compatible schema.

    CSV columns: id, knesset_num, session_id, sess_item_nbr, sess_item_id,
                 sess_item_dscr, vote_item_id, vote_item_dscr, vote_date,
                 vote_time, is_elctrnc_vote, vote_type, is_accepted,
                 total_for, total_against, total_abstain, ...
    """
    vote_date = row.get("vote_date", "") or ""
    vote_time = row.get("vote_time", "") or ""
    # Combine date and time into a single datetime string
    vote_datetime = vote_date
    if vote_time:
        vote_datetime = f"{vote_date}T{vote_time}:00"

    def _int(val):
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    return {
        "Id": _int(row.get("id")),
        "VoteDateTime": vote_datetime or None,
        "SessionID": _int(row.get("session_id")),
        "ItemID": _int(row.get("sess_item_id")),
        "Ordinal": _int(row.get("vote_nbr_in_sess")),
        "VoteMethodID": _int(row.get("vote_type")),
        "VoteMethodDesc": None,     # CSV doesn't have this
        "VoteStatusCode": None,
        "VoteStatusDesc": None,
        "VoteTitle": row.get("sess_item_dscr") or None,
        "VoteSubject": row.get("vote_item_dscr") or None,
        "IsNoConfidenceInGov": None,
        "ForOptionID": None,
        "ForOptionDesc": None,
        "AgainstOptionID": None,
        "AgainstOptionDesc": None,
        "IsAccepted": _int(row.get("is_accepted")),
        "TotalFor": _int(row.get("total_for")),
        "TotalAgainst": _int(row.get("total_against")),
        "TotalAbstain": _int(row.get("total_abstain")),
        "LastUpdatedDate": None,    # CSV doesn't have this
    }


def fetch_rows(conn, since: Optional[str] = None) -> None:
    """Fetch vote headers using CSV-first with OData backfill.

    When ``since`` is None (initial load):
      1. Download old V3 CSV (~24K rows, covers VoteIDs up to 34525).
         Map column names to V4 schema.  CSV has TotalFor/TotalAgainst/
         TotalAbstain/IsAccepted which V4 does not.
      2. Fetch newer rows from OData V4 where Id > max CSV Id.
         OData rows overwrite CSV rows when they exist (richer metadata).

    When ``since`` is provided (incremental update):
      Skip CSV and fetch only OData rows with LastUpdatedDate > since.
    """
    if since is not None:
        rows = fetch_odata_table(table=ODATA_NAME, since=since)
        count, max_updated = _insert_to_db(conn, rows)
        if max_updated:
            update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
        print(f"Fetched and inserted {count} {TABLE_NAME} rows (incremental)")
        return

    # --- Initial load: CSV first, then OData for newer data ---

    max_csv_id = 0

    def _mapped_csv_rows():
        nonlocal max_csv_id
        for row in fetch_csv_table(CSV_URL):
            mapped = _map_csv_row(row)
            vid = mapped.get("Id")
            if vid and vid > max_csv_id:
                max_csv_id = vid
            yield mapped

    csv_count, _ = _insert_to_db(conn, _mapped_csv_rows())
    print(f"CSV: inserted {csv_count} rows, max Id={max_csv_id}")

    # Fetch OData rows beyond CSV coverage using Id-based cursor
    if max_csv_id > 0:
        odata_rows = fetch_odata_table(
            table=ODATA_NAME,
            since=str(max_csv_id),
            since_field="Id",
            orderby="Id asc",
        )
        odata_count, max_updated = _insert_to_db(conn, odata_rows)
        print(f"OData: inserted {odata_count} rows beyond Id {max_csv_id}")
    else:
        max_updated = None

    # Store max LastUpdatedDate for future incremental updates
    cur = conn.cursor()
    cur.execute(
        f"SELECT MAX(LastUpdatedDate) FROM {TABLE_NAME} WHERE LastUpdatedDate IS NOT NULL"
    )
    row = cur.fetchone()
    db_max_updated = row[0] if row else None
    update_metadata(conn, TABLE_NAME, _utc_now_iso(), db_max_updated)

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total = cur.fetchone()[0]
    print(f"Total {TABLE_NAME} rows: {total}")
