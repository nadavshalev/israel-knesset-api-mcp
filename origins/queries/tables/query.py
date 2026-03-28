from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Query"
TABLE_NAME = "query_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_query_knessetnum ON query_raw (KnessetNum)",
    "CREATE INDEX IF NOT EXISTS idx_query_statusid ON query_raw (StatusID)",
    "CREATE INDEX IF NOT EXISTS idx_query_personid ON query_raw (PersonID)",
    "CREATE INDEX IF NOT EXISTS idx_query_govministryid ON query_raw (GovMinistryID)",
    "CREATE INDEX IF NOT EXISTS idx_query_name_fts ON query_raw USING GIN (to_tsvector('simple', normalize_hebrew_fts(name)))",
    "CREATE INDEX IF NOT EXISTS idx_query_name_trgm ON query_raw USING GIN (normalize_hebrew(name) gin_trgm_ops)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/knesset/kns_query/kns_query.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS query_raw (
            QueryID INTEGER PRIMARY KEY,
            Number INTEGER,
            KnessetNum INTEGER,
            Name TEXT,
            TypeID INTEGER,
            TypeDesc TEXT,
            StatusID INTEGER,
            PersonID INTEGER,
            GovMinistryID INTEGER,
            SubmitDate TEXT,
            ReplyMinisterDate TEXT,
            ReplyDatePlanned TEXT,
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
        "(QueryID, Number, KnessetNum, Name, TypeID, TypeDesc, "
        "StatusID, PersonID, GovMinistryID, SubmitDate, "
        "ReplyMinisterDate, ReplyDatePlanned, LastUpdatedDate, fetched_at) "
        "VALUES (%(QueryID)s, %(Number)s, %(KnessetNum)s, %(Name)s, %(TypeID)s, %(TypeDesc)s, "
        "%(StatusID)s, %(PersonID)s, %(GovMinistryID)s, %(SubmitDate)s, "
        "%(ReplyMinisterDate)s, %(ReplyDatePlanned)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (QueryID) DO UPDATE SET "
        "Number=EXCLUDED.Number, KnessetNum=EXCLUDED.KnessetNum, Name=EXCLUDED.Name, "
        "TypeID=EXCLUDED.TypeID, TypeDesc=EXCLUDED.TypeDesc, "
        "StatusID=EXCLUDED.StatusID, PersonID=EXCLUDED.PersonID, "
        "GovMinistryID=EXCLUDED.GovMinistryID, SubmitDate=EXCLUDED.SubmitDate, "
        "ReplyMinisterDate=EXCLUDED.ReplyMinisterDate, ReplyDatePlanned=EXCLUDED.ReplyDatePlanned, "
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
                "QueryID": row.get("QueryID"),
                "Number": row.get("Number") or None,
                "KnessetNum": row.get("KnessetNum"),
                "Name": row.get("Name"),
                "TypeID": row.get("TypeID") or None,
                "TypeDesc": row.get("TypeDesc") or None,
                "StatusID": row.get("StatusID") or None,
                "PersonID": row.get("PersonID") or None,
                "GovMinistryID": row.get("GovMinistryID") or None,
                "SubmitDate": row.get("SubmitDate") or None,
                "ReplyMinisterDate": row.get("ReplyMinisterDate") or None,
                "ReplyDatePlanned": row.get("ReplyDatePlanned") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_table_with_csv_first(
        csv_url=CSV_URL,
        odata_table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
