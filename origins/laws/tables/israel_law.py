from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_IsraelLaw"
TABLE_NAME = "israel_law_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_israellaw_knessetnum ON israel_law_raw (KnessetNum)",
    "CREATE INDEX IF NOT EXISTS idx_israellaw_name ON israel_law_raw (Name text_pattern_ops)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS israel_law_raw (
            Id INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name TEXT,
            IsBasicLaw INTEGER,
            IsFavoriteLaw INTEGER,
            PublicationDate TEXT,
            LatestPublicationDate TEXT,
            IsBudgetLaw INTEGER,
            LawValidityID INTEGER,
            LawValidityDesc TEXT,
            ValidityStartDate TEXT,
            ValidityStartDateNotes TEXT,
            ValidityFinishDate TEXT,
            ValidityFinishDateNotes TEXT,
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
        "(Id, KnessetNum, Name, IsBasicLaw, IsFavoriteLaw, PublicationDate, LatestPublicationDate, "
        "IsBudgetLaw, LawValidityID, LawValidityDesc, ValidityStartDate, ValidityStartDateNotes, "
        "ValidityFinishDate, ValidityFinishDateNotes, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(KnessetNum)s, %(Name)s, %(IsBasicLaw)s, %(IsFavoriteLaw)s, %(PublicationDate)s, "
        "%(LatestPublicationDate)s, %(IsBudgetLaw)s, %(LawValidityID)s, %(LawValidityDesc)s, "
        "%(ValidityStartDate)s, %(ValidityStartDateNotes)s, %(ValidityFinishDate)s, "
        "%(ValidityFinishDateNotes)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "KnessetNum=EXCLUDED.KnessetNum, Name=EXCLUDED.Name, IsBasicLaw=EXCLUDED.IsBasicLaw, "
        "IsFavoriteLaw=EXCLUDED.IsFavoriteLaw, PublicationDate=EXCLUDED.PublicationDate, "
        "LatestPublicationDate=EXCLUDED.LatestPublicationDate, IsBudgetLaw=EXCLUDED.IsBudgetLaw, "
        "LawValidityID=EXCLUDED.LawValidityID, LawValidityDesc=EXCLUDED.LawValidityDesc, "
        "ValidityStartDate=EXCLUDED.ValidityStartDate, ValidityStartDateNotes=EXCLUDED.ValidityStartDateNotes, "
        "ValidityFinishDate=EXCLUDED.ValidityFinishDate, ValidityFinishDateNotes=EXCLUDED.ValidityFinishDateNotes, "
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
                "KnessetNum": row.get("KnessetNum") or None,
                "Name": row.get("Name") or None,
                "IsBasicLaw": 1 if row.get("IsBasicLaw") else 0,
                "IsFavoriteLaw": 1 if row.get("IsFavoriteLaw") else 0,
                "PublicationDate": row.get("PublicationDate") or None,
                "LatestPublicationDate": row.get("LatestPublicationDate") or None,
                "IsBudgetLaw": 1 if row.get("IsBudgetLaw") else 0,
                "LawValidityID": row.get("LawValidityID") or None,
                "LawValidityDesc": row.get("LawValidityDesc") or None,
                "ValidityStartDate": row.get("ValidityStartDate") or None,
                "ValidityStartDateNotes": row.get("ValidityStartDateNotes") or None,
                "ValidityFinishDate": row.get("ValidityFinishDate") or None,
                "ValidityFinishDateNotes": row.get("ValidityFinishDateNotes") or None,
                "LastUpdatedDate": last_updated,
                "fetched_at": now,
            }
        )
    if payload:
        psycopg2.extras.execute_batch(cur, sql, payload)
    conn.commit()
    return len(payload), max_updated


def fetch_rows(conn, since: Optional[str] = None) -> None:
    rows = fetch_odata_table(
        table=ODATA_NAME,
        since=since,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
