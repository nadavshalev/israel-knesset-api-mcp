from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_odata_table
from core.db import update_metadata

ODATA_NAME = "KNS_LawCorrections"
TABLE_NAME = "law_corrections_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_lawcorrections_billid ON law_corrections_raw (BillID)",
]


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS law_corrections_raw (
            Id INTEGER PRIMARY KEY,
            BillID INTEGER,
            CorrectionTypeID INTEGER,
            CorrectionTypeDesc TEXT,
            IsKnessetInvolvement INTEGER,
            CommitteeID INTEGER,
            CorrectionStatusID INTEGER,
            CorrectionStatusDesc TEXT,
            VoteDate TEXT,
            PublicationDate TEXT,
            PublicationSeriesID INTEGER,
            PublicationSeriesDesc TEXT,
            MagazineNumber TEXT,
            PageNumber TEXT,
            CommencementDate TEXT,
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
        "(Id, BillID, CorrectionTypeID, CorrectionTypeDesc, IsKnessetInvolvement, CommitteeID, "
        "CorrectionStatusID, CorrectionStatusDesc, VoteDate, PublicationDate, PublicationSeriesID, "
        "PublicationSeriesDesc, MagazineNumber, PageNumber, CommencementDate, LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(BillID)s, %(CorrectionTypeID)s, %(CorrectionTypeDesc)s, %(IsKnessetInvolvement)s, "
        "%(CommitteeID)s, %(CorrectionStatusID)s, %(CorrectionStatusDesc)s, %(VoteDate)s, %(PublicationDate)s, "
        "%(PublicationSeriesID)s, %(PublicationSeriesDesc)s, %(MagazineNumber)s, %(PageNumber)s, "
        "%(CommencementDate)s, %(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "BillID=EXCLUDED.BillID, CorrectionTypeID=EXCLUDED.CorrectionTypeID, "
        "CorrectionTypeDesc=EXCLUDED.CorrectionTypeDesc, IsKnessetInvolvement=EXCLUDED.IsKnessetInvolvement, "
        "CommitteeID=EXCLUDED.CommitteeID, CorrectionStatusID=EXCLUDED.CorrectionStatusID, "
        "CorrectionStatusDesc=EXCLUDED.CorrectionStatusDesc, VoteDate=EXCLUDED.VoteDate, "
        "PublicationDate=EXCLUDED.PublicationDate, PublicationSeriesID=EXCLUDED.PublicationSeriesID, "
        "PublicationSeriesDesc=EXCLUDED.PublicationSeriesDesc, MagazineNumber=EXCLUDED.MagazineNumber, "
        "PageNumber=EXCLUDED.PageNumber, CommencementDate=EXCLUDED.CommencementDate, "
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
                "BillID": row.get("BillID") or None,
                "CorrectionTypeID": row.get("CorrectionTypeID") or None,
                "CorrectionTypeDesc": row.get("CorrectionTypeDesc") or None,
                "IsKnessetInvolvement": 1 if row.get("IsKnessetInvolvement") else 0,
                "CommitteeID": row.get("CommitteeID") or None,
                "CorrectionStatusID": row.get("CorrectionStatusID") or None,
                "CorrectionStatusDesc": row.get("CorrectionStatusDesc") or None,
                "VoteDate": row.get("VoteDate") or None,
                "PublicationDate": row.get("PublicationDate") or None,
                "PublicationSeriesID": row.get("PublicationSeriesID") or None,
                "PublicationSeriesDesc": row.get("PublicationSeriesDesc") or None,
                "MagazineNumber": row.get("MagazineNumber") or None,
                "PageNumber": row.get("PageNumber") or None,
                "CommencementDate": row.get("CommencementDate") or None,
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
        since_field="Id",
        numeric=True,
    )
    count, max_updated = _insert_to_db(conn, rows)
    if max_updated:
        update_metadata(conn, TABLE_NAME, _utc_now_iso(), max_updated)
    print(f"Fetched and inserted {count} {TABLE_NAME} rows")
