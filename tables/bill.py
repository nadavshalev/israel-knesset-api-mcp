from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Bill"
TABLE_NAME = "bill_raw"
CSV_URL = "https://production.oknesset.org/pipelines/data/bills/kns_bill/kns_bill.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bill_raw (
            Id INTEGER PRIMARY KEY,
            KnessetNum INTEGER,
            Name TEXT,
            SubTypeID INTEGER,
            SubTypeDesc TEXT,
            PrivateNumber INTEGER,
            CommitteeID INTEGER,
            StatusID INTEGER,
            Number INTEGER,
            PostponementReasonID INTEGER,
            PostponementReasonDesc TEXT,
            PublicationDate TEXT,
            PublicationSeriesID INTEGER,
            PublicationSeriesDesc TEXT,
            MagazineNumber INTEGER,
            PageNumber INTEGER,
            IsContinuationBill INTEGER,
            SummaryLaw TEXT,
            LastUpdatedDate TEXT,
            fetched_at TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bill_knessetnum ON bill_raw (KnessetNum)")
    conn.commit()


def _insert_to_db(conn, rows: Iterable[Dict[str, Any]]) -> Tuple[int, Optional[str]]:
    cur = conn.cursor()
    now = _utc_now_iso()
    sql = (
        f"INSERT INTO {TABLE_NAME} "
        "(Id, KnessetNum, Name, SubTypeID, SubTypeDesc, PrivateNumber, "
        "CommitteeID, StatusID, Number, PostponementReasonID, PostponementReasonDesc, "
        "PublicationDate, PublicationSeriesID, PublicationSeriesDesc, "
        "MagazineNumber, PageNumber, IsContinuationBill, SummaryLaw, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(KnessetNum)s, %(Name)s, %(SubTypeID)s, %(SubTypeDesc)s, %(PrivateNumber)s, "
        "%(CommitteeID)s, %(StatusID)s, %(Number)s, %(PostponementReasonID)s, %(PostponementReasonDesc)s, "
        "%(PublicationDate)s, %(PublicationSeriesID)s, %(PublicationSeriesDesc)s, "
        "%(MagazineNumber)s, %(PageNumber)s, %(IsContinuationBill)s, %(SummaryLaw)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "KnessetNum=EXCLUDED.KnessetNum, Name=EXCLUDED.Name, SubTypeID=EXCLUDED.SubTypeID, "
        "SubTypeDesc=EXCLUDED.SubTypeDesc, PrivateNumber=EXCLUDED.PrivateNumber, "
        "CommitteeID=EXCLUDED.CommitteeID, StatusID=EXCLUDED.StatusID, Number=EXCLUDED.Number, "
        "PostponementReasonID=EXCLUDED.PostponementReasonID, PostponementReasonDesc=EXCLUDED.PostponementReasonDesc, "
        "PublicationDate=EXCLUDED.PublicationDate, PublicationSeriesID=EXCLUDED.PublicationSeriesID, "
        "PublicationSeriesDesc=EXCLUDED.PublicationSeriesDesc, MagazineNumber=EXCLUDED.MagazineNumber, "
        "PageNumber=EXCLUDED.PageNumber, IsContinuationBill=EXCLUDED.IsContinuationBill, "
        "SummaryLaw=EXCLUDED.SummaryLaw, LastUpdatedDate=EXCLUDED.LastUpdatedDate, "
        "fetched_at=EXCLUDED.fetched_at"
    )
    payload = []
    max_updated: Optional[str] = None
    for row in rows:
        last_updated = row.get("LastUpdatedDate")
        if last_updated and (max_updated is None or last_updated > max_updated):
            max_updated = last_updated
        cont = row.get("IsContinuationBill")
        payload.append(
            {
                "Id": row.get("BillID") or row.get("Id"),
                "KnessetNum": row.get("KnessetNum"),
                "Name": row.get("Name"),
                "SubTypeID": row.get("SubTypeID"),
                "SubTypeDesc": row.get("SubTypeDesc"),
                "PrivateNumber": row.get("PrivateNumber") or None,
                "CommitteeID": row.get("CommitteeID") or None,
                "StatusID": row.get("StatusID"),
                "Number": row.get("Number") or None,
                "PostponementReasonID": row.get("PostponementReasonID") or None,
                "PostponementReasonDesc": row.get("PostponementReasonDesc") or None,
                "PublicationDate": row.get("PublicationDate") or None,
                "PublicationSeriesID": row.get("PublicationSeriesID") or None,
                "PublicationSeriesDesc": row.get("PublicationSeriesDesc") or None,
                "MagazineNumber": row.get("MagazineNumber") or None,
                "PageNumber": row.get("PageNumber") or None,
                "IsContinuationBill": 1 if cont and str(cont).lower() not in ("", "false", "0", "none") else 0,
                "SummaryLaw": row.get("SummaryLaw") or None,
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
