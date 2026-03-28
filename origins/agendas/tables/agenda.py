from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2.extras

from core.odata_client import _utc_now_iso, fetch_table_with_csv_first
from core.db import update_metadata

ODATA_NAME = "KNS_Agenda"
TABLE_NAME = "agenda_raw"
ENSURE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_agenda_knessetnum ON agenda_raw (KnessetNum)",
    "CREATE INDEX IF NOT EXISTS idx_agenda_statusid ON agenda_raw (StatusID)",
    "CREATE INDEX IF NOT EXISTS idx_agenda_initiatorpersonid ON agenda_raw (InitiatorPersonID)",
    "CREATE INDEX IF NOT EXISTS idx_agenda_committeeid ON agenda_raw (CommitteeID)",
    "CREATE INDEX IF NOT EXISTS idx_agenda_name_fts ON agenda_raw USING GIN (to_tsvector('simple', normalize_hebrew_fts(name)))",
    "CREATE INDEX IF NOT EXISTS idx_agenda_name_trgm ON agenda_raw USING GIN (normalize_hebrew(name) gin_trgm_ops)",
]
CSV_URL = "https://production.oknesset.org/pipelines/data/knesset/kns_agenda/kns_agenda.csv"


def create_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS agenda_raw (
            Id INTEGER PRIMARY KEY,
            Number INTEGER,
            ClassificationID INTEGER,
            ClassificationDesc TEXT,
            LeadingAgendaID INTEGER,
            KnessetNum INTEGER,
            Name TEXT,
            SubTypeID INTEGER,
            SubTypeDesc TEXT,
            StatusID INTEGER,
            InitiatorPersonID INTEGER,
            GovRecommendationID INTEGER,
            GovRecommendationDesc TEXT,
            PresidentDecisionDate TEXT,
            PostopenmentReasonID INTEGER,
            PostopenmentReasonDesc TEXT,
            CommitteeID INTEGER,
            RecommendCommitteeID INTEGER,
            MinisterPersonID INTEGER,
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
        "(Id, Number, ClassificationID, ClassificationDesc, LeadingAgendaID, "
        "KnessetNum, Name, SubTypeID, SubTypeDesc, StatusID, "
        "InitiatorPersonID, GovRecommendationID, GovRecommendationDesc, "
        "PresidentDecisionDate, PostopenmentReasonID, PostopenmentReasonDesc, "
        "CommitteeID, RecommendCommitteeID, MinisterPersonID, "
        "LastUpdatedDate, fetched_at) "
        "VALUES (%(Id)s, %(Number)s, %(ClassificationID)s, %(ClassificationDesc)s, %(LeadingAgendaID)s, "
        "%(KnessetNum)s, %(Name)s, %(SubTypeID)s, %(SubTypeDesc)s, %(StatusID)s, "
        "%(InitiatorPersonID)s, %(GovRecommendationID)s, %(GovRecommendationDesc)s, "
        "%(PresidentDecisionDate)s, %(PostopenmentReasonID)s, %(PostopenmentReasonDesc)s, "
        "%(CommitteeID)s, %(RecommendCommitteeID)s, %(MinisterPersonID)s, "
        "%(LastUpdatedDate)s, %(fetched_at)s) "
        "ON CONFLICT (Id) DO UPDATE SET "
        "Number=EXCLUDED.Number, ClassificationID=EXCLUDED.ClassificationID, "
        "ClassificationDesc=EXCLUDED.ClassificationDesc, LeadingAgendaID=EXCLUDED.LeadingAgendaID, "
        "KnessetNum=EXCLUDED.KnessetNum, Name=EXCLUDED.Name, "
        "SubTypeID=EXCLUDED.SubTypeID, SubTypeDesc=EXCLUDED.SubTypeDesc, "
        "StatusID=EXCLUDED.StatusID, InitiatorPersonID=EXCLUDED.InitiatorPersonID, "
        "GovRecommendationID=EXCLUDED.GovRecommendationID, GovRecommendationDesc=EXCLUDED.GovRecommendationDesc, "
        "PresidentDecisionDate=EXCLUDED.PresidentDecisionDate, "
        "PostopenmentReasonID=EXCLUDED.PostopenmentReasonID, PostopenmentReasonDesc=EXCLUDED.PostopenmentReasonDesc, "
        "CommitteeID=EXCLUDED.CommitteeID, RecommendCommitteeID=EXCLUDED.RecommendCommitteeID, "
        "MinisterPersonID=EXCLUDED.MinisterPersonID, "
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
                "Number": row.get("Number") or None,
                "ClassificationID": row.get("ClassificationID") or None,
                "ClassificationDesc": row.get("ClassificationDesc") or None,
                "LeadingAgendaID": row.get("LeadingAgendaID") or None,
                "KnessetNum": row.get("KnessetNum"),
                "Name": row.get("Name"),
                "SubTypeID": row.get("SubTypeID") or None,
                "SubTypeDesc": row.get("SubTypeDesc") or None,
                "StatusID": row.get("StatusID") or None,
                "InitiatorPersonID": row.get("InitiatorPersonID") or None,
                "GovRecommendationID": row.get("GovRecommendationID") or None,
                "GovRecommendationDesc": row.get("GovRecommendationDesc") or None,
                "PresidentDecisionDate": row.get("PresidentDecisionDate") or None,
                "PostopenmentReasonID": row.get("PostopenmentReasonID") or None,
                "PostopenmentReasonDesc": row.get("PostopenmentReasonDesc") or None,
                "CommitteeID": row.get("CommitteeID") or None,
                "RecommendCommitteeID": row.get("RecommendCommitteeID") or None,
                "MinisterPersonID": row.get("MinisterPersonID") or None,
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
