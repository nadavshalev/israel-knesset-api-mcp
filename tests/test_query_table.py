"""Id migration and OData/CSV ingestion, using session-local temporary tables."""

from types import SimpleNamespace
from unittest.mock import Mock

import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

import config
from origins.queries.tables import query as table
from origins.queries import queries_view


@pytest.fixture
def conn():
    connection = psycopg2.connect(
        host=config.POSTGRES_HOST, port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB, user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD, connect_timeout=8,
    )
    with connection.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE query_raw (
            QueryID INTEGER PRIMARY KEY, Number INTEGER, KnessetNum INTEGER,
            Name TEXT, TypeID INTEGER, TypeDesc TEXT, StatusID INTEGER,
            PersonID INTEGER, GovMinistryID INTEGER, SubmitDate TEXT,
            ReplyMinisterDate TEXT, ReplyDatePlanned TEXT, LastUpdatedDate TEXT,
            fetched_at TEXT)""")
        cur.execute("""CREATE TEMP TABLE document_query_raw (
            Id INTEGER PRIMARY KEY, QueryID INTEGER REFERENCES query_raw(QueryID),
            GroupTypeDesc TEXT, ApplicationDesc TEXT, FilePath TEXT)""")
        cur.execute("""CREATE TEMP TABLE metadata (
            table_name TEXT PRIMARY KEY, last_sync_completed_at TEXT,
            last_updated_cutoff TEXT)""")
    connection.commit()
    try:
        yield connection
    finally:
        connection.close()


def row(**overrides):
    return {"Id": 900000001, "Number": 3, "KnessetNum": 25,
            "Name": "שאילתה לבדיקה", "SubmitDate": "2026-01-01T00:00:00Z",
            "LastUpdatedDate": "2026-10-20T12:00:00Z", **overrides}


def test_rename_preserves_existing_values_primary_key_and_document_foreign_key(conn):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO query_raw (QueryID, Number, Name) VALUES (563908, 3, 'existing')")
        cur.execute("INSERT INTO document_query_raw (Id, QueryID) VALUES (1, 563908)")
    conn.commit()

    table.create_table(conn)
    table.create_table(conn)  # Startup migration is idempotent.

    with conn.cursor() as cur:
        cur.execute("SELECT Id, Number, Name FROM query_raw")
        assert cur.fetchone() == (563908, 3, "existing")
        cur.execute("SELECT attname FROM pg_attribute WHERE attrelid='query_raw'::regclass "
                    "AND attnum > 0 AND NOT attisdropped")
        assert "queryid" not in {record[0] for record in cur.fetchall()}
        cur.execute("SELECT d.QueryID FROM document_query_raw d JOIN query_raw q ON q.Id=d.QueryID")
        assert cur.fetchone()[0] == 563908
    table._insert_to_db(conn, [row(Id=563908, Name="updated")])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*),max(Name) FROM query_raw")
        assert cur.fetchone() == (1, "updated")


def test_live_odata_id_is_persisted_and_sync_cutoff_advances(conn, monkeypatch):
    table.create_table(conn)
    fetch = Mock(return_value=iter([row()]))
    monkeypatch.setattr(table, "fetch_table_with_csv_first", fetch)

    table.fetch_rows(conn, since="2026-03-19T00:00:00Z")

    with conn.cursor() as cur:
        cur.execute("SELECT Id, Number FROM query_raw")
        assert cur.fetchone() == (900000001, 3)
        cur.execute("SELECT last_updated_cutoff FROM metadata WHERE table_name='query_raw'")
        assert cur.fetchone()[0] == "2026-10-20T12:00:00Z"


def test_csv_queryid_and_odata_id_upsert_the_same_entity(conn):
    table.create_table(conn)
    csv = row()
    csv["QueryID"] = csv.pop("Id")
    table._insert_to_db(conn, [csv])
    table._insert_to_db(conn, [row(Name="Updated from OData")])
    with conn.cursor() as cur:
        cur.execute("SELECT Id, Name FROM query_raw")
        assert cur.fetchall() == [(900000001, "Updated from OData")]


def test_missing_identifier_fails_without_partial_writes_or_advancing_metadata(conn, monkeypatch):
    table.create_table(conn)
    invalid = row()
    del invalid["Id"]
    monkeypatch.setattr(table, "fetch_table_with_csv_first", lambda **kwargs: iter([row(), invalid]))
    with pytest.raises(ValueError, match="missing its required Id"):
        table.fetch_rows(conn, since="2026-03-19T00:00:00Z")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM query_raw")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM metadata")
        assert cur.fetchone()[0] == 0


def test_full_detail_and_cross_entity_search_use_id_but_keep_document_queryid(conn, monkeypatch):
    table.create_table(conn)
    table._insert_to_db(conn, [row()])
    with conn.cursor() as cur:
        cur.execute("INSERT INTO document_query_raw (Id,QueryID,GroupTypeDesc,FilePath) "
                    "VALUES (1,900000001,'שאילתה','https://example.test/query.pdf')")
    conn.commit()
    reader = SimpleNamespace(cursor=lambda: conn.cursor(cursor_factory=RealDictCursor), close=lambda: None)
    monkeypatch.setattr(queries_view, "connect_readonly", lambda: reader)

    result = queries_view.queries(query_id=900000001, full_details=True)

    assert result.items[0].query_id == 900000001
    assert result.items[0].documents[0].path == "https://example.test/query.pdf"
    assert result.items[0].model_dump()["query_id"] == 900000001
    count_sql, count_args, search_sql, search_args = queries_view._build_queries_search(
        query=None, knesset_num=25, date="2025-01-01", date_to="2027-01-01", top_n=5,
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(count_sql, count_args)
        assert cur.fetchone()["count"] == 1
        cur.execute(search_sql, search_args)
        assert cur.fetchone()["id"] == 900000001
