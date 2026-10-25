"""Law ministry migration, backfill and MCP output; isolated temporary DB tables."""

from unittest.mock import Mock

import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

import config
from origins.laws.tables import israel_law_ministry as table
from origins.laws.laws_view import _fetch_ministries
from origins.laws.laws_models import LawResultFull


@pytest.fixture
def conn():
    connection = psycopg2.connect(
        host=config.POSTGRES_HOST, port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB, user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD, connect_timeout=8,
    )
    with connection.cursor() as cur:
        # Shadow production tables, including metadata written by fetch_rows.
        cur.execute("""CREATE TEMP TABLE israel_law_ministry_raw (
            Id INTEGER PRIMARY KEY, IsraelLawID INTEGER, GovMinistryID INTEGER,
            LastUpdatedDate TEXT, fetched_at TEXT)""")
        cur.execute("""CREATE TEMP TABLE metadata (
            table_name TEXT PRIMARY KEY, last_sync_completed_at TEXT,
            last_updated_cutoff TEXT)""")
    connection.commit()
    try:
        yield connection
    finally:
        connection.close()


def source_row(id=1, law_id=2000001, category=16, desc="האוצר", date="2020-01-01T00:00:00Z"):
    return {"Id": id, "IsraelLawID": law_id, "MinistryCategoryID": category,
            "MinistryCategoryDesc": desc, "LastUpdatedDate": date}


def test_existing_records_are_backfilled_despite_newer_incremental_cutoff(conn, monkeypatch):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO israel_law_ministry_raw (Id, IsraelLawID, LastUpdatedDate) "
                    "VALUES (1, 2000001, '2020-01-01T00:00:00Z')")
    conn.commit()
    table.create_table(conn)
    fetch = Mock(return_value=iter([source_row()]))
    monkeypatch.setattr(table, "fetch_odata_table", fetch)

    table.fetch_rows(conn, since="2026-10-20T00:00:00Z")

    # Unique-ID paging retrieves old records even if their timestamps never changed.
    fetch.assert_called_once_with(table=table.ODATA_NAME, since_field="Id", numeric=True)
    with conn.cursor() as cur:
        cur.execute("SELECT MinistryCategoryID, MinistryCategoryDesc "
                    "FROM israel_law_ministry_raw WHERE Id=1")
        assert cur.fetchone() == (16, "האוצר")
        cur.execute("SELECT attname FROM pg_attribute WHERE attrelid='israel_law_ministry_raw'::regclass "
                    "AND attnum > 0 AND NOT attisdropped")
        assert "govministryid" not in {row[0] for row in cur.fetchall()}
        cur.execute("SELECT last_updated_cutoff FROM metadata WHERE table_name=%s", (table.TABLE_NAME,))
        assert cur.fetchone()[0] == "2020-01-01T00:00:00Z"
    # Repeated startup migration must preserve the repaired data.
    table.create_table(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT MinistryCategoryDesc FROM israel_law_ministry_raw WHERE Id=1")
        assert cur.fetchone()[0] == "האוצר"


def test_after_backfill_incremental_fetch_preserves_older_records(conn, monkeypatch):
    table.create_table(conn)
    table._insert_to_db(conn, [source_row()])
    fetch = Mock(return_value=iter([]))
    monkeypatch.setattr(table, "fetch_odata_table", fetch)

    table.fetch_rows(conn, since="2026-10-20T00:00:00Z")

    fetch.assert_called_once_with(table=table.ODATA_NAME, since="2026-10-20T00:00:00Z")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        assert _fetch_ministries(cur, 2000001) == ["האוצר"]


def test_new_database_schema_and_upserts_use_categories_not_ministry_ids(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE pg_temp.israel_law_ministry_raw")
        # Make the fresh schema temporary as well.
        cur.execute("CREATE TEMP TABLE israel_law_ministry_raw "
                    "(Id INTEGER PRIMARY KEY, IsraelLawID INTEGER, MinistryCategoryID INTEGER, "
                    "MinistryCategoryDesc TEXT, LastUpdatedDate TEXT, fetched_at TEXT)")
    conn.commit()
    table.create_table(conn)
    table._insert_to_db(conn, [source_row(), source_row(id=2, law_id=2000002)])
    table._insert_to_db(conn, [source_row(category=0, desc="אחר", date="2026-10-20T00:00:00Z")])
    with conn.cursor() as cur:
        cur.execute("SELECT MinistryCategoryID, MinistryCategoryDesc FROM israel_law_ministry_raw WHERE Id=1")
        assert cur.fetchone() == (0, "אחר")
        cur.execute("SELECT count(*) FROM israel_law_ministry_raw")
        assert cur.fetchone()[0] == 2


def test_mcp_ministry_strings_come_directly_from_unique_nonblank_descriptions(conn):
    table.create_table(conn)
    table._insert_to_db(conn, [
        source_row(id=1, desc=" האוצר "), source_row(id=2, desc="האוצר"),
        source_row(id=3, category=17, desc="משפטים"), source_row(id=4, desc=None),
        source_row(id=5, desc="   "), source_row(id=6, law_id=2000002, desc="אחר"),
    ])
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        ministries = _fetch_ministries(cur, 2000001)
        assert ministries == ["האוצר", "משפטים"]
        assert _fetch_ministries(cur, 9999999) is None
    output = LawResultFull(law_id=2000001, ministries=ministries).model_dump()
    assert output["ministries"] == ["האוצר", "משפטים"]


def test_missing_category_fails_before_writing_any_rows(conn):
    table.create_table(conn)
    with pytest.raises(ValueError, match="MinistryCategoryID"):
        table._insert_to_db(conn, [source_row(), source_row(id=2, category=None)])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM israel_law_ministry_raw")
        assert cur.fetchone()[0] == 0
