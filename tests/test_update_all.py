"""Update summaries must retain failures and distinguish them from zero new rows."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
import requests

from scripts import update_all as updater


@pytest.mark.parametrize("parallel", [False, True])
def test_summary_retains_fetch_failure_and_success_with_no_new_rows(monkeypatch, capsys, parallel):
    conn = Mock()
    failed = SimpleNamespace(
        label="bills", table_name="bill_raw",
        module=SimpleNamespace(fetch_rows=Mock(side_effect=requests.exceptions.ProxyError("Proxy unavailable"))))
    successful = SimpleNamespace(
        label="people", table_name="person_raw",
        module=SimpleNamespace(fetch_rows=Mock()))
    monkeypatch.setattr(updater, "_resolve_selected", lambda _: [failed, successful])
    monkeypatch.setattr(updater, "connect_db", lambda: conn)
    monkeypatch.setattr(updater, "_since_and_mode", lambda *args: (None, "full"))
    monkeypatch.setattr(updater, "_get_row_count", lambda *args: 10)
    monkeypatch.setattr(updater, "_get_latest_row_timestamp", lambda *args: "2026-10-20T12:32:51Z")
    monkeypatch.setattr(updater, "ensure_indexes", lambda _: None)
    monkeypatch.setattr(updater, "print_latest_report", lambda *args, **kwargs: None)

    run = updater.update_tables_parallel if parallel else updater.update_tables_sync
    run(conn)

    summary = capsys.readouterr().out.split("Update finished", 1)[1]
    assert "1 failed table(s)" in summary
    assert "FAILED: ProxyError: Proxy unavailable" in summary
    assert "2026-10-20T12:32:51Z" in summary
    failed_line = next(line for line in summary.splitlines() if line.startswith("bills"))
    assert "n/a" in failed_line
    success_line = next(line for line in summary.splitlines() if line.startswith("people"))
    assert "+0" in success_line and success_line.endswith("OK")
    conn.rollback.assert_called()
    successful.module.fetch_rows.assert_called_once()


def test_successful_fetch_reports_actual_delta_and_latest_persisted_timestamp(monkeypatch):
    conn = Mock()
    spec = SimpleNamespace(label="bills", table_name="bill_raw", module=SimpleNamespace(fetch_rows=Mock()))
    counts = iter([10, 13])
    monkeypatch.setattr(updater, "_since_and_mode", lambda *args: ("cutoff", "incremental"))
    monkeypatch.setattr(updater, "_get_row_count", lambda *args: next(counts))
    monkeypatch.setattr(updater, "_get_latest_row_timestamp", lambda *args: "2026-10-20T12:32:51Z")

    result = updater._fetch_table(conn, spec, False, False)

    assert result.delta == 3
    assert result.error is None
    assert result.latest_row == "2026-10-20T12:32:51Z"
    spec.module.fetch_rows.assert_called_once_with(conn, since="cutoff")


def test_dry_run_does_not_report_success_or_fetch(monkeypatch, capsys):
    spec = SimpleNamespace(label="bills", table_name="bill_raw", module=SimpleNamespace(fetch_rows=Mock()))
    monkeypatch.setattr(updater, "_since_and_mode", lambda *args: (None, "full"))
    monkeypatch.setattr(updater, "_get_row_count", lambda *args: 10)
    monkeypatch.setattr(updater, "_get_latest_row_timestamp", lambda *args: "n/a")

    result = updater._fetch_table(Mock(), spec, False, True)
    updater._print_summary([result], 0)

    spec.module.fetch_rows.assert_not_called()
    assert "SKIPPED: dry-run" in capsys.readouterr().out


def test_connection_failure_is_in_summary(monkeypatch):
    def fail_connect():
        raise ConnectionError("Database unreachable")

    monkeypatch.setattr(updater, "connect_db", fail_connect)
    result = updater._fetch_one(SimpleNamespace(label="bills"), False, False)

    assert result.error == "ConnectionError: Database unreachable"
    assert result.delta is None
    assert result.latest_row == "unavailable"


@pytest.mark.parametrize("latest,expected", [(None, "n/a"), (datetime(2026, 10, 20, 12, 32, 51), "2026-10-20T12:32:51Z")])
def test_latest_timestamp_handles_empty_tables_and_formats_utc(latest, expected):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (latest,)
    assert updater._get_latest_row_timestamp(conn, "bill_raw") == expected


def test_multiline_failure_reason_stays_on_one_summary_line(capsys):
    updater._print_summary([
        updater.FetchResult("bills", None, 2, error="ValueError: Invalid JSON\n<html>Blocked</html>"),
    ], 2)
    output = capsys.readouterr().out
    assert "FAILED: ValueError: Invalid JSON <html>Blocked</html>" in output


@pytest.mark.parametrize("parallel", [False, True])
def test_compatibility_report_prints_after_each_summary(monkeypatch, capsys, parallel):
    conn = Mock()
    spec = SimpleNamespace(label="bills")
    result = updater.FetchResult("bills", 0, 1)
    monkeypatch.setattr(updater, "_resolve_selected", lambda _: [spec])
    monkeypatch.setattr(updater, "_fetch_one", lambda *args: result)
    monkeypatch.setattr(updater, "_fetch_table", lambda *args: result)
    monkeypatch.setattr(updater, "ensure_indexes", lambda _: None)
    report = Mock(side_effect=lambda *args, **kwargs: print("Compatibility report"))
    monkeypatch.setattr(updater, "print_latest_report", report)

    run = updater.update_tables_parallel if parallel else updater.update_tables_sync
    run(conn, dry_run=True)

    output = capsys.readouterr().out
    assert output.index("Update complete") < output.index("Compatibility report")
    report.assert_called_once_with(conn, cached_only=True)
