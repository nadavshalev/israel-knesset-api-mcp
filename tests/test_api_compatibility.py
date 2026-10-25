"""Regression coverage for implementation contracts and monthly report caching."""

from datetime import datetime, timezone
import ast
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
import requests

from core import api_compatibility as api

NOW = datetime(2026, 10, 22, 12, tzinfo=timezone.utc)
SCHEMA = {"KNS_Test": {"Id": {"type": "Edm.Int32", "nullable": False}}}
ENTITY = {"endpoint": "KNS_Test", "response_shape": "odata", "fields": [["CsvId", "Id"]]}


def response(data):
    result = requests.Response()
    result.status_code = 200
    result._content = json.dumps(data).encode()
    result.url = "https://example.test/KNS_Test"
    return result


def test_committed_contract_matches_current_importers():
    contract = json.loads(api.CONTRACT_PATH.read_text())
    assert contract["entities"] == api.implementation_contract(), (
        "Upstream ingestion changed. Review and refresh the compatibility contract; see AGENTS.md."
    )
    assert contract["entities"]["bill"]["fields"].count(["BillID", "Id"]) == 1
    # Dynamic row.get(c) loops must be included, not just literal field reads.
    assert ["Name"] in contract["entities"]["secondary_law"]["fields"]
    # Implementation requirements must not disappear into a live-schema snapshot.
    assert ["Id", "QueryID"] in contract["entities"]["query"]["fields"]
    ministry_fields = contract["entities"]["israel_law_ministry"]["fields"]
    assert ["GovMinistryID"] not in ministry_fields
    assert ["MinistryCategoryID"] in ministry_fields
    assert ["MinistryCategoryDesc"] in ministry_fields
    assert contract["entities"]["document_query"]["response_shape"] == "array"
    assert ["Email"] not in contract["entities"]["positions"]["fields"]
    assert ["vote_date"] not in contract["entities"]["plenum_vote"]["fields"]


def test_fingerprints_ignore_empty_ast_fields_added_by_new_python_versions():
    tree = ast.parse("def fetch(): pass")
    original = api._syntax(tree)
    function = tree.body[0]
    if "type_params" not in function._fields:
        function._fields = (*function._fields, "type_params")
    function.type_params = []
    assert api._syntax(tree) == original


@pytest.mark.parametrize("data,expected", [
    ({"value": [{"Id": 1}]}, "OK"),
    ({"value": [{"Unrelated": 1}]}, "FAIL"),
    ({"error": "Blocked"}, "FAIL"),
    ([{"Id": 1}], "FAIL"),
    ({"value": []}, "WARN"),
])
def test_response_shape_and_consumed_field_alternatives(monkeypatch, data, expected):
    monkeypatch.setattr(api, "_request", lambda *args: response(data))
    result = api._check_entity("test", ENTITY, SCHEMA, SCHEMA)
    assert result["status"] == expected


def test_type_change_in_a_consumed_field_fails(monkeypatch):
    monkeypatch.setattr(api, "_request", lambda *args: response({"value": [{"Id": "1"}]}))
    changed = {"KNS_Test": {"Id": {"type": "Edm.String", "nullable": False}}}
    result = api._check_entity("test", ENTITY, changed, SCHEMA)
    assert result["status"] == "FAIL"
    assert "Edm.Int32 -> Edm.String" in result["reason"]


def test_csv_only_field_is_not_required_from_odata(monkeypatch):
    monkeypatch.setattr(api, "_request", lambda *args: response({"value": [{"Id": 1}]}))
    entity = {**ENTITY, "fields": [["Id"], ["TotalFor"]], "optional_fields": ["TotalFor"]}
    assert api._check_entity("test", entity, SCHEMA, SCHEMA)["status"] == "OK"


def test_custom_endpoint_can_work_while_missing_from_metadata(monkeypatch):
    monkeypatch.setattr(api, "_request", lambda *args: response([{"id": 1}]))
    entity = {"endpoint": "KNS_Custom", "response_shape": "array", "fields": [["Id", "id"]]}
    result = api._check_entity("custom", entity, {}, {})
    assert result["status"] == "WARN"
    assert "working endpoint absent from metadata" in result["reason"]


def test_transient_network_failure_is_retryable_and_credentials_are_redacted(monkeypatch):
    monkeypatch.setattr(api, "ISRAEL_PROXY_URL", "http://user:secret@proxy:1234")

    def fail(*args):
        raise requests.exceptions.ProxyError("Could not connect to http://user:secret@proxy:1234")

    monkeypatch.setattr(api, "_request", fail)
    result = api._check_entity("test", ENTITY, SCHEMA, SCHEMA)
    assert result["status"] == "ERROR"
    assert "secret" not in result["reason"]


def setup_cache(monkeypatch, tmp_path, *, checked_at=None, status="OK"):
    contract = {"entities": {"test": ENTITY}, "observed_schema": SCHEMA}
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(contract))
    monkeypatch.setattr(api, "CONTRACT_PATH", path)
    monkeypatch.setattr(api, "implementation_contract", lambda: contract["entities"])
    previous = {
        "checked_at": checked_at or NOW.isoformat(),
        "rows": [{"table": "test", "status": status, "reason": "Existing result"}],
        "contract_sha256": api._fingerprint(contract),
        "implementation_sha256": api._fingerprint(contract["entities"]),
    }
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (previous,)
    check = Mock(return_value={"checked_at": NOW.isoformat(), "rows": []})
    monkeypatch.setattr(api, "check_compatibility", check)
    return conn, previous, check


@pytest.mark.parametrize("status", ["OK", "FAIL", "WARN"])
def test_same_month_uses_persisted_report_without_upstream_requests(monkeypatch, tmp_path, status):
    conn, previous, check = setup_cache(monkeypatch, tmp_path, status=status)
    assert api.latest_report(conn, now=NOW) == previous
    check.assert_not_called()


@pytest.mark.parametrize("reason", ["new_month", "network_error", "new_contract", "changed_code", "force"])
def test_due_check_runs_and_persists_results(monkeypatch, tmp_path, reason):
    conn, previous, check = setup_cache(monkeypatch, tmp_path)
    if reason == "new_month":
        previous["checked_at"] = "2026-09-30T23:59:59+00:00"
    elif reason == "network_error":
        previous["rows"][0]["status"] = "ERROR"
    elif reason == "new_contract":
        previous["contract_sha256"] = "old contract"
    elif reason == "changed_code":
        previous["implementation_sha256"] = "old implementation"
    report = api.latest_report(conn, now=NOW, force=reason == "force")
    check.assert_called_once()
    assert report["checked_at"] == NOW.isoformat()
    statements = conn.cursor.return_value.__enter__.return_value.execute.call_args_list
    assert any("ON CONFLICT" in call.args[0] for call in statements)
    assert conn.commit.call_count == 2


def test_first_cycle_runs_check(monkeypatch, tmp_path):
    conn, _, check = setup_cache(monkeypatch, tmp_path)
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
    api.latest_report(conn, now=NOW)
    check.assert_called_once()


def test_dry_run_does_not_fetch_or_create_cache(monkeypatch, tmp_path):
    conn, previous, check = setup_cache(monkeypatch, tmp_path)
    assert api.latest_report(conn, now=NOW, cached_only=True) == previous
    check.assert_not_called()
    conn.commit.assert_not_called()
    statements = conn.cursor.return_value.__enter__.return_value.execute.call_args_list
    assert all(call.args[0].startswith("SELECT") for call in statements)


def test_new_upstream_entity_is_informational(monkeypatch):
    contract = {"entities": {"test": ENTITY}, "observed_schema": SCHEMA}
    monkeypatch.setattr(api, "_request", lambda *args: SimpleNamespace(content=b"metadata"))
    monkeypatch.setattr(api, "_metadata_schema", lambda _: {**SCHEMA, "KNS_New": {}})
    monkeypatch.setattr(api, "_check_entity", lambda *args: {"table": "test", "status": "OK", "reason": "Match"})
    report = api.check_compatibility(contract, contract["entities"], NOW)
    assert report["rows"][0]["status"] == "INFO"
    assert "KNS_New" in report["rows"][0]["reason"]


def test_failed_metadata_request_produces_cached_error_not_success(monkeypatch):
    def fail(*args):
        raise requests.exceptions.Timeout("Timed out")

    monkeypatch.setattr(api, "_request", fail)
    contract = {"entities": {"test": ENTITY}, "observed_schema": SCHEMA}
    report = api.check_compatibility(contract, contract["entities"], NOW)
    assert report["rows"][0]["status"] == "ERROR"
    assert "Timed out" in report["rows"][0]["reason"]
