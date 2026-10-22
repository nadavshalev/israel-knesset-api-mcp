"""Small, read-only upstream checks; PostgreSQL keeps the last monthly report."""

import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import inspect
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import requests

from config import BASE_URL, ISRAEL_PROXY_URL
from core import odata_client

CONTRACT_PATH = Path(__file__).resolve().parent.parent / "docs/knesset-api-contract.json"
EDM = {"e": "http://docs.oasis-open.org/odata/ns/edm"}


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _syntax(node):
    """Stable across development/deployment Python versions (3.12 adds type_params)."""
    if isinstance(node, ast.AST):
        return {"node": type(node).__name__, **{
            key: _syntax(value) for key, value in ast.iter_fields(node)
            if value is not None and value != []
        }}
    if isinstance(node, list):
        return [_syntax(value) for value in node]
    return node


def implementation_contract():
    """Derive field alternatives from our importers, not from upstream metadata.

    Supports the project's row.get('Field'), row.get('CsvId') or row.get('Id'),
    and _COLS patterns. The committed output must still be reviewed when a
    fetcher changes, especially for new parsing patterns or response shapes.
    """
    from origins import get_table_specs

    shared_fetcher = _syntax(ast.parse(inspect.getsource(odata_client)))
    entities = {}
    for spec in get_table_specs():
        module = spec.module
        source = inspect.getsource(module)
        tree = ast.parse(source)
        # Inspect the persisted payload, not CSV normalization helpers or unused
        # dictionary entries (some legacy payloads contain fields SQL ignores).
        ingestion = ast.parse(inspect.getsource(module._insert_to_db))
        persisted = set(re.findall(r"%\((\w+)\)s", source)) | set(getattr(module, "_COLS", []))
        for node in ast.walk(ingestion):
            if isinstance(node, ast.Dict):
                pairs = [(key, value) for key, value in zip(node.keys, node.values)
                         if not isinstance(key, ast.Constant) or key.value in persisted]
                node.keys = [key for key, _ in pairs]
                node.values = [value for _, value in pairs]
        groups = []

        def row_fields(node):
            return [
                child.args[0].value for child in ast.walk(node)
                if isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute) and child.func.attr == "get"
                and isinstance(child.func.value, ast.Name) and child.func.value.id == "row"
                and child.args and isinstance(child.args[0], ast.Constant)
                and isinstance(child.args[0].value, str)
            ]

        for node in ast.walk(ingestion):
            if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
                fields = row_fields(node)
                if fields:
                    groups.append(sorted(set(fields)))
        for field in row_fields(ingestion) + list(getattr(module, "_COLS", [])):
            if not any(field in group for group in groups):
                groups.append([field])
        endpoint = getattr(module, "ODATA_NAME", None)
        if endpoint is None:
            endpoint = module.ODATA_URL.rsplit("/", 1)[-1]
        entities[spec.label] = {
            "endpoint": endpoint,
            "response_shape": "array" if spec.label == "document_query" else "odata",
            "cursor_mode": spec.cursor_mode,
            "fields": sorted({tuple(group) for group in groups}),
            # The legacy vote CSV supplies these totals; V4 deliberately does
            # not. They may be persisted without being required from OData.
            "optional_fields": (["IsAccepted", "TotalAbstain", "TotalAgainst", "TotalFor"]
                                if spec.label == "plenum_vote" else []),
            # AST ignores comments/formatting but catches changes to fetch/mapping logic.
            "implementation_sha256": _fingerprint([_syntax(tree), shared_fetcher, BASE_URL]),
        }
    # Round-trip tuples to lists so comparisons match the committed JSON.
    return json.loads(json.dumps(entities))


def _metadata_schema(content):
    root = ET.fromstring(content)
    types = {}
    for entity in root.findall(".//e:EntityType", EDM):
        types[entity.attrib["Name"]] = {
            prop.attrib["Name"]: {
                "type": prop.attrib["Type"],
                "nullable": prop.attrib.get("Nullable", "true") != "false",
            }
            for prop in entity.findall("e:Property", EDM)
        }
    schema = {
        entity.attrib["Name"]: types[entity.attrib["EntityType"].rsplit(".", 1)[-1]]
        for entity in root.findall(".//e:EntitySet", EDM)
    }
    if not schema:
        raise ValueError("Metadata contains no OData entity sets")
    return schema


def _request(endpoint, params=None):
    return odata_client._request_with_retry(
        BASE_URL + endpoint, params or {}, timeout=15, max_retries=0,
    )


def refresh_contract():
    """Explicit developer action: capture implementation and observed schema."""
    schema = _metadata_schema(_request("$metadata").content)
    contract = {
        "format": 1,
        "source": BASE_URL,
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entities": implementation_contract(),
        "observed_schema": schema,
    }
    CONTRACT_PATH.write_text(json.dumps(contract, indent=2, ensure_ascii=False) + "\n")
    return contract


def _safe_error(exc):
    text = str(exc)
    if ISRAEL_PROXY_URL:
        text = text.replace(ISRAEL_PROXY_URL, "<configured proxy>")
        # requests can omit credentials/scheme when formatting the proxy address.
        from urllib.parse import urlsplit
        proxy = urlsplit(ISRAEL_PROXY_URL)
        if proxy.password:
            text = text.replace(proxy.password, "<redacted>")
    return " ".join(f"{type(exc).__name__}: {text}".split())


def _check_entity(label, entity, schema, baseline):
    endpoint = entity["endpoint"]
    try:
        response = _request(endpoint, {"$top": 1})
        data = odata_client._get_json(response)
        if entity["response_shape"] == "array":
            if not isinstance(data, list):
                raise ValueError("Expected a raw JSON array")
            rows = data
        else:
            if not isinstance(data, dict) or not isinstance(data.get("value"), list):
                raise ValueError("Expected an OData object with a value list")
            rows = data["value"]
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("Expected JSON objects in the result list")

        properties = schema.get(endpoint)
        missing = []
        changed = []
        for alternatives in entity["fields"]:
            # An empty sample cannot prove a field exists. Metadata is the fallback.
            available = set(rows[0]) if rows else set(properties or {})
            optional = entity.get("optional_fields", [])
            if ((rows or properties is not None)
                    and not any(field in available or field in optional for field in alternatives)):
                missing.append("/".join(alternatives))
            for field in alternatives:
                old = baseline.get(endpoint, {}).get(field)
                new = (properties or {}).get(field)
                if old and new and old["type"] != new["type"]:
                    changed.append(f"{field}: {old['type']} -> {new['type']}")
        reasons = []
        if missing:
            reasons.append("Missing consumed fields: " + ", ".join(missing))
        if changed:
            reasons.append("Changed field types: " + ", ".join(changed))
        if reasons:
            status, reason = "FAIL", "; ".join(reasons)
        elif not rows:
            status = "WARN"
            reason = ("Empty sample; fields checked against metadata only" if properties is not None
                      else "Empty sample and no metadata; consumed fields could not be verified")
        else:
            status, reason = "OK", "Endpoint, response shape and consumed fields match"
        if properties is None:
            if status == "OK":
                status = "WARN"
            reason += "; working endpoint absent from metadata"
        return {"table": label, "status": status, "reason": reason}
    except requests.exceptions.RequestException as exc:
        response = getattr(exc, "response", None)
        # An unavailable network/proxy is retried next daily cycle. A missing
        # endpoint or rejected request is a compatibility failure, cached monthly.
        status = "FAIL" if response is not None and response.status_code in (400, 404) else "ERROR"
        return {"table": label, "status": status, "reason": _safe_error(exc)}
    except (ValueError, TypeError) as exc:
        return {"table": label, "status": "FAIL", "reason": _safe_error(exc)}


def check_compatibility(contract, implementation, now):
    rows = []
    if implementation != contract["entities"] or contract.get("source", BASE_URL) != BASE_URL:
        rows.append({"table": "Implementation contract", "status": "FAIL",
                     "reason": "Importers changed; review and refresh docs/knesset-api-contract.json"})
    try:
        schema = _metadata_schema(_request("$metadata").content)
    except Exception as exc:
        rows.append({"table": "Upstream metadata", "status": "ERROR", "reason": _safe_error(exc)})
        return {"checked_at": now.isoformat(timespec="seconds"), "rows": rows}

    baseline = contract["observed_schema"]
    added = sorted(set(schema) - set(baseline))
    removed = sorted(set(baseline) - set(schema))
    changes = []
    if added:
        changes.append("Added entity sets: " + ", ".join(added))
    if removed:
        changes.append("Removed entity sets: " + ", ".join(removed))
    for endpoint in sorted(set(schema) & set(baseline)):
        if schema[endpoint] != baseline[endpoint]:
            changes.append(f"Fields/types/nullability changed: {endpoint}")
    rows.append({"table": "Upstream schema", "status": "INFO" if changes else "OK",
                 "reason": "; ".join(changes) if changes else "No scalar schema changes since recorded baseline"})
    with ThreadPoolExecutor(max_workers=4) as executor:
        rows.extend(executor.map(
            lambda item: _check_entity(item[0], item[1], schema, baseline),
            contract["entities"].items(),
        ))
    return {"checked_at": now.isoformat(timespec="seconds"),
            "schema_sha256": _fingerprint(schema), "rows": rows}


def latest_report(conn, *, force=False, now=None, cached_only=False):
    """Check once per UTC calendar month or after a contract/code change.

    Persist results across deployments. Transient check errors retry on the
    next updater cycle; genuine incompatibilities stay visible without daily
    upstream requests. Dry runs only display an already persisted report.
    """
    now = now or datetime.now(timezone.utc)
    if not cached_only:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS api_compatibility_state "
                        "(id INTEGER PRIMARY KEY CHECK (id = 1), report JSONB NOT NULL)")
        conn.commit()
    with conn.cursor() as cur:
        if cached_only:
            cur.execute("SELECT to_regclass('api_compatibility_state')")
            if cur.fetchone()[0] is None:
                return None
        cur.execute("SELECT report FROM api_compatibility_state WHERE id = 1")
        row = cur.fetchone()
    previous = row[0] if row else None
    if cached_only:
        return previous
    contract = json.loads(CONTRACT_PATH.read_text())
    implementation = implementation_contract()
    contract_hash = _fingerprint(contract)
    implementation_hash = _fingerprint(implementation)
    if (previous and not force
            and previous["checked_at"][:7] == now.strftime("%Y-%m")
            and previous.get("contract_sha256") == contract_hash
            and previous.get("implementation_sha256") == implementation_hash
            and not any(row["status"] == "ERROR" for row in previous["rows"])):
        return previous
    print("\nRunning monthly Knesset API compatibility check (read-only requests)")
    report = check_compatibility(contract, implementation, now)
    report.update(contract_sha256=contract_hash, implementation_sha256=implementation_hash)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO api_compatibility_state (id, report) VALUES (1, %s::jsonb) "
                    "ON CONFLICT (id) DO UPDATE SET report = EXCLUDED.report", (json.dumps(report),))
    conn.commit()
    return report


def print_report(report):
    print("\nKnesset API compatibility — latest check")
    if report is None:
        print("Not checked yet (first normal update cycle will run the check).")
        return
    failed = sum(row["status"] in ("FAIL", "ERROR") for row in report["rows"])
    print(f"Checked at: {report['checked_at']} | {failed} failed/error check(s)")
    print(f"{'Table / Check':<35} {'Status':<6} Reason")
    print("-" * 110)
    for row in report["rows"]:
        print(f"{row['table']:<35} {row['status']:<6} {' '.join(row['reason'].split())}")


def print_latest_report(conn, *, cached_only=False):
    """A monitoring failure must not suppress the data update summary."""
    try:
        print_report(latest_report(conn, cached_only=cached_only))
    except Exception as exc:
        conn.rollback()
        print("\nKnesset API compatibility — ERROR: " + _safe_error(exc))
