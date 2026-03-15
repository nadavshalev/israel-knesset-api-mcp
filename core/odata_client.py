from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import csv
import io
import time

import requests

from config import BASE_URL, DEFAULT_PAGE_SIZE

# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------
MAX_RETRIES = 4          # total attempts = 1 + MAX_RETRIES
RETRY_BACKOFF_BASE = 2   # seconds; doubles each retry (2, 4, 8, 16)


def _request_with_retry(
    url: str,
    params: Dict[str, Any],
    timeout: int = 60,
    *,
    max_retries: int = MAX_RETRIES,
) -> requests.Response:
    """GET with retry + exponential backoff.

    Retries on:
    - Connection / timeout errors
    - HTTP 429 / 5xx responses
    - Empty response body (common Knesset API transient failure)
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1 + max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            # Retry on server errors and rate limits
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.exceptions.HTTPError(
                    f"HTTP {resp.status_code}", response=resp,
                )
            resp.raise_for_status()
            # Detect empty body (Knesset API sometimes returns 200 with no body)
            if not resp.text.strip():
                raise ValueError("Empty response body from API")
            return resp
        except (requests.exceptions.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                print(f"  Retry {attempt + 1}/{max_retries} in {wait}s "
                      f"({type(exc).__name__}: {exc})")
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


def _get_json(resp: requests.Response) -> Dict[str, Any]:
    """Parse JSON from a response, with a clear error on failure."""
    try:
        return resp.json()
    except Exception:
        snippet = resp.text[:300] if resp.text else "(empty)"
        raise ValueError(
            f"Invalid JSON from {resp.url} "
            f"(status={resp.status_code}, "
            f"content-type={resp.headers.get('Content-Type', '?')}): "
            f"{snippet}"
        )

def _utc_now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z")

def odata_escape(value: str) -> str:
    return value.replace("'", "''")


def odata_datetime_literal(value: str) -> str:
    # Normalize to ISO and ensure UTC Z suffix for filters
    dt = _parse_dt(value)
    if dt:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        timespec = "milliseconds" if dt.microsecond else "seconds"
        value = dt.isoformat(timespec=timespec).replace("+00:00", "Z")
    else:
        if not any(sep in value for sep in ["+", "-", "Z"]):
            value = value + "Z"
    return value


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _parse_numeric(value) -> Optional[int]:
    """Try to parse a value as an integer."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _format_odata_literal(value: str, numeric: bool) -> str:
    """Format a cursor value for use in an OData $filter expression."""
    if numeric:
        return str(int(value))
    return odata_datetime_literal(value)


def fetch_odata_table(
    table: str,
    select: Optional[str] = None,
    expand: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    since: Optional[str] = None,
    orderby: Optional[str] = None,
    since_field: str = "LastUpdatedDate",
    numeric: Optional[bool] = None,
) -> Iterable[Dict[str, Any]]:
    """Generator to fetch all pages from an OData table.

    Always continues until exhaustion: advances a ``since`` cursor based on the
    latest row seen (assumes ascending order by ``since_field``) and never
    relies on @odata.nextLink.

    The ``since_field`` may be a datetime column (default ``LastUpdatedDate``)
    or a numeric column (e.g. ``Id``).  When ``numeric`` is explicitly set,
    that mode is used.  Otherwise detection is automatic based on whether
    ``since`` parses as an integer — but this only works when ``since`` is
    not None.
    """

    # Detect whether since_field is numeric or datetime
    if numeric is not None:
        numeric_mode = numeric
    else:
        numeric_mode = _parse_numeric(since) is not None if since else False

    base_params: Dict[str, Any] = {"$top": page_size}
    if select:
        base_params["$select"] = select
    if expand:
        base_params["$expand"] = expand
    effective_orderby = orderby or f"{since_field} asc"
    if effective_orderby:
        base_params["$orderby"] = effective_orderby
    url = f"{BASE_URL}{table}"
    params: Dict[str, Any] = dict(base_params)
    since_cursor = since
    since_num: Optional[int] = _parse_numeric(since_cursor) if numeric_mode else None
    since_dt: Optional[datetime] = _parse_dt(since_cursor) if (not numeric_mode and since_cursor) else None

    while True:
        if since_cursor:
            params["$filter"] = f"{since_field} gt {_format_odata_literal(since_cursor, numeric_mode)}"
        elif "$filter" in params:
            params.pop("$filter")
        print(f"Requesting {url} with params {params}")
        resp = _request_with_retry(url, params, timeout=60)
        data = _get_json(resp)
        batch = data.get("value", [])
        last_seen: Optional[str] = None
        print(f"    Fetched {len(batch)} rows")
        for row in batch:
            row_ts = row.get(since_field) or row.get("LastUpdateDate")

            if numeric_mode:
                row_num = _parse_numeric(row_ts)
                if since_num is not None and row_num is not None:
                    if row_num <= since_num:
                        continue
                yield row
                if row_num is not None:
                    last_seen_num = _parse_numeric(last_seen)
                    if last_seen_num is None or row_num > last_seen_num:
                        last_seen = str(row_num)
            else:
                row_dt = _parse_dt(row_ts) if row_ts else None
                if since_dt and row_dt:
                    # Normalize to naive UTC for comparison
                    if row_dt.tzinfo:
                        row_dt = row_dt.astimezone(timezone.utc).replace(tzinfo=None)
                    if since_dt.tzinfo:
                        since_dt_cmp = since_dt.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        since_dt_cmp = since_dt
                    if row_dt <= since_dt_cmp:
                        continue
                yield row
                if row_dt:
                    # Normalize row_dt to naive UTC for last_seen tracking
                    row_dt_naive = row_dt
                    if row_dt_naive.tzinfo:
                        row_dt_naive = row_dt_naive.astimezone(timezone.utc).replace(tzinfo=None)
                    last_seen_dt = _parse_dt(last_seen) if last_seen else None
                    if last_seen_dt and last_seen_dt.tzinfo:
                        last_seen_dt = last_seen_dt.astimezone(timezone.utc).replace(tzinfo=None)
                    if last_seen_dt is None or row_dt_naive > last_seen_dt:
                        last_seen = row_ts

        if not batch:
            break

        if last_seen and last_seen != since_cursor:
            since_cursor = last_seen
            if numeric_mode:
                since_num = _parse_numeric(since_cursor)
            else:
                since_dt = _parse_dt(since_cursor) if since_cursor else None
            continue

        # No progress yet full page; avoid infinite loop
        break


def fetch_odata_max_id(table: str, id_field: str = "Id") -> Optional[int]:
    """Fetch the maximum value of a numeric field from an OData table."""
    url = f"{BASE_URL}{table}"
    params = {"$orderby": f"{id_field} desc", "$top": 1, "$select": id_field}
    resp = _request_with_retry(url, params, timeout=60)
    data = _get_json(resp)
    rows = data.get("value", [])
    if rows:
        return _parse_numeric(rows[0].get(id_field))
    return None


def fetch_odata_range(
    table: str,
    id_field: str,
    range_start: int,
    range_end: int,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    """Fetch all rows where ``range_start < id_field <= range_end``.

    Returns a list (not a generator) so it can be used from thread pools.
    Pages internally using ``id_field gt X`` cursor within the range.
    """
    url = f"{BASE_URL}{table}"
    results: List[Dict[str, Any]] = []
    cursor = range_start

    while cursor < range_end:
        params: Dict[str, Any] = {
            "$top": page_size,
            "$orderby": f"{id_field} asc",
            "$filter": f"{id_field} gt {cursor} and {id_field} le {range_end}",
        }
        resp = _request_with_retry(url, params, timeout=120)
        data = _get_json(resp)
        batch = data.get("value", [])
        if not batch:
            break
        results.extend(batch)
        # Advance cursor to the last Id seen
        last_id = _parse_numeric(batch[-1].get(id_field))
        if last_id is None or last_id <= cursor:
            break  # safety: no progress
        cursor = last_id

    return results


def fetch_csv_table(url: str) -> Iterable[Dict[str, Any]]:
    resp = _request_with_retry(url, params={}, timeout=60)
    text = resp.text
    if not text:
        return
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield row


def fetch_table_with_csv_first(
    csv_url: str,
    odata_table: str,
    since: Optional[str] = None,
    *,
    select: Optional[str] = None,
    expand: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    orderby: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """Yield CSV rows first, then OData rows since the CSV's latest update.

    When ``since`` is provided, CSV is skipped and only OData rows are fetched
    from that point onward. When ``since`` is ``None`` we emit all CSV rows,
    track their maximum ``LastUpdatedDate``, and then fetch only newer rows
    from the OData endpoint.
    """

    max_csv_updated: Optional[str] = None

    if since is None:
        csv_rows = list(fetch_csv_table(csv_url))
        for row in csv_rows:
            last = row.get("LastUpdatedDate")
            last_dt = _parse_dt(last) if last else None
            if last_dt:
                if max_csv_updated is None:
                    max_csv_updated = last_dt.isoformat()
                else:
                    prev_dt = _parse_dt(max_csv_updated)
                    if prev_dt is None or last_dt > prev_dt:
                        max_csv_updated = last_dt.isoformat()
            yield row
        since = max_csv_updated
        if max_csv_updated is not None:
            print(f"CSV: fetched {len(csv_rows)} rows, max LastUpdatedDate: {max_csv_updated}")

    yield from fetch_odata_table(
        table=odata_table,
        select=select,
        expand=expand,
        page_size=page_size,
        since=since,
        orderby=orderby,
    )
