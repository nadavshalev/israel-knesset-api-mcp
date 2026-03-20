"""Shared helper functions used across view modules.

Keeps date/time formatting and common patterns in one place instead of
duplicating them in every view file.
"""

from __future__ import annotations

import inspect
import logging
import re
import types
from typing import Annotated, get_args, get_origin, Union

logger = logging.getLogger(__name__)


def simple_date(date_str) -> str | None:
    """Strip time component from an ISO datetime string.

    Handles both ``T``-separated (``2019-01-30T10:00:00``) and
    space-separated (``2019-01-30 10:00:00``) formats.  Returns
    ``None`` for falsy input.
    """
    if not date_str:
        return None
    s = str(date_str)
    if "T" in s:
        return s.split("T")[0]
    if " " in s:
        return s.split(" ")[0]
    return s


def simple_time(datetime_str) -> str | None:
    """Extract ``HH:MM`` time from an ISO datetime string.

    Handles both ``T``-separated and space-separated formats,
    and strips timezone offsets (``+03:00``).  Returns ``None``
    for falsy input or if no time part is found.
    """
    if not datetime_str:
        return None
    s = str(datetime_str)
    if "T" in s:
        time_part = s.split("T")[1]
        if "+" in time_part:
            time_part = time_part.split("+")[0]
        return time_part[:5]
    if " " in s:
        time_part = s.split(" ")[1]
        return time_part[:5]
    return None


def format_person_name(first_name, last_name) -> str | None:
    """Format a full person name from first and last name fields.

    Returns ``None`` when both names are missing.
    """
    first = first_name or ""
    last = last_name or ""
    result = f"{first} {last}".strip()
    return result or None


# ---------------------------------------------------------------------------
# Type introspection helpers
# ---------------------------------------------------------------------------

def _base_annotation(annotation):
    """Extract the core type from a type hint, unwrapping Annotated/Optional/Union.

    Examples::

        int                            -> int
        int | None                     -> int
        Annotated[int, Field(...)]     -> int
        Annotated[int | None, ...]     -> int
        inspect.Parameter.empty        -> None
        int | str                      -> None  (ambiguous)
    """
    if annotation is inspect.Parameter.empty:
        return None
    # Unwrap Annotated[X, ...] -> X, then continue resolving
    origin = get_origin(annotation)
    if origin is Annotated:
        annotation = get_args(annotation)[0]
        origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        return args[0] if len(args) == 1 else None
    return annotation


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------

# Strings that agents commonly send when they mean "no value".
_NONE_STRINGS = frozenset({"none", "null", "undefined", ""})

# Maximum length for string values (prevents abuse from overly long inputs).
MAX_STRING_LENGTH = 500

# Date-like strings must match YYYY-MM-DD (optionally followed by time).
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _coerce_str(key: str, value) -> str:
    """Coerce *value* to ``str``, rejecting collection types."""
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise ValueError(
                f"{key} is too long ({len(value)} chars, max {MAX_STRING_LENGTH})"
            )
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    raise ValueError(f"{key} must be a string")


def _coerce_int(key: str, value) -> int:
    """Coerce *value* to ``int``, rejecting booleans and non-numeric strings."""
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != int(value):
            raise ValueError(f"{key} must be an integer (got float {value!r})")
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be an integer") from exc
    raise ValueError(f"{key} must be an integer")


def _coerce_bool(key: str, value) -> bool:
    """Coerce *value* to ``bool``, accepting common truthy/falsy strings."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        raise ValueError(f"{key} must be a boolean")
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        raise ValueError(f"{key} must be a boolean")
    raise ValueError(f"{key} must be a boolean")


def _coerce_float(key: str, value) -> float:
    """Coerce *value* to ``float``."""
    if isinstance(value, bool):
        raise ValueError(f"{key} must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{key} must be a number") from exc
    raise ValueError(f"{key} must be a number")


_COERCERS = {
    int: _coerce_int,
    bool: _coerce_bool,
    float: _coerce_float,
    str: _coerce_str,
}


def _validate_date_str(key: str, value: str) -> None:
    """Validate that *value* looks like a YYYY-MM-DD date string."""
    if not _DATE_RE.match(value):
        raise ValueError(
            f"{key} must be a date in YYYY-MM-DD format (got {value!r})"
        )


# Parameter names that are expected to contain date strings.
_DATE_PARAM_NAMES = frozenset({"date", "date_to", "from_date", "to_date"})


# ---------------------------------------------------------------------------
# Main normalizer
# ---------------------------------------------------------------------------

def normalize_inputs(params: dict, annotations: dict | None = None) -> dict:
    """Normalize and validate view inputs, coercing types based on hints.

    Handles the common misuse patterns from LLM-based agents:
      - ``''`` or ``'  '`` instead of ``None`` -> converted to ``None``
      - ``'none'``, ``'null'``, ``'undefined'`` -> converted to ``None``
      - ``'25'`` for an ``int`` field -> coerced to ``25``
      - ``'true'`` / ``'false'`` for ``bool`` -> coerced to ``True`` / ``False``
      - ``123`` for a ``str`` field -> coerced to ``'123'``
      - Non-scalar types (lists, dicts) for scalar fields -> rejected
      - Strings longer than 500 chars -> rejected
      - Date params (``date``, ``date_to``) -> validated as YYYY-MM-DD

    Supports ``Annotated[type, Field(description=...)]`` hints — the
    ``Annotated`` wrapper is unwrapped automatically.

    Args:
        params: The raw parameter dict (typically ``locals()`` from the caller).
        annotations: Mapping of param names to their expected types.  If not
            provided, the function inspects the *caller's* type hints
            automatically (works for top-level functions; pass explicitly when
            calling from methods, lambdas, or decorated functions).

    Returns:
        A new dict with cleaned / coerced values.

    Raises:
        ValueError: If a value cannot be coerced to its annotated type.
    """
    if annotations is None:
        annotations = _caller_param_annotations()

    normalized = {}

    for key, value in params.items():
        original = value

        # --- Step 1: Treat agent "null-like" strings as None ---
        if isinstance(value, str):
            value = value.strip()
            if value.lower() in _NONE_STRINGS:
                value = None

        # --- Step 2: Coerce to the annotated type ---
        ann = _base_annotation(annotations.get(key))
        coercer = _COERCERS.get(ann)  # type: ignore[arg-type]
        if coercer is not None and value is not None:
            value = coercer(key, value)

        # --- Step 3: Validate date params ---
        if key in _DATE_PARAM_NAMES and isinstance(value, str):
            _validate_date_str(key, value)

        # --- Step 4: Log coercion ---
        if value is not original and value is not None:
            logger.debug("normalize_inputs: %s: %r -> %r", key, original, value)
        elif value is None and original is not None:
            logger.debug("normalize_inputs: %s: %r -> None", key, original)

        normalized[key] = value

    return normalized


def _caller_param_annotations() -> dict[str, object]:
    """Inspect the caller's caller to extract parameter type annotations.

    This is a convenience so view functions can simply call
    ``normalize_inputs(locals())`` without explicitly passing annotations.

    Falls back to an empty dict if introspection fails (e.g. when called
    from methods, closures, or decorated functions).  In those cases, pass
    ``annotations`` explicitly to :func:`normalize_inputs`.
    """
    frame = inspect.currentframe()
    try:
        if frame is None or frame.f_back is None or frame.f_back.f_back is None:
            return {}
        caller = frame.f_back.f_back
        fn_name = caller.f_code.co_name
        fn = caller.f_globals.get(fn_name)
        if not callable(fn):
            return {}
        sig = inspect.signature(fn)
        return {name: p.annotation for name, p in sig.parameters.items()}
    finally:
        del frame


# ---------------------------------------------------------------------------
# Search count guard
# ---------------------------------------------------------------------------

def check_search_count(cursor, count_sql: str, params: list, entity_name: str = "results") -> int:
    """Run count_sql and raise ValueError if result exceeds MAX_SEARCH_RESULTS.

    Returns the count so the caller can use it if needed.
    Call this BEFORE the main SELECT to fail fast on broad queries.
    """
    from config import MAX_SEARCH_RESULTS
    cursor.execute(count_sql, params)
    count = cursor.fetchone()
    count = list(count.values())[0] if count else 0
    if count > MAX_SEARCH_RESULTS:
        raise ValueError(
            f"Too many {entity_name} ({count:,} matches). "
            "Add more filters (e.g. date, knesset_num, or a search query) to narrow results."
        )
    return count


# ---------------------------------------------------------------------------
# Output cleaning
# ---------------------------------------------------------------------------

def _is_empty(value) -> bool:
    """Return True for values that should be stripped from output.

    Stripped: ``None``, ``""`` (empty string), ``-1`` (sentinel int).
    Preserved: ``False``, ``0``, non-empty strings, non-empty collections.
    """
    if value is None:
        return True
    if value == "":
        return True
    if value == -1:
        return True
    return False


def clean(obj):
    """Recursively strip empty/sentinel values from a dict or list.

    Removes dict keys whose values are ``None``, ``""`` (empty string),
    or ``-1`` (common DB sentinel).

    Preserves ``False``, ``0``, ``[]`` (empty list), and ``{}``
    (empty dict).  Empty lists are valid data (e.g. "zero results")
    and must not be stripped — required Pydantic fields would lose
    their key, causing validation errors downstream.

    Recurses into nested dicts and lists so that nested models are
    cleaned too.
    """
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            if _is_empty(value):
                continue
            cleaned[key] = clean(value)
        return cleaned
    if isinstance(obj, list):
        return [clean(item) for item in obj]
    return obj
