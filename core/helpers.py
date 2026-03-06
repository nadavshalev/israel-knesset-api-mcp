"""Shared helper functions used across view modules.

Keeps date/time formatting and common patterns in one place instead of
duplicating them in every view file.
"""

from __future__ import annotations


def simple_date(date_str) -> str:
    """Strip time component from an ISO datetime string.

    Handles both ``T``-separated (``2019-01-30T10:00:00``) and
    space-separated (``2019-01-30 10:00:00``) formats.  Returns
    an empty string for falsy input.
    """
    if not date_str:
        return ""
    s = str(date_str)
    if "T" in s:
        return s.split("T")[0]
    if " " in s:
        return s.split(" ")[0]
    return s


def simple_time(datetime_str) -> str:
    """Extract ``HH:MM`` time from an ISO datetime string.

    Handles both ``T``-separated and space-separated formats,
    and strips timezone offsets (``+03:00``).  Returns an empty
    string for falsy input or if no time part is found.
    """
    if not datetime_str:
        return ""
    s = str(datetime_str)
    if "T" in s:
        time_part = s.split("T")[1]
        if "+" in time_part:
            time_part = time_part.split("+")[0]
        return time_part[:5]
    if " " in s:
        time_part = s.split(" ")[1]
        return time_part[:5]
    return ""


def format_person_name(first_name, last_name) -> str:
    """Format a full person name from first and last name fields.

    Handles None values gracefully.
    """
    first = first_name or ""
    last = last_name or ""
    return f"{first} {last}".strip()
