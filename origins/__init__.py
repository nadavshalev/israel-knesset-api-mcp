"""Origins package — co-locates table modules, views, and models by entity.

Sub-packages: bills, members, committees, votes, plenums, knesset, search.

Auto-discovery:
  - **Tables**: walks all sub-packages for modules with a ``TABLE_NAME``
    attribute and builds a ``TableSpec`` registry (same API as the old
    ``tables`` package).
  - **Views**: walks all sub-packages for modules ending in ``_view`` and
    imports them, triggering ``@mcp_tool`` registration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from importlib import import_module
from types import ModuleType

# ---------------------------------------------------------------------------
# Sub-package list (explicit so we control order / avoid surprises)
# ---------------------------------------------------------------------------

_SUB_PACKAGES = [
    "bills",
    "members",
    "committees",
    "votes",
    "plenums",
    "agendas",
    "queries",
    "laws",
    "secondary_laws",
    "knesset",
    "search",
]


# ---------------------------------------------------------------------------
# TableSpec registry (replaces tables/__init__.py)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TableSpec:
    label: str
    table_name: str
    module: ModuleType
    cursor_mode: str = "timestamp"
    update_order: int = 1000


def _discover_table_specs() -> list[TableSpec]:
    specs: list[TableSpec] = []
    for sub in _SUB_PACKAGES:
        sub_pkg = f"origins.{sub}"
        tables_dir = os.path.join(os.path.dirname(__file__), sub, "tables")
        if not os.path.isdir(tables_dir):
            continue

        for fname in sorted(os.listdir(tables_dir)):
            if not fname.endswith(".py") or fname.startswith("_"):
                continue
            mod_name = fname[:-3]

            full_name = f"{sub_pkg}.tables.{mod_name}"
            module = import_module(full_name)

            table_name = getattr(module, "TABLE_NAME", None)
            if not table_name:
                continue

            label = getattr(module, "TABLE_LABEL", mod_name)
            cursor_mode = getattr(module, "CURSOR_MODE", "timestamp")
            update_order = int(getattr(module, "UPDATE_ORDER", 1000))
            specs.append(
                TableSpec(
                    label=label,
                    table_name=table_name,
                    module=module,
                    cursor_mode=cursor_mode,
                    update_order=update_order,
                )
            )

    return sorted(specs, key=lambda s: (s.update_order, s.label))


TABLE_SPECS: tuple[TableSpec, ...] = tuple(_discover_table_specs())
_TABLE_SPECS_BY_LABEL: dict[str, TableSpec] = {}
for _spec in TABLE_SPECS:
    if _spec.label in _TABLE_SPECS_BY_LABEL:
        raise RuntimeError(f"Duplicate table label '{_spec.label}'")
    _TABLE_SPECS_BY_LABEL[_spec.label] = _spec


def get_table_specs() -> tuple[TableSpec, ...]:
    return TABLE_SPECS


def get_table_spec(label: str) -> TableSpec:
    try:
        return _TABLE_SPECS_BY_LABEL[label]
    except KeyError as exc:
        valid = ", ".join(sorted(_TABLE_SPECS_BY_LABEL))
        raise KeyError(f"Unknown table '{label}'. Valid names: {valid}") from exc


# ---------------------------------------------------------------------------
# View auto-discovery (replaces views/__init__.py)
# ---------------------------------------------------------------------------

def _discover_views() -> list[str]:
    """Import all *_view modules in sub-packages, triggering @mcp_tool."""
    imported: list[str] = []
    for sub in _SUB_PACKAGES:
        sub_pkg = f"origins.{sub}"
        sub_dir = os.path.join(os.path.dirname(__file__), sub)
        if not os.path.isdir(sub_dir):
            continue

        for fname in sorted(os.listdir(sub_dir)):
            if not fname.endswith("_view.py") or fname.startswith("_"):
                continue
            mod_name = fname[:-3]
            full_name = f"{sub_pkg}.{mod_name}"
            import_module(full_name)
            imported.append(full_name)

    return imported


_DISCOVERED_VIEWS = _discover_views()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    "TableSpec",
    "get_table_specs",
    "get_table_spec",
]
