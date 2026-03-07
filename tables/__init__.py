from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from pkgutil import iter_modules
from types import ModuleType


@dataclass(frozen=True)
class TableSpec:
    label: str
    table_name: str
    module: ModuleType
    cursor_mode: str = "timestamp"
    update_order: int = 1000


def _discover_table_specs() -> list[TableSpec]:
    specs: list[TableSpec] = []
    for info in iter_modules(__path__):
        name = info.name
        if name.startswith("_"):
            continue

        module = import_module(f"{__name__}.{name}")
        globals()[name] = module

        table_name = getattr(module, "TABLE_NAME", None)
        if not table_name:
            continue

        label = getattr(module, "TABLE_LABEL", name)
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


__all__ = [spec.module.__name__.split(".")[-1] for spec in TABLE_SPECS] + [
    "TableSpec",
    "get_table_specs",
    "get_table_spec",
]
