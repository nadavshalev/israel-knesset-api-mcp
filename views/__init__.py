from importlib import import_module
from pkgutil import iter_modules

__all__: list[str] = []

for info in iter_modules(__path__):
    name = info.name
    if name.startswith("_") or not name.endswith("_view"):
        continue
    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    __all__.append(name)

__all__.sort()
