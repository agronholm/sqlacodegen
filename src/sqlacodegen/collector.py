import sys
from importlib import import_module
from typing import Any, List

import sqlalchemy.exc
from sqlalchemy.util import OrderedDict


class ImportCollector(OrderedDict):
    def __init__(self) -> None:
        super().__init__()
        self.builtin_module_names = set(sys.builtin_module_names) | {'dataclasses'}

    def add_import(self, obj: Any) -> None:
        # Don't store builtin imports
        if obj.__module__ == 'builtins':
            return

        type_ = type(obj) if not isinstance(obj, type) else obj
        pkgname = type_.__module__

        # The column types have already been adapted towards generic types if possible, so if this
        # is still a vendor specific type (e.g., MySQL INTEGER) be sure to use that rather than the
        # generic sqlalchemy type as it might have different constructor parameters.
        if pkgname.startswith('sqlalchemy.dialects.'):
            dialect_pkgname = '.'.join(pkgname.split('.')[0:3])
            dialect_pkg = import_module(dialect_pkgname)

            if type_.__name__ in dialect_pkg.__all__:  # type: ignore[attr-defined]
                pkgname = dialect_pkgname
        elif type_.__name__ in sqlalchemy.__all__:  # type: ignore[attr-defined]
            pkgname = 'sqlalchemy'
        else:
            pkgname = type_.__module__

        self.add_literal_import(pkgname, type_.__name__)

    def add_literal_import(self, pkgname: str, name: str) -> None:
        names = self.setdefault(pkgname, set())
        names.add(name)

    def group_imports(self) -> List[List[str]]:
        future_imports: List[str] = []
        stdlib_imports: List[str] = []
        thirdparty_imports: List[str] = []

        for package in sorted(self):
            imports = ', '.join(sorted(self[package]))
            collection = thirdparty_imports
            if package == '__future__':
                collection = future_imports
            elif package in self.builtin_module_names:
                collection = stdlib_imports
            elif package in sys.modules and 'site-packages' not in sys.modules[package].__file__:
                collection = stdlib_imports

            collection.append(f'from {package} import {imports}')

        return [group for group in (future_imports, stdlib_imports, thirdparty_imports) if group]

    def contains_variable(self, name: str) -> bool:
        return any(name in names for names in self.values())
