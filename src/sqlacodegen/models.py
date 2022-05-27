from __future__ import annotations

import sys
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Iterable, MutableSequence, Sequence
from dataclasses import dataclass, field
from importlib import import_module
from inspect import isclass, isfunction
from keyword import iskeyword
from types import ModuleType
from typing import Any, ClassVar

from sqlalchemy.sql.schema import Table

if sys.version_info < (3, 8):
    from typing_extensions import get_origin
else:
    from typing import get_origin


@dataclass
class Named:
    name: str


class ContainsImports:
    def collect_imports(self) -> set[Import]:
        return set()


@dataclass
class CallableModel(ContainsImports):
    func: Callable[..., Any]
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not callable(self.func):
            raise TypeError(f"func ({self.func}) is not callable")

        for key in self.kwargs:
            if iskeyword(key):
                raise ValueError(
                    f"Invalid keyword argument: {key} (this is a reserved keyword)"
                )

    def collect_imports(self) -> set[Import]:
        imports: set[Import] = set()
        imports.add(Import.from_object(self.func))

        for arg in self.args:
            if isinstance(arg, ContainsImports):
                imports.update(arg.collect_imports())
            else:
                imports.add(Import.from_object(arg))

        for kwarg in self.kwargs.values():
            if isinstance(kwarg, ContainsImports):
                imports.update(kwarg.collect_imports())
            else:
                imports.add(Import.from_object(kwarg))

        return imports


class ListModel(ContainsImports):
    def __init__(self, values: Iterable[Any]):
        self.values = list(values)


class Reference(ContainsImports):
    def __init__(self, *targets: Named):
        self.targets = targets


class LambdaReference(Reference):
    pass


class ReprReference(Reference):
    pass


@dataclass
class Lambda(ContainsImports):
    value: object


@dataclass
class Attribute(Named, ContainsImports):
    value: Any
    annotation: type[Any] | None = None

    def collect_imports(self) -> set[Import]:
        imports: set[Import] = set()
        if self.annotation is not None:
            annotation_cls = get_origin(self.annotation) or self.annotation
            imports.add(Import.from_object(annotation_cls))

        if isinstance(self.value, ContainsImports):
            imports.update(self.value.collect_imports())

        return imports


class ColumnAttribute(Attribute):
    @property
    def column_name(self) -> str:
        if isinstance(self.value.args[0], str):
            return self.value.args[0][1:-1]
        else:
            return self.name


# class RelationshipType(Enum):
#     ONE_TO_ONE = auto()
#     ONE_TO_MANY = auto()
#     MANY_TO_ONE = auto()
#     MANY_TO_MANY = auto()


@dataclass
class RelationshipAttribute(Attribute):
    pass


@dataclass
class TableArgsModel(ContainsImports):
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)

    def collect_imports(self) -> set[Import]:
        imports: set[Import] = set()

        for item in self.args:
            if isinstance(item, ContainsImports):
                imports.update(item.collect_imports())

        for value in self.kwargs.values():
            if isinstance(value, ContainsImports):
                imports.update(value.collect_imports())

        return imports

    def __bool__(self) -> bool:
        return bool(self.args) or bool(self.kwargs)


class TableModel(Named, CallableModel):
    def __init__(
        self, name: str, args: list[Any], kwargs: dict[str, Any], table: Table
    ):
        Named.__init__(self, name)
        CallableModel.__init__(self, Table, args, kwargs)
        self.table = table

    @property
    def table_name(self) -> str:
        return self.table.fullname


class ColumnTypeModel(CallableModel):
    pass


@dataclass
class JoinModel(ContainsImports):
    source_class: ClassModel
    target_table: TableModel
    joins: Sequence[tuple[ColumnAttribute, str]]

    def collect_imports(self) -> set[Import]:
        return set()


@dataclass
class ClassModel(Named, ContainsImports):
    bases: MutableSequence[Attribute]
    decorators: MutableSequence[CallableModel]
    attributes: MutableSequence[Attribute]
    docstring: str | None = None
    table: Table | None = None

    def collect_imports(self) -> set[Import]:
        imports: set[Import] = set()
        for base in self.bases:
            imports.update(base.collect_imports())

        for decorator in self.decorators:
            imports.update(decorator.collect_imports())

        for attr in self.attributes:
            imports.update(attr.collect_imports())

        return imports

    @property
    def table_name(self) -> str:
        if self.table is None:
            raise RuntimeError("no table associated with this class")

        return self.table.fullname


@dataclass(unsafe_hash=True)
class Import:
    name: str
    from_: str | None = None
    alias: str | None = field(init=False, default=None, compare=False)

    _all_values: ClassVar[dict[tuple[str, str | None], Import]] = {}

    @classmethod
    def reset_cache(cls) -> None:
        cls._all_values.clear()

    @classmethod
    def from_object(cls, obj: object) -> Import:
        if isinstance(obj, ModuleType):
            return cls(name=object.__name__)
        elif isclass(obj) or isfunction(obj):
            target = obj
        else:
            target = type(obj)

        module = getattr(target, "__module__", None)

        # Try to simplify the import by checking if the same object can be found in
        # lower levels of the package tree
        if module:
            parts = module.split(".")[:-1]
            while parts:
                name = ".".join(parts)
                mod = import_module(name)
                if getattr(mod, target.__name__, None) is target:
                    module = ".".join(parts)

                del parts[-1]

        try:
            return cls._all_values[target.__name__, module]
        except KeyError:
            instance = cls(target.__name__, module)
            cls._all_values[target.__name__, module] = instance
            return instance

    @property
    def visible_name(self) -> str:
        return self.alias or self.name

    @property
    def module(self) -> str:
        return self.from_ or self.name
