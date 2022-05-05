from __future__ import annotations

import sys
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from importlib import import_module
from inspect import isclass, isfunction
from keyword import iskeyword
from types import ModuleType
from typing import Any

from sqlalchemy.sql.schema import Table

if sys.version_info < (3, 8):
    from typing_extensions import get_origin
else:
    from typing import get_origin


@dataclass
class Named:
    name: str

    def __str__(self):
        return self.name


class ContainsImports(metaclass=ABCMeta):
    @abstractmethod
    def collect_imports(self) -> set[Import]:
        pass


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

    def __str__(self):
        args = self.args + [f"{key}={value}" for key, value in self.kwargs.items()]
        rendered_args = ", ".join(str(arg) for arg in args)
        return f"{self.func.__name__}({rendered_args})"


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


class TableModel(Named, CallableModel):
    def __init__(self, name: str, args: list[Any], kwargs: dict[str, Any]):
        Named.__init__(self, name)
        CallableModel.__init__(self, Table, args, kwargs)


class ColumnTypeModel(CallableModel):
    def __str__(self):
        rendered = super().__str__()

        # Remove the () from the end if the column type has no arguments
        if not self.args and not self.kwargs:
            rendered = rendered[:-2]

        return rendered


@dataclass
class ClassModel(Named, ContainsImports):
    bases: Sequence[str]
    attributes: Sequence[Attribute]
    docstring: str | None = None

    def collect_imports(self) -> set[Import]:
        imports: set[Import] = set()
        for attr in self.attributes:
            imports.update(attr.collect_imports())

        return imports


@dataclass(frozen=True)
class Import:
    name: str
    from_: str | None = None
    alias: str | None = field(init=False, default=None, compare=False)

    @classmethod
    def from_object(cls, obj) -> Import:
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

        return cls(target.__name__, module)

    @property
    def module(self) -> str:
        return self.from_ or self.name

    def __str__(self):
        return self.alias or self.name


# class RelationshipType(Enum):
#     ONE_TO_ONE = auto()
#     ONE_TO_MANY = auto()
#     MANY_TO_ONE = auto()
#     MANY_TO_MANY = auto()
#
#
# @dataclass
# class ColumnAttribute:
#     model: ModelClass
#     column: Column[Any]
#     name: str = field(init=False, default="")
#
#     def __repr__(self) -> str:
#         return f"{self.__class__.__name__}(name={self.name!r}, type={self.column.type})"
#
#     def __str__(self) -> str:
#         return self.name
#
#
# JoinType = Tuple[Model, Union[ColumnAttribute, str], Model, Union[ColumnAttribute, str]]
#
#
# @dataclass
# class RelationshipAttribute:
#     type: RelationshipType
#     source: ModelClass
#     target: ModelClass
#     constraint: ForeignKeyConstraint | None = None
#     association_table: Model | None = None
#     backref: RelationshipAttribute | None = None
#     remote_side: list[ColumnAttribute] = field(default_factory=list)
#     foreign_keys: list[ColumnAttribute] = field(default_factory=list)
#     primaryjoin: list[JoinType] = field(default_factory=list)
#     secondaryjoin: list[JoinType] = field(default_factory=list)
#     name: str = field(init=False, default="")
#
#     def __repr__(self) -> str:
#         return (
#             f"{self.__class__.__name__}(name={self.name!r}, type={self.type}, "
#             f"target={self.target.name})"
#         )
#
#     def __str__(self) -> str:
#         return self.name
