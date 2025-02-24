from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Union

from sqlalchemy.sql.schema import Column, ForeignKeyConstraint, Table


@dataclass
class Model:
    table: Table
    name: str = field(init=False, default="")

    @property
    def schema(self) -> str | None:
        return self.table.schema


@dataclass
class ModelClass(Model):
    columns: list[ColumnAttribute] = field(default_factory=list)
    relationships: list[RelationshipAttribute] = field(default_factory=list)
    parent_class: ModelClass | None = None
    children: list[ModelClass] = field(default_factory=list)

    def get_column_attribute(self, column_name: str) -> ColumnAttribute:
        for column in self.columns:
            if column.column.name == column_name:
                return column

        raise LookupError(f"Cannot find column attribute for {column_name!r}")


class RelationshipType(Enum):
    ONE_TO_ONE = auto()
    ONE_TO_MANY = auto()
    MANY_TO_ONE = auto()
    MANY_TO_MANY = auto()


@dataclass
class ColumnAttribute:
    model: ModelClass
    column: Column[Any]
    name: str = field(init=False, default="")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, type={self.column.type})"

    def __str__(self) -> str:
        return self.name


JoinType = tuple[Model, Union[ColumnAttribute, str], Model, Union[ColumnAttribute, str]]


@dataclass
class RelationshipAttribute:
    type: RelationshipType
    source: ModelClass
    target: ModelClass
    constraint: ForeignKeyConstraint | None = None
    association_table: Model | None = None
    backref: RelationshipAttribute | None = None
    remote_side: list[ColumnAttribute] = field(default_factory=list)
    foreign_keys: list[ColumnAttribute] = field(default_factory=list)
    primaryjoin: list[JoinType] = field(default_factory=list)
    secondaryjoin: list[JoinType] = field(default_factory=list)
    name: str = field(init=False, default="")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name!r}, type={self.type}, "
            f"target={self.target.name})"
        )

    def __str__(self) -> str:
        return self.name
