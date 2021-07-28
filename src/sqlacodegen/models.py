from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple, Union

from sqlalchemy.sql.schema import Column, ForeignKeyConstraint, Table


@dataclass
class Model:
    table: Table
    name: str = field(init=False, default='')

    @property
    def schema(self) -> str:
        return self.table.schema


@dataclass
class ModelClass(Model):
    columns: List['ColumnAttribute'] = field(default_factory=list)
    relationships: List['RelationshipAttribute'] = field(default_factory=list)
    parent_class: Optional['ModelClass'] = None
    children: List['ModelClass'] = field(default_factory=list)

    def get_column_attribute(self, column_name: str) -> 'ColumnAttribute':
        for column in self.columns:
            if column.column.name == column_name:
                return column

        raise LookupError(f'Cannot find column attribute for {column_name!r}')


class RelationshipType(Enum):
    ONE_TO_ONE = auto()
    ONE_TO_MANY = auto()
    MANY_TO_ONE = auto()
    MANY_TO_MANY = auto()


@dataclass
class ColumnAttribute:
    model: ModelClass
    column: Column
    name: str = field(init=False, default='')

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name={self.name!r}, type={self.column.type})'

    def __str__(self) -> str:
        return self.name


JoinType = Tuple[Model, Union[ColumnAttribute, str], Model, Union[ColumnAttribute, str]]


@dataclass
class RelationshipAttribute:
    type: RelationshipType
    source: ModelClass
    target: ModelClass
    constraint: Optional[ForeignKeyConstraint] = None
    association_table: Optional[Model] = None
    backref: Optional['RelationshipAttribute'] = None
    remote_side: List[ColumnAttribute] = field(default_factory=list)
    foreign_keys: List[ColumnAttribute] = field(default_factory=list)
    primaryjoin: List[JoinType] = field(default_factory=list)
    secondaryjoin: List[JoinType] = field(default_factory=list)
    name: str = field(init=False, default='')

    def __repr__(self) -> str:
        return (f'{self.__class__.__name__}(name={self.name!r}, type={self.type}, '
                f'target={self.target.name})')

    def __str__(self) -> str:
        return self.name
