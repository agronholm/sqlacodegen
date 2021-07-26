from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional

from sqlalchemy.sql.schema import Column, ForeignKeyConstraint, Table


@dataclass
class Model:
    name: str
    table: Table

    @property
    def schema(self) -> str:
        return self.table.schema


@dataclass
class ModelClass(Model):
    columns: Dict[str, Column] = field(default_factory=OrderedDict)
    relationships: Dict[str, 'Relationship'] = field(default_factory=OrderedDict)
    parent_class: Optional['ModelClass'] = None
    children: List['ModelClass'] = field(default_factory=list)


class RelationshipType(Enum):
    ONE_TO_ONE = auto()
    ONE_TO_MANY = auto()
    MANY_TO_ONE = auto()
    MANY_TO_MANY = auto()


@dataclass
class Relationship:
    type: RelationshipType
    source: ModelClass
    target: ModelClass
    constraint: Optional[ForeignKeyConstraint] = None
    association_table: Optional[Model] = None
    backref: Optional[str] = None
    remote_side: List[str] = field(default_factory=list)
    foreign_keys: List[str] = field(default_factory=list)
    primaryjoin: Optional[str] = None
    secondaryjoin: Optional[str] = None
