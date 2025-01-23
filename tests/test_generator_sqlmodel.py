from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy.engine import Engine
from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Index,
    MetaData,
    Table,
    UniqueConstraint,
)
from sqlalchemy.types import INTEGER, VARCHAR

from sqlacodegen.generators import CodeGenerator, SQLModelGenerator

from .conftest import validate_code


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return SQLModelGenerator(metadata, engine, options)


def test_indexes(generator: CodeGenerator) -> None:
    simple_items = Table(
        "item",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("number", INTEGER),
        Column("text", VARCHAR),
    )
    simple_items.indexes.add(Index("idx_number", simple_items.c.number))
    simple_items.indexes.add(
        Index("idx_text_number", simple_items.c.text, simple_items.c.number)
    )
    simple_items.indexes.add(Index("idx_text", simple_items.c.text, unique=True))

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import Column, Index, Integer, String
            from sqlmodel import Field, SQLModel

            class Item(SQLModel, table=True):
                __table_args__ = (
                    Index('idx_number', 'number'),
                    Index('idx_text', 'text', unique=True),
                    Index('idx_text_number', 'text', 'number')
                )

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))
                number: Optional[int] = Field(default=None, sa_column=Column(\
'number', Integer))
                text: Optional[str] = Field(default=None, sa_column=Column(\
'text', String))
        """,
    )


def test_constraints(generator: CodeGenerator) -> None:
    Table(
        "simple_constraints",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("number", INTEGER),
        CheckConstraint("number > 0"),
        UniqueConstraint("id", "number"),
    )

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import CheckConstraint, Column, Integer, UniqueConstraint
            from sqlmodel import Field, SQLModel

            class SimpleConstraints(SQLModel, table=True):
                __tablename__ = 'simple_constraints'
                __table_args__ = (
                    CheckConstraint('number > 0'),
                    UniqueConstraint('id', 'number')
                )

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))
                number: Optional[int] = Field(default=None, sa_column=Column(\
'number', Integer))
        """,
    )


def test_onetomany(generator: CodeGenerator) -> None:
    Table(
        "simple_goods",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("container_id", INTEGER),
        ForeignKeyConstraint(["container_id"], ["simple_containers.id"]),
    )
    Table(
        "simple_containers",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
            from typing import List, Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class SimpleContainers(SQLModel, table=True):
                __tablename__ = 'simple_containers'

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))

                simple_goods: List['SimpleGoods'] = Relationship(\
back_populates='container')


            class SimpleGoods(SQLModel, table=True):
                __tablename__ = 'simple_goods'

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))
                container_id: Optional[int] = Field(default=None, sa_column=Column(\
'container_id', ForeignKey('simple_containers.id')))

                container: Optional['SimpleContainers'] = Relationship(\
back_populates='simple_goods')
        """,
    )


def test_onetoone(generator: CodeGenerator) -> None:
    Table(
        "simple_onetoone",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("other_item_id", INTEGER),
        ForeignKeyConstraint(["other_item_id"], ["other_items.id"]),
        UniqueConstraint("other_item_id"),
    )
    Table("other_items", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class OtherItems(SQLModel, table=True):
                __tablename__ = 'other_items'

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))

                simple_onetoone: Optional['SimpleOnetoone'] = Relationship(\
sa_relationship_kwargs={'uselist': False}, back_populates='other_item')


            class SimpleOnetoone(SQLModel, table=True):
                __tablename__ = 'simple_onetoone'

                id: Optional[int] = Field(default=None, sa_column=Column(\
'id', Integer, primary_key=True))
                other_item_id: Optional[int] = Field(default=None, sa_column=Column(\
'other_item_id', ForeignKey('other_items.id'), unique=True))

                other_item: Optional['OtherItems'] = Relationship(\
back_populates='simple_onetoone')
            """,
    )
