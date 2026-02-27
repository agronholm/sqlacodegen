from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy import Enum as SAEnum
from sqlalchemy import Uuid
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
from sqlalchemy.types import ARRAY, INTEGER, VARCHAR

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
        Column("number", INTEGER, nullable=False),
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

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                number: int = Field(sa_column=Column(\
'number', Integer, nullable=False))
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

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
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
            from typing import Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class SimpleContainers(SQLModel, table=True):
                __tablename__ = 'simple_containers'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))

                simple_goods: list['SimpleGoods'] = Relationship(\
back_populates='container')


            class SimpleGoods(SQLModel, table=True):
                __tablename__ = 'simple_goods'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                container_id: Optional[int] = Field(default=None, sa_column=Column(\
'container_id', ForeignKey('simple_containers.id')))

                container: Optional['SimpleContainers'] = Relationship(\
back_populates='simple_goods')
        """,
    )


def test_onetomany_multiref(generator: CodeGenerator) -> None:
    Table(
        "simple_items_multiref",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container_id", INTEGER),
        Column("top_container_id", INTEGER, nullable=False),
        ForeignKeyConstraint(
            ["parent_container_id"], ["simple_containers_multiref.id"]
        ),
        ForeignKeyConstraint(["top_container_id"], ["simple_containers_multiref.id"]),
    )
    Table(
        "simple_containers_multiref",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class SimpleContainersMultiref(SQLModel, table=True):
                __tablename__ = 'simple_containers_multiref'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))

                simple_items_multiref_parent_container: list['SimpleItemsMultiref'] = \
Relationship(back_populates='parent_container', sa_relationship_kwargs={\
'foreign_keys': '[SimpleItemsMultiref.parent_container_id]'})
                simple_items_multiref_top_container: list['SimpleItemsMultiref'] = \
Relationship(back_populates='top_container', sa_relationship_kwargs={'foreign_keys': \
'[SimpleItemsMultiref.top_container_id]'})


            class SimpleItemsMultiref(SQLModel, table=True):
                __tablename__ = 'simple_items_multiref'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                top_container_id: int = \
Field(sa_column=Column('top_container_id', \
ForeignKey('simple_containers_multiref.id'), nullable=False))
                parent_container_id: Optional[int] = \
Field(default=None, sa_column=Column('parent_container_id', \
ForeignKey('simple_containers_multiref.id')))

                parent_container: Optional['SimpleContainersMultiref'] = Relationship(\
back_populates='simple_items_multiref_parent_container', sa_relationship_kwargs={\
'foreign_keys': '[SimpleItemsMultiref.parent_container_id]'})
                top_container: 'SimpleContainersMultiref' = Relationship(\
back_populates='simple_items_multiref_top_container', sa_relationship_kwargs={\
'foreign_keys': '[SimpleItemsMultiref.top_container_id]'})
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

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))

                simple_onetoone: Optional['SimpleOnetoone'] = Relationship(\
back_populates='other_item', sa_relationship_kwargs={'uselist': False})


            class SimpleOnetoone(SQLModel, table=True):
                __tablename__ = 'simple_onetoone'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                other_item_id: Optional[int] = Field(default=None, sa_column=Column(\
'other_item_id', ForeignKey('other_items.id'), unique=True))

                other_item: Optional['OtherItems'] = Relationship(\
back_populates='simple_onetoone')
            """,
    )


def test_uuid(generator: CodeGenerator) -> None:
    Table(
        "simple_uuid",
        generator.metadata,
        Column("id", Uuid, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
            import uuid

            from sqlalchemy import Column, Uuid
            from sqlmodel import Field, SQLModel

            class SimpleUuid(SQLModel, table=True):
                __tablename__ = 'simple_uuid'

                id: uuid.UUID = Field(sa_column=Column('id', Uuid, primary_key=True))
        """,
    )


def test_check_constraint_not_converted_to_enum(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20), nullable=False),
        CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
    )

    # Recreate generator with nosyntheticenums option to preserve constraints
    generator = SQLModelGenerator(
        generator.metadata, generator.bind, ["nosyntheticenums"]
    )

    validate_code(
        generator.generate(),
        """\
            from sqlalchemy import CheckConstraint, Column, Integer, String
            from sqlmodel import Field, SQLModel

            class Users(SQLModel, table=True):
                __table_args__ = (
                    CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
                )

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                status: str = Field(sa_column=Column('status', String(20), nullable=False))
        """,
    )


def test_synthetic_enum_generation(generator: CodeGenerator) -> None:
    Table(
        "accounts",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20), nullable=False),
        CheckConstraint("accounts.status IN ('active', 'inactive', 'pending')"),
    )

    validate_code(
        generator.generate(),
        """\
            import enum

            from sqlalchemy import CheckConstraint, Column, Enum, Integer
            from sqlmodel import Field, SQLModel

            class AccountsStatus(str, enum.Enum):
                ACTIVE = 'active'
                INACTIVE = 'inactive'
                PENDING = 'pending'


            class Accounts(SQLModel, table=True):
                __table_args__ = (
                    CheckConstraint("accounts.status IN ('active', 'inactive', 'pending')"),
                )

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                status: AccountsStatus = Field(sa_column=Column('status', Enum(AccountsStatus, values_callable=lambda cls: [member.value for member in cls]), nullable=False))
        """,
    )


def test_array_enum_named_with_schema(generator: CodeGenerator) -> None:
    Table(
        "my_table",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "tags",
            ARRAY(SAEnum("a", "b", name="tag_enum", schema="custom_schema")),
            nullable=False,
        ),
        schema="custom_schema",
    )

    validate_code(
        generator.generate(),
        """\
            import enum

            from sqlalchemy import ARRAY, Column, Enum, Integer
            from sqlmodel import Field, SQLModel

            class TagEnum(str, enum.Enum):
                A = 'a'
                B = 'b'


            class MyTable(SQLModel, table=True):
                __tablename__ = 'my_table'
                __table_args__ = {'schema': 'custom_schema'}

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                tags: list[TagEnum] = Field(sa_column=Column('tags', ARRAY(Enum(TagEnum, values_callable=lambda cls: [member.value for member in cls], name='tag_enum', schema='custom_schema')), nullable=False))
        """,
    )


def test_fallback_table(generator: CodeGenerator) -> None:
    Table(
        "simple_fallback",
        generator.metadata,
        Column("field", VARCHAR(20), nullable=False),
    )

    validate_code(
        generator.generate(),
        """\
            from sqlalchemy import Column, String, Table
            from sqlmodel import SQLModel

            t_simple_fallback = Table(
                'simple_fallback', SQLModel.metadata,
                Column('field', String(20), nullable=False)
            )
        """,
    )


def test_onetomany_selfref(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_item_id", INTEGER),
        ForeignKeyConstraint(["parent_item_id"], ["simple_items.id"]),
    )

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class SimpleItems(SQLModel, table=True):
                __tablename__ = 'simple_items'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                parent_item_id: Optional[int] = Field(default=None, sa_column=Column(\
'parent_item_id', ForeignKey('simple_items.id')))

                parent_item: Optional['SimpleItems'] = Relationship(\
back_populates='parent_item_reverse', sa_relationship_kwargs={\
'remote_side': '[SimpleItems.id]'})
                parent_item_reverse: list['SimpleItems'] = Relationship(\
back_populates='parent_item', sa_relationship_kwargs={\
'remote_side': '[SimpleItems.parent_item_id]'})
        """,
    )


def test_onetomany_selfref_multi(generator: CodeGenerator) -> None:
    Table(
        "simple_items_selfref",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_item_id", INTEGER),
        Column("top_item_id", INTEGER),
        ForeignKeyConstraint(["parent_item_id"], ["simple_items_selfref.id"]),
        ForeignKeyConstraint(["top_item_id"], ["simple_items_selfref.id"]),
    )

    validate_code(
        generator.generate(),
        """\
            from typing import Optional

            from sqlalchemy import Column, ForeignKey, Integer
            from sqlmodel import Field, Relationship, SQLModel

            class SimpleItemsSelfref(SQLModel, table=True):
                __tablename__ = 'simple_items_selfref'

                id: int = Field(sa_column=Column('id', Integer, primary_key=True))
                parent_item_id: Optional[int] = Field(default=None, sa_column=Column(\
'parent_item_id', ForeignKey('simple_items_selfref.id')))
                top_item_id: Optional[int] = Field(default=None, sa_column=Column(\
'top_item_id', ForeignKey('simple_items_selfref.id')))

                parent_item: Optional['SimpleItemsSelfref'] = Relationship(\
back_populates='parent_item_reverse', sa_relationship_kwargs={\
'remote_side': '[SimpleItemsSelfref.id]', \
'foreign_keys': '[SimpleItemsSelfref.parent_item_id]'})
                parent_item_reverse: list['SimpleItemsSelfref'] = Relationship(\
back_populates='parent_item', sa_relationship_kwargs={\
'remote_side': '[SimpleItemsSelfref.parent_item_id]', \
'foreign_keys': '[SimpleItemsSelfref.parent_item_id]'})
                top_item: Optional['SimpleItemsSelfref'] = Relationship(\
back_populates='top_item_reverse', sa_relationship_kwargs={\
'remote_side': '[SimpleItemsSelfref.id]', \
'foreign_keys': '[SimpleItemsSelfref.top_item_id]'})
                top_item_reverse: list['SimpleItemsSelfref'] = Relationship(\
back_populates='top_item', sa_relationship_kwargs={\
'remote_side': '[SimpleItemsSelfref.top_item_id]', \
'foreign_keys': '[SimpleItemsSelfref.top_item_id]'})
        """,
    )
