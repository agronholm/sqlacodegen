from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column, ForeignKeyConstraint, MetaData, Table
from sqlalchemy.sql.expression import text
from sqlalchemy.types import INTEGER, VARCHAR

from sqlacodegen.generators import CodeGenerator, DataclassGenerator

from .conftest import validate_code


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return DataclassGenerator(metadata, engine, options)


def test_basic_class(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("name", VARCHAR(20)),
    )

    validate_code(
        generator.generate(),
        """\
        from typing import Optional

        from sqlalchemy import Integer, String
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class Simple(Base):
            __tablename__ = 'simple'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            name: Mapped[Optional[str]] = mapped_column(String(20))
        """,
    )


def test_mandatory_field_last(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("name", VARCHAR(20), server_default=text("foo")),
        Column("age", INTEGER, nullable=False),
    )

    validate_code(
        generator.generate(),
        """\
        from typing import Optional

        from sqlalchemy import Integer, String, text
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class Simple(Base):
            __tablename__ = 'simple'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            age: Mapped[int] = mapped_column(Integer)
            name: Mapped[Optional[str]] = mapped_column(String(20), \
server_default=text('foo'))
        """,
    )


def test_onetomany_optional(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
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

        from sqlalchemy import ForeignKey, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column, relationship

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)

            simple_items: Mapped[List['SimpleItems']] = relationship('SimpleItems', \
back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

            container: Mapped['SimpleContainers'] = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


def test_manytomany(generator: CodeGenerator) -> None:
    Table("simple_items", generator.metadata, Column("id", INTEGER, primary_key=True))
    Table(
        "simple_containers",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )
    Table(
        "container_items",
        generator.metadata,
        Column("item_id", INTEGER),
        Column("container_id", INTEGER),
        ForeignKeyConstraint(["item_id"], ["simple_items.id"]),
        ForeignKeyConstraint(["container_id"], ["simple_containers.id"]),
    )

    validate_code(
        generator.generate(),
        """\
        from typing import List

        from sqlalchemy import Column, ForeignKey, Integer, Table
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column, relationship

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)

            item: Mapped[List['SimpleItems']] = relationship('SimpleItems', \
secondary='container_items', back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)

            container: Mapped[List['SimpleContainers']] = \
relationship('SimpleContainers', secondary='container_items', back_populates='item')


        t_container_items = Table(
            'container_items', Base.metadata,
            Column('item_id', ForeignKey('simple_items.id')),
            Column('container_id', ForeignKey('simple_containers.id'))
        )
        """,
    )


def test_named_foreign_key_constraints(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("container_id", INTEGER),
        ForeignKeyConstraint(
            ["container_id"], ["simple_containers.id"], name="foreignkeytest"
        ),
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

        from sqlalchemy import ForeignKeyConstraint, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column, relationship

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)

            simple_items: Mapped[List['SimpleItems']] = relationship('SimpleItems', \
back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                ForeignKeyConstraint(['container_id'], ['simple_containers.id'], \
name='foreignkeytest'),
            )

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            container_id: Mapped[Optional[int]] = mapped_column(Integer)

            container: Mapped['SimpleContainers'] = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


def test_uuid_type_annotation(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", UUID, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import UUID
        from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, \
mapped_column
        import uuid

        class Base(MappedAsDataclass, DeclarativeBase):
            pass


        class Simple(Base):
            __tablename__ = 'simple'

            id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True)
        """,
    )
