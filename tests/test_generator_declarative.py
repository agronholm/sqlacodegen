from __future__ import annotations

import sys

import pytest
from _pytest.fixtures import FixtureRequest
from geoalchemy2 import Geography, Geometry
from sqlalchemy import BIGINT, PrimaryKeyConstraint
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    Table,
    UniqueConstraint,
)
from sqlalchemy.sql.expression import text
from sqlalchemy.types import ARRAY, INTEGER, VARCHAR, Text

from sqlacodegen.generators import CodeGenerator, DeclarativeGenerator

from .conftest import validate_code


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return DeclarativeGenerator(metadata, engine, options)


def test_indexes(generator: CodeGenerator) -> None:
    simple_items = Table(
        "simple_items",
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

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        Index('idx_number', 'number'),
        Index('idx_text', 'text', unique=True),
        Index('idx_text_number', 'text', 'number')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    number: Mapped[Optional[int]] = mapped_column(Integer)
    text: Mapped[Optional[str]] = mapped_column(String)
        """,
    )


def test_constraints(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
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

from sqlalchemy import CheckConstraint, Integer, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    number: Mapped[Optional[int]] = mapped_column(Integer)
        """,
    )


@pytest.mark.parametrize("generator", [["include_dialect_options"]], indirect=True)
def test_include_dialect_options_and_info_table_and_column(
    generator: CodeGenerator,
) -> None:
    from .test_generator_tables import _PartitionInfo

    Table(
        "t_opts",
        generator.metadata,
        Column("id", INTEGER, primary_key=True, starrocks_is_agg_key=True),
        Column("name", VARCHAR, starrocks_agg_type="REPLACE"),
        starrocks_aggregate_key="id",
        starrocks_partition_by=_PartitionInfo("RANGE(id)"),
        starrocks_security="DEFINER",
        starrocks_PROPERTIES={"replication_num": "3", "storage_medium": "SSD"},
        info={
            "table_kind": "MATERIALIZED VIEW",
            "definition": "SELECT id, name FROM t_opts_base_table",
        },
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class TOpts(Base):
    __tablename__ = 't_opts'
    __table_args__ = {'info': {'definition': 'SELECT id, name FROM t_opts_base_table',
              'table_kind': 'MATERIALIZED VIEW'},
     'starrocks_PROPERTIES': {'replication_num': '3', 'storage_medium': 'SSD'},
     'starrocks_aggregate_key': 'id',
     'starrocks_partition_by': 'RANGE(id)',
     'starrocks_security': 'DEFINER'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, starrocks_is_agg_key=True)
    name: Mapped[Optional[str]] = mapped_column(String, starrocks_agg_type='REPLACE')
        """,
    )


@pytest.mark.parametrize("generator", [["include_dialect_options"]], indirect=True)
def test_include_dialect_options_and_info_with_hyphen(generator: CodeGenerator) -> None:
    Table(
        "t_opts2",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        mysql_engine="InnoDB",
        info={"table_kind": "View"},
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class TOpts2(Base):
    __tablename__ = 't_opts2'
    __table_args__ = {'info': {'table_kind': 'View'}, 'mysql_engine': 'InnoDB'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_include_dialect_options_not_enabled_skips(generator: CodeGenerator) -> None:
    from .test_generator_tables import _PartitionInfo

    Table(
        "t_plain",
        generator.metadata,
        Column(
            "id",
            INTEGER,
            primary_key=True,
            info={"abc": True},
            starrocks_is_agg_key=True,
        ),
        starrocks_engine="OLAP",
        starrocks_partition_by=_PartitionInfo("RANGE(id)"),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class TPlain(Base):
    __tablename__ = 't_plain'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_keep_dialect_types_adapts_mysql_integer_default(
    generator: CodeGenerator,
) -> None:
    from sqlalchemy.dialects.mysql import INTEGER as MYSQL_INTEGER

    Table(
        "num",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("val", MYSQL_INTEGER(), nullable=False),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Num(Base):
    __tablename__ = 'num'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    val: Mapped[int] = mapped_column(Integer, nullable=False)
        """,
    )


@pytest.mark.parametrize("generator", [["keep_dialect_types"]], indirect=True)
def test_keep_dialect_types_keeps_mysql_integer(generator: CodeGenerator) -> None:
    from sqlalchemy.dialects.mysql import INTEGER as MYSQL_INTEGER

    Table(
        "num2",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("val", MYSQL_INTEGER(), nullable=False),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import INTEGER
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Num2(Base):
    __tablename__ = 'num2'

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    val: Mapped[int] = mapped_column(INTEGER, nullable=False)
        """,
    )


def test_onetomany(generator: CodeGenerator) -> None:
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
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
back_populates='simple_items')
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

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_item_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_items.id'))

    parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', \
remote_side=[id], back_populates='parent_item_reverse')
    parent_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
remote_side=[parent_item_id], back_populates='parent_item')
""",
    )


def test_onetomany_selfref_multi(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_item_id", INTEGER),
        Column("top_item_id", INTEGER),
        ForeignKeyConstraint(["parent_item_id"], ["simple_items.id"]),
        ForeignKeyConstraint(["top_item_id"], ["simple_items.id"]),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_item_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_items.id'))
    top_item_id: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_items.id'))

    parent_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', \
remote_side=[id], foreign_keys=[parent_item_id], back_populates='parent_item_reverse')
    parent_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
remote_side=[parent_item_id], foreign_keys=[parent_item_id], \
back_populates='parent_item')
    top_item: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', remote_side=[id], \
foreign_keys=[top_item_id], back_populates='top_item_reverse')
    top_item_reverse: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
remote_side=[top_item_id], foreign_keys=[top_item_id], back_populates='top_item')
        """,
    )


def test_onetomany_composite(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("container_id1", INTEGER),
        Column("container_id2", INTEGER),
        ForeignKeyConstraint(
            ["container_id1", "container_id2"],
            ["simple_containers.id1", "simple_containers.id2"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )
    Table(
        "simple_containers",
        generator.metadata,
        Column("id1", INTEGER, primary_key=True),
        Column("id2", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id1: Mapped[int] = mapped_column(Integer, primary_key=True)
    id2: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='simple_containers')


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['container_id1', 'container_id2'], \
['simple_containers.id1', 'simple_containers.id2'], ondelete='CASCADE', \
onupdate='CASCADE'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id1: Mapped[Optional[int]] = mapped_column(Integer)
    container_id2: Mapped[Optional[int]] = mapped_column(Integer)

    simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


def test_onetomany_multiref(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container_id", INTEGER),
        Column("top_container_id", INTEGER, nullable=False),
        ForeignKeyConstraint(["parent_container_id"], ["simple_containers.id"]),
        ForeignKeyConstraint(["top_container_id"], ["simple_containers.id"]),
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

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items_parent_container: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.parent_container_id]', back_populates='parent_container')
    simple_items_top_container: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.top_container_id]', back_populates='top_container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    top_container_id: Mapped[int] = \
mapped_column(ForeignKey('simple_containers.id'), nullable=False)
    parent_container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    parent_container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
foreign_keys=[parent_container_id], back_populates='simple_items_parent_container')
    top_container: Mapped['SimpleContainers'] = relationship('SimpleContainers', \
foreign_keys=[top_container_id], back_populates='simple_items_top_container')
        """,
    )


@pytest.mark.parametrize("generator", [["nofknames"]], indirect=True)
def test_onetomany_multiref_with_nofknames(generator: CodeGenerator) -> None:
    """Test backwards compatibility with nofknames option."""
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container_id", INTEGER),
        Column("top_container_id", INTEGER, nullable=False),
        ForeignKeyConstraint(["parent_container_id"], ["simple_containers.id"]),
        ForeignKeyConstraint(["top_container_id"], ["simple_containers.id"]),
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

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.parent_container_id]', back_populates='parent_container')
    simple_items_: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.top_container_id]', back_populates='top_container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    top_container_id: Mapped[int] = \
mapped_column(ForeignKey('simple_containers.id'), nullable=False)
    parent_container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    parent_container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
foreign_keys=[parent_container_id], back_populates='simple_items')
    top_container: Mapped['SimpleContainers'] = relationship('SimpleContainers', \
foreign_keys=[top_container_id], back_populates='simple_items_')
        """,
    )


def test_onetomany_multiref_no_id_suffix(generator: CodeGenerator) -> None:
    """Test FK-based naming when FK columns don't end with _id."""
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container", INTEGER),
        Column("top_container", INTEGER, nullable=False),
        ForeignKeyConstraint(["parent_container"], ["simple_containers.id"]),
        ForeignKeyConstraint(["top_container"], ["simple_containers.id"]),
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

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items_parent_container: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.parent_container]', back_populates='simple_containers')
    simple_items_top_container: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.top_container]', back_populates='simple_containers_')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    top_container: Mapped[int] = mapped_column(ForeignKey('simple_containers.id'), nullable=False)
    parent_container: Mapped[Optional[int]] = mapped_column(ForeignKey('simple_containers.id'))

    simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
foreign_keys=[parent_container], back_populates='simple_items_parent_container')
    simple_containers_: Mapped['SimpleContainers'] = relationship('SimpleContainers', \
foreign_keys=[top_container], back_populates='simple_items_top_container')
        """,
    )


def test_onetomany_multiref_composite(generator: CodeGenerator) -> None:
    """Test FK-based naming with composite (multi-column) foreign keys."""
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_id1", INTEGER),
        Column("parent_id2", INTEGER),
        Column("top_id1", INTEGER),
        Column("top_id2", INTEGER),
        ForeignKeyConstraint(
            ["parent_id1", "parent_id2"],
            ["simple_containers.id1", "simple_containers.id2"],
        ),
        ForeignKeyConstraint(
            ["top_id1", "top_id2"], ["simple_containers.id1", "simple_containers.id2"]
        ),
    )
    Table(
        "simple_containers",
        generator.metadata,
        Column("id1", INTEGER, primary_key=True),
        Column("id2", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id1: Mapped[int] = mapped_column(Integer, primary_key=True)
    id2: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items_parent_id1_parent_id2: Mapped[list['SimpleItems']] = \
relationship('SimpleItems', foreign_keys='[SimpleItems.parent_id1, SimpleItems.parent_id2]', \
back_populates='parent_id1_parent_id2')
    simple_items_top_id1_top_id2: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
foreign_keys='[SimpleItems.top_id1, SimpleItems.top_id2]', \
back_populates='top_id1_top_id2')


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['parent_id1', 'parent_id2'], \
['simple_containers.id1', 'simple_containers.id2']),
        ForeignKeyConstraint(['top_id1', 'top_id2'], \
['simple_containers.id1', 'simple_containers.id2'])
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_id1: Mapped[Optional[int]] = mapped_column(Integer)
    parent_id2: Mapped[Optional[int]] = mapped_column(Integer)
    top_id1: Mapped[Optional[int]] = mapped_column(Integer)
    top_id2: Mapped[Optional[int]] = mapped_column(Integer)

    parent_id1_parent_id2: Mapped[Optional['SimpleContainers']] = \
relationship('SimpleContainers', foreign_keys=[parent_id1, parent_id2], \
back_populates='simple_items_parent_id1_parent_id2')
    top_id1_top_id2: Mapped[Optional['SimpleContainers']] = \
relationship('SimpleContainers', foreign_keys=[top_id1, top_id2], \
back_populates='simple_items_top_id1_top_id2')
        """,
    )


@pytest.mark.parametrize("generator", [["use_inflect"]], indirect=True)
def test_onetomany_multiref_with_inflect(generator: CodeGenerator) -> None:
    """Test FK-based naming with use_inflect option."""
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container_id", INTEGER),
        Column("top_container_id", INTEGER, nullable=False),
        ForeignKeyConstraint(["parent_container_id"], ["simple_containers.id"]),
        ForeignKeyConstraint(["top_container_id"], ["simple_containers.id"]),
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

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items_parent_containers: Mapped[list['SimpleItem']] = relationship('SimpleItem', \
foreign_keys='[SimpleItem.parent_container_id]', back_populates='parent_container')
    simple_items_top_containers: Mapped[list['SimpleItem']] = relationship('SimpleItem', \
foreign_keys='[SimpleItem.top_container_id]', back_populates='top_container')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    top_container_id: Mapped[int] = \
mapped_column(ForeignKey('simple_containers.id'), nullable=False)
    parent_container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    parent_container: Mapped[Optional['SimpleContainer']] = relationship('SimpleContainer', \
foreign_keys=[parent_container_id], back_populates='simple_items_parent_containers')
    top_container: Mapped['SimpleContainer'] = relationship('SimpleContainer', \
foreign_keys=[top_container_id], back_populates='simple_items_top_containers')
        """,
    )


def test_onetoone(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("other_item_id", INTEGER),
        ForeignKeyConstraint(["other_item_id"], ["other_items.id"]),
        UniqueConstraint("other_item_id"),
    )
    Table(
        "other_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class OtherItems(Base):
    __tablename__ = 'other_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[Optional['SimpleItems']] = relationship('SimpleItems', uselist=False, \
back_populates='other_item')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    other_item_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('other_items.id'), unique=True)

    other_item: Mapped[Optional['OtherItems']] = relationship('OtherItems', \
back_populates='simple_items')
        """,
    )


def test_onetomany_noinflect(generator: CodeGenerator) -> None:
    Table(
        "oglkrogk",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("fehwiuhfiwID", INTEGER),
        ForeignKeyConstraint(["fehwiuhfiwID"], ["fehwiuhfiw.id"]),
    )
    Table("fehwiuhfiw", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Fehwiuhfiw(Base):
    __tablename__ = 'fehwiuhfiw'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    oglkrogk: Mapped[list['Oglkrogk']] = relationship('Oglkrogk', \
back_populates='fehwiuhfiw')


class Oglkrogk(Base):
    __tablename__ = 'oglkrogk'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fehwiuhfiwID: Mapped[Optional[int]] = mapped_column(ForeignKey('fehwiuhfiw.id'))

    fehwiuhfiw: Mapped[Optional['Fehwiuhfiw']] = \
relationship('Fehwiuhfiw', back_populates='oglkrogk')
        """,
    )


def test_onetomany_conflicting_column(generator: CodeGenerator) -> None:
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
        Column("relationship", Text),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    relationship_: Mapped[Optional[str]] = mapped_column('relationship', Text)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


def test_onetomany_conflicting_relationship(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("relationship_id", INTEGER),
        ForeignKeyConstraint(["relationship_id"], ["relationship.id"]),
    )
    Table("relationship", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Relationship(Base):
    __tablename__ = 'relationship'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='relationship_')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    relationship_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('relationship.id'))

    relationship_: Mapped[Optional['Relationship']] = relationship('Relationship', \
back_populates='simple_items')
        """,
    )


@pytest.mark.parametrize("generator", [["nobidi"]], indirect=True)
def test_manytoone_nobidi(generator: CodeGenerator) -> None:
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
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('simple_containers.id'))

    container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers')
""",
    )


def test_manytomany_multi(generator: CodeGenerator) -> None:
    """Test multiple many-to-many relationships between same tables using separate junction tables."""
    Table(
        "students",
        generator.metadata,
        Column("student_id", INTEGER, primary_key=True),
        Column("name", VARCHAR),
    )

    Table(
        "courses",
        generator.metadata,
        Column("course_id", INTEGER, primary_key=True),
        Column("title", VARCHAR),
    )

    # First many-to-many relationship (enrollments)
    Table(
        "enrollments",
        generator.metadata,
        Column("student_id", INTEGER, ForeignKey("students.student_id")),
        Column("course_id", INTEGER, ForeignKey("courses.course_id")),
    )

    # Second many-to-many relationship (waitlist)
    Table(
        "waitlist",
        generator.metadata,
        Column("student_id", INTEGER, ForeignKey("students.student_id")),
        Column("course_id", INTEGER, ForeignKey("courses.course_id")),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Courses(Base):
    __tablename__ = 'courses'

    course_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[Optional[str]] = mapped_column(String)

    students_enrollments: Mapped[list['Students']] = relationship('Students', \
secondary='enrollments', back_populates='courses_enrollments')
    students_waitlist: Mapped[list['Students']] = relationship('Students', \
secondary='waitlist', back_populates='courses_waitlist')


class Students(Base):
    __tablename__ = 'students'

    student_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String)

    courses_enrollments: Mapped[list['Courses']] = relationship('Courses', \
secondary='enrollments', back_populates='students_enrollments')
    courses_waitlist: Mapped[list['Courses']] = relationship('Courses', \
secondary='waitlist', back_populates='students_waitlist')


t_enrollments = Table(
    'enrollments', Base.metadata,
    Column('student_id', ForeignKey('students.student_id')),
    Column('course_id', ForeignKey('courses.course_id'))
)


t_waitlist = Table(
    'waitlist', Base.metadata,
    Column('student_id', ForeignKey('students.student_id')),
    Column('course_id', ForeignKey('courses.course_id'))
)
        """,
    )


@pytest.mark.parametrize("generator", [["nofknames"]], indirect=True)
def test_manytomany_multi_with_nofknames(generator: CodeGenerator) -> None:
    """Test backwards compatibility with nofknames option for M2M relationships."""
    Table(
        "students",
        generator.metadata,
        Column("student_id", INTEGER, primary_key=True),
        Column("name", VARCHAR),
    )

    Table(
        "courses",
        generator.metadata,
        Column("course_id", INTEGER, primary_key=True),
        Column("title", VARCHAR),
    )

    # First many-to-many relationship (enrollments)
    Table(
        "enrollments",
        generator.metadata,
        Column("student_id", INTEGER, ForeignKey("students.student_id")),
        Column("course_id", INTEGER, ForeignKey("courses.course_id")),
    )

    # Second many-to-many relationship (waitlist)
    Table(
        "waitlist",
        generator.metadata,
        Column("student_id", INTEGER, ForeignKey("students.student_id")),
        Column("course_id", INTEGER, ForeignKey("courses.course_id")),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Courses(Base):
    __tablename__ = 'courses'

    course_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[Optional[str]] = mapped_column(String)

    student: Mapped[list['Students']] = relationship('Students', secondary='enrollments', \
back_populates='course')
    student_: Mapped[list['Students']] = relationship('Students', secondary='waitlist', \
back_populates='course_')


class Students(Base):
    __tablename__ = 'students'

    student_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String)

    course: Mapped[list['Courses']] = relationship('Courses', secondary='enrollments', \
back_populates='student')
    course_: Mapped[list['Courses']] = relationship('Courses', secondary='waitlist', \
back_populates='student_')


t_enrollments = Table(
    'enrollments', Base.metadata,
    Column('student_id', ForeignKey('students.student_id')),
    Column('course_id', ForeignKey('courses.course_id'))
)


t_waitlist = Table(
    'waitlist', Base.metadata,
    Column('student_id', ForeignKey('students.student_id')),
    Column('course_id', ForeignKey('courses.course_id'))
)
        """,
    )


def test_manytomany(generator: CodeGenerator) -> None:
    Table("left_table", generator.metadata, Column("id", INTEGER, primary_key=True))
    Table(
        "right_table",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )
    Table(
        "association_table",
        generator.metadata,
        Column("left_id", INTEGER),
        Column("right_id", INTEGER),
        ForeignKeyConstraint(["left_id"], ["left_table.id"]),
        ForeignKeyConstraint(["right_id"], ["right_table.id"]),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class LeftTable(Base):
    __tablename__ = 'left_table'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    right: Mapped[list['RightTable']] = relationship('RightTable', \
secondary='association_table', back_populates='left')


class RightTable(Base):
    __tablename__ = 'right_table'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    left: Mapped[list['LeftTable']] = relationship('LeftTable', \
secondary='association_table', back_populates='right')


t_association_table = Table(
    'association_table', Base.metadata,
    Column('left_id', ForeignKey('left_table.id')),
    Column('right_id', ForeignKey('right_table.id'))
)
        """,
    )


@pytest.mark.parametrize("generator", [["nobidi"]], indirect=True)
def test_manytomany_nobidi(generator: CodeGenerator) -> None:
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
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    item: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
secondary='container_items')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)


t_container_items = Table(
    'container_items', Base.metadata,
    Column('item_id', ForeignKey('simple_items.id')),
    Column('container_id', ForeignKey('simple_containers.id'))
)
            """,
    )


def test_manytomany_selfref(generator: CodeGenerator) -> None:
    Table("simple_items", generator.metadata, Column("id", INTEGER, primary_key=True))
    Table(
        "child_items",
        generator.metadata,
        Column("parent_id", INTEGER),
        Column("child_id", INTEGER),
        ForeignKeyConstraint(["parent_id"], ["simple_items.id"]),
        ForeignKeyConstraint(["child_id"], ["simple_items.id"]),
        schema="otherschema",
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    parent: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
secondary='otherschema.child_items', primaryjoin=lambda: SimpleItems.id \
== t_child_items.c.child_id, \
secondaryjoin=lambda: SimpleItems.id == \
t_child_items.c.parent_id, back_populates='child')
    child: Mapped[list['SimpleItems']] = \
relationship('SimpleItems', secondary='otherschema.child_items', \
primaryjoin=lambda: SimpleItems.id == t_child_items.c.parent_id, \
secondaryjoin=lambda: SimpleItems.id == t_child_items.c.child_id, \
back_populates='parent')


t_child_items = Table(
    'child_items', Base.metadata,
    Column('parent_id', ForeignKey('simple_items.id')),
    Column('child_id', ForeignKey('simple_items.id')),
    schema='otherschema'
)
        """,
    )


def test_manytomany_composite(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id1", INTEGER, primary_key=True),
        Column("id2", INTEGER, primary_key=True),
    )
    Table(
        "simple_containers",
        generator.metadata,
        Column("id1", INTEGER, primary_key=True),
        Column("id2", INTEGER, primary_key=True),
    )
    Table(
        "container_items",
        generator.metadata,
        Column("item_id1", INTEGER),
        Column("item_id2", INTEGER),
        Column("container_id1", INTEGER),
        Column("container_id2", INTEGER),
        ForeignKeyConstraint(
            ["item_id1", "item_id2"], ["simple_items.id1", "simple_items.id2"]
        ),
        ForeignKeyConstraint(
            ["container_id1", "container_id2"],
            ["simple_containers.id1", "simple_containers.id2"],
        ),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Column, ForeignKeyConstraint, Integer, Table
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id1: Mapped[int] = mapped_column(Integer, primary_key=True)
    id2: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
secondary='container_items', back_populates='simple_containers')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id1: Mapped[int] = mapped_column(Integer, primary_key=True)
    id2: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_containers: Mapped[list['SimpleContainers']] = \
relationship('SimpleContainers', secondary='container_items', \
back_populates='simple_items')


t_container_items = Table(
    'container_items', Base.metadata,
    Column('item_id1', Integer),
    Column('item_id2', Integer),
    Column('container_id1', Integer),
    Column('container_id2', Integer),
    ForeignKeyConstraint(['container_id1', 'container_id2'], \
['simple_containers.id1', 'simple_containers.id2']),
    ForeignKeyConstraint(['item_id1', 'item_id2'], \
['simple_items.id1', 'simple_items.id2'])
)
        """,
    )


def test_composite_nullable_pk(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id1", INTEGER, primary_key=True),
        Column("id2", INTEGER, primary_key=True, nullable=True),
    )
    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id1: Mapped[int] = mapped_column(Integer, primary_key=True)
    id2: Mapped[Optional[int]] = mapped_column(Integer, primary_key=True, nullable=True)
        """,
    )


def test_joined_inheritance(generator: CodeGenerator) -> None:
    Table(
        "simple_sub_items",
        generator.metadata,
        Column("simple_items_id", INTEGER, primary_key=True),
        Column("data3", INTEGER),
        ForeignKeyConstraint(["simple_items_id"], ["simple_items.super_item_id"]),
    )
    Table(
        "simple_super_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("data1", INTEGER),
    )
    Table(
        "simple_items",
        generator.metadata,
        Column("super_item_id", INTEGER, primary_key=True),
        Column("data2", INTEGER),
        ForeignKeyConstraint(["super_item_id"], ["simple_super_items.id"]),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleSuperItems(Base):
    __tablename__ = 'simple_super_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    data1: Mapped[Optional[int]] = mapped_column(Integer)


class SimpleItems(SimpleSuperItems):
    __tablename__ = 'simple_items'

    super_item_id: Mapped[int] = mapped_column(ForeignKey('simple_super_items.id'), \
primary_key=True)
    data2: Mapped[Optional[int]] = mapped_column(Integer)


class SimpleSubItems(SimpleItems):
    __tablename__ = 'simple_sub_items'

    simple_items_id: Mapped[int] = \
mapped_column(ForeignKey('simple_items.super_item_id'), primary_key=True)
    data3: Mapped[Optional[int]] = mapped_column(Integer)
        """,
    )


def test_joined_inheritance_same_table_name(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, ForeignKey("simple.id"), primary_key=True),
        schema="altschema",
    )

    validate_code(
        generator.generate(),
        """\
    from sqlalchemy import ForeignKey, Integer
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class Base(DeclarativeBase):
        pass


    class Simple(Base):
        __tablename__ = 'simple'

        id: Mapped[int] = mapped_column(Integer, primary_key=True)


    class Simple_(Simple):
        __tablename__ = 'simple'
        __table_args__ = {'schema': 'altschema'}

        id: Mapped[int] = mapped_column(ForeignKey('simple.id'), primary_key=True)
        """,
    )


@pytest.mark.parametrize("generator", [["use_inflect"]], indirect=True)
def test_use_inflect(generator: CodeGenerator) -> None:
    Table("simple_items", generator.metadata, Column("id", INTEGER, primary_key=True))

    Table("singular", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class Singular(Base):
    __tablename__ = 'singular'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


@pytest.mark.parametrize("generator", [["use_inflect"]], indirect=True)
@pytest.mark.parametrize(
    argnames=("table_name", "class_name", "relationship_name"),
    argvalues=[
        ("manufacturers", "manufacturer", "manufacturer"),
        ("statuses", "status", "status"),
        ("studies", "study", "study"),
        ("moose", "moose", "moose"),
    ],
    ids=[
        "test_inflect_manufacturer",
        "test_inflect_status",
        "test_inflect_study",
        "test_inflect_moose",
    ],
)
def test_use_inflect_plural(
    generator: CodeGenerator,
    table_name: str,
    class_name: str,
    relationship_name: str,
) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(f"{relationship_name}_id", INTEGER),
        ForeignKeyConstraint([f"{relationship_name}_id"], [f"{table_name}.id"]),
        UniqueConstraint(f"{relationship_name}_id"),
    )
    Table(table_name, generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        f"""\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class {class_name.capitalize()}(Base):
    __tablename__ = '{table_name}'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_item: Mapped[Optional['SimpleItem']] = relationship('SimpleItem', uselist=False, \
back_populates='{relationship_name}')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    {relationship_name}_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('{table_name}.id'), unique=True)

    {relationship_name}: Mapped[Optional['{class_name.capitalize()}']] = \
relationship('{class_name.capitalize()}', back_populates='simple_item')
        """,
    )


@pytest.mark.parametrize("generator", [["use_inflect"]], indirect=True)
def test_use_inflect_plural_double_pluralize(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("users_id", INTEGER),
        Column("groups_id", INTEGER),
        ForeignKeyConstraint(
            ["groups_id"], ["groups.groups_id"], name="fk_users_groups_id"
        ),
        PrimaryKeyConstraint("users_id", name="users_pkey"),
    )

    Table(
        "groups",
        generator.metadata,
        Column("groups_id", INTEGER),
        Column("group_name", Text(50), nullable=False),
        PrimaryKeyConstraint("groups_id", name="groups_pkey"),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Integer, PrimaryKeyConstraint, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class Group(Base):
    __tablename__ = 'groups'
    __table_args__ = (
        PrimaryKeyConstraint('groups_id', name='groups_pkey'),
    )

    groups_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    group_name: Mapped[str] = mapped_column(Text(50), nullable=False)

    users: Mapped[list['User']] = relationship('User', back_populates='group')


class User(Base):
    __tablename__ = 'users'
    __table_args__ = (
        ForeignKeyConstraint(['groups_id'], ['groups.groups_id'], name='fk_users_groups_id'),
        PrimaryKeyConstraint('users_id', name='users_pkey')
    )

    users_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    groups_id: Mapped[Optional[int]] = mapped_column(Integer)

    group: Mapped[Optional['Group']] = relationship('Group', back_populates='users')
    """,
    )


def test_table_kwargs(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        schema="testschema",
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = {'schema': 'testschema'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_table_args_kwargs(generator: CodeGenerator) -> None:
    simple_items = Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("name", VARCHAR),
        schema="testschema",
    )
    simple_items.indexes.add(Index("testidx", simple_items.c.id, simple_items.c.name))

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        Index('testidx', 'id', 'name'),
        {'schema': 'testschema'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String)
        """,
    )


def test_foreign_key_schema(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("other_item_id", INTEGER),
        ForeignKeyConstraint(["other_item_id"], ["otherschema.other_items.id"]),
    )
    Table(
        "other_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        schema="otherschema",
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class OtherItems(Base):
    __tablename__ = 'other_items'
    __table_args__ = {'schema': 'otherschema'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='other_item')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    other_item_id: Mapped[Optional[int]] = \
mapped_column(ForeignKey('otherschema.other_items.id'))

    other_item: Mapped[Optional['OtherItems']] = relationship('OtherItems', \
back_populates='simple_items')
        """,
    )


def test_invalid_attribute_names(generator: CodeGenerator) -> None:
    Table(
        "simple-items",
        generator.metadata,
        Column("id-test", INTEGER, primary_key=True),
        Column("4test", INTEGER),
        Column("_4test", INTEGER),
        Column("def", INTEGER),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SimpleItems(Base):
    __tablename__ = 'simple-items'

    id_test: Mapped[int] = mapped_column('id-test', Integer, primary_key=True)
    _4test: Mapped[Optional[int]] = mapped_column('4test', Integer)
    _4test_: Mapped[Optional[int]] = mapped_column('_4test', Integer)
    def_: Mapped[Optional[int]] = mapped_column('def', Integer)
        """,
    )


def test_pascal(generator: CodeGenerator) -> None:
    Table(
        "CustomerAPIPreference",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class CustomerAPIPreference(Base):
    __tablename__ = 'CustomerAPIPreference'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_underscore(generator: CodeGenerator) -> None:
    Table(
        "customer_api_preference",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class CustomerApiPreference(Base):
    __tablename__ = 'customer_api_preference'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_pascal_underscore(generator: CodeGenerator) -> None:
    Table(
        "customer_API_Preference",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API_Preference'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


def test_pascal_multiple_underscore(generator: CodeGenerator) -> None:
    Table(
        "customer_API__Preference",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API__Preference'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
        """,
    )


@pytest.mark.parametrize(
    "generator, nocomments",
    [([], False), (["nocomments"], True)],
    indirect=["generator"],
)
def test_column_comment(generator: CodeGenerator, nocomments: bool) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True, comment="this is a 'comment'"),
    )

    comment_part = "" if nocomments else ", comment=\"this is a 'comment'\""
    validate_code(
        generator.generate(),
        f"""\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'

    id: Mapped[int] = mapped_column(Integer, primary_key=True{comment_part})
""",
    )


def test_table_comment(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        comment="this is a 'comment'",
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'
    __table_args__ = {'comment': "this is a 'comment'"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
""",
    )


def test_metadata_column(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("metadata", VARCHAR),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    metadata_: Mapped[Optional[str]] = mapped_column('metadata', String)
""",
    )


def test_invalid_variable_name_from_column(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column(" id ", INTEGER, primary_key=True),
    )

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'

    id: Mapped[int] = mapped_column(' id ', Integer, primary_key=True)
""",
    )


def test_only_tables(generator: CodeGenerator) -> None:
    Table("simple", generator.metadata, Column("id", INTEGER))

    validate_code(
        generator.generate(),
        """\
from sqlalchemy import Column, Integer, MetaData, Table

metadata = MetaData()


t_simple = Table(
    'simple', metadata,
    Column('id', Integer)
)
            """,
    )


def test_named_constraints(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER),
        Column("text", VARCHAR),
        CheckConstraint("id > 0", name="checktest"),
        PrimaryKeyConstraint("id", name="primarytest"),
        UniqueConstraint("text", name="uniquetest"),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import CheckConstraint, Integer, PrimaryKeyConstraint, \
String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'
    __table_args__ = (
        CheckConstraint('id > 0', name='checktest'),
        PrimaryKeyConstraint('id', name='primarytest'),
        UniqueConstraint('text', name='uniquetest')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    text: Mapped[Optional[str]] = mapped_column(String)
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
from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['container_id'], ['simple_containers.id'], \
name='foreignkeytest'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id: Mapped[Optional[int]] = mapped_column(Integer)

    container: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
back_populates='simple_items')
""",
    )


@pytest.mark.parametrize("generator", [["noidsuffix"]], indirect=True)
def test_named_foreign_key_constraints_with_noidsuffix(
    generator: CodeGenerator,
) -> None:
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
from typing import Optional

from sqlalchemy import ForeignKeyConstraint, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class SimpleContainers(Base):
    __tablename__ = 'simple_containers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    simple_items: Mapped[list['SimpleItems']] = relationship('SimpleItems', \
back_populates='simple_containers')


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['container_id'], ['simple_containers.id'], \
name='foreignkeytest'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    container_id: Mapped[Optional[int]] = mapped_column(Integer)

    simple_containers: Mapped[Optional['SimpleContainers']] = relationship('SimpleContainers', \
back_populates='simple_items')
""",
    )


# @pytest.mark.xfail(strict=True)
def test_colname_import_conflict(generator: CodeGenerator) -> None:
    Table(
        "simple",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("text", VARCHAR),
        Column("textwithdefault", VARCHAR, server_default=text("'test'")),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import Integer, String, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Simple(Base):
    __tablename__ = 'simple'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    text_: Mapped[Optional[str]] = mapped_column('text', String)
    textwithdefault: Mapped[Optional[str]] = mapped_column(String, \
server_default=text("'test'"))
""",
    )


def test_table_with_arrays(generator: CodeGenerator) -> None:
    Table(
        "with_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("int_items_not_optional", ARRAY(INTEGER()), nullable=False),
        Column("str_matrix", ARRAY(VARCHAR(), dimensions=2)),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import ARRAY, INTEGER, Integer, VARCHAR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class WithItems(Base):
    __tablename__ = 'with_items'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    int_items_not_optional: Mapped[list[int]] = mapped_column(ARRAY(INTEGER()), nullable=False)
    str_matrix: Mapped[Optional[list[list[str]]]] = mapped_column(ARRAY(VARCHAR(), dimensions=2))
""",
    )


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_domain_json(generator: CodeGenerator) -> None:
    Table(
        "test_domain_json",
        generator.metadata,
        Column("id", BIGINT, primary_key=True),
        Column(
            "foo",
            postgresql.DOMAIN(
                "domain_json",
                JSON,
                not_null=False,
            ),
            nullable=True,
        ),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Optional

from sqlalchemy import BigInteger
from sqlalchemy.dialects.postgresql import DOMAIN, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class TestDomainJson(Base):
    __tablename__ = 'test_domain_json'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    foo: Mapped[Optional[dict]] = mapped_column(DOMAIN('domain_json', JSON(), not_null=False))
""",
    )


@pytest.mark.parametrize(
    "domain_type",
    [JSONB, JSON],
)
def test_domain_non_default_json(
    generator: CodeGenerator,
    domain_type: type[JSON] | type[JSONB],
) -> None:
    Table(
        "test_domain_json",
        generator.metadata,
        Column("id", BIGINT, primary_key=True),
        Column(
            "foo",
            postgresql.DOMAIN(
                "domain_json",
                domain_type(astext_type=Text(128)),
                not_null=False,
            ),
            nullable=True,
        ),
    )

    validate_code(
        generator.generate(),
        f"""\
from typing import Optional

from sqlalchemy import BigInteger, Text
from sqlalchemy.dialects.postgresql import DOMAIN, {domain_type.__name__}
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class TestDomainJson(Base):
    __tablename__ = 'test_domain_json'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    foo: Mapped[Optional[dict]] = mapped_column(DOMAIN('domain_json', {domain_type.__name__}(astext_type=Text(length=128)), not_null=False))
""",
    )


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="This test assumes GeoAlchemy2 0.18.x and above, which does not support python 3.9",
)
@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_geoalchemy2_types(generator: CodeGenerator) -> None:
    Table(
        "spatial_table",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("geom", Geometry("POINT", srid=4326, dimension=2), nullable=False),
        Column("geog", Geography("POLYGON", dimension=2)),
    )

    validate_code(
        generator.generate(),
        """\
from typing import Any, Optional

from geoalchemy2.types import Geography, Geometry
from sqlalchemy import Index, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class SpatialTable(Base):
    __tablename__ = 'spatial_table'
    __table_args__ = (
        Index('idx_spatial_table_geog', 'geog'),
        Index('idx_spatial_table_geom', 'geom')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    geom: Mapped[Any] = mapped_column(Geometry('POINT', 4326, 2, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False)
    geog: Mapped[Optional[Any]] = mapped_column(Geography('POLYGON', dimension=2, from_text='ST_GeogFromText', name='geography'))
""",
    )


def test_enum_nonativeenums_option(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "status",
            SAEnum("active", "inactive", "pending", name="status_enum"),
            nullable=False,
        ),
    )

    # Recreate generator with nonativeenums option
    generator = DeclarativeGenerator(
        generator.metadata, generator.bind, ["nonativeenums"]
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class Users(Base):
            __tablename__ = 'users'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[str] = mapped_column(Enum('active', 'inactive', 'pending', name='status_enum'), nullable=False)
        """,
    )


def test_enum_shared_values(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "status",
            SAEnum("active", "inactive", "pending", name="status_enum"),
            nullable=False,
        ),
    )
    Table(
        "accounts",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "status",
            SAEnum("active", "inactive", "pending", name="status_enum"),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class StatusEnum(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        class Accounts(Base):
            __tablename__ = 'accounts'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls]), nullable=False)


        class Users(Base):
            __tablename__ = 'users'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[StatusEnum] = mapped_column(Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )


def test_enum_unnamed(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "status",
            SAEnum("active", "inactive", "pending"),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class UsersStatus(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        class Users(Base):
            __tablename__ = 'users'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[UsersStatus] = mapped_column(Enum(UsersStatus, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )


def test_enum_unnamed_reuse_same_values(generator: CodeGenerator) -> None:
    # table "a_b", column "c" -> A + B + C = ABC
    # table "a", column "b_c" -> A + B + C = ABC
    # Both generate same name with same values, so reuse
    Table(
        "a_b",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "c",
            SAEnum("active", "inactive"),
            nullable=False,
        ),
    )
    Table(
        "a",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "b_c",
            SAEnum("active", "inactive"),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class ABC(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'


        class A(Base):
            __tablename__ = 'a'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            b_c: Mapped[ABC] = mapped_column(Enum(ABC, values_callable=lambda cls: [member.value for member in cls]), nullable=False)


        class AB(Base):
            __tablename__ = 'a_b'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            c: Mapped[ABC] = mapped_column(Enum(ABC, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )


def test_enum_unnamed_name_collision_different_values(generator: CodeGenerator) -> None:
    # table "a_b", column "c" -> A + B + C = ABC
    # table "a", column "b_c" -> A + B + C = ABC
    # Same name but different values, so append counter
    Table(
        "a_b",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "c",
            SAEnum("active", "inactive"),
            nullable=False,
        ),
    )
    Table(
        "a",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "b_c",
            SAEnum("pending", "complete"),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class ABC(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'


        class ABC1(str, enum.Enum):
            PENDING = 'pending'
            COMPLETE = 'complete'


        class A(Base):
            __tablename__ = 'a'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            b_c: Mapped[ABC1] = mapped_column(Enum(ABC1, values_callable=lambda cls: [member.value for member in cls]), nullable=False)


        class AB(Base):
            __tablename__ = 'a_b'

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            c: Mapped[ABC] = mapped_column(Enum(ABC, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )


def test_synthetic_enum_generation(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20), nullable=False),
        CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import CheckConstraint, Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class UsersStatus(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        class Users(Base):
            __tablename__ = 'users'
            __table_args__ = (
                CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
            )

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[UsersStatus] = mapped_column(Enum(UsersStatus, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )


def test_synthetic_enum_nosyntheticenums_option(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20), nullable=False),
        CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
    )

    # Recreate generator with nosyntheticenums option
    generator = DeclarativeGenerator(
        generator.metadata, generator.bind, ["nosyntheticenums"]
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import CheckConstraint, Integer, String
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class Users(Base):
            __tablename__ = 'users'
            __table_args__ = (
                CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
            )

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[str] = mapped_column(String(20), nullable=False)
        """,
    )


def test_synthetic_enum_shared_values(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20), nullable=False),
        CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
    )
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

        from sqlalchemy import CheckConstraint, Enum, Integer
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass


        class AccountsStatus(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        class UsersStatus(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        class Accounts(Base):
            __tablename__ = 'accounts'
            __table_args__ = (
                CheckConstraint("accounts.status IN ('active', 'inactive', 'pending')"),
            )

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[AccountsStatus] = mapped_column(Enum(AccountsStatus, values_callable=lambda cls: [member.value for member in cls]), nullable=False)


        class Users(Base):
            __tablename__ = 'users'
            __table_args__ = (
                CheckConstraint("users.status IN ('active', 'inactive', 'pending')"),
            )

            id: Mapped[int] = mapped_column(Integer, primary_key=True)
            status: Mapped[UsersStatus] = mapped_column(Enum(UsersStatus, values_callable=lambda cls: [member.value for member in cls]), nullable=False)
        """,
    )
