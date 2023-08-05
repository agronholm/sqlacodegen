from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy import PrimaryKeyConstraint
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
from sqlalchemy.types import INTEGER, VARCHAR, Text

from sqlacodegen.generators import CodeGenerator, DeclarativeGenerator

from .conftest import requires_sqlalchemy_1_4, validate_code

pytestmark = requires_sqlalchemy_1_4


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
        from sqlalchemy import Column, Index, Integer, String
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                Index('idx_number', 'number'),
                Index('idx_text', 'text', unique=True),
                Index('idx_text_number', 'text', 'number')
            )

            id = Column(Integer, primary_key=True)
            number = Column(Integer)
            text = Column(String)
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
        from sqlalchemy import CheckConstraint, Column, Integer, UniqueConstraint
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                CheckConstraint('number > 0'),
                UniqueConstraint('id', 'number')
            )

            id = Column(Integer, primary_key=True)
            number = Column(Integer)
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            container_id = Column(ForeignKey('simple_containers.id'))

            container = relationship('SimpleContainers', \
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            parent_item_id = Column(ForeignKey('simple_items.id'))

            parent_item = relationship('SimpleItems', remote_side=[id], \
back_populates='parent_item_reverse')
            parent_item_reverse = relationship('SimpleItems', \
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            parent_item_id = Column(ForeignKey('simple_items.id'))
            top_item_id = Column(ForeignKey('simple_items.id'))

            parent_item = relationship('SimpleItems', remote_side=[id], \
foreign_keys=[parent_item_id], back_populates='parent_item_reverse')
            parent_item_reverse = relationship('SimpleItems', \
remote_side=[parent_item_id], foreign_keys=[parent_item_id], \
back_populates='parent_item')
            top_item = relationship('SimpleItems', remote_side=[id], \
foreign_keys=[top_item_id], back_populates='top_item_reverse')
            top_item_reverse = relationship('SimpleItems', \
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
        from sqlalchemy import Column, ForeignKeyConstraint, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id1 = Column(Integer, primary_key=True, nullable=False)
            id2 = Column(Integer, primary_key=True, nullable=False)

            simple_items = relationship('SimpleItems', \
back_populates='simple_containers')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                ForeignKeyConstraint(['container_id1', 'container_id2'], \
['simple_containers.id1', 'simple_containers.id2'], ondelete='CASCADE', \
onupdate='CASCADE'),
            )

            id = Column(Integer, primary_key=True)
            container_id1 = Column(Integer)
            container_id2 = Column(Integer)

            simple_containers = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


def test_onetomany_multiref(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("parent_container_id", INTEGER),
        Column("top_container_id", INTEGER),
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', \
foreign_keys='[SimpleItems.parent_container_id]', back_populates='parent_container')
            simple_items_ = relationship('SimpleItems', \
foreign_keys='[SimpleItems.top_container_id]', back_populates='top_container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            parent_container_id = Column(ForeignKey('simple_containers.id'))
            top_container_id = Column(ForeignKey('simple_containers.id'))

            parent_container = relationship('SimpleContainers', \
foreign_keys=[parent_container_id], back_populates='simple_items')
            top_container = relationship('SimpleContainers', \
foreign_keys=[top_container_id], back_populates='simple_items_')
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
    Table("other_items", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class OtherItems(Base):
            __tablename__ = 'other_items'

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', uselist=False, \
back_populates='other_item')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            other_item_id = Column(ForeignKey('other_items.id'), unique=True)

            other_item = relationship('OtherItems', back_populates='simple_items')
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
    from sqlalchemy import Column, ForeignKey, Integer
    from sqlalchemy.orm import declarative_base, relationship

    Base = declarative_base()


    class Fehwiuhfiw(Base):
        __tablename__ = 'fehwiuhfiw'

        id = Column(Integer, primary_key=True)

        oglkrogk = relationship('Oglkrogk', back_populates='fehwiuhfiw')


    class Oglkrogk(Base):
        __tablename__ = 'oglkrogk'

        id = Column(Integer, primary_key=True)
        fehwiuhfiwID = Column(ForeignKey('fehwiuhfiw.id'))

        fehwiuhfiw = relationship('Fehwiuhfiw', back_populates='oglkrogk')
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
        from sqlalchemy import Column, ForeignKey, Integer, Text
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)
            relationship_ = Column('relationship', Text)

            simple_items = relationship('SimpleItems', back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            container_id = Column(ForeignKey('simple_containers.id'))

            container = relationship('SimpleContainers', back_populates='simple_items')
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class Relationship(Base):
            __tablename__ = 'relationship'

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', back_populates='relationship_')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            relationship_id = Column(ForeignKey('relationship.id'))

            relationship_ = relationship('Relationship', back_populates='simple_items')
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            container_id = Column(ForeignKey('simple_containers.id'))

            container = relationship('SimpleContainers')
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
        from sqlalchemy import Column, ForeignKey, Integer, Table
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()
        metadata = Base.metadata


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)

            item = relationship('SimpleItems', secondary='container_items', \
back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)

            container = relationship('SimpleContainers', secondary='container_items', \
back_populates='item')


        t_container_items = Table(
            'container_items', metadata,
            Column('item_id', ForeignKey('simple_items.id')),
            Column('container_id', ForeignKey('simple_containers.id'))
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
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()
        metadata = Base.metadata


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)

            item = relationship('SimpleItems', secondary='container_items')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)


        t_container_items = Table(
            'container_items', metadata,
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
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()
        metadata = Base.metadata


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)

            parent = relationship('SimpleItems', secondary='otherschema.child_items', \
primaryjoin=lambda: SimpleItems.id == t_child_items.c.child_id, \
secondaryjoin=lambda: SimpleItems.id == t_child_items.c.parent_id, \
back_populates='child')
            child = relationship('SimpleItems', secondary='otherschema.child_items', \
primaryjoin=lambda: SimpleItems.id == t_child_items.c.parent_id, \
secondaryjoin=lambda: SimpleItems.id == t_child_items.c.child_id, \
back_populates='parent')


        t_child_items = Table(
            'child_items', metadata,
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
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()
        metadata = Base.metadata


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id1 = Column(Integer, primary_key=True, nullable=False)
            id2 = Column(Integer, primary_key=True, nullable=False)

            simple_items = relationship('SimpleItems', secondary='container_items', \
back_populates='simple_containers')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id1 = Column(Integer, primary_key=True, nullable=False)
            id2 = Column(Integer, primary_key=True, nullable=False)

            simple_containers = relationship('SimpleContainers', \
secondary='container_items', back_populates='simple_items')


        t_container_items = Table(
            'container_items', metadata,
            Column('item_id1', Integer),
            Column('item_id2', Integer),
            Column('container_id1', Integer),
            Column('container_id2', Integer),
            ForeignKeyConstraint(['container_id1', 'container_id2'], \
['simple_containers.id1', 'simple_containers.id2']),
            ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', \
'simple_items.id2'])
        )
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleSuperItems(Base):
            __tablename__ = 'simple_super_items'

            id = Column(Integer, primary_key=True)
            data1 = Column(Integer)


        class SimpleItems(SimpleSuperItems):
            __tablename__ = 'simple_items'

            super_item_id = Column(ForeignKey('simple_super_items.id'), \
primary_key=True)
            data2 = Column(Integer)


        class SimpleSubItems(SimpleItems):
            __tablename__ = 'simple_sub_items'

            simple_items_id = Column(ForeignKey('simple_items.super_item_id'), \
primary_key=True)
            data3 = Column(Integer)
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'

            id = Column(Integer, primary_key=True)


        class Simple_(Simple):
            __tablename__ = 'simple'
            __table_args__ = {'schema': 'altschema'}

            id = Column(ForeignKey('simple.id'), primary_key=True)
        """,
    )


@pytest.mark.parametrize("generator", [["use_inflect"]], indirect=True)
def test_use_inflect(generator: CodeGenerator) -> None:
    Table("simple_items", generator.metadata, Column("id", INTEGER, primary_key=True))

    Table("singular", generator.metadata, Column("id", INTEGER, primary_key=True))

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItem(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)


        class Singular(Base):
            __tablename__ = 'singular'

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class {class_name.capitalize()}(Base):
            __tablename__ = '{table_name}'

            id = Column(Integer, primary_key=True)

            simple_item = relationship('SimpleItem', uselist=False, \
back_populates='{relationship_name}')


        class SimpleItem(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            {relationship_name}_id = Column(ForeignKey('{table_name}.id'), unique=True)

            {relationship_name} = relationship('{class_name.capitalize()}', \
back_populates='simple_item')
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = {'schema': 'testschema'}

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Index, Integer, String
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                Index('testidx', 'id', 'name'),
                {'schema': 'testschema'}
            )

            id = Column(Integer, primary_key=True)
            name = Column(String)
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
        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class OtherItems(Base):
            __tablename__ = 'other_items'
            __table_args__ = {'schema': 'otherschema'}

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', back_populates='other_item')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'

            id = Column(Integer, primary_key=True)
            other_item_id = Column(ForeignKey('otherschema.other_items.id'))

            other_item = relationship('OtherItems', back_populates='simple_items')
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class SimpleItems(Base):
            __tablename__ = 'simple-items'

            id_test = Column('id-test', Integer, primary_key=True)
            _4test = Column('4test', Integer)
            _4test_ = Column('_4test', Integer)
            def_ = Column('def', Integer)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class CustomerAPIPreference(Base):
            __tablename__ = 'CustomerAPIPreference'

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class CustomerApiPreference(Base):
            __tablename__ = 'customer_api_preference'

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class CustomerAPIPreference(Base):
            __tablename__ = 'customer_API_Preference'

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class CustomerAPIPreference(Base):
            __tablename__ = 'customer_API__Preference'

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'

            id = Column(Integer, primary_key=True{comment_part})
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'
            __table_args__ = {'comment': "this is a 'comment'"}

            id = Column(Integer, primary_key=True)
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
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'

            id = Column(Integer, primary_key=True)
            metadata_ = Column('metadata', String)
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
        from sqlalchemy import Column, Integer
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'

            id = Column(' id ', Integer, primary_key=True)
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
        from sqlalchemy import CheckConstraint, Column, Integer, \
PrimaryKeyConstraint, String, UniqueConstraint
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'
            __table_args__ = (
                CheckConstraint('id > 0', name='checktest'),
                PrimaryKeyConstraint('id', name='primarytest'),
                UniqueConstraint('text', name='uniquetest')
            )

            id = Column(Integer, primary_key=True)
            text = Column(String)
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
        from sqlalchemy import Column, ForeignKeyConstraint, Integer
        from sqlalchemy.orm import declarative_base, relationship

        Base = declarative_base()


        class SimpleContainers(Base):
            __tablename__ = 'simple_containers'

            id = Column(Integer, primary_key=True)

            simple_items = relationship('SimpleItems', back_populates='container')


        class SimpleItems(Base):
            __tablename__ = 'simple_items'
            __table_args__ = (
                ForeignKeyConstraint(['container_id'], ['simple_containers.id'], \
name='foreignkeytest'),
            )

            id = Column(Integer, primary_key=True)
            container_id = Column(Integer)

            container = relationship('SimpleContainers', \
back_populates='simple_items')
        """,
    )


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
        from sqlalchemy import Column, Integer, String, text
        from sqlalchemy.orm import declarative_base

        Base = declarative_base()


        class Simple(Base):
            __tablename__ = 'simple'

            id = Column(Integer, primary_key=True)
            text_ = Column('text', String)
            textwithdefault = Column(String, server_default=text("'test'"))
        """,
    )
