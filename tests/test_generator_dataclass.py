from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column, ForeignKeyConstraint, MetaData, Table
from sqlalchemy.sql.expression import text
from sqlalchemy.types import INTEGER, VARCHAR

from sqlacodegen.generators import CodeGenerator, DataclassGenerator

from .conftest import requires_sqlalchemy_1_4, validate_code

pytestmark = requires_sqlalchemy_1_4


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
        from __future__ import annotations

        from dataclasses import dataclass, field
        from typing import Optional

        from sqlalchemy import Column, Integer, String
        from sqlalchemy.orm import registry

        mapper_registry = registry()


        @mapper_registry.mapped
        @dataclass
        class Simple:
            __tablename__ = 'simple'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})
            name: Optional[str] = field(default=None, metadata={'sa': \
Column(String(20))})
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
        from __future__ import annotations

        from dataclasses import dataclass, field
        from typing import Optional

        from sqlalchemy import Column, Integer, String, text
        from sqlalchemy.orm import registry

        mapper_registry = registry()


        @mapper_registry.mapped
        @dataclass
        class Simple:
            __tablename__ = 'simple'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})
            age: int = field(metadata={'sa': Column(Integer, nullable=False)})
            name: Optional[str] = field(default=None, metadata={'sa': \
Column(String(20), server_default=text('foo'))})
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
        from __future__ import annotations

        from dataclasses import dataclass, field
        from typing import List, Optional

        from sqlalchemy import Column, ForeignKey, Integer
        from sqlalchemy.orm import registry, relationship

        mapper_registry = registry()


        @mapper_registry.mapped
        @dataclass
        class SimpleContainers:
            __tablename__ = 'simple_containers'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})

            simple_items: List[SimpleItems] = field(default_factory=list, \
metadata={'sa': relationship('SimpleItems', back_populates='container')})


        @mapper_registry.mapped
        @dataclass
        class SimpleItems:
            __tablename__ = 'simple_items'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})
            container_id: Optional[int] = field(default=None, \
metadata={'sa': Column(ForeignKey('simple_containers.id'))})

            container: Optional[SimpleContainers] = field(default=None, \
metadata={'sa': relationship('SimpleContainers', back_populates='simple_items')})
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
        from __future__ import annotations

        from dataclasses import dataclass, field
        from typing import List

        from sqlalchemy import Column, ForeignKey, Integer, Table
        from sqlalchemy.orm import registry, relationship

        mapper_registry = registry()
        metadata = mapper_registry.metadata


        @mapper_registry.mapped
        @dataclass
        class SimpleContainers:
            __tablename__ = 'simple_containers'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})

            item: List[SimpleItems] = field(default_factory=list, metadata=\
{'sa': relationship('SimpleItems', secondary='container_items', \
back_populates='container')})


        @mapper_registry.mapped
        @dataclass
        class SimpleItems:
            __tablename__ = 'simple_items'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})

            container: List[SimpleContainers] = \
field(default_factory=list, metadata={'sa': relationship('SimpleContainers', \
secondary='container_items', back_populates='item')})


        t_container_items = Table(
            'container_items', metadata,
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
        from __future__ import annotations

        from dataclasses import dataclass, field
        from typing import List, Optional

        from sqlalchemy import Column, ForeignKeyConstraint, Integer
        from sqlalchemy.orm import registry, relationship

        mapper_registry = registry()


        @mapper_registry.mapped
        @dataclass
        class SimpleContainers:
            __tablename__ = 'simple_containers'
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})

            simple_items: List[SimpleItems] = field(default_factory=list, \
metadata={'sa': relationship('SimpleItems', back_populates='container')})


        @mapper_registry.mapped
        @dataclass
        class SimpleItems:
            __tablename__ = 'simple_items'
            __table_args__ = (
                ForeignKeyConstraint(['container_id'], ['simple_containers.id'], \
name='foreignkeytest'),
            )
            __sa_dataclass_metadata_key__ = 'sa'

            id: int = field(init=False, metadata={'sa': Column(Integer, \
primary_key=True)})
            container_id: Optional[int] = field(default=None, metadata={'sa': \
Column(Integer)})

            container: Optional[SimpleContainers] = field(default=None, \
metadata={'sa': relationship('SimpleContainers', back_populates='simple_items')})
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
        from __future__ import annotations

        from dataclasses import dataclass, field

        from sqlalchemy import Column
        from sqlalchemy.dialects.postgresql import UUID
        from sqlalchemy.orm import registry

        mapper_registry = registry()


        @mapper_registry.mapped
        @dataclass
        class Simple:
            __tablename__ = 'simple'
            __sa_dataclass_metadata_key__ = 'sa'

            id: str = field(init=False, metadata={'sa': \
Column(UUID, primary_key=True)})
        """,
    )
