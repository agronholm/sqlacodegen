from __future__ import annotations

from textwrap import dedent

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy import PrimaryKeyConstraint, Sequence
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    Computed,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    MetaData,
    Table,
    UniqueConstraint,
)
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import INTEGER, NUMERIC, SMALLINT, VARCHAR, Text

from sqlacodegen.generators import (
    CodeGenerator,
    DataclassGenerator,
    DeclarativeGenerator,
    TablesGenerator,
)


def validate_code(generated_code: str, expected_code: str) -> None:
    expected_code = dedent(expected_code)
    assert generated_code == expected_code
    exec(generated_code, {})


@pytest.fixture
def engine(request: FixtureRequest) -> Engine:
    dialect = getattr(request, "param", None)
    if dialect == "postgresql":
        return create_engine("postgresql:///testdb")
    elif dialect == "mysql":
        return create_engine("mysql+mysqlconnector://testdb")
    else:
        return create_engine("sqlite:///:memory:")


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


class TestTablesGenerator:
    @pytest.fixture
    def generator(
        self, request: FixtureRequest, metadata: MetaData, engine: Engine
    ) -> CodeGenerator:
        options = getattr(request, "param", [])
        return TablesGenerator(metadata, engine, options)

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_fancy_coltypes(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("enum", postgresql.ENUM("A", "B", name="blah")),
            Column("bool", postgresql.BOOLEAN),
            Column("number", NUMERIC(10, asdecimal=False)),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Boolean, Column, Enum, MetaData, Numeric, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('enum', Enum('A', 'B', name='blah')),
                Column('bool', Boolean),
                Column('number', Numeric(10, asdecimal=False))
            )
            """,
        )

    def test_boolean_detection(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("bool1", INTEGER),
            Column("bool2", SMALLINT),
            Column("bool3", mysql.TINYINT),
            CheckConstraint("simple_items.bool1 IN (0, 1)"),
            CheckConstraint("simple_items.bool2 IN (0, 1)"),
            CheckConstraint("simple_items.bool3 IN (0, 1)"),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Boolean, Column, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('bool1', Boolean),
                Column('bool2', Boolean),
                Column('bool3', Boolean)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_arrays(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "dp_array", postgresql.ARRAY(postgresql.DOUBLE_PRECISION(precision=53))
            ),
            Column("int_array", postgresql.ARRAY(INTEGER)),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import ARRAY, Column, Float, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('dp_array', ARRAY(Float(precision=53))),
                Column('int_array', ARRAY(Integer()))
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_jsonb(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("jsonb", postgresql.JSONB(astext_type=Text(50))),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, MetaData, Table, Text
            from sqlalchemy.dialects.postgresql import JSONB

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('jsonb', JSONB(astext_type=Text(length=50)))
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_jsonb_default(self, generator: CodeGenerator) -> None:
        Table("simple_items", generator.metadata, Column("jsonb", postgresql.JSONB))

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, MetaData, Table
            from sqlalchemy.dialects.postgresql import JSONB

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('jsonb', JSONB)
            )
            """,
        )

    def test_enum_detection(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("enum", VARCHAR(255)),
            CheckConstraint(r"simple_items.enum IN ('A', '\'B', 'C')"),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Enum, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('enum', Enum('A', "\\\\'B", 'C'))
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_column_adaptation(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", postgresql.BIGINT),
            Column("length", postgresql.DOUBLE_PRECISION),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import BigInteger, Column, Float, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', BigInteger),
                Column('length', Float)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_column_types(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", mysql.INTEGER),
            Column("name", mysql.VARCHAR(255)),
            Column("set", mysql.SET("one", "two")),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, String, Table
            from sqlalchemy.dialects.mysql import SET

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer),
                Column('name', String(255)),
                Column('set', SET('one', 'two'))
            )
            """,
        )

    def test_constraints(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER),
            Column("number", INTEGER),
            CheckConstraint("number > 0"),
            UniqueConstraint("id", "number"),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table, \
UniqueConstraint

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer),
                Column('number', Integer),
                CheckConstraint('number > 0'),
                UniqueConstraint('id', 'number')
            )
            """,
        )

    def test_indexes(self, generator: CodeGenerator) -> None:
        simple_items = Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER),
            Column("number", INTEGER),
            Column("text", VARCHAR),
            Index("ix_empty"),
        )
        simple_items.indexes.add(Index("ix_number", simple_items.c.number))
        simple_items.indexes.add(
            Index(
                "ix_text_number",
                simple_items.c.text,
                simple_items.c.number,
                unique=True,
            )
        )
        simple_items.indexes.add(Index("ix_text", simple_items.c.text, unique=True))

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Index, Integer, MetaData, String, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer),
                Column('number', Integer, index=True),
                Column('text', String, unique=True),
                Index('ix_empty'),
                Index('ix_text_number', 'text', 'number', unique=True)
            )
            """,
        )

    def test_table_comment(self, generator: CodeGenerator) -> None:
        Table(
            "simple",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            comment="this is a 'comment'",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple = Table(
                'simple', metadata,
                Column('id', Integer, primary_key=True),
                comment="this is a 'comment'"
            )
            """,
        )

    def test_table_name_identifiers(self, generator: CodeGenerator) -> None:
        Table(
            "simple-items table",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items_table = Table(
                'simple-items table', metadata,
                Column('id', Integer, primary_key=True)
            )
            """,
        )

    @pytest.mark.parametrize("generator", [["noindexes"]], indirect=True)
    def test_option_noindexes(self, generator: CodeGenerator) -> None:
        simple_items = Table(
            "simple_items",
            generator.metadata,
            Column("number", INTEGER),
            CheckConstraint("number > 2"),
        )
        simple_items.indexes.add(Index("idx_number", simple_items.c.number))

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('number', Integer),
                CheckConstraint('number > 2')
            )
            """,
        )

    @pytest.mark.parametrize("generator", [["noconstraints"]], indirect=True)
    def test_option_noconstraints(self, generator: CodeGenerator) -> None:
        simple_items = Table(
            "simple_items",
            generator.metadata,
            Column("number", INTEGER),
            CheckConstraint("number > 2"),
        )
        simple_items.indexes.add(Index("ix_number", simple_items.c.number))

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('number', Integer, index=True)
            )
            """,
        )

    @pytest.mark.parametrize("generator", [["nocomments"]], indirect=True)
    def test_option_nocomments(self, generator: CodeGenerator) -> None:
        Table(
            "simple",
            generator.metadata,
            Column("id", INTEGER, primary_key=True, comment="pk column comment"),
            comment="this is a 'comment'",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple = Table(
                'simple', metadata,
                Column('id', Integer, primary_key=True)
            )
            """,
        )

    @pytest.mark.parametrize(
        "persisted, extra_args",
        [(None, ""), (False, ", persisted=False"), (True, ", persisted=True")],
    )
    def test_computed_column(
        self, generator: CodeGenerator, persisted: bool | None, extra_args: str
    ) -> None:
        Table(
            "computed",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("computed", INTEGER, Computed("1 + 2", persisted=persisted)),
        )

        validate_code(
            generator.generate(),
            f"""\
            from sqlalchemy import Column, Computed, Integer, MetaData, Table

            metadata = MetaData()


            t_computed = Table(
                'computed', metadata,
                Column('id', Integer, primary_key=True),
                Column('computed', Integer, Computed('1 + 2'{extra_args}))
            )
            """,
        )

    def test_schema(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("name", VARCHAR),
            schema="testschema",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, MetaData, String, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('name', String),
                schema='testschema'
            )
            """,
        )

    def test_foreign_key_options(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "name",
                VARCHAR,
                ForeignKey(
                    "simple_items.name",
                    ondelete="CASCADE",
                    onupdate="CASCADE",
                    deferrable=True,
                    initially="DEFERRED",
                ),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, ForeignKey, MetaData, String, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('name', String, ForeignKey('simple_items.name', ondelete='CASCADE', \
onupdate='CASCADE', deferrable=True, initially='DEFERRED'))
            )
            """,
        )

    def test_pk_default(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=text("uuid_generate_v4()"),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table, text

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True, server_default=text('uuid_generate_v4()'))
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_timestamp(self, generator: CodeGenerator) -> None:
        Table(
            "simple",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("timestamp", mysql.TIMESTAMP),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, TIMESTAMP, Table

            metadata = MetaData()


            t_simple = Table(
                'simple', metadata,
                Column('id', Integer, primary_key=True),
                Column('timestamp', TIMESTAMP)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_integer_display_width(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("number", mysql.INTEGER(11)),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table
            from sqlalchemy.dialects.mysql import INTEGER

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True),
                Column('number', INTEGER(11))
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_tinytext(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("my_tinytext", mysql.TINYTEXT),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table
            from sqlalchemy.dialects.mysql import TINYTEXT

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True),
                Column('my_tinytext', TINYTEXT)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_mediumtext(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("my_mediumtext", mysql.MEDIUMTEXT),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table
            from sqlalchemy.dialects.mysql import MEDIUMTEXT

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True),
                Column('my_mediumtext', MEDIUMTEXT)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
    def test_mysql_longtext(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("my_longtext", mysql.LONGTEXT),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table
            from sqlalchemy.dialects.mysql import LONGTEXT

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True),
                Column('my_longtext', LONGTEXT)
            )
            """,
        )

    def test_schema_boolean(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("bool1", INTEGER),
            CheckConstraint("testschema.simple_items.bool1 IN (0, 1)"),
            schema="testschema",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Boolean, Column, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('bool1', Boolean),
                schema='testschema'
            )
            """,
        )

    def test_server_default_multiline(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=text(
                    dedent(
                        """\
                /*Comment*/
                /*Next line*/
                something()"""
                    )
                ),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table, text

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True, server_default=\
text('/*Comment*/\\n/*Next line*/\\nsomething()'))
            )
            """,
        )

    def test_server_default_colon(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("problem", VARCHAR, server_default=text("':001'")),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, MetaData, String, Table, text

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('problem', String, server_default=text("':001'"))
            )
            """,
        )

    def test_null_type(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("problem", NullType),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, MetaData, Table
            from sqlalchemy.sql.sqltypes import NullType

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('problem', NullType)
            )
            """,
        )

    def test_identity_column(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=Identity(start=1, increment=2),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Identity, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, Identity(start=1, increment=2), primary_key=True)
            )
            """,
        )

    def test_multiline_column_comment(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, comment="This\nis a multi-line\ncomment"),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, comment='This\\nis a multi-line\\ncomment')
            )
            """,
        )

    def test_multiline_table_comment(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER),
            comment="This\nis a multi-line\ncomment",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer),
                comment='This\\nis a multi-line\\ncomment'
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_postgresql_sequence_standard_name(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=text("nextval('simple_items_id_seq'::regclass)"),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, primary_key=True)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_postgresql_sequence_nonstandard_name(
        self, generator: CodeGenerator
    ) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=text("nextval('test_seq'::regclass)"),
            ),
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Sequence, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, Sequence('test_seq'), primary_key=True)
            )
            """,
        )

    @pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
    def test_postgresql_sequence_with_schema(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column(
                "id",
                INTEGER,
                primary_key=True,
                server_default=text("nextval('\"myschema\".test_seq'::regclass)"),
            ),
            schema="myschema",
        )

        validate_code(
            generator.generate(),
            """\
            from sqlalchemy import Column, Integer, MetaData, Sequence, Table

            metadata = MetaData()


            t_simple_items = Table(
                'simple_items', metadata,
                Column('id', Integer, Sequence('test_seq', schema='myschema'), \
primary_key=True),
                schema='myschema'
            )
            """,
        )


class TestDeclarativeGenerator:
    @pytest.fixture
    def generator(
        self, request: FixtureRequest, metadata: MetaData, engine: Engine
    ) -> CodeGenerator:
        options = getattr(request, "param", [])
        return DeclarativeGenerator(metadata, engine, options)

    def test_indexes(self, generator: CodeGenerator) -> None:
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
                    Index('idx_text_number', 'text', 'number'),
                )

                id = Column(Integer, primary_key=True)
                number = Column(Integer, index=True)
                text = Column(String, unique=True)
            """,
        )

    def test_constraints(self, generator: CodeGenerator) -> None:
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

    def test_onetomany(self, generator: CodeGenerator) -> None:
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

                container = relationship('SimpleContainers', back_populates='simple_items')
            """,
        )

    def test_onetomany_selfref(self, generator: CodeGenerator) -> None:
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
                parent_item_reverse = relationship('SimpleItems', remote_side=[parent_item_id], \
back_populates='parent_item')
            """,
        )

    def test_onetomany_selfref_multi(self, generator: CodeGenerator) -> None:
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
                parent_item_reverse = relationship('SimpleItems', remote_side=[parent_item_id], \
foreign_keys=[parent_item_id], back_populates='parent_item')
                top_item = relationship('SimpleItems', remote_side=[id], \
foreign_keys=[top_item_id], back_populates='top_item_reverse')
                top_item_reverse = relationship('SimpleItems', remote_side=[top_item_id], \
foreign_keys=[top_item_id], back_populates='top_item')
            """,
        )

    def test_onetomany_composite(self, generator: CodeGenerator) -> None:
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

    simple_items = relationship('SimpleItems', back_populates='simple_containers')


class SimpleItems(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', \
'simple_containers.id2'], ondelete='CASCADE', onupdate='CASCADE'),
    )

    id = Column(Integer, primary_key=True)
    container_id1 = Column(Integer)
    container_id2 = Column(Integer)

    simple_containers = relationship('SimpleContainers', back_populates='simple_items')
            """,
        )

    def test_onetomany_multiref(self, generator: CodeGenerator) -> None:
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

    simple_items = relationship('SimpleItems', foreign_keys='[SimpleItems.parent_container_id]', \
back_populates='parent_container')
    simple_items_ = relationship('SimpleItems', foreign_keys='[SimpleItems.top_container_id]', \
back_populates='top_container')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_container_id = Column(ForeignKey('simple_containers.id'))
    top_container_id = Column(ForeignKey('simple_containers.id'))

    parent_container = relationship('SimpleContainers', foreign_keys=[parent_container_id], \
back_populates='simple_items')
    top_container = relationship('SimpleContainers', foreign_keys=[top_container_id], \
back_populates='simple_items_')
            """,
        )

    def test_onetoone(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("other_item_id", INTEGER),
            ForeignKeyConstraint(["other_item_id"], ["other_items.id"]),
            UniqueConstraint("other_item_id"),
        )
        Table(
            "other_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )

        validate_code(
            generator.generate(),
            """\
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class OtherItems(Base):
    __tablename__ = 'other_items'

    id = Column(Integer, primary_key=True)

    simple_items = relationship('SimpleItems', uselist=False, back_populates='other_item')


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    other_item_id = Column(ForeignKey('other_items.id'), unique=True)

    other_item = relationship('OtherItems', back_populates='simple_items')
            """,
        )

    def test_onetomany_noinflect(self, generator: CodeGenerator) -> None:
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

    def test_onetomany_conflicting_column(self, generator: CodeGenerator) -> None:
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

    def test_onetomany_conflicting_relationship(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("relationship_id", INTEGER),
            ForeignKeyConstraint(["relationship_id"], ["relationship.id"]),
        )
        Table(
            "relationship", generator.metadata, Column("id", INTEGER, primary_key=True)
        )

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
    def test_manytoone_nobidi(self, generator: CodeGenerator) -> None:
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

    def test_manytomany(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )
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
    def test_manytomany_nobidi(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )
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

    def test_manytomany_selfref(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )
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
primaryjoin='SimpleItems.id == t_child_items.c.child_id', \
secondaryjoin='SimpleItems.id == t_child_items.c.parent_id', back_populates='child')
    child = relationship('SimpleItems', secondary='otherschema.child_items', \
primaryjoin='SimpleItems.id == t_child_items.c.parent_id', \
secondaryjoin='SimpleItems.id == t_child_items.c.child_id', back_populates='parent')


t_child_items = Table(
    'child_items', metadata,
    Column('parent_id', ForeignKey('simple_items.id')),
    Column('child_id', ForeignKey('simple_items.id')),
    schema='otherschema'
)
            """,
        )

    def test_manytomany_composite(self, generator: CodeGenerator) -> None:
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

    simple_containers = relationship('SimpleContainers', secondary='container_items', \
back_populates='simple_items')


t_container_items = Table(
    'container_items', metadata,
    Column('item_id1', Integer),
    Column('item_id2', Integer),
    Column('container_id1', Integer),
    Column('container_id2', Integer),
    ForeignKeyConstraint(['container_id1', 'container_id2'], \
['simple_containers.id1', 'simple_containers.id2']),
    ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', 'simple_items.id2'])
)
            """,
        )

    def test_joined_inheritance(self, generator: CodeGenerator) -> None:
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

    super_item_id = Column(ForeignKey('simple_super_items.id'), primary_key=True)
    data2 = Column(Integer)


class SimpleSubItems(SimpleItems):
    __tablename__ = 'simple_sub_items'

    simple_items_id = Column(ForeignKey('simple_items.super_item_id'), primary_key=True)
    data3 = Column(Integer)
            """,
        )

    def test_joined_inheritance_same_table_name(self, generator: CodeGenerator) -> None:
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
    def test_use_inflect(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )

        validate_code(
            generator.generate(),
            """\
from sqlalchemy import Column, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
            """,
        )

    def test_table_kwargs(self, generator: CodeGenerator) -> None:
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

    def test_table_args_kwargs(self, generator: CodeGenerator) -> None:
        simple_items = Table(
            "simple_items",
            generator.metadata,
            Column("id", INTEGER, primary_key=True),
            Column("name", VARCHAR),
            schema="testschema",
        )
        simple_items.indexes.add(
            Index("testidx", simple_items.c.id, simple_items.c.name)
        )

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

    def test_foreign_key_schema(self, generator: CodeGenerator) -> None:
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

    def test_invalid_attribute_names(self, generator: CodeGenerator) -> None:
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

    def test_pascal(self, generator: CodeGenerator) -> None:
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

    def test_underscore(self, generator: CodeGenerator) -> None:
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

    def test_pascal_underscore(self, generator: CodeGenerator) -> None:
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

    def test_pascal_multiple_underscore(self, generator: CodeGenerator) -> None:
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
    def test_column_comment(self, generator: CodeGenerator, nocomments: bool) -> None:
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

    def test_table_comment(self, generator: CodeGenerator) -> None:
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

    def test_metadata_column(self, generator: CodeGenerator) -> None:
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

    def test_invalid_variable_name_from_column(self, generator: CodeGenerator) -> None:
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

    def test_only_tables(self, generator: CodeGenerator) -> None:
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

    def test_named_constraints(self, generator: CodeGenerator) -> None:
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
            from sqlalchemy import CheckConstraint, Column, Integer, PrimaryKeyConstraint, \
String, UniqueConstraint
            from sqlalchemy.orm import declarative_base

            Base = declarative_base()


            class Simple(Base):
                __tablename__ = 'simple'
                __table_args__ = (
                    CheckConstraint('id > 0', name='checktest'),
                    PrimaryKeyConstraint('id', name='primarytest'),
                    UniqueConstraint('text', name='uniquetest')
                )

                id = Column(Integer)
                text = Column(String)
            """,
        )

    def test_named_foreign_key_constraints(self, generator: CodeGenerator) -> None:
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
                    ForeignKeyConstraint(['container_id'], ['simple_containers.id'], name='foreignkeytest'),
                )

                id = Column(Integer, primary_key=True)
                container_id = Column(Integer)

                container = relationship('SimpleContainers', back_populates='simple_items')
            """,
        )

    # @pytest.mark.xfail(strict=True)
    def test_colname_import_conflict(self, generator: CodeGenerator) -> None:
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


class TestDataclassGenerator:
    @pytest.fixture
    def generator(
        self, request: FixtureRequest, metadata: MetaData, engine: Engine
    ) -> CodeGenerator:
        options = getattr(request, "param", [])
        return DataclassGenerator(metadata, engine, options)

    def test_basic_class(self, generator: CodeGenerator) -> None:
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

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})
                name: Optional[str] = field(default=None, metadata={'sa': Column(String(20))})
            """,
        )

    def test_mandatory_field_last(self, generator: CodeGenerator) -> None:
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

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})
                age: int = field(metadata={'sa': Column(Integer, nullable=False)})
                name: Optional[str] = field(default=None, metadata={'sa': Column(String(20), server_default=text('foo'))})
            """,
        )

    def test_onetomany_optional(self, generator: CodeGenerator) -> None:
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

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})

                simple_items: List[SimpleItems] = field(default_factory=list, \
metadata={'sa': relationship('SimpleItems', back_populates='container')})


            @mapper_registry.mapped
            @dataclass
            class SimpleItems:
                __tablename__ = 'simple_items'
                __sa_dataclass_metadata_key__ = 'sa'

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})
                container_id: Optional[int] = field(default=None, \
metadata={'sa': Column(ForeignKey('simple_containers.id'))})

                container: Optional[SimpleContainers] = field(default=None, \
metadata={'sa': relationship('SimpleContainers', back_populates='simple_items')})
            """,
        )

    def test_manytomany(self, generator: CodeGenerator) -> None:
        Table(
            "simple_items", generator.metadata, Column("id", INTEGER, primary_key=True)
        )
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

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})

                item: List[SimpleItems] = field(default_factory=list, metadata=\
{'sa': relationship('SimpleItems', secondary='container_items', back_populates='container')})


            @mapper_registry.mapped
            @dataclass
            class SimpleItems:
                __tablename__ = 'simple_items'
                __sa_dataclass_metadata_key__ = 'sa'

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})

                container: List[SimpleContainers] = field(default_factory=list, metadata=\
{'sa': relationship('SimpleContainers', secondary='container_items', back_populates='item')})


            t_container_items = Table(
                'container_items', metadata,
                Column('item_id', ForeignKey('simple_items.id')),
                Column('container_id', ForeignKey('simple_containers.id'))
            )
            """,
        )

    def test_named_foreign_key_constraints(self, generator: CodeGenerator) -> None:
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

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})

                simple_items: List[SimpleItems] = field(default_factory=list, metadata={'sa': relationship('SimpleItems', back_populates='container')})


            @mapper_registry.mapped
            @dataclass
            class SimpleItems:
                __tablename__ = 'simple_items'
                __table_args__ = (
                    ForeignKeyConstraint(['container_id'], ['simple_containers.id'], name='foreignkeytest'),
                )
                __sa_dataclass_metadata_key__ = 'sa'

                id: int = field(init=False, metadata={'sa': Column(Integer, primary_key=True)})
                container_id: Optional[int] = field(default=None, metadata={'sa': Column(Integer)})

                container: Optional[SimpleContainers] = field(default=None, metadata={'sa': relationship('SimpleContainers', back_populates='simple_items')})
            """,
        )

    def test_uuid_type_annotation(self, generator: CodeGenerator) -> None:
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

                id: str = field(init=False, metadata={'sa': Column(UUID, primary_key=True)})
            """,
        )
