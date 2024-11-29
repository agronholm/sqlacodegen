from __future__ import annotations

from textwrap import dedent

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    Computed,
    ForeignKey,
    Identity,
    Index,
    MetaData,
    Table,
    UniqueConstraint,
)
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import INTEGER, NUMERIC, SMALLINT, VARCHAR, Text

from sqlacodegen.generators import CodeGenerator, TablesGenerator

from .conftest import validate_code


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return TablesGenerator(metadata, engine, options)


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_fancy_coltypes(generator: CodeGenerator) -> None:
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


def test_boolean_detection(generator: CodeGenerator) -> None:
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
def test_arrays(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("dp_array", postgresql.ARRAY(postgresql.DOUBLE_PRECISION(precision=53))),
        Column("int_array", postgresql.ARRAY(INTEGER)),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import ARRAY, Column, Double, Integer, MetaData, Table

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('dp_array', ARRAY(Double(precision=53))),
            Column('int_array', ARRAY(Integer()))
        )
        """,
    )


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_jsonb(generator: CodeGenerator) -> None:
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
def test_jsonb_default(generator: CodeGenerator) -> None:
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


def test_enum_detection(generator: CodeGenerator) -> None:
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
def test_column_adaptation(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", postgresql.BIGINT),
        Column("length", postgresql.DOUBLE_PRECISION),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import BigInteger, Column, Double, MetaData, Table

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('id', BigInteger),
            Column('length', Double)
        )
        """,
    )


@pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
def test_mysql_column_types(generator: CodeGenerator) -> None:
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


def test_constraints(generator: CodeGenerator) -> None:
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


def test_indexes(generator: CodeGenerator) -> None:
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
            Column('text', String, unique=True, index=True),
            Index('ix_empty'),
            Index('ix_text_number', 'text', 'number', unique=True)
        )
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
        from sqlalchemy import Column, Integer, MetaData, Table

        metadata = MetaData()


        t_simple = Table(
            'simple', metadata,
            Column('id', Integer, primary_key=True),
            comment="this is a 'comment'"
        )
        """,
    )


def test_table_name_identifiers(generator: CodeGenerator) -> None:
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
def test_option_noindexes(generator: CodeGenerator) -> None:
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
def test_option_noconstraints(generator: CodeGenerator) -> None:
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
def test_option_nocomments(generator: CodeGenerator) -> None:
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
    generator: CodeGenerator, persisted: bool | None, extra_args: str
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


def test_schema(generator: CodeGenerator) -> None:
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


def test_foreign_key_options(generator: CodeGenerator) -> None:
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
            Column('name', String, ForeignKey('simple_items.name', \
ondelete='CASCADE', onupdate='CASCADE', deferrable=True, initially='DEFERRED'))
        )
        """,
    )


def test_pk_default(generator: CodeGenerator) -> None:
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
            Column('id', Integer, primary_key=True, \
server_default=text('uuid_generate_v4()'))
        )
        """,
    )


@pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
def test_mysql_timestamp(generator: CodeGenerator) -> None:
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
def test_mysql_integer_display_width(generator: CodeGenerator) -> None:
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
def test_mysql_tinytext(generator: CodeGenerator) -> None:
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
def test_mysql_mediumtext(generator: CodeGenerator) -> None:
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
def test_mysql_longtext(generator: CodeGenerator) -> None:
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


def test_schema_boolean(generator: CodeGenerator) -> None:
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


def test_server_default_multiline(generator: CodeGenerator) -> None:
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


def test_server_default_colon(generator: CodeGenerator) -> None:
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


def test_null_type(generator: CodeGenerator) -> None:
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


def test_identity_column(generator: CodeGenerator) -> None:
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


def test_multiline_column_comment(generator: CodeGenerator) -> None:
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


def test_multiline_table_comment(generator: CodeGenerator) -> None:
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
def test_postgresql_sequence_standard_name(generator: CodeGenerator) -> None:
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
def test_postgresql_sequence_nonstandard_name(generator: CodeGenerator) -> None:
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


@pytest.mark.parametrize(
    "schemaname, seqname",
    [
        pytest.param("myschema", "test_seq"),
        pytest.param("myschema", '"test_seq"'),
        pytest.param('"my.schema"', "test_seq"),
        pytest.param('"my.schema"', '"test_seq"'),
    ],
)
@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_postgresql_sequence_with_schema(
    generator: CodeGenerator, schemaname: str, seqname: str
) -> None:
    expected_schema = schemaname.strip('"')
    Table(
        "simple_items",
        generator.metadata,
        Column(
            "id",
            INTEGER,
            primary_key=True,
            server_default=text(f"nextval('{schemaname}.{seqname}'::regclass)"),
        ),
        schema=expected_schema,
    )

    validate_code(
        generator.generate(),
        f"""\
        from sqlalchemy import Column, Integer, MetaData, Sequence, Table

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('id', Integer, Sequence('test_seq', \
schema='{expected_schema}'), primary_key=True),
            schema='{expected_schema}'
        )
        """,
    )
