from __future__ import annotations

from textwrap import dedent

import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy import Enum as SAEnum
from sqlalchemy import TypeDecorator
from sqlalchemy.dialects import mysql, postgresql, registry
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
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
from sqlalchemy.sql.sqltypes import DateTime, NullType
from sqlalchemy.types import ARRAY, INTEGER, NUMERIC, SMALLINT, VARCHAR, Text

from sqlacodegen.generators import CodeGenerator, TablesGenerator

from .conftest import validate_code


# This needs to be uppercased to trigger #315
class TIMESTAMP_DECORATOR(TypeDecorator[DateTime]):
    impl = DateTime


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return TablesGenerator(metadata, engine, options)


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_fancy_coltypes(generator: CodeGenerator) -> None:
    from pgvector.sqlalchemy.vector import VECTOR

    Table(
        "simple_items",
        generator.metadata,
        Column("enum", postgresql.ENUM("A", "B", name="blah", schema="someschema")),
        Column("bool", postgresql.BOOLEAN),
        Column("vector", VECTOR(3)),
        Column("number", NUMERIC(10, asdecimal=False)),
        Column("timestamp", TIMESTAMP_DECORATOR()),
        schema="someschema",
    )

    validate_code(
        generator.generate(),
        """\
        from tests.test_generator_tables import TIMESTAMP_DECORATOR
        import enum

        from pgvector.sqlalchemy.vector import VECTOR
        from sqlalchemy import Boolean, Column, Enum, MetaData, Numeric, Table

        metadata = MetaData()


        class Blah(str, enum.Enum):
            A = 'A'
            B = 'B'


        t_simple_items = Table(
            'simple_items', metadata,
            Column('enum', Enum(Blah, values_callable=lambda cls: [member.value for member in cls], name='blah', schema='someschema')),
            Column('bool', Boolean),
            Column('vector', VECTOR(3)),
            Column('number', Numeric(10, asdecimal=False)),
            Column('timestamp', TIMESTAMP_DECORATOR),
            schema='someschema'
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


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_json_default(generator: CodeGenerator) -> None:
    Table("simple_items", generator.metadata, Column("json", postgresql.JSON))

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, JSON, MetaData, Table

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('json', JSON)
        )
        """,
    )


def test_check_constraint_preserved(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("enum", VARCHAR(255)),
        CheckConstraint(r"simple_items.enum IN ('A', '\'B', 'C')"),
    )

    # Recreate generator with nosyntheticenums option to preserve constraints
    generator = TablesGenerator(
        generator.metadata, generator.bind, ["nosyntheticenums"]
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import CheckConstraint, Column, MetaData, String, Table

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('enum', String(255)),
            CheckConstraint("simple_items.enum IN ('A', '\\\\'B', 'C')")
        )
        """,
    )


def test_synthetic_enum_generation(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", VARCHAR(20)),
        CheckConstraint("simple_items.status IN ('active', 'inactive', 'pending')"),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import CheckConstraint, Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class SimpleItemsStatus(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        t_simple_items = Table(
            'simple_items', metadata,
            Column('id', Integer, primary_key=True),
            Column('status', Enum(SimpleItemsStatus, values_callable=lambda cls: [member.value for member in cls])),
            CheckConstraint("simple_items.status IN ('active', 'inactive', 'pending')")
        )
        """,
    )


def test_enum_shared_values(generator: CodeGenerator) -> None:
    from sqlalchemy import Enum as SAEnum

    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", SAEnum("active", "inactive", "pending", name="status_enum")),
    )
    Table(
        "accounts",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("status", SAEnum("active", "inactive", "pending", name="status_enum")),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class StatusEnum(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'
            PENDING = 'pending'


        t_accounts = Table(
            'accounts', metadata,
            Column('id', Integer, primary_key=True),
            Column('status', Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum'))
        )

        t_users = Table(
            'users', metadata,
            Column('id', Integer, primary_key=True),
            Column('status', Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum'))
        )
        """,
    )


def test_array_enum_named(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("roles", ARRAY(SAEnum("admin", "user", "moderator", name="role_enum"))),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import ARRAY, Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class RoleEnum(str, enum.Enum):
            ADMIN = 'admin'
            USER = 'user'
            MODERATOR = 'moderator'


        t_users = Table(
            'users', metadata,
            Column('id', Integer, primary_key=True),
            Column('roles', ARRAY(Enum(RoleEnum, values_callable=lambda cls: [member.value for member in cls], name='role_enum')))
        )
        """,
    )


def test_array_enum_shared(generator: CodeGenerator) -> None:
    Table(
        "users",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("roles", ARRAY(SAEnum("admin", "user", name="role_enum"))),
    )
    Table(
        "groups",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column("allowed_roles", ARRAY(SAEnum("admin", "user", name="role_enum"))),
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import ARRAY, Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class RoleEnum(str, enum.Enum):
            ADMIN = 'admin'
            USER = 'user'


        t_groups = Table(
            'groups', metadata,
            Column('id', Integer, primary_key=True),
            Column('allowed_roles', ARRAY(Enum(RoleEnum, values_callable=lambda cls: [member.value for member in cls], name='role_enum')))
        )

        t_users = Table(
            'users', metadata,
            Column('id', Integer, primary_key=True),
            Column('roles', ARRAY(Enum(RoleEnum, values_callable=lambda cls: [member.value for member in cls], name='role_enum')))
        )
        """,
    )


def test_enum_named_with_schema(generator: CodeGenerator) -> None:
    Table(
        "my_table",
        generator.metadata,
        Column("id", INTEGER, primary_key=True),
        Column(
            "status",
            SAEnum("active", "inactive", name="status_enum", schema="custom_schema"),
        ),
        schema="custom_schema",
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class StatusEnum(str, enum.Enum):
            ACTIVE = 'active'
            INACTIVE = 'inactive'


        t_my_table = Table(
            'my_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('status', Enum(StatusEnum, values_callable=lambda cls: [member.value for member in cls], name='status_enum', schema='custom_schema')),
            schema='custom_schema'
        )
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
        ),
        schema="custom_schema",
    )

    validate_code(
        generator.generate(),
        """\
        import enum

        from sqlalchemy import ARRAY, Column, Enum, Integer, MetaData, Table

        metadata = MetaData()


        class TagEnum(str, enum.Enum):
            A = 'a'
            B = 'b'


        t_my_table = Table(
            'my_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('tags', ARRAY(Enum(TagEnum, values_callable=lambda cls: [member.value for member in cls], name='tag_enum', schema='custom_schema'))),
            schema='custom_schema'
        )
        """,
    )


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_domain_text(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column(
            "postal_code",
            postgresql.DOMAIN(
                "us_postal_code",
                Text,
                constraint_name="valid_us_postal_code",
                not_null=False,
                check=text("VALUE ~ '^\\d{5}$' OR VALUE ~ '^\\d{5}-\\d{4}$'"),
            ),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, MetaData, Table, Text, text
        from sqlalchemy.dialects.postgresql import DOMAIN

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('postal_code', DOMAIN('us_postal_code', Text(), \
constraint_name='valid_us_postal_code', not_null=False, \
check=text("VALUE ~ '^\\\\d{5}$' OR VALUE ~ '^\\\\d{5}-\\\\d{4}$'")), nullable=False)
        )
        """,
    )


@pytest.mark.parametrize("engine", ["postgresql"], indirect=["engine"])
def test_domain_int(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column(
            "n",
            postgresql.DOMAIN(
                "positive_int",
                INTEGER,
                constraint_name="positive",
                not_null=False,
                check=text("VALUE > 0"),
            ),
            nullable=False,
        ),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, INTEGER, MetaData, Table, text
        from sqlalchemy.dialects.postgresql import DOMAIN

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('n', DOMAIN('positive_int', INTEGER(), \
constraint_name='positive', not_null=False, \
check=text('VALUE > 0')), nullable=False)
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
        Column("double", mysql.DOUBLE(1, 2)),
        Column("set", mysql.SET("one", "two")),
    )

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, Integer, MetaData, String, Table
        from sqlalchemy.dialects.mysql import DOUBLE, SET

        metadata = MetaData()


        t_simple_items = Table(
            'simple_items', metadata,
            Column('id', Integer),
            Column('name', String(255)),
            Column('double', DOUBLE(1, 2)),
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


class MockStarRocksDialect(MySQLDialect_pymysql):
    name = "starrocks"
    construct_arguments = [
        (
            Column,
            {
                "is_agg_key": None,
                "agg_type": None,
                "IS_AGG_KEY": None,
                "AGG_TYPE": None,
            },
        ),
        (
            Table,
            {
                "primary_key": None,
                "aggregate_key": None,
                "unique_key": None,
                "duplicate_key": None,
                "engine": "OLAP",
                "partition_by": None,
                "order_by": None,
                "security": None,
                "properties": {},
                "ENGINE": "OLAP",
                "PARTITION_BY": None,
                "ORDER_BY": None,
                "SECURITY": None,
                "PROPERTIES": {},
            },
        ),
    ]


# Register StarRocksDialect
registry.register("starrocks", __name__, "MockStarRocksDialect")


class _PartitionInfo:
    def __init__(self, partition_by: str) -> None:
        self.partition_by = partition_by

    def __str__(self) -> str:
        return self.partition_by

    def __repr__(self) -> str:
        return repr(self.partition_by)


@pytest.mark.parametrize("generator", [["include_dialect_options"]], indirect=True)
def test_include_dialect_options_starrocks_tables(generator: CodeGenerator) -> None:
    Table(
        "t_starrocks",
        generator.metadata,
        Column("id", INTEGER, primary_key=True, starrocks_is_agg_key=True),
        starrocks_ENGINE="OLAP",
        starrocks_PARTITION_BY=_PartitionInfo("RANGE(id)"),
        starrocks_ORDER_BY="id, name",
        starrocks_PROPERTIES={"replication_num": "3", "storage_medium": "SSD"},
    ).info = {"table_kind": "TABLE"}

    validate_code(
        generator.generate(),
        """\
        from sqlalchemy import Column, Integer, MetaData, Table

        metadata = MetaData()


        t_t_starrocks = Table(
            't_starrocks', metadata,
            Column('id', Integer, primary_key=True, starrocks_is_agg_key=True),
            info={'table_kind': 'TABLE'},
            starrocks_ENGINE='OLAP',
            starrocks_ORDER_BY='id, name',
            starrocks_PARTITION_BY='RANGE(id)',
            starrocks_PROPERTIES={'replication_num': '3', 'storage_medium': 'SSD'}
        )
        """,
    )
