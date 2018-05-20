from __future__ import unicode_literals, division, print_function, absolute_import

import re
import sys
from io import StringIO

import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import (
    MetaData, Table, Column, CheckConstraint, UniqueConstraint, Index, ForeignKey,
    ForeignKeyConstraint)
from sqlalchemy.sql.expression import text
from sqlalchemy.types import INTEGER, SMALLINT, VARCHAR, NUMERIC

from sqlacodegen.codegen import CodeGenerator

if sys.version_info < (3,):
    unicode_re = re.compile(r"u(['\"])(.*?)(?<!\\)\1")

    def remove_unicode_prefixes(text):
        return unicode_re.sub(r"\1\2\1", text)
else:
    def remove_unicode_prefixes(text):
        return text


@pytest.fixture
def metadata(request):
    dialect = getattr(request, 'param', None)
    if dialect == 'postgresql':
        engine = create_engine('postgresql:///testdb')
    elif dialect == 'mysql':
        engine = create_engine('mysql+mysqlconnector://testdb')
    else:
        engine = create_engine('sqlite:///:memory:')

    return MetaData(engine)


def generate_code(metadata, **kwargs):
    codegen = CodeGenerator(metadata, **kwargs)
    sio = StringIO()
    codegen.render(sio)
    return remove_unicode_prefixes(sio.getvalue())


@pytest.mark.parametrize('metadata', ['postgresql'], indirect=['metadata'])
def test_fancy_coltypes(metadata):
    Table(
        'simple_items', metadata,
        Column('enum', postgresql.ENUM('A', 'B', name='blah')),
        Column('bool', postgresql.BOOLEAN),
        Column('number', NUMERIC(10, asdecimal=False)),
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Boolean, Column, Enum, MetaData, Numeric, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('enum', Enum('A', 'B', name='blah')),
    Column('bool', Boolean),
    Column('number', Numeric(10, asdecimal=False))
)
"""


def test_boolean_detection(metadata):
    Table(
        'simple_items', metadata,
        Column('bool1', INTEGER),
        Column('bool2', SMALLINT),
        Column('bool3', mysql.TINYINT),
        CheckConstraint('simple_items.bool1 IN (0, 1)'),
        CheckConstraint('simple_items.bool2 IN (0, 1)'),
        CheckConstraint('simple_items.bool3 IN (0, 1)')
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Boolean, Column, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('bool1', Boolean),
    Column('bool2', Boolean),
    Column('bool3', Boolean)
)
"""


@pytest.mark.parametrize('metadata', ['postgresql'], indirect=['metadata'])
def test_arrays(metadata):
    Table(
        'simple_items', metadata,
        Column('dp_array', postgresql.ARRAY(postgresql.DOUBLE_PRECISION(precision=53))),
        Column('int_array', postgresql.ARRAY(INTEGER))
    )

    if sqlalchemy.__version__ < '1.1':
        assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Float, Integer, MetaData, Table
from sqlalchemy.dialects.postgresql.base import ARRAY

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('dp_array', ARRAY(Float(precision=53))),
    Column('int_array', ARRAY(Integer()))
)
"""
    else:
        assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import ARRAY, Column, Float, Integer, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('dp_array', ARRAY(Float(precision=53))),
    Column('int_array', ARRAY(Integer()))
)
"""


def test_enum_detection(metadata):
    Table(
        'simple_items', metadata,
        Column('enum', VARCHAR(255)),
        CheckConstraint(r"simple_items.enum IN ('A', '\'B', 'C')")
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Enum, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('enum', Enum('A', "\\\\'B", 'C'))
)
"""


@pytest.mark.parametrize('metadata', ['postgresql'], indirect=['metadata'])
def test_column_adaptation(metadata):
    Table(
        'simple_items', metadata,
        Column('id', postgresql.BIGINT),
        Column('length', postgresql.DOUBLE_PRECISION)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import BigInteger, Column, Float, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', BigInteger),
    Column('length', Float)
)
"""


@pytest.mark.parametrize('metadata', ['mysql'], indirect=['metadata'])
def test_mysql_column_types(metadata):
    Table(
        'simple_items', metadata,
        Column('id', mysql.INTEGER),
        Column('name', mysql.VARCHAR(255))
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer),
    Column('name', String(255))
)
"""


def test_constraints_table(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER),
        Column('number', INTEGER),
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table, UniqueConstraint

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer),
    Column('number', Integer),
    CheckConstraint('number > 0'),
    UniqueConstraint('id', 'number')
)
"""


def test_constraints_class(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('number', INTEGER),
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import CheckConstraint, Column, Integer, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
"""


def test_noindexes_table(metadata):
    simple_items = Table(
        'simple_items', metadata,
        Column('number', INTEGER),
        CheckConstraint('number > 2')
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))

    assert generate_code(metadata, noindexes=True) == """\
# coding: utf-8
from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer),
    CheckConstraint('number > 2')
)
"""


def test_noconstraints_table(metadata):
    simple_items = Table(
        'simple_items', metadata,
        Column('number', INTEGER),
        CheckConstraint('number > 2')
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))

    assert generate_code(metadata, noconstraints=True) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer, index=True)
)
"""


def test_indexes_table(metadata):
    simple_items = Table(
        'simple_items', metadata,
        Column('id', INTEGER),
        Column('number', INTEGER),
        Column('text', VARCHAR)
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))
    simple_items.indexes.add(Index('idx_text_number', simple_items.c.text,
                                   simple_items.c.number, unique=True))
    simple_items.indexes.add(Index('idx_text', simple_items.c.text, unique=True))

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Index, Integer, MetaData, String, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer),
    Column('number', Integer, index=True),
    Column('text', String, unique=True),
    Index('idx_text_number', 'text', 'number', unique=True)
)
"""


def test_indexes_class(metadata):
    simple_items = Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('number', INTEGER),
        Column('text', VARCHAR)
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))
    simple_items.indexes.add(Index('idx_text_number', simple_items.c.text,
                                   simple_items.c.number))
    simple_items.indexes.add(Index('idx_text', simple_items.c.text, unique=True))

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        Index('idx_text_number', 'text', 'number'),
    )

    id = Column(Integer, primary_key=True)
    number = Column(Integer, index=True)
    text = Column(String, unique=True)
"""


def test_onetomany(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('container_id', INTEGER),
        ForeignKeyConstraint(['container_id'], ['simple_containers.id']),
    )
    Table(
        'simple_containers', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    container_id = Column(ForeignKey('simple_containers.id'))

    container = relationship('SimpleContainer')
"""


def test_onetomany_selfref(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('parent_item_id', INTEGER),
        ForeignKeyConstraint(['parent_item_id'], ['simple_items.id'])
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_item_id = Column(ForeignKey('simple_items.id'))

    parent_item = relationship('SimpleItem', remote_side=[id])
"""


def test_onetomany_selfref_multi(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('parent_item_id', INTEGER),
        Column('top_item_id', INTEGER),
        ForeignKeyConstraint(['parent_item_id'], ['simple_items.id']),
        ForeignKeyConstraint(['top_item_id'], ['simple_items.id'])
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_item_id = Column(ForeignKey('simple_items.id'))
    top_item_id = Column(ForeignKey('simple_items.id'))

    parent_item = relationship('SimpleItem', remote_side=[id], \
primaryjoin='SimpleItem.parent_item_id == SimpleItem.id')
    top_item = relationship('SimpleItem', remote_side=[id], \
primaryjoin='SimpleItem.top_item_id == SimpleItem.id')
"""


def test_onetomany_composite(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('container_id1', INTEGER),
        Column('container_id2', INTEGER),
        ForeignKeyConstraint(['container_id1', 'container_id2'],
                             ['simple_containers.id1', 'simple_containers.id2'],
                             ondelete='CASCADE', onupdate='CASCADE')
    )
    Table(
        'simple_containers', metadata,
        Column('id1', INTEGER, primary_key=True),
        Column('id2', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKeyConstraint, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)


class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', \
'simple_containers.id2'], ondelete='CASCADE', onupdate='CASCADE'),
    )

    id = Column(Integer, primary_key=True)
    container_id1 = Column(Integer)
    container_id2 = Column(Integer)

    simple_container = relationship('SimpleContainer')
"""


def test_onetomany_multiref(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('parent_container_id', INTEGER),
        Column('top_container_id', INTEGER),
        ForeignKeyConstraint(['parent_container_id'], ['simple_containers.id']),
        ForeignKeyConstraint(['top_container_id'], ['simple_containers.id'])
    )
    Table(
        'simple_containers', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_container_id = Column(ForeignKey('simple_containers.id'))
    top_container_id = Column(ForeignKey('simple_containers.id'))

    parent_container = relationship('SimpleContainer', \
primaryjoin='SimpleItem.parent_container_id == SimpleContainer.id')
    top_container = relationship('SimpleContainer', \
primaryjoin='SimpleItem.top_container_id == SimpleContainer.id')
"""


def test_onetoone(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('other_item_id', INTEGER),
        ForeignKeyConstraint(['other_item_id'], ['other_items.id']),
        UniqueConstraint('other_item_id')
    )
    Table(
        'other_items', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class OtherItem(Base):
    __tablename__ = 'other_items'

    id = Column(Integer, primary_key=True)


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    other_item_id = Column(ForeignKey('other_items.id'), unique=True)

    other_item = relationship('OtherItem', uselist=False)
"""


def test_onetomany_noinflect(metadata):
    Table(
        'oglkrogk', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('fehwiuhfiwID', INTEGER),
        ForeignKeyConstraint(['fehwiuhfiwID'], ['fehwiuhfiw.id']),
    )
    Table(
        'fehwiuhfiw', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Fehwiuhfiw(Base):
    __tablename__ = 'fehwiuhfiw'

    id = Column(Integer, primary_key=True)


class Oglkrogk(Base):
    __tablename__ = 'oglkrogk'

    id = Column(Integer, primary_key=True)
    fehwiuhfiwID = Column(ForeignKey('fehwiuhfiw.id'))

    fehwiuhfiw = relationship('Fehwiuhfiw')
"""


@pytest.mark.skipif(sqlalchemy.__version__ < '1.0', reason='SQLA < 1.0 gives inconsistent results')
def test_manytomany(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True)
    )
    Table(
        'simple_containers', metadata,
        Column('id', INTEGER, primary_key=True)
    )
    Table(
        'container_items', metadata,
        Column('item_id', INTEGER),
        Column('container_id', INTEGER),
        ForeignKeyConstraint(['item_id'], ['simple_items.id']),
        ForeignKeyConstraint(['container_id'], ['simple_containers.id'])
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)

    items = relationship('SimpleItem', secondary='container_items')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)


t_container_items = Table(
    'container_items', metadata,
    Column('item_id', ForeignKey('simple_items.id')),
    Column('container_id', ForeignKey('simple_containers.id'))
)
"""


def test_manytomany_selfref(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True)
    )
    Table(
        'child_items', metadata,
        Column('parent_id', INTEGER),
        Column('child_id', INTEGER),
        ForeignKeyConstraint(['parent_id'], ['simple_items.id']),
        ForeignKeyConstraint(['child_id'], ['simple_items.id']),
        schema='otherschema'
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)

    parents = relationship(
        'SimpleItem',
        secondary='otherschema.child_items',
        primaryjoin='SimpleItem.id == child_items.c.child_id',
        secondaryjoin='SimpleItem.id == child_items.c.parent_id'
    )


t_child_items = Table(
    'child_items', metadata,
    Column('parent_id', ForeignKey('simple_items.id')),
    Column('child_id', ForeignKey('simple_items.id')),
    schema='otherschema'
)
"""


@pytest.mark.skipif(sqlalchemy.__version__ < '1.0', reason='SQLA < 1.0 gives inconsistent results')
def test_manytomany_composite(metadata):
    Table(
        'simple_items', metadata,
        Column('id1', INTEGER, primary_key=True),
        Column('id2', INTEGER, primary_key=True)
    )
    Table(
        'simple_containers', metadata,
        Column('id1', INTEGER, primary_key=True),
        Column('id2', INTEGER, primary_key=True)
    )
    Table(
        'container_items', metadata,
        Column('item_id1', INTEGER),
        Column('item_id2', INTEGER),
        Column('container_id1', INTEGER),
        Column('container_id2', INTEGER),
        ForeignKeyConstraint(['item_id1', 'item_id2'],
                             ['simple_items.id1', 'simple_items.id2']),
        ForeignKeyConstraint(['container_id1', 'container_id2'],
                             ['simple_containers.id1', 'simple_containers.id2'])
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKeyConstraint, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)

    simple_items = relationship('SimpleItem', secondary='container_items')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)


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
"""


def test_joined_inheritance(metadata):
    Table(
        'simple_sub_items', metadata,
        Column('simple_items_id', INTEGER, primary_key=True),
        Column('data3', INTEGER),
        ForeignKeyConstraint(['simple_items_id'], ['simple_items.super_item_id'])
    )
    Table(
        'simple_super_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('data1', INTEGER)
    )
    Table(
        'simple_items', metadata,
        Column('super_item_id', INTEGER, primary_key=True),
        Column('data2', INTEGER),
        ForeignKeyConstraint(['super_item_id'], ['simple_super_items.id'])
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleSuperItem(Base):
    __tablename__ = 'simple_super_items'

    id = Column(Integer, primary_key=True)
    data1 = Column(Integer)


class SimpleItem(SimpleSuperItem):
    __tablename__ = 'simple_items'

    super_item_id = Column(ForeignKey('simple_super_items.id'), primary_key=True)
    data2 = Column(Integer)


class SimpleSubItem(SimpleItem):
    __tablename__ = 'simple_sub_items'

    simple_items_id = Column(ForeignKey('simple_items.super_item_id'), primary_key=True)
    data3 = Column(Integer)
"""


def test_no_inflect(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata, noinflect=True) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
"""


def test_no_classes(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata, noclasses=True) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer, primary_key=True)
)
"""


def test_table_kwargs(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        schema='testschema'
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = {'schema': 'testschema'}

    id = Column(Integer, primary_key=True)
"""


def test_table_args_kwargs(metadata):
    simple_items = Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR),
        schema='testschema'
    )
    simple_items.indexes.add(Index('testidx', simple_items.c.id, simple_items.c.name))

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        Index('testidx', 'id', 'name'),
        {'schema': 'testschema'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String)
"""


def test_schema_table(metadata):
    Table(
        'simple_items', metadata,
        Column('name', VARCHAR),
        schema='testschema'
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, MetaData, String, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('name', String),
    schema='testschema'
)
"""


def test_schema_boolean(metadata):
    Table(
        'simple_items', metadata,
        Column('bool1', INTEGER),
        CheckConstraint('testschema.simple_items.bool1 IN (0, 1)'),
        schema='testschema'
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Boolean, Column, MetaData, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('bool1', Boolean),
    schema='testschema'
)
"""


def test_foreign_key_options(metadata):
    Table(
        'simple_items', metadata,
        Column('name', VARCHAR, ForeignKey(
            'simple_items.name', ondelete='CASCADE', onupdate='CASCADE', deferrable=True,
            initially='DEFERRED'))
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, MetaData, String, Table

metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('name', String, ForeignKey('simple_items.name', ondelete='CASCADE', \
onupdate='CASCADE', deferrable=True, initially='DEFERRED'))
)
"""


def test_foreign_key_schema(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('other_item_id', INTEGER),
        ForeignKeyConstraint(['other_item_id'], ['otherschema.other_items.id'])
    )
    Table(
        'other_items', metadata,
        Column('id', INTEGER, primary_key=True),
        schema='otherschema'
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class OtherItem(Base):
    __tablename__ = 'other_items'
    __table_args__ = {'schema': 'otherschema'}

    id = Column(Integer, primary_key=True)


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    other_item_id = Column(ForeignKey('otherschema.other_items.id'))

    other_item = relationship('OtherItem')
"""


def test_pk_default(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True, server_default=text('uuid_generate_v4()'))
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True, server_default=text("uuid_generate_v4()"))
"""


def test_server_default_multiline(metadata):
    Table(
        'simple_items', metadata,
        Column('id', INTEGER, primary_key=True, server_default=text("""\
/*Comment*/
/*Next line*/
something()"""))
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True, server_default=text(\"""\\
/*Comment*/
/*Next line*/
something()\"""))
"""


def test_server_default_double_quotes(metadata):
    Table(
        'simple', metadata,
        Column('id', INTEGER, primary_key=True, server_default=text("nextval(\"foo\")")),
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Simple(Base):
    __tablename__ = 'simple'

    id = Column(Integer, primary_key=True, server_default=text("nextval(\\"foo\\")"))
"""


def test_invalid_attribute_names(metadata):
    Table(
        'simple-items', metadata,
        Column('id-test', INTEGER, primary_key=True),
        Column('4test', INTEGER),
        Column('_4test', INTEGER),
        Column('def', INTEGER)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple-items'

    id_test = Column('id-test', Integer, primary_key=True)
    _4test = Column('4test', Integer)
    _4test1 = Column('_4test', Integer)
    _def = Column('def', Integer)
"""


def test_pascal(metadata):
    Table(
        'CustomerAPIPreference', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'CustomerAPIPreference'

    id = Column(Integer, primary_key=True)
"""


def test_underscore(metadata):
    Table(
        'customer_api_preference', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CustomerApiPreference(Base):
    __tablename__ = 'customer_api_preference'

    id = Column(Integer, primary_key=True)
"""


def test_pascal_underscore(metadata):
    Table(
        'customer_API_Preference', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API_Preference'

    id = Column(Integer, primary_key=True)
"""


def test_pascal_multiple_underscore(metadata):
    Table(
        'customer_API__Preference', metadata,
        Column('id', INTEGER, primary_key=True)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API__Preference'

    id = Column(Integer, primary_key=True)
"""


def test_metadata_column(metadata):
    Table(
        'simple', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('metadata', VARCHAR)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Simple(Base):
    __tablename__ = 'simple'

    id = Column(Integer, primary_key=True)
    metadata_ = Column('metadata', String)
"""


@pytest.mark.parametrize('metadata', ['mysql'], indirect=['metadata'])
def test_mysql_timestamp(metadata):
    Table(
        'simple', metadata,
        Column('id', INTEGER, primary_key=True),
        Column('timestamp', mysql.TIMESTAMP)
    )

    assert generate_code(metadata) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Simple(Base):
    __tablename__ = 'simple'

    id = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP)
"""
