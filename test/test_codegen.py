from __future__ import unicode_literals, division, print_function, absolute_import
from io import StringIO
import sys  # @UnusedImport
import re

from nose.tools import eq_
from sqlalchemy import *

from sqlacodegen.codegen import CodeGenerator, _singular
from sqlalchemy.dialects.postgresql.base import DOUBLE_PRECISION


if sys.version_info < (3,):
    unicode_re = re.compile(r"u('|\")(.*?)(?<!\\)\1")

    def remove_unicode_prefixes(text):
        return unicode_re.sub(r"\1\2\1", text)
else:
    remove_unicode_prefixes = lambda text: text


def test_singular_ies():
    eq_(_singular('bunnies'), 'bunny')


def test_singular_ss():
    eq_(_singular('address'), 'address')


class TestCodeGenerator(object):
    def generate_code(self, metadata, **kwargs):
        codegen = CodeGenerator(metadata, **kwargs)
        sio = StringIO()
        codegen.render(sio)
        return remove_unicode_prefixes(sio.getvalue())

    def test_fancy_coltypes(self):
        testmeta = MetaData(create_engine('sqlite:///'))
        Table(
            'simple_items', testmeta,
            Column('enum', Enum('A', 'B', name='blah')),
            Column('bool', Boolean),
            Column('number', Numeric(10, asdecimal=False)),
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Boolean, Column, Enum, MetaData, Numeric, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('enum', Enum('A', 'B', name='blah')),
    Column('bool', Boolean),
    Column('number', Numeric(10, asdecimal=False))
)
""")

    def test_boolean_detection(self):
        testmeta = MetaData(create_engine('sqlite:///'))
        Table(
            'simple_items', testmeta,
            Column('bool', SmallInteger),
            CheckConstraint('simple_items.bool IN (0, 1)')
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Boolean, Column, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('bool', Boolean)
)
""")

    def test_enum_detection(self):
        testmeta = MetaData(create_engine('sqlite:///'))
        Table(
            'simple_items', testmeta,
            Column('enum', String),
            CheckConstraint(r"simple_items.enum IN ('A', '\'B', 'C')")
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Column, Enum, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('enum', Enum('A', "\\\\'B", 'C'))
)
""")

    def test_column_adaptation(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', BIGINT),
            Column('length', DOUBLE_PRECISION)
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import BigInteger, Column, Float, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', BigInteger),
    Column('length', Float)
)
""")

    def test_constraints_table(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer),
            Column('number', Integer),
            CheckConstraint('number > 0'),
            UniqueConstraint('id', 'number')
        )

        eq_(self.generate_code(testmeta), """\
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
""")

    def test_constraints_class(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('number', Integer),
            CheckConstraint('number > 0'),
            UniqueConstraint('id', 'number')
        )

        eq_(self.generate_code(testmeta), """\
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
""")

    def test_noindexes_table(self):
        testmeta = MetaData()
        simple_items = Table(
            'simple_items', testmeta,
            Column('number', Integer),
            CheckConstraint('number > 2')
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))

        eq_(self.generate_code(testmeta, noindexes=True), """\
# coding: utf-8
from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer),
    CheckConstraint('number > 2')
)
""")

    def test_noconstraints_table(self):
        testmeta = MetaData()
        simple_items = Table(
            'simple_items', testmeta,
            Column('number', Integer),
            CheckConstraint('number > 2')
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))

        eq_(self.generate_code(testmeta, noconstraints=True), """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer, index=True)
)
""")

    def test_indexes_table(self):
        testmeta = MetaData()
        simple_items = Table(
            'simple_items', testmeta,
            Column('id', Integer),
            Column('number', Integer),
            Column('text', String)
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))
        simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number))

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Column, Index, Integer, MetaData, String, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer),
    Column('number', Integer, index=True),
    Column('text', String),
    Index('idx_text_number', 'text', 'number')
)
""")

    def test_indexes_class(self):
        testmeta = MetaData()
        simple_items = Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('number', Integer),
            Column('text', String)
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))
        simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number))

        eq_(self.generate_code(testmeta), """\
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
    text = Column(String)
""")

    def test_onetomany(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('container_id', Integer),
            ForeignKeyConstraint(['container_id'], ['simple_containers.id']),
        )
        Table(
            'simple_containers', testmeta,
            Column('id', Integer, primary_key=True)
        )

        eq_(self.generate_code(testmeta), """\
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
""")

    def test_onetomany_selfref(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('parent_item_id', Integer),
            ForeignKeyConstraint(['parent_item_id'], ['simple_items.id'])
        )

        eq_(self.generate_code(testmeta), """\
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
""")

    def test_onetomany_selfref_multi(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('parent_item_id', Integer),
            Column('top_item_id', Integer),
            ForeignKeyConstraint(['parent_item_id'], ['simple_items.id']),
            ForeignKeyConstraint(['top_item_id'], ['simple_items.id'])
        )

        eq_(self.generate_code(testmeta), """\
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

    parent_item = relationship('SimpleItem', remote_side=[id], primaryjoin='SimpleItem.parent_item_id == SimpleItem.id')
    top_item = relationship('SimpleItem', remote_side=[id], primaryjoin='SimpleItem.top_item_id == SimpleItem.id')
""")

    def test_onetomany_composite(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('container_id1', Integer),
            Column('container_id2', Integer),
            ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2'])
        )
        Table(
            'simple_containers', testmeta,
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True)
        )

        eq_(self.generate_code(testmeta), """\
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
        ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2']),
    )

    id = Column(Integer, primary_key=True)
    container_id1 = Column(Integer)
    container_id2 = Column(Integer)

    simple_container = relationship('SimpleContainer')
""")

    def test_onetomany_multiref(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('parent_container_id', Integer),
            Column('top_container_id', Integer),
            ForeignKeyConstraint(['parent_container_id'], ['simple_containers.id']),
            ForeignKeyConstraint(['top_container_id'], ['simple_containers.id'])
        )
        Table(
            'simple_containers', testmeta,
            Column('id', Integer, primary_key=True)
        )

        eq_(self.generate_code(testmeta), """\
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

    parent_container = relationship('SimpleContainer', primaryjoin='SimpleItem.parent_container_id == SimpleContainer.id')
    top_container = relationship('SimpleContainer', primaryjoin='SimpleItem.top_container_id == SimpleContainer.id')
""")

    def test_onetoone(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True),
            Column('other_item_id', Integer),
            ForeignKeyConstraint(['other_item_id'], ['other_items.id']),
            UniqueConstraint('other_item_id')
        )
        Table(
            'other_items', testmeta,
            Column('id', Integer, primary_key=True)
        )

        eq_(self.generate_code(testmeta), """\
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
""")

    def test_manytomany(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True)
        )
        Table(
            'simple_containers', testmeta,
            Column('id', Integer, primary_key=True)
        )
        Table(
            'container_items', testmeta,
            Column('item_id', Integer),
            Column('container_id', Integer),
            ForeignKeyConstraint(['item_id'], ['simple_items.id']),
            ForeignKeyConstraint(['container_id'], ['simple_containers.id'])
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


t_container_items = Table(
    'container_items', metadata,
    Column('item_id', ForeignKey('simple_items.id')),
    Column('container_id', ForeignKey('simple_containers.id'))
)


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)

    items = relationship('SimpleItem', secondary='container_items')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
""")

    def test_manytomany_selfref(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id', Integer, primary_key=True)
        )
        Table(
            'child_items', testmeta,
            Column('parent_id', Integer),
            Column('child_id', Integer),
            ForeignKeyConstraint(['parent_id'], ['simple_items.id']),
            ForeignKeyConstraint(['child_id'], ['simple_items.id'])
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


t_child_items = Table(
    'child_items', metadata,
    Column('parent_id', ForeignKey('simple_items.id')),
    Column('child_id', ForeignKey('simple_items.id'))
)


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)

    parents = relationship(
        'SimpleItem',
        secondary='child_items',
        primaryjoin='SimpleItem.id == child_items.c.child_id',
        secondaryjoin='SimpleItem.id == child_items.c.parent_id'
    )
""")

    def test_manytomany_composite(self):
        testmeta = MetaData()
        Table(
            'simple_items', testmeta,
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True)
        )
        Table(
            'simple_containers', testmeta,
            Column('id1', Integer, primary_key=True),
            Column('id2', Integer, primary_key=True)
        )
        Table(
            'container_items', testmeta,
            Column('item_id1', Integer),
            Column('item_id2', Integer),
            Column('container_id1', Integer),
            Column('container_id2', Integer),
            ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', 'simple_items.id2']),
            ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2'])
        )

        eq_(self.generate_code(testmeta), """\
# coding: utf-8
from sqlalchemy import Column, ForeignKeyConstraint, Integer, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


t_container_items = Table(
    'container_items', metadata,
    Column('item_id1', Integer),
    Column('item_id2', Integer),
    Column('container_id1', Integer),
    Column('container_id2', Integer),
    ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2']),
    ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', 'simple_items.id2'])
)


class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)

    simple_items = relationship('SimpleItem', secondary='container_items')


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)
""")
