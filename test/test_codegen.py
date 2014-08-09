from __future__ import unicode_literals, division, print_function, absolute_import
from io import StringIO
import sys
import re

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import (MetaData, Table, Column, CheckConstraint, UniqueConstraint, Index, ForeignKey,
                               ForeignKeyConstraint)
from sqlalchemy.sql.expression import text
from sqlalchemy.types import INTEGER, SMALLINT, VARCHAR, NUMERIC
from sqlalchemy.dialects.postgresql.base import BIGINT, DOUBLE_PRECISION, BOOLEAN, ENUM
from sqlalchemy.dialects.mysql.base import TINYINT
from sqlalchemy.dialects.mysql import base as mysql

from sqlacodegen.codegen import CodeGenerator


if sys.version_info < (3,):
    unicode_re = re.compile(r"u('|\")(.*?)(?<!\\)\1")

    def remove_unicode_prefixes(text):
        return unicode_re.sub(r"\1\2\1", text)
else:
    remove_unicode_prefixes = lambda text: text


class TestModelGenerator(object):
    def setup(self):
        self.metadata = MetaData(create_engine('sqlite:///'))

    def generate_code(self, **kwargs):
        codegen = CodeGenerator(self.metadata, **kwargs)
        sio = StringIO()
        codegen.render(sio)
        return remove_unicode_prefixes(sio.getvalue())

    def test_fancy_coltypes(self):
        Table(
            'simple_items', self.metadata,
            Column('enum', ENUM('A', 'B', name='blah')),
            Column('bool', BOOLEAN),
            Column('number', NUMERIC(10, asdecimal=False)),
        )

        assert self.generate_code() == """\
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

    def test_boolean_detection(self):
        Table(
            'simple_items', self.metadata,
            Column('bool1', INTEGER),
            Column('bool2', SMALLINT),
            Column('bool3', TINYINT),
            CheckConstraint('simple_items.bool1 IN (0, 1)'),
            CheckConstraint('simple_items.bool2 IN (0, 1)'),
            CheckConstraint('simple_items.bool3 IN (0, 1)')
        )

        assert self.generate_code() == """\
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

    def test_enum_detection(self):
        Table(
            'simple_items', self.metadata,
            Column('enum', VARCHAR(255)),
            CheckConstraint(r"simple_items.enum IN ('A', '\'B', 'C')")
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Enum, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('enum', Enum('A', "\\\\'B", 'C'))
)
"""

    def test_column_adaptation(self):
        Table(
            'simple_items', self.metadata,
            Column('id', BIGINT),
            Column('length', DOUBLE_PRECISION)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import BigInteger, Column, Float, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', BigInteger),
    Column('length', Float)
)
"""

    def test_mysql_column_types(self):
        Table(
            'simple_items', self.metadata,
            Column('id', mysql.INTEGER),
            Column('name', mysql.VARCHAR(255))
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, String, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('id', Integer),
    Column('name', String(255))
)
"""

    def test_constraints_table(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER),
            Column('number', INTEGER),
            CheckConstraint('number > 0'),
            UniqueConstraint('id', 'number')
        )

        assert self.generate_code() == """\
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

    def test_constraints_class(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('number', INTEGER),
            CheckConstraint('number > 0'),
            UniqueConstraint('id', 'number')
        )

        assert self.generate_code() == """\
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

    def test_noindexes_table(self):
        simple_items = Table(
            'simple_items', self.metadata,
            Column('number', INTEGER),
            CheckConstraint('number > 2')
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))

        assert self.generate_code(noindexes=True) == """\
# coding: utf-8
from sqlalchemy import CheckConstraint, Column, Integer, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer),
    CheckConstraint('number > 2')
)
"""

    def test_noconstraints_table(self):
        simple_items = Table(
            'simple_items', self.metadata,
            Column('number', INTEGER),
            CheckConstraint('number > 2')
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))

        assert self.generate_code(noconstraints=True) == """\
# coding: utf-8
from sqlalchemy import Column, Integer, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('number', Integer, index=True)
)
"""

    def test_indexes_table(self):
        simple_items = Table(
            'simple_items', self.metadata,
            Column('id', INTEGER),
            Column('number', INTEGER),
            Column('text', VARCHAR)
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))
        simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number, unique=True))
        simple_items.indexes.add(Index('idx_text', simple_items.c.text, unique=True))

        assert self.generate_code() == """\
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

    def test_indexes_class(self):
        simple_items = Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('number', INTEGER),
            Column('text', VARCHAR)
        )
        simple_items.indexes.add(Index('idx_number', simple_items.c.number))
        simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number))
        simple_items.indexes.add(Index('idx_text', simple_items.c.text, unique=True))

        assert self.generate_code() == """\
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

    def test_onetomany(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('container_id', INTEGER),
            ForeignKeyConstraint(['container_id'], ['simple_containers.id']),
        )
        Table(
            'simple_containers', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
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

    def test_onetomany_selfref(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('parent_item_id', INTEGER),
            ForeignKeyConstraint(['parent_item_id'], ['simple_items.id'])
        )

        assert self.generate_code() == """\
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

    def test_onetomany_selfref_multi(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('parent_item_id', INTEGER),
            Column('top_item_id', INTEGER),
            ForeignKeyConstraint(['parent_item_id'], ['simple_items.id']),
            ForeignKeyConstraint(['top_item_id'], ['simple_items.id'])
        )

        assert self.generate_code() == """\
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
"""

    def test_onetomany_composite(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('container_id1', INTEGER),
            Column('container_id2', INTEGER),
            ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2'],
                                 ondelete='CASCADE', onupdate='CASCADE')
        )
        Table(
            'simple_containers', self.metadata,
            Column('id1', INTEGER, primary_key=True),
            Column('id2', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
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
        ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2'], \
ondelete='CASCADE', onupdate='CASCADE'),
    )

    id = Column(Integer, primary_key=True)
    container_id1 = Column(Integer)
    container_id2 = Column(Integer)

    simple_container = relationship('SimpleContainer')
"""

    def test_onetomany_multiref(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('parent_container_id', INTEGER),
            Column('top_container_id', INTEGER),
            ForeignKeyConstraint(['parent_container_id'], ['simple_containers.id']),
            ForeignKeyConstraint(['top_container_id'], ['simple_containers.id'])
        )
        Table(
            'simple_containers', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
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
    top_container = relationship('SimpleContainer', primaryjoin='SimpleItem.top_container_id == SimpleContainer.id')
"""

    def test_onetoone(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('other_item_id', INTEGER),
            ForeignKeyConstraint(['other_item_id'], ['other_items.id']),
            UniqueConstraint('other_item_id')
        )
        Table(
            'other_items', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
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

    def test_onetomany_noinflect(self):
        Table(
            'oglkrogk', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('fehwiuhfiwID', INTEGER),
            ForeignKeyConstraint(['fehwiuhfiwID'], ['fehwiuhfiw.id']),
        )
        Table(
            'fehwiuhfiw', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
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

    def test_manytomany(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )
        Table(
            'simple_containers', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )
        Table(
            'container_items', self.metadata,
            Column('item_id', INTEGER),
            Column('container_id', INTEGER),
            ForeignKeyConstraint(['item_id'], ['simple_items.id']),
            ForeignKeyConstraint(['container_id'], ['simple_containers.id'])
        )

        assert self.generate_code() == """\
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
"""

    def test_manytomany_selfref(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )
        Table(
            'child_items', self.metadata,
            Column('parent_id', INTEGER),
            Column('child_id', INTEGER),
            ForeignKeyConstraint(['parent_id'], ['simple_items.id']),
            ForeignKeyConstraint(['child_id'], ['simple_items.id'])
        )

        assert self.generate_code() == """\
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
"""

    def test_manytomany_composite(self):
        Table(
            'simple_items', self.metadata,
            Column('id1', INTEGER, primary_key=True),
            Column('id2', INTEGER, primary_key=True)
        )
        Table(
            'simple_containers', self.metadata,
            Column('id1', INTEGER, primary_key=True),
            Column('id2', INTEGER, primary_key=True)
        )
        Table(
            'container_items', self.metadata,
            Column('item_id1', INTEGER),
            Column('item_id2', INTEGER),
            Column('container_id1', INTEGER),
            Column('container_id2', INTEGER),
            ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', 'simple_items.id2']),
            ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2'])
        )

        assert self.generate_code() == """\
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
"""

    def test_joined_inheritance(self):
        Table(
            'simple_sub_items', self.metadata,
            Column('simple_items_id', INTEGER, primary_key=True),
            Column('data3', INTEGER),
            ForeignKeyConstraint(['simple_items_id'], ['simple_items.super_item_id'])
        )
        Table(
            'simple_super_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('data1', INTEGER)
        )
        Table(
            'simple_items', self.metadata,
            Column('super_item_id', INTEGER, primary_key=True),
            Column('data2', INTEGER),
            ForeignKeyConstraint(['super_item_id'], ['simple_super_items.id'])
        )

        assert self.generate_code() == """\
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

    def test_no_inflect(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code(noinflect=True) == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class SimpleItems(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
"""

    def test_table_kwargs(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            schema='testschema'
        )

        assert self.generate_code() == """\
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

    def test_table_args_kwargs(self):
        simple_items = Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('name', VARCHAR),
            schema='testschema'
        )
        simple_items.indexes.add(Index('testidx', simple_items.c.id, simple_items.c.name))

        assert self.generate_code() == """\
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

    def test_schema_table(self):
        Table(
            'simple_items', self.metadata,
            Column('name', VARCHAR),
            schema='testschema'
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, MetaData, String, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('name', String),
    schema='testschema'
)
"""

    def test_schema_boolean(self):
        Table(
            'simple_items', self.metadata,
            Column('bool1', INTEGER),
            CheckConstraint('testschema.simple_items.bool1 IN (0, 1)'),
            schema='testschema'
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Boolean, Column, MetaData, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('bool1', Boolean),
    schema='testschema'
)
"""

    def test_foreign_key_options(self):
        Table(
            'simple_items', self.metadata,
            Column('name', VARCHAR, ForeignKey('simple_items.name', ondelete='CASCADE', onupdate='CASCADE',
                                               deferrable=True, initially='DEFERRED'))
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, MetaData, String, Table


metadata = MetaData()


t_simple_items = Table(
    'simple_items', metadata,
    Column('name', String, ForeignKey('simple_items.name', ondelete='CASCADE', onupdate='CASCADE', \
deferrable=True, initially='DEFERRED'))
)
"""

    def test_foreign_key_schema(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            Column('other_item_id', INTEGER),
            ForeignKeyConstraint(['other_item_id'], ['otherschema.other_items.id'])
        )
        Table(
            'other_items', self.metadata,
            Column('id', INTEGER, primary_key=True),
            schema='otherschema'
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    other_item_id = Column(ForeignKey('otherschema.other_items.id'))

    other_item = relationship('OtherItem')


class OtherItem(Base):
    __tablename__ = 'other_items'
    __table_args__ = {'schema': 'otherschema'}

    id = Column(Integer, primary_key=True)
"""

    def test_pk_default(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True, server_default=text('uuid_generate_v4()'))
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True, server_default=text("uuid_generate_v4()"))
"""

    def test_server_default_multiline(self):
        Table(
            'simple_items', self.metadata,
            Column('id', INTEGER, primary_key=True, server_default=text("""\
/*Comment*/
/*Next line*/
something()"""))
        )

        assert self.generate_code() == """\
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

    def test_invalid_attribute_names(self):
        Table(
            'simple_items', self.metadata,
            Column('id-test', INTEGER, primary_key=True),
            Column('4test', INTEGER),
            Column('_4test', INTEGER),
            Column('def', INTEGER)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id_test = Column('id-test', Integer, primary_key=True)
    _4test = Column('4test', Integer)
    _4test1 = Column('_4test', Integer)
    _def = Column('def', Integer)
"""

    def test_pascal(self):
        Table(
            'CustomerAPIPreference', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'CustomerAPIPreference'

    id = Column(Integer, primary_key=True)
"""

    def test_underscore(self):
        Table(
            'customer_api_preference', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class CustomerApiPreference(Base):
    __tablename__ = 'customer_api_preference'

    id = Column(Integer, primary_key=True)
"""

    def test_pascal_underscore(self):
        Table(
            'customer_API_Preference', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API_Preference'

    id = Column(Integer, primary_key=True)
"""

    def test_pascal_multiple_underscore(self):
        Table(
            'customer_API__Preference', self.metadata,
            Column('id', INTEGER, primary_key=True)
        )

        assert self.generate_code() == """\
# coding: utf-8
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class CustomerAPIPreference(Base):
    __tablename__ = 'customer_API__Preference'

    id = Column(Integer, primary_key=True)
"""
