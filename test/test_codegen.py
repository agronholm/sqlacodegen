from __future__ import unicode_literals, division, print_function, absolute_import
import sys  # @UnusedImport
import re

from nose.tools import eq_
from sqlalchemy import *

from sqlacodegen.codegen import (generate_declarative_models, generate_type_code, singular, generate_relationship_name,
                                 generate_table, generate_class)


if sys.version_info < (3,):
    unicode_re = re.compile(r"u'([^']*)'")

    def remove_unicode_prefixes(text):
        return unicode_re.sub(r"'\1'", text)
else:
    remove_unicode_prefixes = lambda text: text


def test_singular_ies():
    eq_(singular('bunnies'), 'bunny')


def test_singular_ss():
    eq_(singular('address'), 'address')


def test_generate_relationship_id():
    eq_(generate_relationship_name('item_id', 'Item', set()), 'item')


def test_generate_relationship_multi():
    used_names = set()
    eq_(generate_relationship_name('fk1', 'RemoteClass', used_names), 'remote_class')
    eq_(generate_relationship_name('fk2', 'RemoteClass', used_names), 'remote_class1')


def test_typecode_plain():
    eq_(generate_type_code(Integer()), 'Integer')


def test_typecode_arg():
    eq_(generate_type_code(String(20)), 'String(20)')


def test_typecode_kwarg():
    typecode = generate_type_code(Numeric(10, asdecimal=False))
    eq_(typecode, 'Numeric(10, asdecimal=False)')


def test_typecode_enum():
    typecode = generate_type_code(Enum('A', 'B', name='blah'))
    eq_(remove_unicode_prefixes(typecode), "Enum('A', 'B', name='blah')")


def test_constraints_table():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('number', Integer),
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    table_def = remove_unicode_prefixes(generate_table(simple_items))
    eq_(table_def, """\
t_simple_items = Table(
    'simple_items', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('number', Integer),
    CheckConstraint('number > 0'),
    UniqueConstraint('id', 'number')
)
""")


def test_constraints_class():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('number', Integer),
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    table_def = remove_unicode_prefixes(generate_class(simple_items))
    eq_(table_def, """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        CheckConstraint('number > 0'),
        UniqueConstraint('id', 'number')
    )

    id = Column(Integer, primary_key=True)
    number = Column(Integer)
""")


def test_noindexes_table():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('number', Integer),
        CheckConstraint('number > 2')
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))

    table_def = remove_unicode_prefixes(next(generate_declarative_models(testmeta, noindexes=True)))
    eq_(table_def, """\
t_simple_items = Table(
    'simple_items', Base.metadata,
    Column('number', Integer),
    CheckConstraint('number > 2')
)
""")


def test_noconstraints_table():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('number', Integer),
        CheckConstraint('number > 2')
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))

    table_def = remove_unicode_prefixes(next(generate_declarative_models(testmeta, noconstraints=True)))
    eq_(table_def, """\
t_simple_items = Table(
    'simple_items', Base.metadata,
    Column('number', Integer, index=True)
)
""")


def test_indexes_table():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('number', Integer),
        Column('text', String)
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))
    simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number))

    table_def = remove_unicode_prefixes(generate_table(simple_items))
    eq_(table_def, """\
t_simple_items = Table(
    'simple_items', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('number', Integer, index=True),
    Column('text', String),
    Index('idx_text_number', 'text', 'number')
)
""")


def test_indexes_class():
    testmeta = MetaData()
    simple_items = Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('number', Integer),
        Column('text', String)
    )
    simple_items.indexes.add(Index('idx_number', simple_items.c.number))
    simple_items.indexes.add(Index('idx_text_number', simple_items.c.text, simple_items.c.number))

    table_def = remove_unicode_prefixes(generate_class(simple_items))
    eq_(table_def, """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'
    __table_args__ = (
        Index('idx_text_number', 'text', 'number'),
    )

    id = Column(Integer, primary_key=True)
    number = Column(Integer, index=True)
    text = Column(String)
""")


def test_onetomany():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 2)
    eq_(table_defs[0], """\
class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)
""")
    eq_(table_defs[1], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    container_id = Column(ForeignKey('simple_containers.id'))

    container = relationship('SimpleContainer')
""")


def test_onetomany_selfref():
    testmeta = MetaData()
    Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('parent_item_id', Integer),
        ForeignKeyConstraint(['parent_item_id'], ['simple_items.id'])
    )

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    eq_(len(table_defs), 1)
    eq_(table_defs[0], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_item_id = Column(ForeignKey('simple_items.id'))

    parent_item = relationship('SimpleItem', remote_side=[id])
""")


def test_onetomany_selfref_multi():
    testmeta = MetaData()
    Table(
        'simple_items', testmeta,
        Column('id', Integer, primary_key=True),
        Column('parent_item_id', Integer),
        Column('top_item_id', Integer),
        ForeignKeyConstraint(['parent_item_id'], ['simple_items.id']),
        ForeignKeyConstraint(['top_item_id'], ['simple_items.id'])
    )

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    eq_(len(table_defs), 1)
    eq_(table_defs[0], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_item_id = Column(ForeignKey('simple_items.id'))
    top_item_id = Column(ForeignKey('simple_items.id'))

    parent_item = relationship('SimpleItem', remote_side=[id], primaryjoin='parent_item_id == SimpleItem.id')
    top_item = relationship('SimpleItem', remote_side=[id], primaryjoin='top_item_id == SimpleItem.id')
""")


def test_onetomany_composite():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 2)
    eq_(table_defs[0], """\
class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)
""")
    eq_(table_defs[1], """\
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


def test_onetomany_multiref():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 2)
    eq_(table_defs[0], """\
class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)
""")
    eq_(table_defs[1], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    parent_container_id = Column(ForeignKey('simple_containers.id'))
    top_container_id = Column(ForeignKey('simple_containers.id'))

    parent_container = relationship('SimpleContainer', primaryjoin='parent_container_id == SimpleContainer.id')
    top_container = relationship('SimpleContainer', primaryjoin='top_container_id == SimpleContainer.id')
""")


def test_onetoone():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 2)
    eq_(table_defs[0], """\
class OtherItem(Base):
    __tablename__ = 'other_items'

    id = Column(Integer, primary_key=True)
""")
    eq_(table_defs[1], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
    other_item_id = Column(ForeignKey('other_items.id'), unique=True)

    other_item = relationship('OtherItem', uselist=False)
""")


def test_manytomany():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 3)
    eq_(table_defs[0], """\
class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id = Column(Integer, primary_key=True)

    items = relationship('SimpleItem', secondary='container_items')
""")
    eq_(table_defs[1], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)
""")
    eq_(table_defs[2], """\
t_container_items = Table(
    'container_items', Base.metadata,
    Column('item_id', ForeignKey('simple_items.id')),
    Column('container_id', ForeignKey('simple_containers.id'))
)
""")


def test_manytomany_selfref():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 2)
    eq_(table_defs[0], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id = Column(Integer, primary_key=True)

    parents = relationship(
        'SimpleItem',
        secondary='child_items',
        primaryjoin='id == child_items.c.child_id',
        secondaryjoin='id == child_items.c.parent_id'
    )
""")
    eq_(table_defs[1], """\
t_child_items = Table(
    'child_items', Base.metadata,
    Column('parent_id', ForeignKey('simple_items.id')),
    Column('child_id', ForeignKey('simple_items.id'))
)
""")


def test_manytomany_composite():
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

    table_defs = [remove_unicode_prefixes(table_def) for table_def in generate_declarative_models(testmeta)]
    table_defs.sort()
    eq_(len(table_defs), 3)
    eq_(table_defs[0], """\
class SimpleContainer(Base):
    __tablename__ = 'simple_containers'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)

    simple_items = relationship('SimpleItem', secondary='container_items')
""")
    eq_(table_defs[1], """\
class SimpleItem(Base):
    __tablename__ = 'simple_items'

    id1 = Column(Integer, primary_key=True, nullable=False)
    id2 = Column(Integer, primary_key=True, nullable=False)
""")
    eq_(table_defs[2], """\
t_container_items = Table(
    'container_items', Base.metadata,
    Column('item_id1', Integer),
    Column('item_id2', Integer),
    Column('container_id1', Integer),
    Column('container_id2', Integer),
    ForeignKeyConstraint(['container_id1', 'container_id2'], ['simple_containers.id1', 'simple_containers.id2']),
    ForeignKeyConstraint(['item_id1', 'item_id2'], ['simple_items.id1', 'simple_items.id2'])
)
""")
