"""Contains the code generation logic and helper functions."""
from __future__ import unicode_literals, division, print_function, absolute_import
from collections import defaultdict
from keyword import iskeyword
import inspect

from sqlalchemy.types import Enum
from sqlalchemy.schema import ForeignKeyConstraint, PrimaryKeyConstraint, CheckConstraint, UniqueConstraint

try:
    from sqlalchemy.sql.expression import TextClause
except ImportError:
    # SQLAlchemy < 0.8
    from sqlalchemy.sql.expression import _TextClause as TextClause


DEFAULT_HEADER = """\
# coding: utf-8
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


"""
DEFAULT_FOOTER = ""


def get_compiled_expression(statement):
    """Returns the statement in a form where any placeholders have been filled in."""
    if isinstance(statement, TextClause):
        return statement.text

    dialect = statement.left.table.bind.dialect
    compiler = statement._compiler(dialect)

    # Adapted from http://stackoverflow.com/a/5698357/242021
    class LiteralCompiler(compiler.__class__):
        def visit_bindparam(self, bindparam, within_columns_clause=False, literal_binds=False, **kwargs):
            return super(LiteralCompiler, self).render_literal_bindparam(
                bindparam, within_columns_clause=within_columns_clause,
                literal_binds=literal_binds, **kwargs
            )

    compiler = LiteralCompiler(dialect, statement)
    return compiler.process(statement)


def singular(plural):
    """A feeble attempt at converting plural English nouns into singular form."""
    if plural.endswith('ies'):
        return plural[:-3] + 'y'
    if plural.endswith('s') and not plural.endswith('ss'):
        return plural[:-1]
    return plural


def get_typename(type_):
    """Returns the most reasonable column type name to use (ie. String instead of VARCHAR)."""
    cls = type_.__class__
    typename = cls.__class__.__name__
    for supercls in cls.__mro__:
        if hasattr(supercls, '__visit_name__'):
            typename = supercls.__name__
        if supercls.__name__ != supercls.__name__.upper():
            break

    return typename


def get_constraint_sort_key(constraint):
    if isinstance(constraint, CheckConstraint):
        return 'C{0}'.format(constraint.sqltext)
    return constraint.__class__.__name__[0] + repr(constraint.columns)


def get_common_fk_constraints(table1, table2):
    """Returns a set of foreign key constraints the two tables have against each other."""
    c1 = set(c for c in table1.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table2)
    c2 = set(c for c in table2.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table1)
    return c1.union(c2)


def tablename_to_classname(tablename):
    return singular(''.join(part.capitalize() for part in tablename.split('_')))


def classname_to_tablename(classname):
    tablename = classname[0].lower()
    for c in classname[1:]:
        if c.isupper():
            c = '_' + c.lower()
        tablename += c

    return tablename


def generate_type_code(type_):
    text = get_typename(type_)
    args = []

    if isinstance(type_, Enum):
        args.extend(repr(arg) for arg in type_.enums)
        if type_.name is not None:
            args.append('name={0!r}'.format(type_.name))
    else:
        # All other types
        argspec = inspect.getargspec(type_.__class__.__init__)
        defaults = dict(zip(argspec.args[-len(argspec.defaults or ()):], argspec.defaults or ()))
        missing = object()
        use_kwargs = False
        for attr in argspec.args[1:]:
            value = getattr(type_, attr, missing)
            default = defaults.get(attr, missing)
            if value is missing or value == default:
                use_kwargs = True
            elif use_kwargs:
                args.append('{0}={1}'.format(attr, repr(value)))
            else:
                args.append(repr(value))

    if args:
        text += '({0})'.format(', '.join(args))

    return text


def generate_column_code(column, show_name):
    kwarg = []
    is_sole_pk = column.primary_key and len(column.table.primary_key) == 1
    dedicated_fks = [c for c in column.foreign_keys if len(c.constraint.columns) == 1]
    is_unique = any(isinstance(c, UniqueConstraint) and set(c.columns) == set([column])
                    for c in column.table.constraints)
    has_index = any(set(i.columns) == set([column]) for i in column.table.indexes)

    if column.key != column.name:
        kwarg.append('key')
    if column.primary_key:
        kwarg.append('primary_key')
    if not column.nullable and not is_sole_pk:
        kwarg.append('nullable')
    if is_unique:
        column.unique = True
        kwarg.append('unique')
    elif has_index:
        column.index = True
        kwarg.append('index')
    if column.server_default and not is_sole_pk:
        column.server_default = get_compiled_expression(column.server_default.arg)
        kwarg.append('server_default')

    return 'Column({0})'.format(', '.join(
        ([repr(column.name)] if show_name else []) +
        ([generate_type_code(column.type)] if not dedicated_fks else []) +
        [repr(x) for x in dedicated_fks] +
        [repr(x) for x in column.constraints] +
        ['{0}={1}'.format(k, repr(getattr(column, k))) for k in kwarg]))


def generate_constraint_reprs(constraints):
    for constraint in sorted(constraints, key=get_constraint_sort_key):
        if isinstance(constraint, ForeignKeyConstraint):
            if len(constraint.columns) > 1:
                local_columns = constraint.columns
                remote_columns = ['{0}.{1}'.format(fk.column.table.name, fk.column.name) for fk in constraint.elements]
                yield 'ForeignKeyConstraint({0!r}, {1!r})'.format(local_columns, remote_columns)
        elif isinstance(constraint, CheckConstraint):
            yield 'CheckConstraint({0!r})'.format(get_compiled_expression(constraint.sqltext))
        elif isinstance(constraint, UniqueConstraint):
            if len(constraint.columns) > 1:
                columns = [repr(col.name) for col in constraint.columns]
                yield 'UniqueConstraint({0})'.format(', '.join(columns))


def generate_index_reprs(indexes):
    for index in indexes:
        if len(index.columns) > 1:
            columns = [repr(col.name) for col in index.columns]
            yield 'Index({0!r}, {1})'.format(index.name, ', '.join(columns))


def generate_table(table):
    elements = [generate_column_code(column, True) for column in table.c]
    elements.extend(generate_constraint_reprs(table.constraints))
    elements.extend(generate_index_reprs(table.indexes))
    return """\
t_{0} = Table(
    {0!r}, Base.metadata,
    {1}
)
""".format(table.name, ',\n    '.join(elements))


def generate_relationship_name(colname, remote_classname, used_names):
    name = base = classname_to_tablename(remote_classname) if not colname.endswith('_id') else colname[:-3]
    iteration = 0
    while name in used_names:
        iteration += 1
        name = base + str(iteration)

    used_names.add(name)
    return name


def generate_relationship(classname, used_names, fk_constraint=None, link_table=None):
    remote_side = uselist = secondary = primaryjoin = secondaryjoin = None
    if link_table is not None:
        # Many-to-Many
        secondary = 'secondary=' + repr(link_table.name)
        fk_constraints = [c for c in link_table.constraints if isinstance(c, ForeignKeyConstraint)]
        fk_constraints.sort(key=get_constraint_sort_key)
        remote_tablename = fk_constraints[1].elements[0].column.table.name
        remote_classname = tablename_to_classname(remote_tablename)
        relationship_name = generate_relationship_name(fk_constraints[1].columns[0], remote_classname, used_names) + 's'

        # Handle self referential relationships
        if classname == remote_classname:
            pri_pairs = zip(fk_constraints[0].columns, fk_constraints[0].elements)
            sec_pairs = zip(fk_constraints[1].columns, fk_constraints[1].elements)
            pri_joins = ['{0} == {1}.c.{2}'.format(elem.column.name, link_table.name, col) for col, elem in pri_pairs]
            sec_joins = ['{0} == {1}.c.{2}'.format(elem.column.name, link_table.name, col) for col, elem in sec_pairs]
            primaryjoin = 'primaryjoin=' + (
                repr('and_({0})'.format(', '.join(pri_joins))) if len(pri_joins) > 1 else repr(pri_joins[0]))
            secondaryjoin = 'secondaryjoin=' + (
                repr('and_({0})'.format(', '.join(sec_joins))) if len(sec_joins) > 1 else repr(sec_joins[0]))
    else:
        # One-to-Many or One-to-One
        remote_classname = tablename_to_classname(fk_constraint.elements[0].column.table.name)
        relationship_name = generate_relationship_name(fk_constraint.columns[0], remote_classname, used_names)

        # Add uselist=False to One-to-One relationships
        if any(isinstance(c, (PrimaryKeyConstraint, UniqueConstraint)) and
               set(col.name for col in c.columns) == set(fk_constraint.columns)
               for c in fk_constraint.table.constraints):
            uselist = 'uselist=False'

        # Handle self referential relationships
        if classname == remote_classname:
            pk_col_names = [col.name for col in fk_constraint.table.primary_key]
            remote_side = 'remote_side=[{0}]'.format(', '.join(pk_col_names))

        # If the two tables share more than one foreign key constraint,
        # SQLAlchemy needs an explicit primaryjoin to figure out which column(s) to join with
        common_fk_constraints = get_common_fk_constraints(fk_constraint.table, fk_constraint.elements[0].column.table)
        if len(common_fk_constraints) > 1:
            primaryjoin = "primaryjoin='{0} == {1}.{2}'".format(fk_constraint.columns[0], remote_classname,
                                                                fk_constraint.elements[0].column.name)

    args = [arg for arg in (repr(remote_classname), remote_side, uselist, secondary, primaryjoin, secondaryjoin) if arg]
    if secondaryjoin:
        return '{0} = relationship(\n        {1}\n    )'.format(relationship_name, ',\n        '.join(args))
    else:
        return '{0} = relationship({1})'.format(relationship_name, ', '.join(args))


def generate_class(table, links=()):
    used_names = set()
    classname = tablename_to_classname(table.name)
    text = 'class {0}(Base):\n    __tablename__ = {1!r}\n'.format(classname, table.name)

    constraints = sorted(table.constraints, key=get_constraint_sort_key)
    table_args = list(generate_constraint_reprs(constraints)) + list(generate_index_reprs(table.indexes))
    if table_args:
        if len(table_args) == 1:
            table_args[0] += ','  # Required for this to be a tuple
        text += '    __table_args__ = (\n        {0}\n    )\n'.format(',\n        '.join(table_args))
    text += '\n'

    # Generate columns
    for column in table.c:
        attrname = column.name + '_' if iskeyword(column.name) else column.name
        used_names.add(attrname)
        col_repr = generate_column_code(column, attrname != column.name)
        text += '    {0} = {1}\n'.format(attrname, col_repr)

    if links or table.foreign_keys:
        text += '\n'

    # Generate many-to-one relationships
    for constraint in constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            relationship = generate_relationship(classname, used_names, fk_constraint=constraint)
            text += '    {0}\n'.format(relationship)

    # Generate many-to-many relationships
    for link_table in links:
        relationship = generate_relationship(classname, used_names, link_table=link_table)
        text += '    {0}\n'.format(relationship)

    return text


def generate_declarative_models(metadata, noindexes=False, noconstraints=False):
    links = defaultdict(lambda: [])
    link_tables = set()
    for table in metadata.tables.values():
        # Link tables have exactly two foreign key constraints and all columns are involved in them
        fk_constraints = [constr for constr in table.constraints if isinstance(constr, ForeignKeyConstraint)]
        if (len(fk_constraints) == 2 and all(col.foreign_keys for col in table.columns)):
            link_tables.add(table.name)
            tablename = sorted(fk_constraints, key=get_constraint_sort_key)[0].elements[0].column.table.name
            links[tablename].append(table)

    for table in sorted(metadata.tables.values(), key=lambda t: t.name):
        # Support for Alembic and sqlalchemy-migrate -- never expose the schema version tables
        if table.name in ('alembic_version', 'migrate_version'):
            continue

        if noindexes:
            table.indexes.clear()

        if noconstraints:
            table.constraints = set([table.primary_key])
            table.foreign_keys.clear()
            for col in table.columns:
                col.foreign_keys.clear()

        if not table.primary_key or table.name in link_tables:
            yield generate_table(table)
        else:
            yield generate_class(table, links[table.name])


def generate_model_code(metadata, noindexes, noconstraints, header=DEFAULT_HEADER, footer=DEFAULT_FOOTER):
    models = generate_declarative_models(metadata, noindexes, noconstraints)
    return header + '\n\n'.join(models).rstrip() + footer
