"""Contains the code generation logic and helper functions."""
from __future__ import unicode_literals, division, print_function, absolute_import
from collections import defaultdict
from keyword import iskeyword
import inspect
import sys
import re

from sqlalchemy import (Enum, ForeignKeyConstraint, PrimaryKeyConstraint, CheckConstraint, UniqueConstraint, Table,
                        Column)
from sqlalchemy.util import OrderedDict
from sqlalchemy.types import Boolean, String
import sqlalchemy

try:
    from sqlalchemy.sql.expression import TextClause
except ImportError:
    # SQLAlchemy < 0.8
    from sqlalchemy.sql.expression import _TextClause as TextClause


_re_boolean_check_constraint = re.compile(r"(?:(?:.*?)\.)?(.*?) IN \(0, 1\)")
_re_enum_check_constraint = re.compile(r"(?:(?:.*?)\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")


def _singular(plural):
    """A feeble attempt at converting plural English nouns into singular form."""
    if plural.endswith('ies'):
        return plural[:-3] + 'y'
    if plural.endswith('s') and not plural.endswith('ss'):
        return plural[:-1]
    return plural


def _tablename_to_classname(tablename):
    return _singular(''.join(part.capitalize() for part in tablename.split('_')))


def _get_compiled_expression(statement):
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


def _get_constraint_sort_key(constraint):
    if isinstance(constraint, CheckConstraint):
        return 'C{0}'.format(constraint.sqltext)
    return constraint.__class__.__name__[0] + repr(constraint.columns)


def _get_common_fk_constraints(table1, table2):
    """Returns a set of foreign key constraints the two tables have against each other."""
    c1 = set(c for c in table1.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table2)
    c2 = set(c for c in table2.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table1)
    return c1.union(c2)


def _render_column_type(coltype):
    args = []
    if isinstance(coltype, Enum):
        args.extend(repr(arg) for arg in coltype.enums)
        if coltype.name is not None:
            args.append('name={0!r}'.format(coltype.name))
    else:
        # All other types
        argspec = inspect.getargspec(coltype.__class__.__init__)
        defaults = dict(zip(argspec.args[-len(argspec.defaults or ()):], argspec.defaults or ()))
        missing = object()
        use_kwargs = False
        for attr in argspec.args[1:]:
            value = getattr(coltype, attr, missing)
            default = defaults.get(attr, missing)
            if value is missing or value == default:
                use_kwargs = True
            elif use_kwargs:
                args.append('{0}={1}'.format(attr, repr(value)))
            else:
                args.append(repr(value))

    text = coltype.__class__.__name__
    if args:
        text += '({0})'.format(', '.join(args))

    return text


class ImportCollector(OrderedDict):
    def add_import(self, obj):
        type_ = type(obj) if not isinstance(obj, type) else obj
        pkgname = 'sqlalchemy' if type_.__name__ in sqlalchemy.__all__ else type_.__module__
        self.add_literal_import(pkgname, type_.__name__)

    def add_literal_import(self, pkgname, name):
        names = self.setdefault(pkgname, set())
        names.add(name)

    def render(self):
        return '\n'.join('from {0} import {1}'.format(package, ', '.join(sorted(names)))
                         for package, names in self.items())


class Model(object):
    def __init__(self, table):
        super(Model, self).__init__()
        self.table = table

        # Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        for column in table.columns:
            cls = column.type.__class__
            for supercls in cls.__mro__:
                if hasattr(supercls, '__visit_name__'):
                    cls = supercls
                if supercls.__name__ != supercls.__name__.upper():
                    break
            
            column.type = column.type.adapt(cls)

    def add_imports(self, collector):
        if self.table.columns:
            collector.add_import(Column)

        for column in self.table.columns:
            collector.add_import(column.type)

        for constraint in sorted(self.table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                if len(constraint.columns) > 1:
                    collector.add_literal_import('sqlalchemy', 'ForeignKeyConstraint')
                else:
                    collector.add_literal_import('sqlalchemy', 'ForeignKey')
            elif isinstance(constraint, UniqueConstraint):
                if len(constraint.columns) > 1:
                    collector.add_literal_import('sqlalchemy', 'UniqueConstraint')
            elif not isinstance(constraint, PrimaryKeyConstraint):
                collector.add_import(constraint)

        for index in self.table.indexes:
            if len(index.columns) > 1:
                collector.add_import(index)

    @staticmethod
    def _render_column(column, show_name):
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
            column.server_default = _get_compiled_expression(column.server_default.arg)
            kwarg.append('server_default')

        return 'Column({0})'.format(', '.join(
            ([repr(column.name)] if show_name else []) +
            ([_render_column_type(column.type)] if not dedicated_fks else []) +
            [repr(x) for x in dedicated_fks] +
            [repr(x) for x in column.constraints] +
            ['{0}={1}'.format(k, repr(getattr(column, k))) for k in kwarg]))

    @staticmethod
    def _render_constraint(constraint):
        if isinstance(constraint, ForeignKeyConstraint):
            local_columns = constraint.columns
            remote_columns = ['{0}.{1}'.format(fk.column.table.name, fk.column.name)
                              for fk in constraint.elements]
            return 'ForeignKeyConstraint({0!r}, {1!r})'.format(local_columns, remote_columns)
        elif isinstance(constraint, CheckConstraint):
            return 'CheckConstraint({0!r})'.format(_get_compiled_expression(constraint.sqltext))
        elif isinstance(constraint, UniqueConstraint):
            columns = [repr(col.name) for col in constraint.columns]
            return 'UniqueConstraint({0})'.format(', '.join(columns))

    @staticmethod
    def _render_index(index):
        columns = [repr(col.name) for col in index.columns]
        return 'Index({0!r}, {1})'.format(index.name, ', '.join(columns))


class ModelTable(Model):
    def add_imports(self, collector):
        super(ModelTable, self).add_imports(collector)
        collector.add_import(Table)

    def render(self):
        text = 't_{0} = Table(\n    {0!r}, metadata,\n'.format(self.table.name)

        for column in self.table.columns:
            text += '    {0},\n'.format(self._render_column(column, True))

        for constraint in sorted(self.table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and len(constraint.columns) == 1:
                continue
            text += '    {0},\n'.format(self._render_constraint(constraint))

        for index in self.table.indexes:
            if len(index.columns) > 1:
                text += '    {0},\n'.format(self._render_index(index))

        return text.rstrip('\n,') + '\n)'


class ModelClass(Model):
    def __init__(self, table, association_tables):
        super(ModelClass, self).__init__(table)
        self.name = _tablename_to_classname(table.name)
        self.children = []
        self.attributes = OrderedDict()

        # Assign attribute names for columns
        for column in table.columns:
            attrname = column.name + '_' if iskeyword(column.name) else column.name
            self.attributes[attrname] = column

        # Add many-to-one relationships
        for constraint in sorted(table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                target_cls = _tablename_to_classname(constraint.elements[0].column.table.name)
                self._add_relationship(ManyToOneRelationship(self.name, target_cls, constraint))

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = [c for c in association_table.constraints if isinstance(c, ForeignKeyConstraint)]
            fk_constraints.sort(key=_get_constraint_sort_key)
            target_cls = _tablename_to_classname(fk_constraints[1].elements[0].column.table.name)
            self._add_relationship(ManyToManyRelationship(self.name, target_cls, association_table))

    def _add_relationship(self, relationship):
        for attrname in relationship.suggested_names:
            if attrname not in self.attributes and not iskeyword(attrname):
                self.attributes[attrname] = relationship
                break

    def add_imports(self, collector):
        super(ModelClass, self).add_imports(collector)

        if any(isinstance(value, Relationship) for value in self.attributes.values()):
            collector.add_literal_import('sqlalchemy.orm', 'relationship')

        for child in self.children:
            child.add_imports(collector)

    def render(self, parentname='Base'):
        text = 'class {0}({1}):\n'.format(self.name, parentname)
        text += '    __tablename__ = {0!r}\n'.format(self.table.name)

        table_args = []
        for constraint in sorted(self.table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and len(constraint.columns) == 1:
                continue
            table_args.append(self._render_constraint(constraint))
        for index in self.table.indexes:
            if len(index.columns) > 1:
                table_args.append(self._render_index(index))
        if table_args:
            if len(table_args) == 1:
                table_args[0] += ','
            text += '    __table_args__ = (\n        {0}\n    )\n'.format(',\n        '.join(table_args))

        text += '\n'
        for attr, column in self.attributes.items():
            if isinstance(column, Column):
                show_name = attr != column.name
                text += '    {0} = {1}\n'.format(attr, self._render_column(column, show_name))

        if any(isinstance(value, Relationship) for value in self.attributes.values()):
            text += '\n'
        for attr, relationship in self.attributes.items():
            if isinstance(relationship, Relationship):
                text += '    {0} = {1}\n'.format(attr, relationship.render())

        for child_class in self.children:
            text += '\n' + child_class.render(self.classname)

        return text


class Relationship(object):
    def __init__(self, source_cls, target_cls):
        super(Relationship, self).__init__()
        self.source_cls = source_cls
        self.target_cls = target_cls
        self.kwargs = OrderedDict()

    @property
    def suggested_names(self):
        yield self.preferred_name if not iskeyword(self.preferred_name) else self.preferred_name + '_'

        iteration = 0
        while True:
            iteration += 1
            yield self.preferred_name + str(iteration)

    def render(self):
        text = 'relationship('
        args = [repr(self.target_cls)]

        if 'secondaryjoin' in self.kwargs:
            text += '\n        '
            delimiter, end = ',\n        ', '\n    )'
        else:
            delimiter, end = ', ', ')'

        args.extend([key + '=' + value for key, value in self.kwargs.items()])
        return text + delimiter.join(args) + end


class ManyToOneRelationship(Relationship):
    def __init__(self, source_cls, target_cls, constraint):
        super(ManyToOneRelationship, self).__init__(source_cls, target_cls)

        colname = constraint.columns[0]
        tablename = constraint.elements[0].column.table.name
        self.preferred_name = _singular(tablename) if not colname.endswith('_id') else colname[:-3]

        # Add uselist=False to One-to-One relationships
        if any(isinstance(c, (PrimaryKeyConstraint, UniqueConstraint)) and
               set(col.name for col in c.columns) == set(constraint.columns)
               for c in constraint.table.constraints):
            self.kwargs['uselist'] = 'False'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parent' if not colname.endswith('_id') else colname[:-3]
            pk_col_names = [col.name for col in constraint.table.primary_key]
            self.kwargs['remote_side'] = '[{0}]'.format(', '.join(pk_col_names))

        # If the two tables share more than one foreign key constraint,
        # SQLAlchemy needs an explicit primaryjoin to figure out which column(s) to join with
        common_fk_constraints = _get_common_fk_constraints(constraint.table, constraint.elements[0].column.table)
        if len(common_fk_constraints) > 1:
            self.kwargs['primaryjoin'] = "'{0}.{1} == {2}.{3}'".format(source_cls, constraint.columns[0], target_cls,
                                                                       constraint.elements[0].column.name)


class ManyToManyRelationship(Relationship):
    def __init__(self, source_cls, target_cls, assocation_table):
        super(ManyToManyRelationship, self).__init__(source_cls, target_cls)

        self.kwargs['secondary'] = repr(assocation_table.name)
        constraints = [c for c in assocation_table.constraints if isinstance(c, ForeignKeyConstraint)]
        constraints.sort(key=_get_constraint_sort_key)
        colname = constraints[1].columns[0]
        tablename = constraints[1].elements[0].column.table.name
        self.preferred_name = tablename if not colname.endswith('_id') else colname[:-3] + 's'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parents' if not colname.endswith('_id') else colname[:-3] + 's'
            pri_pairs = zip(constraints[0].columns, constraints[0].elements)
            sec_pairs = zip(constraints[1].columns, constraints[1].elements)
            pri_joins = ['{0}.{1} == {2}.c.{3}'.format(source_cls, elem.column.name, assocation_table.name, col)
                         for col, elem in pri_pairs]
            sec_joins = ['{0}.{1} == {2}.c.{3}'.format(target_cls, elem.column.name, assocation_table.name, col)
                         for col, elem in sec_pairs]
            self.kwargs['primaryjoin'] = (
                repr('and_({0})'.format(', '.join(pri_joins))) if len(pri_joins) > 1 else repr(pri_joins[0]))
            self.kwargs['secondaryjoin'] = (
                repr('and_({0})'.format(', '.join(sec_joins))) if len(sec_joins) > 1 else repr(sec_joins[0]))


class CodeGenerator(object):
    header = '# coding: utf-8'
    footer = ''

    def __init__(self, metadata, noindexes=False, noconstraints=False):
        super(CodeGenerator, self).__init__()
        self.models = []
        self.collector = ImportCollector()

        # Pick association tables from the metadata into their own set, don't process them normally
        links = defaultdict(lambda: [])
        association_tables = set()
        for table in metadata.tables.values():
            # Link tables have exactly two foreign key constraints and all columns are involved in them
            fk_constraints = [constr for constr in table.constraints if isinstance(constr, ForeignKeyConstraint)]
            if (len(fk_constraints) == 2 and all(col.foreign_keys for col in table.columns)):
                association_tables.add(table.name)
                tablename = sorted(fk_constraints, key=_get_constraint_sort_key)[0].elements[0].column.table.name
                links[tablename].append(table)

        # Iterate through the tables and create model classes when possible
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
            else:
                # Detect check constraints for boolean and enum columns
                for constraint in table.constraints.copy():
                    if isinstance(constraint, CheckConstraint):
                        sqltext = _get_compiled_expression(constraint.sqltext)
                        match = _re_boolean_check_constraint.match(sqltext)
                        if match:
                            colname = match.group(1)
                            table.constraints.remove(constraint)
                            table.c[colname].type = Boolean()
                            continue

                        match = _re_enum_check_constraint.match(sqltext)
                        if match:
                            colname = match.group(1)
                            items = match.group(2)
                            if isinstance(table.c[colname].type, String):
                                table.constraints.remove(constraint)
                                if not isinstance(table.c[colname].type, Enum):
                                    options = _re_enum_item.findall(items)
                                    table.c[colname].type = Enum(*options, native_enum=False)
                                continue

            # Only form model classes for tables that have a primary key and are not association tables
            if not table.primary_key or table.name in association_tables:
                model = ModelTable(table)
            else:
                model = ModelClass(table, links[table.name])

            self.models.append(model)
            model.add_imports(self.collector)

        if not any(isinstance(model, ModelClass) for model in self.models):
            self.collector.add_literal_import('sqlalchemy', 'MetaData')
        else:
            self.collector.add_literal_import('sqlalchemy.ext.declarative', 'declarative_base')

    def render(self, outfile=sys.stdout):
        print(self.header, file=outfile)

        # Render the collected imports
        print(self.collector.render() + '\n\n', file=outfile)

        if any(isinstance(model, ModelClass) for model in self.models):
            print('Base = declarative_base()\nmetadata = Base.metadata', file=outfile)
        else:
            print('metadata = MetaData()', file=outfile)

        # Render the model tables and classes
        for model in self.models:
            print('\n\n' + model.render().rstrip('\n'), file=outfile)

        if self.footer:
            print(self.footer, file=outfile)
