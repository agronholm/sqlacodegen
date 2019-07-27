"""Contains the code generation logic and helper functions."""
from __future__ import unicode_literals, division, print_function, absolute_import

import inspect
import re
import sys
from collections import defaultdict
from importlib import import_module
from inspect import ArgSpec
from keyword import iskeyword

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import (
    Enum, ForeignKeyConstraint, PrimaryKeyConstraint, CheckConstraint, UniqueConstraint, Table,
    Column, Float)
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import Boolean, String
from sqlalchemy.util import OrderedDict

# The generic ARRAY type was introduced in SQLAlchemy 1.1
try:
    from sqlalchemy import ARRAY
except ImportError:
    from sqlalchemy.dialects.postgresql import ARRAY

# Conditionally import Geoalchemy2 to enable reflection support
try:
    import geoalchemy2  # noqa: F401
except ImportError:
    pass

_re_boolean_check_constraint = re.compile(r"(?:(?:.*?)\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?)(?:.*)\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:(?:.*?)\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")
_re_invalid_identifier = re.compile(r'[^a-zA-Z0-9_]' if sys.version_info[0] < 3 else r'(?u)\W')


class _DummyInflectEngine(object):
    @staticmethod
    def singular_noun(noun):
        return noun


# In SQLAlchemy 0.x, constraint.columns is sometimes a list, on 1.x onwards, always a
# ColumnCollection
def _get_column_names(constraint):
    if isinstance(constraint.columns, list):
        return constraint.columns
    return list(constraint.columns.keys())


def _get_constraint_sort_key(constraint):
    if isinstance(constraint, CheckConstraint):
        return 'C{0}'.format(constraint.sqltext)
    return constraint.__class__.__name__[0] + repr(_get_column_names(constraint))


class ImportCollector(OrderedDict):
    def add_import(self, obj):
        type_ = type(obj) if not isinstance(obj, type) else obj
        pkgname = type_.__module__

        # The column types have already been adapted towards generic types if possible, so if this
        # is still a vendor specific type (e.g., MySQL INTEGER) be sure to use that rather than the
        # generic sqlalchemy type as it might have different constructor parameters.
        if pkgname.startswith('sqlalchemy.dialects.'):
            dialect_pkgname = '.'.join(pkgname.split('.')[0:3])
            dialect_pkg = import_module(dialect_pkgname)

            if type_.__name__ in dialect_pkg.__all__:
                pkgname = dialect_pkgname
        else:
            pkgname = 'sqlalchemy' if type_.__name__ in sqlalchemy.__all__ else type_.__module__
        self.add_literal_import(pkgname, type_.__name__)

    def add_literal_import(self, pkgname, name):
        names = self.setdefault(pkgname, set())
        names.add(name)


class Model(object):
    def __init__(self, table):
        super(Model, self).__init__()
        self.table = table
        self.schema = table.schema

        # Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        for column in table.columns:
            if not isinstance(column.type, NullType):
                column.type = self._get_adapted_type(column.type, column.table.bind)

    def _get_adapted_type(self, coltype, bind):
        compiled_type = coltype.compile(bind.dialect)
        for supercls in coltype.__class__.__mro__:
            if not supercls.__name__.startswith('_') and hasattr(supercls, '__visit_name__'):
                # Hack to fix adaptation of the Enum class which is broken since SQLAlchemy 1.2
                kw = {}
                if supercls is Enum:
                    kw['name'] = coltype.name

                new_coltype = coltype.adapt(supercls)
                for key, value in kw.items():
                    setattr(new_coltype, key, value)

                if isinstance(coltype, ARRAY):
                    new_coltype.item_type = self._get_adapted_type(new_coltype.item_type, bind)

                try:
                    # If the adapted column type does not render the same as the original, don't
                    # substitute it
                    if new_coltype.compile(bind.dialect) != compiled_type:
                        # Make an exception to the rule for Float and arrays of Float, since at
                        # least on PostgreSQL, Float can accurately represent both REAL and
                        # DOUBLE_PRECISION
                        if not isinstance(new_coltype, Float) and \
                           not (isinstance(new_coltype, ARRAY) and
                                isinstance(new_coltype.item_type, Float)):
                            break
                except sqlalchemy.exc.CompileError:
                    # If the adapted column type can't be compiled, don't substitute it
                    break

                # Stop on the first valid non-uppercase column type class
                coltype = new_coltype
                if supercls.__name__ != supercls.__name__.upper():
                    break

        return coltype

    def add_imports(self, collector):
        if self.table.columns:
            collector.add_import(Column)

        for column in self.table.columns:
            collector.add_import(column.type)
            if column.server_default:
                collector.add_literal_import('sqlalchemy', 'text')

            if isinstance(column.type, ARRAY):
                collector.add_import(column.type.item_type.__class__)

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


class ModelTable(Model):
    def add_imports(self, collector):
        super(ModelTable, self).add_imports(collector)
        collector.add_import(Table)


class ModelClass(Model):
    parent_name = 'Base'

    def __init__(self, table, association_tables, inflect_engine, detect_joined):
        super(ModelClass, self).__init__(table)
        self.name = self._tablename_to_classname(table.name, inflect_engine)
        self.children = []
        self.attributes = OrderedDict()

        # Assign attribute names for columns
        for column in table.columns:
            self._add_attribute(column.name, column)

        # Add many-to-one relationships
        pk_column_names = set(col.name for col in table.primary_key.columns)
        for constraint in sorted(table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                target_cls = self._tablename_to_classname(constraint.elements[0].column.table.name,
                                                          inflect_engine)
                if (detect_joined and self.parent_name == 'Base' and
                        set(_get_column_names(constraint)) == pk_column_names):
                    self.parent_name = target_cls
                else:
                    relationship_ = ManyToOneRelationship(self.name, target_cls, constraint,
                                                          inflect_engine)
                    self._add_attribute(relationship_.preferred_name, relationship_)

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = [c for c in association_table.constraints
                              if isinstance(c, ForeignKeyConstraint)]
            fk_constraints.sort(key=_get_constraint_sort_key)
            target_cls = self._tablename_to_classname(
                fk_constraints[1].elements[0].column.table.name, inflect_engine)
            relationship_ = ManyToManyRelationship(self.name, target_cls, association_table)
            self._add_attribute(relationship_.preferred_name, relationship_)

    @classmethod
    def _tablename_to_classname(cls, tablename, inflect_engine):
        tablename = cls._convert_to_valid_identifier(tablename)
        camel_case_name = ''.join(part[:1].upper() + part[1:] for part in tablename.split('_'))
        return inflect_engine.singular_noun(camel_case_name) or camel_case_name

    @staticmethod
    def _convert_to_valid_identifier(name):
        assert name, 'Identifier cannot be empty'
        if name[0].isdigit() or iskeyword(name):
            name = '_' + name
        elif name == 'metadata':
            name = 'metadata_'

        return _re_invalid_identifier.sub('_', name)

    def _add_attribute(self, attrname, value):
        attrname = tempname = self._convert_to_valid_identifier(attrname)
        counter = 1
        while tempname in self.attributes:
            tempname = attrname + str(counter)
            counter += 1

        self.attributes[tempname] = value
        return tempname

    def add_imports(self, collector):
        super(ModelClass, self).add_imports(collector)

        if any(isinstance(value, Relationship) for value in self.attributes.values()):
            collector.add_literal_import('sqlalchemy.orm', 'relationship')

        for child in self.children:
            child.add_imports(collector)


class Relationship(object):
    def __init__(self, source_cls, target_cls):
        super(Relationship, self).__init__()
        self.source_cls = source_cls
        self.target_cls = target_cls
        self.kwargs = OrderedDict()


class ManyToOneRelationship(Relationship):
    def __init__(self, source_cls, target_cls, constraint, inflect_engine):
        super(ManyToOneRelationship, self).__init__(source_cls, target_cls)

        column_names = _get_column_names(constraint)
        colname = column_names[0]
        tablename = constraint.elements[0].column.table.name
        if not colname.endswith('_id'):
            self.preferred_name = inflect_engine.singular_noun(tablename) or tablename
        else:
            self.preferred_name = colname[:-3]

        # Add uselist=False to One-to-One relationships
        if any(isinstance(c, (PrimaryKeyConstraint, UniqueConstraint)) and
               set(col.name for col in c.columns) == set(column_names)
               for c in constraint.table.constraints):
            self.kwargs['uselist'] = 'False'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parent' if not colname.endswith('_id') else colname[:-3]
            pk_col_names = [col.name for col in constraint.table.primary_key]
            self.kwargs['remote_side'] = '[{0}]'.format(', '.join(pk_col_names))

        # If the two tables share more than one foreign key constraint,
        # SQLAlchemy needs an explicit primaryjoin to figure out which column(s) to join with
        common_fk_constraints = self.get_common_fk_constraints(
            constraint.table, constraint.elements[0].column.table)
        if len(common_fk_constraints) > 1:
            self.kwargs['primaryjoin'] = "'{0}.{1} == {2}.{3}'".format(
                source_cls, column_names[0], target_cls, constraint.elements[0].column.name)

    @staticmethod
    def get_common_fk_constraints(table1, table2):
        """Returns a set of foreign key constraints the two tables have against each other."""
        c1 = set(c for c in table1.constraints if isinstance(c, ForeignKeyConstraint) and
                 c.elements[0].column.table == table2)
        c2 = set(c for c in table2.constraints if isinstance(c, ForeignKeyConstraint) and
                 c.elements[0].column.table == table1)
        return c1.union(c2)


class ManyToManyRelationship(Relationship):
    def __init__(self, source_cls, target_cls, assocation_table):
        super(ManyToManyRelationship, self).__init__(source_cls, target_cls)

        prefix = (assocation_table.schema + '.') if assocation_table.schema else ''
        self.kwargs['secondary'] = repr(prefix + assocation_table.name)
        constraints = [c for c in assocation_table.constraints
                       if isinstance(c, ForeignKeyConstraint)]
        constraints.sort(key=_get_constraint_sort_key)
        colname = _get_column_names(constraints[1])[0]
        tablename = constraints[1].elements[0].column.table.name
        self.preferred_name = tablename if not colname.endswith('_id') else colname[:-3] + 's'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parents' if not colname.endswith('_id') else colname[:-3] + 's'
            pri_pairs = zip(_get_column_names(constraints[0]), constraints[0].elements)
            sec_pairs = zip(_get_column_names(constraints[1]), constraints[1].elements)
            pri_joins = ['{0}.{1} == {2}.c.{3}'.format(source_cls, elem.column.name,
                                                       assocation_table.name, col)
                         for col, elem in pri_pairs]
            sec_joins = ['{0}.{1} == {2}.c.{3}'.format(target_cls, elem.column.name,
                                                       assocation_table.name, col)
                         for col, elem in sec_pairs]
            self.kwargs['primaryjoin'] = (
                repr('and_({0})'.format(', '.join(pri_joins)))
                if len(pri_joins) > 1 else repr(pri_joins[0]))
            self.kwargs['secondaryjoin'] = (
                repr('and_({0})'.format(', '.join(sec_joins)))
                if len(sec_joins) > 1 else repr(sec_joins[0]))


class CodeGenerator(object):
    template = """\
# coding: utf-8
{imports}

{metadata_declarations}


{models}"""

    def __init__(self, metadata, noindexes=False, noconstraints=False, nojoined=False,
                 noinflect=False, noclasses=False, indentation='    ', model_separator='\n\n',
                 ignored_tables=('alembic_version', 'migrate_version'), table_model=ModelTable,
                 class_model=ModelClass,  template=None, nocomments=False):
        super(CodeGenerator, self).__init__()
        self.metadata = metadata
        self.noindexes = noindexes
        self.noconstraints = noconstraints
        self.nojoined = nojoined
        self.noinflect = noinflect
        self.noclasses = noclasses
        self.indentation = indentation
        self.model_separator = model_separator
        self.ignored_tables = ignored_tables
        self.table_model = table_model
        self.class_model = class_model
        self.nocomments = nocomments
        self.inflect_engine = self.create_inflect_engine()
        if template:
            self.template = template

        # Pick association tables from the metadata into their own set, don't process them normally
        links = defaultdict(lambda: [])
        association_tables = set()
        for table in metadata.tables.values():
            # Link tables have exactly two foreign key constraints and all columns are involved in
            # them
            fk_constraints = [constr for constr in table.constraints
                              if isinstance(constr, ForeignKeyConstraint)]
            if len(fk_constraints) == 2 and all(col.foreign_keys for col in table.columns):
                association_tables.add(table.name)
                tablename = sorted(
                    fk_constraints, key=_get_constraint_sort_key)[0].elements[0].column.table.name
                links[tablename].append(table)

        # Iterate through the tables and create model classes when possible
        self.models = []
        self.collector = ImportCollector()
        classes = {}
        for table in metadata.sorted_tables:
            # Support for Alembic and sqlalchemy-migrate -- never expose the schema version tables
            if table.name in self.ignored_tables:
                continue

            if noindexes:
                table.indexes.clear()

            if noconstraints:
                table.constraints = {table.primary_key}
                table.foreign_keys.clear()
                for col in table.columns:
                    col.foreign_keys.clear()
            else:
                # Detect check constraints for boolean and enum columns
                for constraint in table.constraints.copy():
                    if isinstance(constraint, CheckConstraint):
                        sqltext = self._get_compiled_expression(constraint.sqltext)

                        # Turn any integer-like column with a CheckConstraint like
                        # "column IN (0, 1)" into a Boolean
                        match = _re_boolean_check_constraint.match(sqltext)
                        if match:
                            colname = _re_column_name.match(match.group(1)).group(3)
                            table.constraints.remove(constraint)
                            table.c[colname].type = Boolean()
                            continue

                        # Turn any string-type column with a CheckConstraint like
                        # "column IN (...)" into an Enum
                        match = _re_enum_check_constraint.match(sqltext)
                        if match:
                            colname = _re_column_name.match(match.group(1)).group(3)
                            items = match.group(2)
                            if isinstance(table.c[colname].type, String):
                                table.constraints.remove(constraint)
                                if not isinstance(table.c[colname].type, Enum):
                                    options = _re_enum_item.findall(items)
                                    table.c[colname].type = Enum(*options, native_enum=False)
                                continue

            # Only form model classes for tables that have a primary key and are not association
            # tables
            if noclasses or not table.primary_key or table.name in association_tables:
                model = self.table_model(table)
            else:
                model = self.class_model(table, links[table.name], self.inflect_engine,
                                         not nojoined)
                classes[model.name] = model

            self.models.append(model)
            model.add_imports(self.collector)

        # Nest inherited classes in their superclasses to ensure proper ordering
        for model in classes.values():
            if model.parent_name != 'Base':
                classes[model.parent_name].children.append(model)
                self.models.remove(model)

        # Add either the MetaData or declarative_base import depending on whether there are mapped
        # classes or not
        if not any(isinstance(model, self.class_model) for model in self.models):
            self.collector.add_literal_import('sqlalchemy', 'MetaData')
        else:
            self.collector.add_literal_import('sqlalchemy.ext.declarative', 'declarative_base')

    def create_inflect_engine(self):
        if self.noinflect:
            return _DummyInflectEngine()
        else:
            import inflect
            return inflect.engine()

    def render_imports(self):
        return '\n'.join('from {0} import {1}'.format(package, ', '.join(sorted(names)))
                         for package, names in self.collector.items())

    def render_metadata_declarations(self):
        if 'sqlalchemy.ext.declarative' in self.collector:
            return 'Base = declarative_base()\nmetadata = Base.metadata'
        return 'metadata = MetaData()'

    def _get_compiled_expression(self, statement):
        """Return the statement in a form where any placeholders have been filled in."""
        return str(statement.compile(
            self.metadata.bind, compile_kwargs={"literal_binds": True}))

    @staticmethod
    def _getargspec_init(method):
        try:
            if hasattr(inspect, 'getfullargspec'):
                return inspect.getfullargspec(method)
            else:
                return inspect.getargspec(method)
        except TypeError:
            if method is object.__init__:
                return ArgSpec(['self'], None, None, None)
            else:
                return ArgSpec(['self'], 'args', 'kwargs', None)

    @classmethod
    def render_column_type(cls, coltype):
        args = []
        if isinstance(coltype, Enum):
            args.extend(repr(arg) for arg in coltype.enums)
            if coltype.name is not None:
                args.append('name={0!r}'.format(coltype.name))
        else:
            # All other types
            argspec = cls._getargspec_init(coltype.__class__.__init__)
            defaults = dict(zip(argspec.args[-len(argspec.defaults or ()):],
                                argspec.defaults or ()))
            missing = object()
            use_kwargs = False
            for attr in argspec.args[1:]:
                # Remove annoyances like _warn_on_bytestring
                if attr.startswith('_'):
                    continue

                value = getattr(coltype, attr, missing)
                default = defaults.get(attr, missing)
                if value is missing or value == default:
                    use_kwargs = True
                elif use_kwargs:
                    args.append('{0}={1}'.format(attr, repr(value)))
                else:
                    args.append(repr(value))

        rendered = coltype.__class__.__name__
        if args:
            rendered += '({0})'.format(', '.join(args))

        return rendered

    def render_constraint(self, constraint):
        def render_fk_options(*opts):
            opts = [repr(opt) for opt in opts]
            for attr in 'ondelete', 'onupdate', 'deferrable', 'initially', 'match':
                value = getattr(constraint, attr, None)
                if value:
                    opts.append('{0}={1!r}'.format(attr, value))

            return ', '.join(opts)

        if isinstance(constraint, ForeignKey):
            remote_column = '{0}.{1}'.format(constraint.column.table.fullname,
                                             constraint.column.name)
            return 'ForeignKey({0})'.format(render_fk_options(remote_column))
        elif isinstance(constraint, ForeignKeyConstraint):
            local_columns = _get_column_names(constraint)
            remote_columns = ['{0}.{1}'.format(fk.column.table.fullname, fk.column.name)
                              for fk in constraint.elements]
            return 'ForeignKeyConstraint({0})'.format(
                render_fk_options(local_columns, remote_columns))
        elif isinstance(constraint, CheckConstraint):
            return 'CheckConstraint({0!r})'.format(
                self._get_compiled_expression(constraint.sqltext))
        elif isinstance(constraint, UniqueConstraint):
            columns = [repr(col.name) for col in constraint.columns]
            return 'UniqueConstraint({0})'.format(', '.join(columns))

    @staticmethod
    def render_index(index):
        extra_args = [repr(col.name) for col in index.columns]
        if index.unique:
            extra_args.append('unique=True')
        return 'Index({0!r}, {1})'.format(index.name, ', '.join(extra_args))

    def render_column(self, column, show_name):
        kwarg = []
        is_sole_pk = column.primary_key and len(column.table.primary_key) == 1
        dedicated_fks = [c for c in column.foreign_keys if len(c.constraint.columns) == 1]
        is_unique = any(isinstance(c, UniqueConstraint) and set(c.columns) == {column}
                        for c in column.table.constraints)
        is_unique = is_unique or any(i.unique and set(i.columns) == {column}
                                     for i in column.table.indexes)
        has_index = any(set(i.columns) == {column} for i in column.table.indexes)
        server_default = None

        # Render the column type if there are no foreign keys on it or any of them points back to
        # itself
        render_coltype = not dedicated_fks or any(fk.column is column for fk in dedicated_fks)

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
        if column.server_default:
            # The quote escaping does not cover pathological cases but should mostly work
            default_expr = self._get_compiled_expression(column.server_default.arg)
            if '\n' in default_expr:
                server_default = 'server_default=text("""\\\n{0}""")'.format(default_expr)
            else:
                default_expr = default_expr.replace('"', '\\"')
                server_default = 'server_default=text("{0}")'.format(default_expr)

        comment = getattr(column, 'comment', None)
        return 'Column({0})'.format(', '.join(
            ([repr(column.name)] if show_name else []) +
            ([self.render_column_type(column.type)] if render_coltype else []) +
            [self.render_constraint(x) for x in dedicated_fks] +
            [repr(x) for x in column.constraints] +
            ['{0}={1}'.format(k, repr(getattr(column, k))) for k in kwarg] +
            ([server_default] if server_default else []) +
            (['comment={!r}'.format(comment)] if comment else [])
        ))

    def render_relationship(self, relationship):
        rendered = 'relationship('
        args = [repr(relationship.target_cls)]

        if 'secondaryjoin' in relationship.kwargs:
            rendered += '\n{0}{0}'.format(self.indentation)
            delimiter, end = (',\n{0}{0}'.format(self.indentation),
                              '\n{0})'.format(self.indentation))
        else:
            delimiter, end = ', ', ')'

        args.extend([key + '=' + value for key, value in relationship.kwargs.items()])
        return rendered + delimiter.join(args) + end

    def render_table(self, model):
        rendered = 't_{0} = Table(\n{1}{0!r}, metadata,\n'.format(
            model.table.name, self.indentation)

        for column in model.table.columns:
            rendered += '{0}{1},\n'.format(self.indentation, self.render_column(column, True))

        for constraint in sorted(model.table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if (isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and
                    len(constraint.columns) == 1):
                continue
            rendered += '{0}{1},\n'.format(self.indentation, self.render_constraint(constraint))

        for index in model.table.indexes:
            if len(index.columns) > 1:
                rendered += '{0}{1},\n'.format(self.indentation, self.render_index(index))

        if model.schema:
            rendered += "{0}schema='{1}',\n".format(self.indentation, model.schema)

        return rendered.rstrip('\n,') + '\n)\n'

    def render_class(self, model):
        rendered = 'class {0}({1}):\n'.format(model.name, model.parent_name)
        rendered += '{0}__tablename__ = {1!r}\n'.format(self.indentation, model.table.name)

        # Render constraints and indexes as __table_args__
        table_args = []
        for constraint in sorted(model.table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if (isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and
                    len(constraint.columns) == 1):
                continue
            table_args.append(self.render_constraint(constraint))
        for index in model.table.indexes:
            if len(index.columns) > 1:
                table_args.append(self.render_index(index))

        table_kwargs = {}
        if model.schema:
            table_kwargs['schema'] = model.schema

        kwargs_items = ', '.join('{0!r}: {1!r}'.format(key, table_kwargs[key])
                                 for key in table_kwargs)
        kwargs_items = '{{{0}}}'.format(kwargs_items) if kwargs_items else None
        if table_kwargs and not table_args:
            rendered += '{0}__table_args__ = {1}\n'.format(self.indentation, kwargs_items)
        elif table_args:
            if kwargs_items:
                table_args.append(kwargs_items)
            if len(table_args) == 1:
                table_args[0] += ','
            table_args_joined = ',\n{0}{0}'.format(self.indentation).join(table_args)
            rendered += '{0}__table_args__ = (\n{0}{0}{1}\n{0})\n'.format(
                self.indentation, table_args_joined)

        # Render columns
        rendered += '\n'
        for attr, column in model.attributes.items():
            if isinstance(column, Column):
                show_name = attr != column.name
                rendered += '{0}{1} = {2}\n'.format(
                    self.indentation, attr, self.render_column(column, show_name))

        # Render relationships
        if any(isinstance(value, Relationship) for value in model.attributes.values()):
            rendered += '\n'
        for attr, relationship in model.attributes.items():
            if isinstance(relationship, Relationship):
                rendered += '{0}{1} = {2}\n'.format(
                    self.indentation, attr, self.render_relationship(relationship))

        # Render subclasses
        for child_class in model.children:
            rendered += self.model_separator + self.render_class(child_class)

        return rendered

    def render(self, outfile=sys.stdout):
        rendered_models = []
        for model in self.models:
            if isinstance(model, self.class_model):
                rendered_models.append(self.render_class(model))
            elif isinstance(model, self.table_model):
                rendered_models.append(self.render_table(model))

        output = self.template.format(
            imports=self.render_imports(),
            metadata_declarations=self.render_metadata_declarations(),
            models=self.model_separator.join(rendered_models).rstrip('\n'))
        print(output, file=outfile)
