"""Contains the code generation logic and helper functions."""
import inspect
import re
import sys
from collections import defaultdict
from importlib import import_module
from inspect import FullArgSpec
from keyword import iskeyword
from typing import Any, Callable, Dict, List, Optional, Set, TextIO, Tuple, Union, cast

import sqlalchemy
import sqlalchemy.exc
from inflect import engine
from sqlalchemy import ARRAY, CheckConstraint, DefaultClause, Enum, Float
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.elements import ClauseElement, quoted_name
from sqlalchemy.sql.schema import (
    Column, ColumnCollectionConstraint, Constraint, ForeignKeyConstraint, Index, MetaData,
    PrimaryKeyConstraint, Table, UniqueConstraint)
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import Boolean, String
from sqlalchemy.util import OrderedDict

# SQLAlchemy 1.3.11+
try:
    from sqlalchemy import Computed
except ImportError:
    Computed = None  # type: ignore

# Conditionally import Geoalchemy2 to enable reflection support
try:
    import geoalchemy2  # noqa: F401
except ImportError:
    pass

# Support CIText and CIText[] in PostgreSQL via sqlalchemy-citext
try:
    import citext  # noqa: F401
except ImportError:
    pass

_re_boolean_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?).*\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")
_re_invalid_identifier = re.compile(r'[^a-zA-Z0-9_]' if sys.version_info[0] < 3 else r'(?u)\W')


class _DummyInflectEngine(engine):
    def singular_noun(self, text: str, count: Optional[Union[int, str]] = None,
                      gender: Optional[str] = None) -> Union[str, bool]:
        return text


def _get_column_names(constraint: ColumnCollectionConstraint) -> List[str]:
    return list(constraint.columns.keys())


def _get_constraint_sort_key(constraint: Constraint) -> str:
    if isinstance(constraint, CheckConstraint):
        return 'C{0}'.format(constraint.sqltext)
    elif isinstance(constraint, ColumnCollectionConstraint):
        return constraint.__class__.__name__[0] + repr(_get_column_names(constraint))
    else:
        return str(constraint)


class ImportCollector(OrderedDict):
    def add_import(self, obj: Any) -> None:
        type_ = type(obj) if not isinstance(obj, type) else obj
        pkgname = type_.__module__

        # The column types have already been adapted towards generic types if possible, so if this
        # is still a vendor specific type (e.g., MySQL INTEGER) be sure to use that rather than the
        # generic sqlalchemy type as it might have different constructor parameters.
        if pkgname.startswith('sqlalchemy.dialects.'):
            dialect_pkgname = '.'.join(pkgname.split('.')[0:3])
            dialect_pkg = import_module(dialect_pkgname)

            if type_.__name__ in dialect_pkg.__all__:  # type: ignore[attr-defined]
                pkgname = dialect_pkgname
        elif type_.__name__ in sqlalchemy.__all__:  # type: ignore[attr-defined]
            pkgname = 'sqlalchemy'
        else:
            pkgname = type_.__module__

        self.add_literal_import(pkgname, type_.__name__)

    def add_literal_import(self, pkgname: str, name: str) -> None:
        names = self.setdefault(pkgname, set())
        names.add(name)


class Relationship:
    def __init__(self, source_cls: str, target_cls: str) -> None:
        super(Relationship, self).__init__()
        self.source_cls = source_cls
        self.target_cls = target_cls
        self.kwargs: Dict[str, Any] = OrderedDict()


class ManyToOneRelationship(Relationship):
    def __init__(self, source_cls: str, target_cls: str, constraint: ForeignKeyConstraint,
                 inflect_engine: engine) -> None:
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
    def get_common_fk_constraints(table1: Table, table2: Table) -> Set[ForeignKeyConstraint]:
        """Returns a set of foreign key constraints the two tables have against each other."""
        c1 = set(c for c in table1.constraints if isinstance(c, ForeignKeyConstraint) and
                 c.elements[0].column.table == table2)
        c2 = set(c for c in table2.constraints if isinstance(c, ForeignKeyConstraint) and
                 c.elements[0].column.table == table1)
        return c1.union(c2)


class ManyToManyRelationship(Relationship):
    def __init__(self, source_cls: str, target_cls: str, assocation_table: Table) -> None:
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


AttributeType = Union[ManyToManyRelationship, ManyToOneRelationship, ColumnElement]


class Model:
    def __init__(self, table: Table, table_name_prefix: str = "") -> None:
        super().__init__()
        self.table = table
        self.schema = table.schema
        self.table_name_prefix = table_name_prefix

        # Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        for column in table.columns:
            if not isinstance(column.type, NullType):
                column.type = self._get_adapted_type(column.type, column.table.bind)

    def _get_adapted_type(self, coltype: Any, bind: Engine) -> Any:
        compiled_type = coltype.compile(bind.dialect)
        for supercls in coltype.__class__.__mro__:
            if not supercls.__name__.startswith('_') and hasattr(supercls, '__visit_name__'):
                # Hack to fix adaptation of the Enum class which is broken since SQLAlchemy 1.2
                kw = {}
                if supercls is Enum:
                    kw['name'] = coltype.name

                try:
                    new_coltype = coltype.adapt(supercls)
                except TypeError:
                    # If the adaptation fails, don't try again
                    break

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

    def add_imports(self, collector: ImportCollector) -> None:
        if self.table.columns:
            collector.add_import(Column)

        for column in self.table.columns:
            collector.add_import(column.type)
            if column.server_default:
                if Computed and isinstance(column.server_default, Computed):
                    collector.add_literal_import('sqlalchemy', 'Computed')
                else:
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

    @staticmethod
    def remove_prefix(text: str, prefix: str):
        if prefix == "":
            return text
        if text.startswith(prefix):
            return text[len(prefix):]
        return text  # or whatever

    @staticmethod
    def _convert_to_valid_identifier(name: Union[quoted_name, str], table_name_prefix="") -> str:
        assert name, 'Identifier cannot be empty'
        if name[0].isdigit() or iskeyword(name):
            name = '_' + name
        elif name == 'metadata':
            name = 'metadata_'

        name = Model.remove_prefix(name, table_name_prefix)

        return _re_invalid_identifier.sub('_', name)


class ModelTable(Model):
    def __init__(self, table: Table, table_name_prefix: str = "") -> None:
        super(ModelTable, self).__init__(table, table_name_prefix=table_name_prefix)
        self.name = self._convert_to_valid_identifier(table.name)

    def add_imports(self, collector: ImportCollector) -> None:
        super(ModelTable, self).add_imports(collector)
        collector.add_import(Table)


class ModelClass(Model):
    parent_name = 'Base'

    def __init__(self, table: Table, association_tables: List[Table], inflect_engine: engine,
                 detect_joined: bool, table_name_prefix: str = "") -> None:
        super(ModelClass, self).__init__(table, table_name_prefix=table_name_prefix)
        self.name = self._tablename_to_classname(table.name, inflect_engine, self.table_name_prefix)
        self.children: List[ModelClass] = []
        self.attributes: Dict[str, AttributeType] = OrderedDict()
        relationship_: Union[ManyToOneRelationship, ManyToManyRelationship]

        # Assign attribute names for columns
        for column in table.columns:
            self._add_attribute(column.name, column)

        # Add many-to-one relationships
        pk_column_names = set(col.name for col in table.primary_key.columns)
        for constraint in sorted(table.constraints, key=_get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                target_cls = self._tablename_to_classname(constraint.elements[0].column.table.name,
                                                          inflect_engine, self.table_name_prefix)
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
                fk_constraints[1].elements[0].column.table.name, inflect_engine, self.table_name_prefix)
            relationship_ = ManyToManyRelationship(self.name, target_cls, association_table)
            self._add_attribute(relationship_.preferred_name, relationship_)

    @classmethod
    def _tablename_to_classname(cls, tablename: str, inflect_engine: engine, table_name_prefix="") -> str:
        tablename = cls._convert_to_valid_identifier(tablename, table_name_prefix)
        camel_case_name = ''.join(part[:1].upper() + part[1:] for part in tablename.split('_'))
        return inflect_engine.singular_noun(camel_case_name) or camel_case_name

    def _add_attribute(self, attrname: Union[quoted_name, str], value: AttributeType) -> str:
        attrname = tempname = self._convert_to_valid_identifier(attrname, self.table_name_prefix)
        counter = 1
        while tempname in self.attributes:
            tempname = attrname + str(counter)
            counter += 1

        self.attributes[tempname] = value
        return tempname

    def add_imports(self, collector: ImportCollector) -> None:
        super(ModelClass, self).add_imports(collector)

        if any(isinstance(value, Relationship) for value in self.attributes.values()):
            collector.add_literal_import('sqlalchemy.orm', 'relationship')

        for child in self.children:
            child.add_imports(collector)


class CodeGenerator:
    template = """\
# coding: utf-8
{imports}

{metadata_declarations}


{models}"""

    def __init__(self, metadata: MetaData, noindexes: bool = False, noconstraints: bool = False,
                 nojoined: bool = False, noinflect: bool = False, noclasses: bool = False,
                 indentation: str = '    ', model_separator: str = '\n\n',
                 ignored_tables: Tuple[str, str] = ('alembic_version', 'migrate_version'),
                 table_model: type = ModelTable, class_model: type = ModelClass,
                 template: Optional[Any] = None, nocomments: bool = False,
                 table_name_prefix: str = "") -> None:
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
        self.table_name_prefix = table_name_prefix
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
        self.models: List[Model] = []
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
                            colname_match = _re_column_name.match(match.group(1))
                            if colname_match:
                                colname = colname_match.group(3)
                                table.constraints.remove(constraint)
                                table.c[colname].type = Boolean()
                                continue

                        # Turn any string-type column with a CheckConstraint like
                        # "column IN (...)" into an Enum
                        match = _re_enum_check_constraint.match(sqltext)
                        if match:
                            colname_match = _re_column_name.match(match.group(1))
                            if colname_match:
                                colname = colname_match.group(3)
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
                model = self.table_model(table, table_name_prefix=self.table_name_prefix)
            else:
                model = self.class_model(table, links[table.name], self.inflect_engine,
                                         not nojoined, table_name_prefix=self.table_name_prefix)
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

    def create_inflect_engine(self) -> engine:
        if self.noinflect:
            return _DummyInflectEngine()
        else:
            import inflect
            return inflect.engine()

    def render_imports(self) -> str:
        return '\n'.join('from {0} import {1}'.format(package, ', '.join(sorted(names)))
                         for package, names in self.collector.items())

    def render_metadata_declarations(self) -> str:
        if 'sqlalchemy.ext.declarative' in self.collector:
            return 'Base = declarative_base()\nmetadata = Base.metadata'
        return 'metadata = MetaData()'

    def _get_compiled_expression(self, statement: ClauseElement) -> str:
        """Return the statement in a form where any placeholders have been filled in."""
        return str(statement.compile(
            self.metadata.bind, compile_kwargs={"literal_binds": True}))

    @staticmethod
    def _getargspec_init(method: Callable) -> FullArgSpec:
        try:
            return inspect.getfullargspec(method)
        except TypeError:
            if method is object.__init__:
                return FullArgSpec(['self'], None, None, None, [], None, {})
            else:
                return FullArgSpec(['self'], 'args', 'kwargs', None, [], None, {})

    @classmethod
    def render_column_type(cls, coltype: Any) -> str:
        args = []
        kwargs: Dict[str, Any] = OrderedDict()
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
                kwargs[attr] = repr(value)
            else:
                args.append(repr(value))

        if argspec.varargs and hasattr(coltype, argspec.varargs):
            varargs_repr = [repr(arg) for arg in getattr(coltype, argspec.varargs)]
            args.extend(varargs_repr)

        if isinstance(coltype, Enum) and coltype.name is not None:
            kwargs['name'] = repr(coltype.name)

        for key, value in kwargs.items():
            args.append('{}={}'.format(key, value))

        rendered = coltype.__class__.__name__
        if args:
            rendered += '({0})'.format(', '.join(args))

        return rendered

    def render_constraint(self, constraint: Any) -> str:
        def render_fk_options(*opts: Any) -> str:
            options = [repr(opt) for opt in opts]
            for attr in 'ondelete', 'onupdate', 'deferrable', 'initially', 'match':
                value = getattr(constraint, attr, None)
                if value:
                    options.append('{0}={1!r}'.format(attr, value))

            return ', '.join(options)

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
        else:
            raise TypeError(f'Cannot render constraint of type {constraint.__class__.__name__}')

    @staticmethod
    def render_index(index: Index) -> str:
        extra_args = [repr(col.name) for col in index.columns]
        if index.unique:
            extra_args.append('unique=True')
        return 'Index({0!r}, {1})'.format(index.name, ', '.join(extra_args))

    def render_column(self, column: Column, show_name: bool) -> str:
        kwarg = []
        is_sole_pk = column.primary_key and len(column.table.primary_key) == 1
        dedicated_fks = [c for c in column.foreign_keys
                         if c.constraint and len(c.constraint.columns) == 1]
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

        if Computed and isinstance(column.server_default, Computed):
            expression = self._get_compiled_expression(column.server_default.sqltext)

            persist_arg = ''
            if column.server_default.persisted is not None:
                persist_arg = ', persisted={}'.format(column.server_default.persisted)

            server_default = 'Computed({!r}{})'.format(expression, persist_arg)
        elif isinstance(column.server_default, DefaultClause):
            # The quote escaping does not cover pathological cases but should mostly work
            default_expr = self._get_compiled_expression(column.server_default.arg)
            if '\n' in default_expr:
                server_default = 'server_default=text("""\\\n{0}""")'.format(default_expr)
            else:
                default_expr = default_expr.replace('"', '\\"')
                server_default = 'server_default=text("{0}")'.format(default_expr)

        comment = getattr(column, 'comment', None)
        server_default = ""
        return 'Column({0})'.format(', '.join(
            ([repr(column.name)] if show_name else []) +
            ([self.render_column_type(column.type)] if render_coltype else []) +
            [self.render_constraint(x) for x in dedicated_fks] +
            [repr(x) for x in column.constraints] +
            ['{0}={1}'.format(k, repr(getattr(column, k))) for k in kwarg] +
            ([server_default] if server_default else []) +
            (['comment={!r}'.format(comment)] if comment and not self.nocomments else [])
        ))

    def render_relationship(self, relationship: Relationship) -> str:
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

    def render_table(self, model: ModelTable) -> str:
        rendered = 't_{0} = Table(\n{2}{1!r}, metadata,\n'.format(
            model.name, model.table.name, self.indentation)

        for column in model.table.columns:
            # Cast is required because of a bug in the SQLAlchemy stubs regarding Table.columns
            rendered_column = self.render_column(cast(Column, column), True)
            rendered += '{0}{1},\n'.format(self.indentation, rendered_column)

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

        table_comment = getattr(model.table, 'comment', None)
        if table_comment:
            quoted_comment = table_comment.replace("'", "\\'").replace('"', '\\"')
            rendered += "{0}comment='{1}',\n".format(self.indentation, quoted_comment)

        return rendered.rstrip('\n,') + '\n)\n'

    def render_class(self, model: ModelClass) -> str:
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

        table_comment = getattr(model.table, 'comment', None)
        if table_comment:
            table_kwargs['comment'] = table_comment

        kwargs_items: Optional[str] = ', '.join('{0!r}: {1!r}'.format(key, table_kwargs[key])
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

    def render(self, outfile: TextIO = sys.stdout) -> None:
        rendered_models = []
        for model in self.models:
            if isinstance(model, self.class_model):
                rendered_models.append(self.render_class(cast(ModelClass, model)))
            elif isinstance(model, self.table_model):
                rendered_models.append(self.render_table(cast(ModelTable, model)))

        output = self.template.format(
            imports=self.render_imports(),
            metadata_declarations=self.render_metadata_declarations(),
            models=self.model_separator.join(rendered_models).rstrip('\n'))
        print(output, file=outfile)
