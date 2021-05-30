"""Contains the code generation logic and helper functions."""
import inspect
import re
import sys
from collections import defaultdict
from inspect import FullArgSpec
from typing import Any, Callable, Dict, List, Optional, TextIO, Tuple, Union, cast

from inflect import engine
from sqlalchemy import CheckConstraint, DefaultClause, Enum
from sqlalchemy.schema import ForeignKey
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.schema import (
    Column, ForeignKeyConstraint, Index, MetaData, PrimaryKeyConstraint, UniqueConstraint)
from sqlalchemy.types import Boolean, String
from sqlalchemy.util import OrderedDict

from .collector import ImportCollector
from .models import Model, ModelClass, ModelTable, Relationship
from .utils import get_column_names, get_constraint_sort_key

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

if sys.version_info < (3, 8):
    from importlib_metadata import version
else:
    from importlib.metadata import version

_re_boolean_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?).*\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")
sqla_version = tuple(int(x) for x in version('sqlalchemy').split('.')[:2])
declarative_package = 'sqlalchemy.ext.declarative' if sqla_version < (1, 4) else 'sqlalchemy.orm'


class _DummyInflectEngine(engine):
    def singular_noun(self, text: str, count: Optional[Union[int, str]] = None,
                      gender: Optional[str] = None) -> Union[str, bool]:
        return text


class CodeGenerator:
    template = """\
{imports}

{metadata_declarations}


{models}"""

    def __init__(self, metadata: MetaData, noindexes: bool = False, noconstraints: bool = False,
                 nojoined: bool = False, noinflect: bool = False, noclasses: bool = False,
                 indentation: str = '    ', model_separator: str = '\n\n',
                 ignored_tables: Tuple[str, str] = (
                     'alembic_version', 'migrate_version'),
                 table_model: type = ModelTable, class_model: type = ModelClass,
                 template: Optional[Any] = None, nocomments: bool = False,
                 base_class_name: Optional[str] = 'Base') -> None:
        super().__init__()
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
        self.base_class_name = base_class_name
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
                    fk_constraints, key=get_constraint_sort_key)[0].elements[0].column.table.name
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
                        sqltext = self._get_compiled_expression(
                            constraint.sqltext)

                        # Turn any integer-like column with a CheckConstraint like
                        # "column IN (0, 1)" into a Boolean
                        match = _re_boolean_check_constraint.match(sqltext)
                        if match:
                            colname_match = _re_column_name.match(
                                match.group(1))
                            if colname_match:
                                colname = colname_match.group(3)
                                table.constraints.remove(constraint)
                                table.c[colname].type = Boolean()
                                continue

                        # Turn any string-type column with a CheckConstraint like
                        # "column IN (...)" into an Enum
                        match = _re_enum_check_constraint.match(sqltext)
                        if match:
                            colname_match = _re_column_name.match(
                                match.group(1))
                            if colname_match:
                                colname = colname_match.group(3)
                                items = match.group(2)
                                if isinstance(table.c[colname].type, String):
                                    table.constraints.remove(constraint)
                                    if not isinstance(table.c[colname].type, Enum):
                                        options = _re_enum_item.findall(items)
                                        table.c[colname].type = Enum(
                                            *options, native_enum=False)
                                    continue

            # Only form model classes for tables that have a primary key and are not association
            # tables
            if noclasses or not table.primary_key or table.name in association_tables:
                model = self.table_model(table)
            else:
                model = self.class_model(table, links[table.name], self.inflect_engine,
                                         not nojoined, self.base_class_name)
                classes[model.name] = model

            self.models.append(model)
            model.add_imports(self.collector)

        # Nest inherited classes in their superclasses to ensure proper ordering
        for model in classes.values():
            if model.parent_name != self.base_class_name:
                classes[model.parent_name].children.append(model)
                self.models.remove(model)

        # Add either the MetaData or declarative_base import depending on whether there are mapped
        # classes or not
        if not any(isinstance(model, self.class_model) for model in self.models):
            self.collector.add_literal_import('sqlalchemy', 'MetaData')
        else:
            self.collector.add_literal_import(declarative_package, 'declarative_base')

    def create_inflect_engine(self) -> engine:
        if self.noinflect:
            return _DummyInflectEngine()
        else:
            import inflect
            return inflect.engine()

    def render_imports(self) -> str:
        groups = self.collector.group_imports()
        return '\n\n'.join('\n'.join(line for line in group) for group in groups)

    def render_metadata_declarations(self) -> str:
        if declarative_package in self.collector:
            return (f'{self.base_class_name} = declarative_base()\n'
                    f'metadata = {self.base_class_name}.metadata')

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
            varargs_repr = [repr(arg)
                            for arg in getattr(coltype, argspec.varargs)]
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
            local_columns = get_column_names(constraint)
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
            raise TypeError(
                f'Cannot render constraint of type {constraint.__class__.__name__}')

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
        has_index = any(set(i.columns) == {column}
                        for i in column.table.indexes)
        server_default = None

        # Render the column type if there are no foreign keys on it or any of them points back to
        # itself
        render_coltype = not dedicated_fks or any(
            fk.column is column for fk in dedicated_fks)

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
            expression = self._get_compiled_expression(
                column.server_default.sqltext)

            persist_arg = ''
            if column.server_default.persisted is not None:
                persist_arg = ', persisted={}'.format(
                    column.server_default.persisted)

            server_default = 'Computed({!r}{})'.format(expression, persist_arg)
        elif isinstance(column.server_default, DefaultClause):
            # The quote escaping does not cover pathological cases but should mostly work
            default_expr = self._get_compiled_expression(
                column.server_default.arg)
            if '\n' in default_expr:
                server_default = 'server_default=text("""\\\n{0}""")'.format(
                    default_expr)
            else:
                default_expr = default_expr.replace('"', '\\"')
                server_default = 'server_default=text("{0}")'.format(
                    default_expr)

        comment = getattr(column, 'comment', None)
        return 'Column({0})'.format(', '.join(
            ([repr(column.name)] if show_name else []) +
            ([self.render_column_type(column.type)] if render_coltype else []) +
            [self.render_constraint(x) for x in dedicated_fks] +
            [repr(x) for x in column.constraints] +
            ['{0}={1}'.format(k, repr(getattr(column, k))) for k in kwarg] +
            ([server_default] if server_default else []) +
            (['comment={!r}'.format(comment)]
             if comment and not self.nocomments else [])
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

        args.extend([key + '=' + value for key,
                    value in relationship.kwargs.items()])
        return rendered + delimiter.join(args) + end

    def render_table(self, model: ModelTable) -> str:
        rendered = 't_{0} = Table(\n{2}{1!r}, metadata,\n'.format(
            model.name, model.table.name, self.indentation)

        for column in model.table.columns:
            # Cast is required because of a bug in the SQLAlchemy stubs regarding Table.columns
            rendered_column = self.render_column(cast(Column, column), True)
            rendered += '{0}{1},\n'.format(self.indentation, rendered_column)

        for constraint in sorted(model.table.constraints, key=get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if (isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and
                    len(constraint.columns) == 1):
                continue

            rendered += '{0}{1},\n'.format(self.indentation,
                                           self.render_constraint(constraint))

        for index in model.table.indexes:
            if len(index.columns) > 1:
                rendered += '{0}{1},\n'.format(self.indentation,
                                               self.render_index(index))

        if model.schema:
            rendered += "{0}schema='{1}',\n".format(
                self.indentation, model.schema)

        table_comment = getattr(model.table, 'comment', None)
        if table_comment:
            quoted_comment = table_comment.replace(
                "'", "\\'").replace('"', '\\"')
            rendered += "{0}comment='{1}',\n".format(
                self.indentation, quoted_comment)

        return rendered.rstrip('\n,') + '\n)\n'

    def render_class(self, model: ModelClass) -> str:
        rendered = 'class {0}({1}):\n'.format(model.name, model.parent_name)
        rendered += '{0}__tablename__ = {1!r}\n'.format(
            self.indentation, model.table.name)

        # Render constraints and indexes as __table_args__
        table_args = []
        for constraint in sorted(model.table.constraints, key=get_constraint_sort_key):
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
            rendered += '{0}__table_args__ = {1}\n'.format(
                self.indentation, kwargs_items)
        elif table_args:
            if kwargs_items:
                table_args.append(kwargs_items)
            if len(table_args) == 1:
                table_args[0] += ','
            table_args_joined = ',\n{0}{0}'.format(
                self.indentation).join(table_args)
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
                rendered_models.append(
                    self.render_class(cast(ModelClass, model)))
            elif isinstance(model, self.table_model):
                rendered_models.append(
                    self.render_table(cast(ModelTable, model)))

        output = self.template.format(
            imports=self.render_imports(),
            metadata_declarations=self.render_metadata_declarations(),
            models=self.model_separator.join(rendered_models).rstrip('\n'))
        print(output, file=outfile)
