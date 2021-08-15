import inspect
import re
import sys
from abc import ABCMeta, abstractmethod
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from importlib import import_module
from inspect import Parameter
from itertools import count
from keyword import iskeyword
from pprint import pformat
from textwrap import indent
from typing import Any, ClassVar, Collection, DefaultDict, Dict, Iterable, List, Optional, Set

import inflect
import sqlalchemy
from sqlalchemy import (
    ARRAY, Boolean, CheckConstraint, Column, Constraint, DefaultClause, Enum, Float, ForeignKey,
    ForeignKeyConstraint, Index, MetaData, PrimaryKeyConstraint, String, Table, Text,
    UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connectable
from sqlalchemy.exc import CompileError

from .models import (
    ColumnAttribute, JoinType, Model, ModelClass, RelationshipAttribute, RelationshipType)
from .utils import (
    get_column_names, get_common_fk_constraints, get_compiled_expression, get_constraint_sort_key)

if sys.version_info < (3, 8):
    from importlib_metadata import version
else:
    from importlib.metadata import version

# SQLAlchemy 1.3.11+
try:
    from sqlalchemy import Computed
except ImportError:
    Computed = None  # type: ignore

# SQLAlchemy 1.4+
try:
    from sqlalchemy import Identity
except ImportError:
    Identity = None

_sqla_version = tuple(int(x) for x in version('sqlalchemy').split('.')[:2])
_re_boolean_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?).*\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")
_re_invalid_identifier = re.compile(r'(?u)\W')


class CodeGenerator(metaclass=ABCMeta):
    valid_options: ClassVar[Set[str]] = set()

    def __init__(self, metadata: MetaData, bind: Connectable, options: Set[str]):
        self.metadata = metadata
        self.bind = bind
        self.options = options

    @abstractmethod
    def generate(self) -> str:
        """
        Generate the code for the given metadata.

        .. note:: May modify the metadata.
        """


@dataclass(eq=False)
class TablesGenerator(CodeGenerator):
    valid_options: ClassVar[Set[str]] = {'noindexes', 'noconstraints', 'nocomments'}
    builtin_module_names: ClassVar[Set[str]] = set(sys.builtin_module_names) | {'dataclasses'}

    def __init__(self, metadata: MetaData, bind: Connectable, options: Set[str], *,
                 indentation: str = '    '):
        # Validate options
        invalid_options = {opt for opt in options if opt not in self.valid_options}
        if invalid_options:
            raise ValueError('Unrecognized options: ' + ', '.join(invalid_options))

        super().__init__(metadata, bind, options)
        self.indentation = indentation
        self.imports: Dict[str, Set] = defaultdict(set)

    def generate(self) -> str:
        sections: List[str] = []

        # Remove unwanted elements from the metadata
        for table in list(self.metadata.tables.values()):
            if self.should_ignore_table(table):
                self.metadata.remove(table)
                continue

            if 'noindexes' in self.options:
                table.indexes.clear()

            if 'noconstraints' in self.options:
                table.constraints.clear()

            if 'nocomments' in self.options:
                table.comment = None

            for column in table.columns:
                if 'nocomments' in self.options:
                    column.comment = None

        # Use information from column constraints to figure out the intended column types
        for table in self.metadata.tables.values():
            self.fix_column_types(table)

        # Generate the models
        models: List[Model] = self.generate_models()

        # Render collected imports
        groups = self.group_imports()
        imports = '\n\n'.join('\n'.join(line for line in group) for group in groups)
        if imports:
            sections.append(imports)

        # Render module level variables
        variables = self.render_module_variables(models)
        if variables:
            sections.append(variables + '\n')

        # Render models
        rendered_models = self.render_models(models)
        if rendered_models:
            sections.append(rendered_models)

        return '\n\n'.join(sections) + '\n'

    def collect_imports(self, models: Iterable[Model]) -> None:
        self.collect_global_imports()
        for model in models:
            self.collect_imports_for_model(model)

    def collect_global_imports(self) -> None:
        self.add_import(MetaData)

    def collect_imports_for_model(self, model: Model) -> None:
        if model.__class__ is Model:
            self.add_import(Table)

        for column in model.table.c:
            self.collect_imports_for_column(column)

        for constraint in model.table.constraints:
            self.collect_imports_for_constraint(constraint)

        for index in model.table.indexes:
            self.collect_imports_for_index(index)

    def collect_imports_for_column(self, column: Column) -> None:
        self.add_import(Column)
        self.add_import(column.type)

        if isinstance(column.type, ARRAY):
            self.add_import(column.type.item_type.__class__)
        elif isinstance(column.type, JSONB):
            if (not isinstance(column.type.astext_type, Text)
                    or column.type.astext_type.length is not None):
                self.add_import(column.type.astext_type)

        if column.server_default:
            if isinstance(column.server_default, DefaultClause):
                self.add_literal_import('sqlalchemy', 'text')
            else:
                self.add_import(column.server_default)

    def collect_imports_for_constraint(self, constraint: Constraint) -> None:
        if isinstance(constraint, PrimaryKeyConstraint):
            pass
        elif isinstance(constraint, UniqueConstraint):
            if len(constraint.columns) > 1:
                self.add_literal_import('sqlalchemy', 'UniqueConstraint')
        elif isinstance(constraint, ForeignKeyConstraint):
            if len(constraint.columns) > 1:
                self.add_literal_import('sqlalchemy', 'ForeignKeyConstraint')
            else:
                self.add_import(ForeignKey)
        else:
            self.add_import(constraint)

    def collect_imports_for_index(self, index: Index) -> None:
        if len(index.columns) > 1:
            self.add_import(Index)

    def add_import(self, obj: Any) -> None:
        # Don't store builtin imports
        if getattr(obj, '__module__', 'builtins') == 'builtins':
            return

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
        names = self.imports.setdefault(pkgname, set())
        names.add(name)

    def group_imports(self) -> List[List[str]]:
        future_imports: List[str] = []
        stdlib_imports: List[str] = []
        thirdparty_imports: List[str] = []

        for package in sorted(self.imports):
            imports = ', '.join(sorted(self.imports[package]))
            collection = thirdparty_imports
            if package == '__future__':
                collection = future_imports
            elif package in self.builtin_module_names:
                collection = stdlib_imports
            elif package in sys.modules and 'site-packages' not in sys.modules[package].__file__:
                collection = stdlib_imports

            collection.append(f'from {package} import {imports}')

        return [group for group in (future_imports, stdlib_imports, thirdparty_imports) if group]

    def generate_models(self) -> List[Model]:
        models = [Model(table) for table in self.metadata.sorted_tables]

        # Collect the imports
        self.collect_imports(models)

        # Generate names for models
        global_names = set(name for namespace in self.imports.values() for name in namespace)
        for model in models:
            self.generate_model_name(model, global_names)
            global_names.add(model.name)

        return models

    def generate_model_name(self, model: Model, global_names: Set[str]) -> None:
        preferred_name = f't_{model.table.name}'
        model.name = self.find_free_name(preferred_name, global_names)

    def render_module_variables(self, models: List[Model]) -> str:
        return 'metadata = MetaData()'

    def render_models(self, models: List[Model]) -> str:
        rendered = []
        for model in models:
            rendered_table = self.render_table(model.table)
            rendered.append(f'{model.name} = {rendered_table}')

        return '\n\n'.join(rendered)

    def render_table(self, table: Table) -> str:
        args: List[str] = [f'{table.name!r}, metadata']
        for column in table.columns:
            # Cast is required because of a bug in the SQLAlchemy stubs regarding Table.columns
            args.append(self.render_column(column, True))

        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if (isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and
                    len(constraint.columns) == 1):
                continue

            args.append(self.render_constraint(constraint))

        for index in table.indexes:
            # One-column indexes should be rendered as index=True on columns
            if len(index.columns) > 1:
                args.append(self.render_index(index))

        if table.schema:
            args.append(f'schema={table.schema!r}')

        table_comment = getattr(table, 'comment', None)
        if table_comment:
            args.append(f'comment={table_comment!r}')

        rendered_args = f',\n{self.indentation}'.join(args)
        return f'Table(\n{self.indentation}{rendered_args}\n)'

    def render_index(self, index: Index) -> str:
        extra_args = [repr(col.name) for col in index.columns]
        if index.unique:
            extra_args.append('unique=True')

        return f'Index({index.name!r}, {", ".join(extra_args)})'

    def render_column(self, column: Column, show_name: bool) -> str:
        args = []
        kwargs = OrderedDict()
        kwarg = []
        is_sole_pk = column.primary_key and len(column.table.primary_key) == 1
        dedicated_fks = [c for c in column.foreign_keys
                         if c.constraint and len(c.constraint.columns) == 1]
        is_unique = any(isinstance(c, UniqueConstraint) and set(c.columns) == {column}
                        for c in column.table.constraints)
        is_unique = is_unique or any(i.unique and set(i.columns) == {column}
                                     for i in column.table.indexes)
        has_index = any(set(i.columns) == {column} for i in column.table.indexes)

        if show_name:
            args.append(repr(column.name))

        # Render the column type if there are no foreign keys on it or any of them points back to
        # itself
        if not dedicated_fks or any(fk.column is column for fk in dedicated_fks):
            args.append(self.render_column_type(column.type))

        for constraint in dedicated_fks:
            args.append(self.render_constraint(constraint))

        for constraint in column.constraints:
            args.append(repr(constraint))

        if column.key != column.name:
            kwargs['key'] = column.key
        if column.primary_key:
            kwargs['primary_key'] = True
        if not column.nullable and not is_sole_pk:
            kwargs['nullable'] = False

        if is_unique:
            column.unique = True
            kwargs['unique'] = True
        elif has_index:
            column.index = True
            kwarg.append('index')
            kwargs['index'] = True

        if isinstance(column.server_default, DefaultClause):
            kwargs['server_default'] = f'text({column.server_default.arg.text!r})'
        elif Computed and isinstance(column.server_default, Computed):
            expression = column.server_default.sqltext.text

            persist_arg = ''
            if column.server_default.persisted is not None:
                persist_arg = f', persisted={column.server_default.persisted}'

            args.append(f'Computed({expression!r}{persist_arg})')
        elif Identity and isinstance(column.server_default, Identity):
            args.append(repr(column.server_default))
        elif column.server_default:
            kwargs['server_default'] = repr(column.server_default)

        comment = getattr(column, 'comment', None)
        if comment:
            kwargs['comment'] = repr(comment)

        for key, value in kwargs.items():
            args.append(f'{key}={value}')

        return f'Column({", ".join(args)})'

    def render_column_type(self, coltype: Any) -> str:
        args = []
        kwargs: Dict[str, Any] = OrderedDict()
        sig = inspect.signature(coltype.__class__.__init__)
        defaults = {param.name: param.default for param in sig.parameters.values()}
        missing = object()
        use_kwargs = False
        for param in list(sig.parameters.values())[1:]:
            # Remove annoyances like _warn_on_bytestring
            if param.name.startswith('_'):
                continue
            elif param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
                continue

            value = getattr(coltype, param.name, missing)
            default = defaults.get(param.name, missing)
            if value is missing or value == default:
                use_kwargs = True
            elif use_kwargs:
                kwargs[param.name] = repr(value)
            else:
                args.append(repr(value))

        vararg = next((param.name for param in sig.parameters.values()
                       if param.kind is Parameter.VAR_POSITIONAL), None)
        if vararg and hasattr(coltype, vararg):
            varargs_repr = [repr(arg) for arg in getattr(coltype, vararg)]
            args.extend(varargs_repr)

        if isinstance(coltype, Enum) and coltype.name is not None:
            kwargs['name'] = repr(coltype.name)

        if isinstance(coltype, JSONB):
            # Remove astext_type if it's the default
            if isinstance(coltype.astext_type, Text) and coltype.astext_type.length is None:
                del kwargs['astext_type']

        for key, value in kwargs.items():
            args.append(f'{key}={value}')

        rendered = coltype.__class__.__name__
        if args:
            rendered += f"({', '.join(args)})"

        return rendered

    def render_constraint(self, constraint: Any) -> str:
        def render_fk_options(*opts: Any) -> str:
            options = [repr(opt) for opt in opts]
            for attr in 'ondelete', 'onupdate', 'deferrable', 'initially', 'match':
                value = getattr(constraint, attr, None)
                if value:
                    options.append(f'{attr}={value!r}')

            return ', '.join(options)

        if isinstance(constraint, ForeignKey):
            remote_column = f'{constraint.column.table.fullname}.{constraint.column.name}'
            options = render_fk_options(remote_column)
            return f'ForeignKey({options})'
        elif isinstance(constraint, ForeignKeyConstraint):
            local_columns = get_column_names(constraint)
            remote_columns = [f'{fk.column.table.fullname}.{fk.column.name}'
                              for fk in constraint.elements]
            options = render_fk_options(local_columns, remote_columns)
            return f'ForeignKeyConstraint({options})'
        elif isinstance(constraint, CheckConstraint):
            expression = get_compiled_expression(constraint.sqltext, self.bind)
            return f'CheckConstraint({expression!r})'
        elif isinstance(constraint, UniqueConstraint):
            columns = ', '.join(repr(col.name) for col in constraint.columns)
            return f'UniqueConstraint({columns})'
        else:
            raise TypeError(f'Cannot render constraint of type {constraint.__class__.__name__}')

    def should_ignore_table(self, table: Table) -> bool:
        # Support for Alembic and sqlalchemy-migrate -- never expose the schema version tables
        return table.name in ('alembic_version', 'migrate_version')

    def find_free_name(self, name: str, global_names: Set[str],
                       local_names: Collection[str] = ()) -> str:
        """Generate an attribute name that does not clash with other local or global names."""
        assert name, 'Identifier cannot be empty'
        name = _re_invalid_identifier.sub('_', name)
        if name[0].isdigit():
            name = '_' + name
        elif iskeyword(name):
            name += '_'

        original = name
        for i in count():
            if name not in global_names and name not in local_names:
                break

            name = original + (str(i) if i else '_')

        return name

    def fix_column_types(self, table: Table) -> None:
        """Adjust the reflected column types."""
        # Detect check constraints for boolean and enum columns
        for constraint in table.constraints.copy():
            if isinstance(constraint, CheckConstraint):
                sqltext = get_compiled_expression(constraint.sqltext, self.bind)

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

        for column in table.c:
            try:
                column.type = self.get_adapted_type(column.type)
            except CompileError:
                pass

    def get_adapted_type(self, coltype: Any) -> Any:
        compiled_type = coltype.compile(self.bind.dialect)
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
                    new_coltype.item_type = self.get_adapted_type(new_coltype.item_type)

                try:
                    # If the adapted column type does not render the same as the original, don't
                    # substitute it
                    if new_coltype.compile(self.bind.dialect) != compiled_type:
                        # Make an exception to the rule for Float and arrays of Float, since at
                        # least on PostgreSQL, Float can accurately represent both REAL and
                        # DOUBLE_PRECISION
                        if not isinstance(new_coltype, Float) and \
                           not (isinstance(new_coltype, ARRAY) and
                                isinstance(new_coltype.item_type, Float)):
                            break
                except CompileError:
                    # If the adapted column type can't be compiled, don't substitute it
                    break

                # Stop on the first valid non-uppercase column type class
                coltype = new_coltype
                if supercls.__name__ != supercls.__name__.upper():
                    break

        return coltype


class DeclarativeGenerator(TablesGenerator):
    valid_options: ClassVar[Set[str]] = TablesGenerator.valid_options | {'use_inflect', 'nojoined',
                                                                         'nobidi'}

    def __init__(self, metadata: MetaData, bind: Connectable, options: Set[str], *,
                 indentation: str = '    ', base_class_name: str = 'Base'):
        super().__init__(metadata, bind, options, indentation=indentation)
        self.base_class_name = base_class_name
        self.inflect_engine = inflect.engine()

    def collect_global_imports(self) -> None:
        if _sqla_version < (1, 4):
            self.add_literal_import('sqlalchemy.ext.declarative', 'declarative_base')
        else:
            self.add_literal_import('sqlalchemy.orm', 'declarative_base')

    def collect_imports_for_model(self, model: Model) -> None:
        super().collect_imports_for_model(model)
        if isinstance(model, ModelClass):
            if model.relationships:
                self.add_literal_import('sqlalchemy.orm', 'relationship')

    def generate_models(self) -> List[Model]:
        models_by_table_name: Dict[str, Model] = OrderedDict()

        # Pick association tables from the metadata into their own set, don't process them normally
        links: DefaultDict[str, List[Model]] = defaultdict(lambda: [])
        for table in self.metadata.sorted_tables:
            # Link tables have exactly two foreign key constraints and all columns are involved in
            # them
            fk_constraints = sorted(table.foreign_key_constraints, key=get_constraint_sort_key)
            if len(fk_constraints) == 2 and all(col.foreign_keys for col in table.columns):
                model = models_by_table_name[table.name] = Model(table)
                tablename = fk_constraints[0].elements[0].column.table.name
                links[tablename].append(model)
                continue

            # Only form model classes for tables that have a primary key and are not association
            # tables
            if not table.primary_key:
                models_by_table_name[table.name] = Model(table)
            else:
                model = ModelClass(table)
                models_by_table_name[table.name] = model

                # Fill in the columns
                for column in table.c:
                    column_attr = ColumnAttribute(model, column)
                    model.columns.append(column_attr)

        # Add relationships
        for model in models_by_table_name.values():
            if isinstance(model, ModelClass):
                self.generate_relationships(model, models_by_table_name, links[model.table.name])

        # Nest inherited classes in their superclasses to ensure proper ordering
        if 'nojoined' not in self.options:
            for model in list(models_by_table_name.values()):
                if not isinstance(model, ModelClass):
                    continue

                pk_column_names = set(col.name for col in model.table.primary_key.columns)
                for constraint in model.table.foreign_key_constraints:
                    if set(get_column_names(constraint)) == pk_column_names:
                        target = models_by_table_name[constraint.elements[0].column.table.name]
                        if isinstance(target, ModelClass):
                            model.parent_class = target
                            target.children.append(model)

        # Collect the imports
        self.collect_imports(models_by_table_name.values())

        # Rename models and their attributes that conflict with imports or other attributes
        global_names = set(name for namespace in self.imports.values() for name in namespace)
        for model in models_by_table_name.values():
            self.generate_model_name(model, global_names)

        return list(models_by_table_name.values())

    def generate_relationships(self, source: ModelClass,
                               models_by_table_name: Dict[str, Model],
                               association_tables: List[Model]) -> List[RelationshipAttribute]:
        relationships: List[RelationshipAttribute] = []
        reverse_relationship: Optional[RelationshipAttribute]

        # Add many-to-one (and one-to-many) relationships
        pk_column_names = set(col.name for col in source.table.primary_key.columns)
        for constraint in sorted(source.table.foreign_key_constraints,
                                 key=get_constraint_sort_key):
            target = models_by_table_name[constraint.elements[0].column.table.name]
            if isinstance(target, ModelClass):
                if 'nojoined' not in self.options:
                    if set(get_column_names(constraint)) == pk_column_names:
                        parent = models_by_table_name[constraint.elements[0].column.table.name]
                        if isinstance(parent, ModelClass):
                            source.parent_class = parent
                            parent.children.append(source)
                            continue

                # Add uselist=False to One-to-One relationships
                column_names = get_column_names(constraint)
                if any(isinstance(c, (PrimaryKeyConstraint, UniqueConstraint)) and
                       set(col.name for col in c.columns) == set(column_names)
                       for c in constraint.table.constraints):
                    r_type = RelationshipType.ONE_TO_ONE
                else:
                    r_type = RelationshipType.MANY_TO_ONE

                relationship = RelationshipAttribute(r_type, source, target, constraint)
                source.relationships.append(relationship)

                # For self referential relationships, remote_side needs to be set
                if source is target:
                    relationship.remote_side = [
                        source.get_column_attribute(col.name)
                        for col in constraint.referred_table.primary_key]

                # If the two tables share more than one foreign key constraint,
                # SQLAlchemy needs an explicit primaryjoin to figure out which column(s) it needs
                common_fk_constraints = get_common_fk_constraints(source.table, target.table)
                if len(common_fk_constraints) > 1:
                    relationship.foreign_keys = [source.get_column_attribute(key)
                                                 for key in constraint.column_keys]

                # Generate the opposite end of the relationship in the target class
                if 'nobidi' not in self.options:
                    if r_type is RelationshipType.MANY_TO_ONE:
                        r_type = RelationshipType.ONE_TO_MANY

                    reverse_relationship = RelationshipAttribute(
                        r_type, target, source, constraint,
                        foreign_keys=relationship.foreign_keys, backref=relationship)
                    relationship.backref = reverse_relationship
                    target.relationships.append(reverse_relationship)

                    # For self referential relationships, remote_side needs to be set
                    if source is target:
                        reverse_relationship.remote_side = [
                            source.get_column_attribute(colname)
                            for colname in constraint.column_keys]

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = sorted(association_table.table.foreign_key_constraints,
                                    key=get_constraint_sort_key)
            target = models_by_table_name[fk_constraints[1].elements[0].column.table.name]
            if isinstance(target, ModelClass):
                relationship = RelationshipAttribute(
                    RelationshipType.MANY_TO_MANY, source, target,
                    fk_constraints[1], association_table)
                source.relationships.append(relationship)

                # Generate the opposite end of the relationship in the target class
                reverse_relationship = None
                if 'nobidi' not in self.options:
                    reverse_relationship = RelationshipAttribute(
                        RelationshipType.MANY_TO_MANY, target, source, fk_constraints[0],
                        association_table, relationship)
                    relationship.backref = reverse_relationship
                    target.relationships.append(reverse_relationship)

                # Add a primary/secondary join for self-referential many-to-many relationships
                if source is target:
                    both_relationships = [relationship]
                    reverse_flags = [False, True]
                    if reverse_relationship:
                        both_relationships.append(reverse_relationship)

                    for relationship, reverse in zip(both_relationships, reverse_flags):
                        if not relationship.association_table or not relationship.constraint:
                            continue

                        constraints = sorted(relationship.constraint.table.foreign_key_constraints,
                                             key=get_constraint_sort_key, reverse=reverse)
                        pri_pairs = zip(get_column_names(constraints[0]), constraints[0].elements)
                        sec_pairs = zip(get_column_names(constraints[1]), constraints[1].elements)
                        relationship.primaryjoin = [
                            (relationship.source, elem.column.name,
                             relationship.association_table, col)
                            for col, elem in pri_pairs]
                        relationship.secondaryjoin = [
                            (relationship.target, elem.column.name,
                             relationship.association_table, col)
                            for col, elem in sec_pairs]

        return relationships

    def generate_model_name(self, model: Model, global_names: Set[str]) -> None:
        if isinstance(model, ModelClass):
            preferred_name = _re_invalid_identifier.sub('_', model.table.name)
            preferred_name = ''.join(part[:1].upper() + part[1:]
                                     for part in preferred_name.split('_'))
            if 'use_inflect' in self.options:
                preferred_name = self.inflect_engine.singular_noun(preferred_name)

            model.name = self.find_free_name(preferred_name, global_names)

            # Fill in the names for column attributes
            local_names: Set[str] = set()
            for column_attr in model.columns:
                self.generate_column_attr_name(column_attr, global_names, local_names)
                local_names.add(column_attr.name)

            # Fill in the names for relationship attributes
            for relationship in model.relationships:
                self.generate_relationship_name(relationship, global_names, local_names)
                local_names.add(relationship.name)
        else:
            super().generate_model_name(model, global_names)

    def generate_column_attr_name(self, column_attr: ColumnAttribute,
                                  global_names: Set[str], local_names: Set[str]) -> None:
        column_attr.name = self.find_free_name(column_attr.column.name, global_names, local_names)

    def generate_relationship_name(self, relationship: RelationshipAttribute,
                                   global_names: Set[str], local_names: Set[str]) -> None:
        # Self referential reverse relationships
        if (relationship.type in (RelationshipType.ONE_TO_MANY, RelationshipType.ONE_TO_ONE)
                and relationship.source is relationship.target
                and relationship.backref and relationship.backref.name):
            preferred_name = relationship.backref.name + '_reverse'
        else:
            preferred_name = relationship.target.table.name

            # If there's a constraint with a single column that ends with "_id", use the preceding
            # part as the relationship name
            if relationship.constraint:
                is_source = relationship.source.table is relationship.constraint.table
                if is_source or relationship.type not in (RelationshipType.ONE_TO_ONE,
                                                          RelationshipType.ONE_TO_MANY):
                    column_names = [c.name for c in relationship.constraint.columns]
                    if len(column_names) == 1 and column_names[0].endswith('_id'):
                        preferred_name = column_names[0][:-3]

            if 'use_inflect' in self.options:
                if relationship.type in (RelationshipType.ONE_TO_MANY,
                                         RelationshipType.MANY_TO_MANY):
                    preferred_name = self.inflect_engine.plural_noun(preferred_name)
                else:
                    preferred_name = self.inflect_engine.singular_noun(preferred_name)

        relationship.name = self.find_free_name(preferred_name, global_names, local_names)

    def render_module_variables(self, models: List[Model]) -> str:
        if not any(isinstance(model, ModelClass) for model in models):
            return super().render_module_variables(models)

        declarations = [f'{self.base_class_name} = declarative_base()']
        if any(not isinstance(model, ModelClass) for model in models):
            declarations.append(f'metadata = {self.base_class_name}.metadata')

        return '\n'.join(declarations)

    def render_models(self, models: List[Model]) -> str:
        rendered = []
        for model in models:
            if isinstance(model, ModelClass):
                rendered.append(self.render_class(model))
            else:
                rendered.append(f'{model.name} = {self.render_table(model.table)}')

        return '\n\n\n'.join(rendered)

    def render_class(self, model: ModelClass) -> str:
        sections: List[str] = []

        # Render class variables / special declarations
        class_vars: str = self.render_class_variables(model)
        if class_vars:
            sections.append(class_vars)

        # Render column attributes
        rendered_column_attributes: List[str] = []
        for nullable in (False, True):
            for column_attr in model.columns:
                if column_attr.column.nullable is nullable:
                    rendered_column_attributes.append(self.render_column_attribute(column_attr))

        if rendered_column_attributes:
            sections.append('\n'.join(rendered_column_attributes))

        # Render relationship attributes
        rendered_relationship_attributes: List[str] = [
            self.render_relationship(relationship) for relationship in model.relationships]

        if rendered_relationship_attributes:
            sections.append('\n'.join(rendered_relationship_attributes))

        declaration = self.render_class_declaration(model)
        rendered_sections = '\n\n'.join(indent(section, self.indentation) for section in sections)
        return f'{declaration}\n{rendered_sections}'

    def render_class_declaration(self, model: ModelClass) -> str:
        parent_class_name = model.parent_class.name if model.parent_class else self.base_class_name
        return f'class {model.name}({parent_class_name}):'

    def render_class_variables(self, model: ModelClass) -> str:
        variables = [f'__tablename__ = {model.table.name!r}']

        # Render constraints and indexes as __table_args__
        table_args = self.render_table_args(model.table)
        if table_args:
            variables.append(f'__table_args__ = {table_args}')

        return '\n'.join(variables)

    def render_table_args(self, table: Table) -> str:
        args: List[str] = []
        kwargs: Dict[str, str] = {}

        # Render constraints
        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            if isinstance(constraint, PrimaryKeyConstraint):
                continue
            if (isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)) and
                    len(constraint.columns) == 1):
                continue

            args.append(self.render_constraint(constraint))

        # Render indexes
        for index in table.indexes:
            if len(index.columns) > 1:
                args.append(self.render_index(index))

        if table.schema:
            kwargs['schema'] = table.schema

        if table.comment:
            kwargs['comment'] = table.comment

        if kwargs:
            formatted_kwargs = pformat(kwargs)
            if not args:
                return formatted_kwargs
            else:
                args.append(formatted_kwargs)

        if args:
            rendered_args = f',\n{self.indentation}'.join(args)
            if len(args) == 1:
                rendered_args += ','

            return f'(\n{self.indentation}{rendered_args}\n)'
        else:
            return ''

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        rendered_column = self.render_column(column, column_attr.name != column.name)
        return f'{column_attr.name} = {rendered_column}'

    def render_relationship(self, relationship: RelationshipAttribute) -> str:
        def render_column_attrs(column_attrs: List[ColumnAttribute]) -> str:
            rendered = []
            for attr in column_attrs:
                if attr.model is relationship.source:
                    rendered.append(attr.name)
                else:
                    rendered.append(repr(f'{attr.model.name}.{attr.name}'))

            return '[' + ', '.join(rendered) + ']'

        def render_join(terms: List[JoinType]) -> str:
            rendered_joins = []
            for source, source_col, target, target_col in terms:
                rendered = f'{source.name}.{source_col} == {target.name}.'
                if target.__class__ is Model:
                    rendered += 'c.'

                rendered += str(target_col)
                rendered_joins.append(rendered)

            if len(rendered_joins) > 1:
                rendered = ', '.join(rendered_joins)
                return f'and_({rendered})'
            else:
                return repr(rendered_joins[0])

        # Render keyword arguments
        kwargs: Dict[str, Any] = OrderedDict()
        if relationship.type is RelationshipType.ONE_TO_ONE and relationship.constraint:
            if relationship.constraint.referred_table is relationship.source.table:
                kwargs['uselist'] = False

        # Add the "secondary" keyword for many-to-many relationships
        if relationship.association_table:
            table_ref = relationship.association_table.table.name
            if relationship.association_table.schema:
                table_ref = f'{relationship.association_table.schema}.{table_ref}'

            kwargs['secondary'] = repr(table_ref)

        if relationship.remote_side:
            kwargs['remote_side'] = render_column_attrs(relationship.remote_side)

        if relationship.foreign_keys:
            kwargs['foreign_keys'] = render_column_attrs(relationship.foreign_keys)

        if relationship.primaryjoin:
            kwargs['primaryjoin'] = render_join(relationship.primaryjoin)

        if relationship.secondaryjoin:
            kwargs['secondaryjoin'] = render_join(relationship.secondaryjoin)

        if relationship.backref:
            kwargs['back_populates'] = repr(relationship.backref.name)

        rendered_kwargs = ''
        if kwargs:
            rendered_kwargs = ', ' + ', '.join(f'{key}={value}' for key, value in kwargs.items())

        return f'{relationship.name} = relationship({relationship.target.name!r}{rendered_kwargs})'


class DataclassGenerator(DeclarativeGenerator):
    def __init__(self, metadata: MetaData, bind: Connectable, options: Set[str], *,
                 indentation: str = '    ', base_class_name: str = 'Base',
                 quote_annotations: Optional[bool] = None, metadata_key: str = 'sa'):
        super().__init__(metadata, bind, options, indentation=indentation,
                         base_class_name=base_class_name)
        self.metadata_key = metadata_key
        if quote_annotations is not None:
            self.quote_annotations = quote_annotations
        else:
            self.quote_annotations = sys.version_info < (3, 7)

    def collect_global_imports(self) -> None:
        if not self.quote_annotations:
            self.add_literal_import('__future__', 'annotations')

        self.add_literal_import('dataclasses', 'dataclass')
        self.add_literal_import('dataclasses', 'field')
        self.add_literal_import('sqlalchemy.orm', 'registry')

    def collect_imports_for_model(self, model: Model) -> None:
        super().collect_imports_for_model(model)
        if isinstance(model, ModelClass):
            for column_attr in model.columns:
                if column_attr.column.nullable:
                    self.add_literal_import('typing', 'Optional')
                    break

            for relationship_attr in model.relationships:
                if relationship_attr.type in (RelationshipType.ONE_TO_MANY,
                                              RelationshipType.MANY_TO_MANY):
                    self.add_literal_import('typing', 'List')

    def collect_imports_for_column(self, column: Column) -> None:
        super().collect_imports_for_column(column)
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            self.add_literal_import('typing', 'Any')
        else:
            self.add_import(python_type)

    def render_module_variables(self, models: List[Model]) -> str:
        if not any(isinstance(model, ModelClass) for model in models):
            return super().render_module_variables(models)

        declarations: List[str] = ['mapper_registry = registry()']
        if any(not isinstance(model, ModelClass) for model in models):
            declarations.append('metadata = mapper_registry.metadata')

        if not self.quote_annotations:
            self.add_literal_import('__future__', 'annotations')

        return '\n'.join(declarations)

    def render_class_declaration(self, model: ModelClass) -> str:
        superclass_part = f'({model.parent_class.name})' if model.parent_class else ''
        return f'@mapper_registry.mapped\n@dataclass\nclass {model.name}{superclass_part}:'

    def render_class_variables(self, model: ModelClass) -> str:
        variables = [super().render_class_variables(model),
                     f'__sa_dataclass_metadata_key__ = {self.metadata_key!r}']
        return '\n'.join(variables)

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            python_type_name = 'Any'
        else:
            python_type_name = python_type.__name__

        kwargs: Dict[str, Any] = OrderedDict()
        if column.autoincrement and column.name in column.table.primary_key:
            kwargs['init'] = False
        elif column.nullable:
            self.add_literal_import('typing', 'Optional')
            kwargs['default'] = None
            python_type_name = f'Optional[{python_type_name}]'

        rendered_column = self.render_column(column, column_attr.name != column.name)
        kwargs['metadata'] = f'{{{self.metadata_key!r}: {rendered_column}}}'
        rendered_kwargs = ', '.join(f'{key}={value}' for key, value in kwargs.items())
        return f'{column_attr.name}: {python_type_name} = field({rendered_kwargs})'

    def render_relationship(self, relationship: RelationshipAttribute) -> str:
        rendered = super().render_relationship(relationship).partition(' = ')[2]
        kwargs: Dict[str, Any] = OrderedDict()

        annotation = relationship.target.name
        if self.quote_annotations:
            annotation = repr(relationship.target.name)

        if relationship.type in (RelationshipType.ONE_TO_MANY, RelationshipType.MANY_TO_MANY):
            self.add_literal_import('typing', 'List')
            annotation = f'List[{annotation}]'
            kwargs['default_factory'] = 'list'
        else:
            if relationship.constraint:
                if all(column.nullable for column in relationship.constraint.columns):
                    self.add_literal_import('typing', 'Optional')
                    kwargs['default'] = 'None'
                    annotation = f'Optional[{annotation}]'

        kwargs['metadata'] = f'{{{self.metadata_key!r}: {rendered}}}'
        rendered_kwargs = ', '.join(f'{key}={value}' for key, value in kwargs.items())
        return f'{relationship.name}: {annotation} = field({rendered_kwargs})'
