from __future__ import annotations

import inspect
import re
import sys
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from importlib import import_module
from inspect import Parameter
from itertools import count
from keyword import iskeyword
from pprint import pformat
from textwrap import indent
from typing import Any, ClassVar, Literal, cast

import inflect
import sqlalchemy
from sqlalchemy import (
    ARRAY,
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    Constraint,
    DefaultClause,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import DOMAIN, JSON, JSONB
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.type_api import UserDefinedType
from sqlalchemy.types import TypeEngine

from .models import (
    ColumnAttribute,
    JoinType,
    Model,
    ModelClass,
    RelationshipAttribute,
    RelationshipType,
)
from .utils import (
    decode_postgresql_sequence,
    get_column_names,
    get_common_fk_constraints,
    get_compiled_expression,
    get_constraint_sort_key,
    get_stdlib_module_names,
    qualified_table_name,
    render_callable,
    uses_default_name,
)

_re_boolean_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?).*\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")
_re_invalid_identifier = re.compile(r"(?u)\W")


@dataclass
class LiteralImport:
    pkgname: str
    name: str


@dataclass
class Base:
    """Representation of MetaData for Tables, respectively Base for classes"""

    literal_imports: list[LiteralImport]
    declarations: list[str]
    metadata_ref: str
    decorator: str | None = None
    table_metadata_declaration: str | None = None


class CodeGenerator(metaclass=ABCMeta):
    valid_options: ClassVar[set[str]] = set()

    def __init__(
        self, metadata: MetaData, bind: Connection | Engine, options: Sequence[str]
    ):
        self.metadata: MetaData = metadata
        self.bind: Connection | Engine = bind
        self.options: set[str] = set(options)

        # Validate options
        invalid_options = {opt for opt in options if opt not in self.valid_options}
        if invalid_options:
            raise ValueError("Unrecognized options: " + ", ".join(invalid_options))

    @property
    @abstractmethod
    def views_supported(self) -> bool:
        pass

    @abstractmethod
    def generate(self) -> str:
        """
        Generate the code for the given metadata.
        .. note:: May modify the metadata.
        """


@dataclass(eq=False)
class TablesGenerator(CodeGenerator):
    valid_options: ClassVar[set[str]] = {"noindexes", "noconstraints", "nocomments"}
    stdlib_module_names: ClassVar[set[str]] = get_stdlib_module_names()

    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
    ):
        super().__init__(metadata, bind, options)
        self.indentation: str = indentation
        self.imports: dict[str, set[str]] = defaultdict(set)
        self.module_imports: set[str] = set()

    @property
    def views_supported(self) -> bool:
        return True

    def generate_base(self) -> None:
        self.base = Base(
            literal_imports=[LiteralImport("sqlalchemy", "MetaData")],
            declarations=["metadata = MetaData()"],
            metadata_ref="metadata",
        )

    def generate(self) -> str:
        self.generate_base()

        sections: list[str] = []

        # Remove unwanted elements from the metadata
        for table in list(self.metadata.tables.values()):
            if self.should_ignore_table(table):
                self.metadata.remove(table)
                continue

            if "noindexes" in self.options:
                table.indexes.clear()

            if "noconstraints" in self.options:
                table.constraints.clear()

            if "nocomments" in self.options:
                table.comment = None

            for column in table.columns:
                if "nocomments" in self.options:
                    column.comment = None

        # Use information from column constraints to figure out the intended column
        # types
        for table in self.metadata.tables.values():
            self.fix_column_types(table)

        # Generate the models
        models: list[Model] = self.generate_models()

        # Render module level variables
        variables = self.render_module_variables(models)
        if variables:
            sections.append(variables + "\n")

        # Render models
        rendered_models = self.render_models(models)
        if rendered_models:
            sections.append(rendered_models)

        # Render collected imports
        groups = self.group_imports()
        imports = "\n\n".join("\n".join(line for line in group) for group in groups)
        if imports:
            sections.insert(0, imports)

        return "\n\n".join(sections) + "\n"

    def collect_imports(self, models: Iterable[Model]) -> None:
        for literal_import in self.base.literal_imports:
            self.add_literal_import(literal_import.pkgname, literal_import.name)

        for model in models:
            self.collect_imports_for_model(model)

    def collect_imports_for_model(self, model: Model) -> None:
        if model.__class__ is Model:
            self.add_import(Table)

        for column in model.table.c:
            self.collect_imports_for_column(column)

        for constraint in model.table.constraints:
            self.collect_imports_for_constraint(constraint)

        for index in model.table.indexes:
            self.collect_imports_for_constraint(index)

    def collect_imports_for_column(self, column: Column[Any]) -> None:
        self.add_import(column.type)

        if isinstance(column.type, ARRAY):
            self.add_import(column.type.item_type.__class__)
        elif isinstance(column.type, (JSONB, JSON)):
            if (
                not isinstance(column.type.astext_type, Text)
                or column.type.astext_type.length is not None
            ):
                self.add_import(column.type.astext_type)
        elif isinstance(column.type, DOMAIN):
            self.add_import(column.type.data_type.__class__)

        if column.default:
            self.add_import(column.default)

        if column.server_default:
            if isinstance(column.server_default, (Computed, Identity)):
                self.add_import(column.server_default)
            elif isinstance(column.server_default, DefaultClause):
                self.add_literal_import("sqlalchemy", "text")

    def collect_imports_for_constraint(self, constraint: Constraint | Index) -> None:
        if isinstance(constraint, Index):
            if len(constraint.columns) > 1 or not uses_default_name(constraint):
                self.add_literal_import("sqlalchemy", "Index")
        elif isinstance(constraint, PrimaryKeyConstraint):
            if not uses_default_name(constraint):
                self.add_literal_import("sqlalchemy", "PrimaryKeyConstraint")
        elif isinstance(constraint, UniqueConstraint):
            if len(constraint.columns) > 1 or not uses_default_name(constraint):
                self.add_literal_import("sqlalchemy", "UniqueConstraint")
        elif isinstance(constraint, ForeignKeyConstraint):
            if len(constraint.columns) > 1 or not uses_default_name(constraint):
                self.add_literal_import("sqlalchemy", "ForeignKeyConstraint")
            else:
                self.add_import(ForeignKey)
        else:
            self.add_import(constraint)

    def add_import(self, obj: Any) -> None:
        # Don't store builtin imports
        if getattr(obj, "__module__", "builtins") == "builtins":
            return

        type_ = type(obj) if not isinstance(obj, type) else obj
        pkgname = type_.__module__

        # The column types have already been adapted towards generic types if possible,
        # so if this is still a vendor specific type (e.g., MySQL INTEGER) be sure to
        # use that rather than the generic sqlalchemy type as it might have different
        # constructor parameters.
        if pkgname.startswith("sqlalchemy.dialects."):
            dialect_pkgname = ".".join(pkgname.split(".")[0:3])
            dialect_pkg = import_module(dialect_pkgname)

            if type_.__name__ in dialect_pkg.__all__:
                pkgname = dialect_pkgname
        elif type_ is getattr(sqlalchemy, type_.__name__, None):
            pkgname = "sqlalchemy"
        else:
            pkgname = type_.__module__

        self.add_literal_import(pkgname, type_.__name__)

    def add_literal_import(self, pkgname: str, name: str) -> None:
        names = self.imports.setdefault(pkgname, set())
        names.add(name)

    def remove_literal_import(self, pkgname: str, name: str) -> None:
        names = self.imports.setdefault(pkgname, set())
        if name in names:
            names.remove(name)

    def add_module_import(self, pgkname: str) -> None:
        self.module_imports.add(pgkname)

    def group_imports(self) -> list[list[str]]:
        future_imports: list[str] = []
        stdlib_imports: list[str] = []
        thirdparty_imports: list[str] = []

        def get_collection(package: str) -> list[str]:
            collection = thirdparty_imports
            if package == "__future__":
                collection = future_imports
            elif package in self.stdlib_module_names:
                collection = stdlib_imports
            elif package in sys.modules:
                if "site-packages" not in (sys.modules[package].__file__ or ""):
                    collection = stdlib_imports
            return collection

        for package in sorted(self.imports):
            imports = ", ".join(sorted(self.imports[package]))

            collection = get_collection(package)
            collection.append(f"from {package} import {imports}")

        for module in sorted(self.module_imports):
            collection = get_collection(module)
            collection.append(f"import {module}")

        return [
            group
            for group in (future_imports, stdlib_imports, thirdparty_imports)
            if group
        ]

    def generate_models(self) -> list[Model]:
        models = [Model(table) for table in self.metadata.sorted_tables]

        # Collect the imports
        self.collect_imports(models)

        # Generate names for models
        global_names = {
            name for namespace in self.imports.values() for name in namespace
        }
        for model in models:
            self.generate_model_name(model, global_names)
            global_names.add(model.name)

        return models

    def generate_model_name(self, model: Model, global_names: set[str]) -> None:
        preferred_name = f"t_{model.table.name}"
        model.name = self.find_free_name(preferred_name, global_names)

    def render_module_variables(self, models: list[Model]) -> str:
        declarations = self.base.declarations

        if any(not isinstance(model, ModelClass) for model in models):
            if self.base.table_metadata_declaration is not None:
                declarations.append(self.base.table_metadata_declaration)

        return "\n".join(declarations)

    def render_models(self, models: list[Model]) -> str:
        rendered: list[str] = []
        for model in models:
            rendered_table = self.render_table(model.table)
            rendered.append(f"{model.name} = {rendered_table}")

        return "\n\n".join(rendered)

    def render_table(self, table: Table) -> str:
        args: list[str] = [f"{table.name!r}, {self.base.metadata_ref}"]
        kwargs: dict[str, object] = {}
        for column in table.columns:
            # Cast is required because of a bug in the SQLAlchemy stubs regarding
            # Table.columns
            args.append(self.render_column(column, True, is_table=True))

        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            if uses_default_name(constraint):
                if isinstance(constraint, PrimaryKeyConstraint):
                    continue
                elif isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint)):
                    if len(constraint.columns) == 1:
                        continue

            args.append(self.render_constraint(constraint))

        for index in sorted(table.indexes, key=lambda i: cast(str, i.name)):
            # One-column indexes should be rendered as index=True on columns
            if len(index.columns) > 1 or not uses_default_name(index):
                args.append(self.render_index(index))

        if table.schema:
            kwargs["schema"] = repr(table.schema)

        table_comment = getattr(table, "comment", None)
        if table_comment:
            kwargs["comment"] = repr(table.comment)

        return render_callable("Table", *args, kwargs=kwargs, indentation="    ")

    def render_index(self, index: Index) -> str:
        extra_args = [repr(col.name) for col in index.columns]
        kwargs = {}
        if index.unique:
            kwargs["unique"] = True

        return render_callable("Index", repr(index.name), *extra_args, kwargs=kwargs)

    # TODO find better solution for is_table
    def render_column(
        self, column: Column[Any], show_name: bool, is_table: bool = False
    ) -> str:
        args = []
        kwargs: dict[str, Any] = {}
        kwarg = []
        is_part_of_composite_pk = (
            column.primary_key and len(column.table.primary_key) > 1
        )
        dedicated_fks = [
            c
            for c in column.foreign_keys
            if c.constraint
            and len(c.constraint.columns) == 1
            and uses_default_name(c.constraint)
        ]
        is_unique = any(
            isinstance(c, UniqueConstraint)
            and set(c.columns) == {column}
            and uses_default_name(c)
            for c in column.table.constraints
        )
        is_unique = is_unique or any(
            i.unique and set(i.columns) == {column} and uses_default_name(i)
            for i in column.table.indexes
        )
        is_primary = (
            any(
                isinstance(c, PrimaryKeyConstraint)
                and column.name in c.columns
                and uses_default_name(c)
                for c in column.table.constraints
            )
            or column.primary_key
        )
        has_index = any(
            set(i.columns) == {column} and uses_default_name(i)
            for i in column.table.indexes
        )

        if show_name:
            args.append(repr(column.name))

        # Render the column type if there are no foreign keys on it or any of them
        # points back to itself
        if not dedicated_fks or any(fk.column is column for fk in dedicated_fks):
            args.append(self.render_column_type(column.type))

        for fk in dedicated_fks:
            args.append(self.render_constraint(fk))

        if column.default:
            args.append(repr(column.default))

        if column.key != column.name:
            kwargs["key"] = column.key
        if is_primary:
            kwargs["primary_key"] = True
        if not column.nullable and not column.primary_key:
            kwargs["nullable"] = False
        if column.nullable and is_part_of_composite_pk:
            kwargs["nullable"] = True

        if is_unique:
            column.unique = True
            kwargs["unique"] = True
        if has_index:
            column.index = True
            kwarg.append("index")
            kwargs["index"] = True

        if isinstance(column.server_default, DefaultClause):
            kwargs["server_default"] = render_callable(
                "text", repr(cast(TextClause, column.server_default.arg).text)
            )
        elif isinstance(column.server_default, Computed):
            expression = str(column.server_default.sqltext)

            computed_kwargs = {}
            if column.server_default.persisted is not None:
                computed_kwargs["persisted"] = column.server_default.persisted

            args.append(
                render_callable("Computed", repr(expression), kwargs=computed_kwargs)
            )
        elif isinstance(column.server_default, Identity):
            args.append(repr(column.server_default))
        elif column.server_default:
            kwargs["server_default"] = repr(column.server_default)

        comment = getattr(column, "comment", None)
        if comment:
            kwargs["comment"] = repr(comment)

        return self.render_column_callable(is_table, *args, **kwargs)

    def render_column_callable(self, is_table: bool, *args: Any, **kwargs: Any) -> str:
        if is_table:
            self.add_import(Column)
            return render_callable("Column", *args, kwargs=kwargs)
        else:
            return render_callable("mapped_column", *args, kwargs=kwargs)

    def render_column_type(self, coltype: TypeEngine[Any]) -> str:
        args = []
        kwargs: dict[str, Any] = {}
        sig = inspect.signature(coltype.__class__.__init__)
        defaults = {param.name: param.default for param in sig.parameters.values()}
        missing = object()
        use_kwargs = False
        for param in list(sig.parameters.values())[1:]:
            # Remove annoyances like _warn_on_bytestring
            if param.name.startswith("_"):
                continue
            elif param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
                use_kwargs = True
                continue

            value = getattr(coltype, param.name, missing)

            if isinstance(value, (JSONB, JSON)):
                # Remove astext_type if it's the default
                if (
                    isinstance(value.astext_type, Text)
                    and value.astext_type.length is None
                ):
                    value.astext_type = None  # type: ignore[assignment]
                else:
                    self.add_import(Text)

            default = defaults.get(param.name, missing)
            if isinstance(value, TextClause):
                self.add_literal_import("sqlalchemy", "text")
                rendered_value = render_callable("text", repr(value.text))
            else:
                rendered_value = repr(value)

            if value is missing or value == default:
                use_kwargs = True
            elif use_kwargs:
                kwargs[param.name] = rendered_value
            else:
                args.append(rendered_value)

        vararg = next(
            (
                param.name
                for param in sig.parameters.values()
                if param.kind is Parameter.VAR_POSITIONAL
            ),
            None,
        )
        if vararg and hasattr(coltype, vararg):
            varargs_repr = [repr(arg) for arg in getattr(coltype, vararg)]
            args.extend(varargs_repr)

        # These arguments cannot be autodetected from the Enum initializer
        if isinstance(coltype, Enum):
            for colname in "name", "schema":
                if (value := getattr(coltype, colname)) is not None:
                    kwargs[colname] = repr(value)

        if isinstance(coltype, (JSONB, JSON)):
            # Remove astext_type if it's the default
            if (
                isinstance(coltype.astext_type, Text)
                and coltype.astext_type.length is None
            ):
                del kwargs["astext_type"]

        if args or kwargs:
            return render_callable(coltype.__class__.__name__, *args, kwargs=kwargs)
        else:
            return coltype.__class__.__name__

    def render_constraint(self, constraint: Constraint | ForeignKey) -> str:
        def add_fk_options(*opts: Any) -> None:
            args.extend(repr(opt) for opt in opts)
            for attr in "ondelete", "onupdate", "deferrable", "initially", "match":
                value = getattr(constraint, attr, None)
                if value:
                    kwargs[attr] = repr(value)

        args: list[str] = []
        kwargs: dict[str, Any] = {}
        if isinstance(constraint, ForeignKey):
            remote_column = (
                f"{constraint.column.table.fullname}.{constraint.column.name}"
            )
            add_fk_options(remote_column)
        elif isinstance(constraint, ForeignKeyConstraint):
            local_columns = get_column_names(constraint)
            remote_columns = [
                f"{fk.column.table.fullname}.{fk.column.name}"
                for fk in constraint.elements
            ]
            add_fk_options(local_columns, remote_columns)
        elif isinstance(constraint, CheckConstraint):
            args.append(repr(get_compiled_expression(constraint.sqltext, self.bind)))
        elif isinstance(constraint, (UniqueConstraint, PrimaryKeyConstraint)):
            args.extend(repr(col.name) for col in constraint.columns)
        else:
            raise TypeError(
                f"Cannot render constraint of type {constraint.__class__.__name__}"
            )

        if isinstance(constraint, Constraint) and not uses_default_name(constraint):
            kwargs["name"] = repr(constraint.name)

        return render_callable(constraint.__class__.__name__, *args, kwargs=kwargs)

    def should_ignore_table(self, table: Table) -> bool:
        # Support for Alembic and sqlalchemy-migrate -- never expose the schema version
        # tables
        return table.name in ("alembic_version", "migrate_version")

    def find_free_name(
        self, name: str, global_names: set[str], local_names: Collection[str] = ()
    ) -> str:
        """
        Generate an attribute name that does not clash with other local or global names.
        """
        name = name.strip()
        assert name, "Identifier cannot be empty"
        name = _re_invalid_identifier.sub("_", name)
        if name[0].isdigit():
            name = "_" + name
        elif iskeyword(name) or name == "metadata":
            name += "_"

        original = name
        for i in count():
            if name not in global_names and name not in local_names:
                break

            name = original + (str(i) if i else "_")

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
                                table.c[colname].type = Enum(
                                    *options, native_enum=False
                                )

                            continue

        for column in table.c:
            try:
                column.type = self.get_adapted_type(column.type)
            except CompileError:
                pass

            # PostgreSQL specific fix: detect sequences from server_default
            if column.server_default and self.bind.dialect.name == "postgresql":
                if isinstance(column.server_default, DefaultClause) and isinstance(
                    column.server_default.arg, TextClause
                ):
                    schema, seqname = decode_postgresql_sequence(
                        column.server_default.arg
                    )
                    if seqname:
                        # Add an explicit sequence
                        if seqname != f"{column.table.name}_{column.name}_seq":
                            column.default = sqlalchemy.Sequence(seqname, schema=schema)

                        column.server_default = None

    def get_adapted_type(self, coltype: Any) -> Any:
        compiled_type = coltype.compile(self.bind.engine.dialect)
        for supercls in coltype.__class__.__mro__:
            if not supercls.__name__.startswith("_") and hasattr(
                supercls, "__visit_name__"
            ):
                # Don't try to adapt UserDefinedType as it's not a proper column type
                if supercls is UserDefinedType or issubclass(supercls, TypeDecorator):
                    return coltype

                # Hack to fix adaptation of the Enum class which is broken since
                # SQLAlchemy 1.2
                kw = {}
                if supercls is Enum:
                    kw["name"] = coltype.name
                    if coltype.schema:
                        kw["schema"] = coltype.schema

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
                    # If the adapted column type does not render the same as the
                    # original, don't substitute it
                    if new_coltype.compile(self.bind.engine.dialect) != compiled_type:
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
    valid_options: ClassVar[set[str]] = TablesGenerator.valid_options | {
        "use_inflect",
        "nojoined",
        "nobidi",
    }

    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
        base_class_name: str = "Base",
    ):
        super().__init__(metadata, bind, options, indentation=indentation)
        self.base_class_name: str = base_class_name
        self.inflect_engine = inflect.engine()

    def generate_base(self) -> None:
        self.base = Base(
            literal_imports=[LiteralImport("sqlalchemy.orm", "DeclarativeBase")],
            declarations=[
                f"class {self.base_class_name}(DeclarativeBase):",
                f"{self.indentation}pass",
            ],
            metadata_ref=f"{self.base_class_name}.metadata",
        )

    def collect_imports(self, models: Iterable[Model]) -> None:
        super().collect_imports(models)
        if any(isinstance(model, ModelClass) for model in models):
            self.add_literal_import("sqlalchemy.orm", "Mapped")
            self.add_literal_import("sqlalchemy.orm", "mapped_column")

    def collect_imports_for_model(self, model: Model) -> None:
        super().collect_imports_for_model(model)
        if isinstance(model, ModelClass):
            if model.relationships:
                self.add_literal_import("sqlalchemy.orm", "relationship")

    def generate_models(self) -> list[Model]:
        models_by_table_name: dict[str, Model] = {}

        # Pick association tables from the metadata into their own set, don't process
        # them normally
        links: defaultdict[str, list[Model]] = defaultdict(lambda: [])
        for table in self.metadata.sorted_tables:
            qualified_name = qualified_table_name(table)

            # Link tables have exactly two foreign key constraints and all columns are
            # involved in them
            fk_constraints = sorted(
                table.foreign_key_constraints, key=get_constraint_sort_key
            )
            if len(fk_constraints) == 2 and all(
                col.foreign_keys for col in table.columns
            ):
                model = models_by_table_name[qualified_name] = Model(table)
                tablename = fk_constraints[0].elements[0].column.table.name
                links[tablename].append(model)
                continue

            # Only form model classes for tables that have a primary key and are not
            # association tables
            if not table.primary_key:
                models_by_table_name[qualified_name] = Model(table)
            else:
                model = ModelClass(table)
                models_by_table_name[qualified_name] = model

                # Fill in the columns
                for column in table.c:
                    column_attr = ColumnAttribute(model, column)
                    model.columns.append(column_attr)

        # Add relationships
        for model in models_by_table_name.values():
            if isinstance(model, ModelClass):
                self.generate_relationships(
                    model, models_by_table_name, links[model.table.name]
                )

        # Nest inherited classes in their superclasses to ensure proper ordering
        if "nojoined" not in self.options:
            for model in list(models_by_table_name.values()):
                if not isinstance(model, ModelClass):
                    continue

                pk_column_names = {col.name for col in model.table.primary_key.columns}
                for constraint in model.table.foreign_key_constraints:
                    if set(get_column_names(constraint)) == pk_column_names:
                        target = models_by_table_name[
                            qualified_table_name(constraint.elements[0].column.table)
                        ]
                        if isinstance(target, ModelClass):
                            model.parent_class = target
                            target.children.append(model)

        # Change base if we only have tables
        if not any(
            isinstance(model, ModelClass) for model in models_by_table_name.values()
        ):
            super().generate_base()

        # Collect the imports
        self.collect_imports(models_by_table_name.values())

        # Rename models and their attributes that conflict with imports or other
        # attributes
        global_names = {
            name for namespace in self.imports.values() for name in namespace
        }
        for model in models_by_table_name.values():
            self.generate_model_name(model, global_names)
            global_names.add(model.name)

        return list(models_by_table_name.values())

    def generate_relationships(
        self,
        source: ModelClass,
        models_by_table_name: dict[str, Model],
        association_tables: list[Model],
    ) -> list[RelationshipAttribute]:
        relationships: list[RelationshipAttribute] = []
        reverse_relationship: RelationshipAttribute | None

        # Add many-to-one (and one-to-many) relationships
        pk_column_names = {col.name for col in source.table.primary_key.columns}
        for constraint in sorted(
            source.table.foreign_key_constraints, key=get_constraint_sort_key
        ):
            target = models_by_table_name[
                qualified_table_name(constraint.elements[0].column.table)
            ]
            if isinstance(target, ModelClass):
                if "nojoined" not in self.options:
                    if set(get_column_names(constraint)) == pk_column_names:
                        parent = models_by_table_name[
                            qualified_table_name(constraint.elements[0].column.table)
                        ]
                        if isinstance(parent, ModelClass):
                            source.parent_class = parent
                            parent.children.append(source)
                            continue

                # Add uselist=False to One-to-One relationships
                column_names = get_column_names(constraint)
                if any(
                    isinstance(c, (PrimaryKeyConstraint, UniqueConstraint))
                    and {col.name for col in c.columns} == set(column_names)
                    for c in constraint.table.constraints
                ):
                    r_type = RelationshipType.ONE_TO_ONE
                else:
                    r_type = RelationshipType.MANY_TO_ONE

                relationship = RelationshipAttribute(r_type, source, target, constraint)
                source.relationships.append(relationship)

                # For self referential relationships, remote_side needs to be set
                if source is target:
                    relationship.remote_side = [
                        source.get_column_attribute(col.name)
                        for col in constraint.referred_table.primary_key
                    ]

                # If the two tables share more than one foreign key constraint,
                # SQLAlchemy needs an explicit primaryjoin to figure out which column(s)
                # it needs
                common_fk_constraints = get_common_fk_constraints(
                    source.table, target.table
                )
                if len(common_fk_constraints) > 1:
                    relationship.foreign_keys = [
                        source.get_column_attribute(key)
                        for key in constraint.column_keys
                    ]

                # Generate the opposite end of the relationship in the target class
                if "nobidi" not in self.options:
                    if r_type is RelationshipType.MANY_TO_ONE:
                        r_type = RelationshipType.ONE_TO_MANY

                    reverse_relationship = RelationshipAttribute(
                        r_type,
                        target,
                        source,
                        constraint,
                        foreign_keys=relationship.foreign_keys,
                        backref=relationship,
                    )
                    relationship.backref = reverse_relationship
                    target.relationships.append(reverse_relationship)

                    # For self referential relationships, remote_side needs to be set
                    if source is target:
                        reverse_relationship.remote_side = [
                            source.get_column_attribute(colname)
                            for colname in constraint.column_keys
                        ]

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = sorted(
                association_table.table.foreign_key_constraints,
                key=get_constraint_sort_key,
            )
            target = models_by_table_name[
                qualified_table_name(fk_constraints[1].elements[0].column.table)
            ]
            if isinstance(target, ModelClass):
                relationship = RelationshipAttribute(
                    RelationshipType.MANY_TO_MANY,
                    source,
                    target,
                    fk_constraints[1],
                    association_table,
                )
                source.relationships.append(relationship)

                # Generate the opposite end of the relationship in the target class
                reverse_relationship = None
                if "nobidi" not in self.options:
                    reverse_relationship = RelationshipAttribute(
                        RelationshipType.MANY_TO_MANY,
                        target,
                        source,
                        fk_constraints[0],
                        association_table,
                        relationship,
                    )
                    relationship.backref = reverse_relationship
                    target.relationships.append(reverse_relationship)

                # Add a primary/secondary join for self-referential many-to-many
                # relationships
                if source is target:
                    both_relationships = [relationship]
                    reverse_flags = [False, True]
                    if reverse_relationship:
                        both_relationships.append(reverse_relationship)

                    for relationship, reverse in zip(both_relationships, reverse_flags):
                        if (
                            not relationship.association_table
                            or not relationship.constraint
                        ):
                            continue

                        constraints = sorted(
                            relationship.constraint.table.foreign_key_constraints,
                            key=get_constraint_sort_key,
                            reverse=reverse,
                        )
                        pri_pairs = zip(
                            get_column_names(constraints[0]), constraints[0].elements
                        )
                        sec_pairs = zip(
                            get_column_names(constraints[1]), constraints[1].elements
                        )
                        relationship.primaryjoin = [
                            (
                                relationship.source,
                                elem.column.name,
                                relationship.association_table,
                                col,
                            )
                            for col, elem in pri_pairs
                        ]
                        relationship.secondaryjoin = [
                            (
                                relationship.target,
                                elem.column.name,
                                relationship.association_table,
                                col,
                            )
                            for col, elem in sec_pairs
                        ]

        return relationships

    def generate_model_name(self, model: Model, global_names: set[str]) -> None:
        if isinstance(model, ModelClass):
            preferred_name = _re_invalid_identifier.sub("_", model.table.name)
            preferred_name = "".join(
                part[:1].upper() + part[1:] for part in preferred_name.split("_")
            )
            if "use_inflect" in self.options:
                singular_name = self.inflect_engine.singular_noun(preferred_name)
                if singular_name:
                    preferred_name = singular_name

            model.name = self.find_free_name(preferred_name, global_names)

            # Fill in the names for column attributes
            local_names: set[str] = set()
            for column_attr in model.columns:
                self.generate_column_attr_name(column_attr, global_names, local_names)
                local_names.add(column_attr.name)

            # Fill in the names for relationship attributes
            for relationship in model.relationships:
                self.generate_relationship_name(relationship, global_names, local_names)
                local_names.add(relationship.name)
        else:
            super().generate_model_name(model, global_names)

    def generate_column_attr_name(
        self,
        column_attr: ColumnAttribute,
        global_names: set[str],
        local_names: set[str],
    ) -> None:
        column_attr.name = self.find_free_name(
            column_attr.column.name, global_names, local_names
        )

    def generate_relationship_name(
        self,
        relationship: RelationshipAttribute,
        global_names: set[str],
        local_names: set[str],
    ) -> None:
        # Self referential reverse relationships
        preferred_name: str
        if (
            relationship.type
            in (RelationshipType.ONE_TO_MANY, RelationshipType.ONE_TO_ONE)
            and relationship.source is relationship.target
            and relationship.backref
            and relationship.backref.name
        ):
            preferred_name = relationship.backref.name + "_reverse"
        else:
            preferred_name = relationship.target.table.name

            # If there's a constraint with a single column that ends with "_id", use the
            # preceding part as the relationship name
            if relationship.constraint:
                is_source = relationship.source.table is relationship.constraint.table
                if is_source or relationship.type not in (
                    RelationshipType.ONE_TO_ONE,
                    RelationshipType.ONE_TO_MANY,
                ):
                    column_names = [c.name for c in relationship.constraint.columns]
                    if len(column_names) == 1 and column_names[0].endswith("_id"):
                        preferred_name = column_names[0][:-3]

            if "use_inflect" in self.options:
                inflected_name: str | Literal[False]
                if relationship.type in (
                    RelationshipType.ONE_TO_MANY,
                    RelationshipType.MANY_TO_MANY,
                ):
                    if not self.inflect_engine.singular_noun(preferred_name):
                        preferred_name = self.inflect_engine.plural_noun(preferred_name)
                else:
                    inflected_name = self.inflect_engine.singular_noun(preferred_name)
                    if inflected_name:
                        preferred_name = inflected_name

        relationship.name = self.find_free_name(
            preferred_name, global_names, local_names
        )

    def render_models(self, models: list[Model]) -> str:
        rendered: list[str] = []
        for model in models:
            if isinstance(model, ModelClass):
                rendered.append(self.render_class(model))
            else:
                rendered.append(f"{model.name} = {self.render_table(model.table)}")

        return "\n\n\n".join(rendered)

    def render_class(self, model: ModelClass) -> str:
        sections: list[str] = []

        # Render class variables / special declarations
        class_vars: str = self.render_class_variables(model)
        if class_vars:
            sections.append(class_vars)

        # Render column attributes
        rendered_column_attributes: list[str] = []
        for nullable in (False, True):
            for column_attr in model.columns:
                if column_attr.column.nullable is nullable:
                    rendered_column_attributes.append(
                        self.render_column_attribute(column_attr)
                    )

        if rendered_column_attributes:
            sections.append("\n".join(rendered_column_attributes))

        # Render relationship attributes
        rendered_relationship_attributes: list[str] = [
            self.render_relationship(relationship)
            for relationship in model.relationships
        ]

        if rendered_relationship_attributes:
            sections.append("\n".join(rendered_relationship_attributes))

        declaration = self.render_class_declaration(model)
        rendered_sections = "\n\n".join(
            indent(section, self.indentation) for section in sections
        )
        return f"{declaration}\n{rendered_sections}"

    def render_class_declaration(self, model: ModelClass) -> str:
        parent_class_name = (
            model.parent_class.name if model.parent_class else self.base_class_name
        )
        return f"class {model.name}({parent_class_name}):"

    def render_class_variables(self, model: ModelClass) -> str:
        variables = [f"__tablename__ = {model.table.name!r}"]

        # Render constraints and indexes as __table_args__
        table_args = self.render_table_args(model.table)
        if table_args:
            variables.append(f"__table_args__ = {table_args}")

        return "\n".join(variables)

    def render_table_args(self, table: Table) -> str:
        args: list[str] = []
        kwargs: dict[str, str] = {}

        # Render constraints
        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            if uses_default_name(constraint):
                if isinstance(constraint, PrimaryKeyConstraint):
                    continue
                if (
                    isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint))
                    and len(constraint.columns) == 1
                ):
                    continue

            args.append(self.render_constraint(constraint))

        # Render indexes
        for index in sorted(table.indexes, key=lambda i: cast(str, i.name)):
            if len(index.columns) > 1 or not uses_default_name(index):
                args.append(self.render_index(index))

        if table.schema:
            kwargs["schema"] = table.schema

        if table.comment:
            kwargs["comment"] = table.comment

        if kwargs:
            formatted_kwargs = pformat(kwargs)
            if not args:
                return formatted_kwargs
            else:
                args.append(formatted_kwargs)

        if args:
            rendered_args = f",\n{self.indentation}".join(args)
            if len(args) == 1:
                rendered_args += ","

            return f"(\n{self.indentation}{rendered_args}\n)"
        else:
            return ""

    def render_column_python_type(self, column: Column[Any]) -> str:
        def get_type_qualifiers() -> tuple[str, TypeEngine[Any], str]:
            column_type = column.type
            pre: list[str] = []
            post_size = 0
            if column.nullable:
                self.add_literal_import("typing", "Optional")
                pre.append("Optional[")
                post_size += 1

            if isinstance(column_type, ARRAY):
                dim = getattr(column_type, "dimensions", None) or 1
                pre.extend("list[" for _ in range(dim))
                post_size += dim

                column_type = column_type.item_type

            return "".join(pre), column_type, "]" * post_size

        def render_python_type(column_type: TypeEngine[Any]) -> str:
            if isinstance(column_type, DOMAIN):
                python_type = column_type.data_type.python_type
            else:
                python_type = column_type.python_type

            python_type_name = python_type.__name__
            python_type_module = python_type.__module__
            if python_type_module == "builtins":
                return python_type_name

            try:
                self.add_module_import(python_type_module)
                return f"{python_type_module}.{python_type_name}"
            except NotImplementedError:
                self.add_literal_import("typing", "Any")
                return "Any"

        pre, col_type, post = get_type_qualifiers()
        column_python_type = f"{pre}{render_python_type(col_type)}{post}"
        return column_python_type

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        rendered_column = self.render_column(column, column_attr.name != column.name)
        rendered_column_python_type = self.render_column_python_type(column)

        return f"{column_attr.name}: Mapped[{rendered_column_python_type}] = {rendered_column}"

    def render_relationship(self, relationship: RelationshipAttribute) -> str:
        def render_column_attrs(column_attrs: list[ColumnAttribute]) -> str:
            rendered = []
            for attr in column_attrs:
                if attr.model is relationship.source:
                    rendered.append(attr.name)
                else:
                    rendered.append(repr(f"{attr.model.name}.{attr.name}"))

            return "[" + ", ".join(rendered) + "]"

        def render_foreign_keys(column_attrs: list[ColumnAttribute]) -> str:
            rendered = []
            render_as_string = False
            # Assume that column_attrs are all in relationship.source or none
            for attr in column_attrs:
                if attr.model is relationship.source:
                    rendered.append(attr.name)
                else:
                    rendered.append(f"{attr.model.name}.{attr.name}")
                    render_as_string = True

            if render_as_string:
                return "'[" + ", ".join(rendered) + "]'"
            else:
                return "[" + ", ".join(rendered) + "]"

        def render_join(terms: list[JoinType]) -> str:
            rendered_joins = []
            for source, source_col, target, target_col in terms:
                rendered = f"lambda: {source.name}.{source_col} == {target.name}."
                if target.__class__ is Model:
                    rendered += "c."

                rendered += str(target_col)
                rendered_joins.append(rendered)

            if len(rendered_joins) > 1:
                rendered = ", ".join(rendered_joins)
                return f"and_({rendered})"
            else:
                return rendered_joins[0]

        # Render keyword arguments
        kwargs: dict[str, Any] = {}
        if relationship.type is RelationshipType.ONE_TO_ONE and relationship.constraint:
            if relationship.constraint.referred_table is relationship.source.table:
                kwargs["uselist"] = False

        # Add the "secondary" keyword for many-to-many relationships
        if relationship.association_table:
            table_ref = relationship.association_table.table.name
            if relationship.association_table.schema:
                table_ref = f"{relationship.association_table.schema}.{table_ref}"

            kwargs["secondary"] = repr(table_ref)

        if relationship.remote_side:
            kwargs["remote_side"] = render_column_attrs(relationship.remote_side)

        if relationship.foreign_keys:
            kwargs["foreign_keys"] = render_foreign_keys(relationship.foreign_keys)

        if relationship.primaryjoin:
            kwargs["primaryjoin"] = render_join(relationship.primaryjoin)

        if relationship.secondaryjoin:
            kwargs["secondaryjoin"] = render_join(relationship.secondaryjoin)

        if relationship.backref:
            kwargs["back_populates"] = repr(relationship.backref.name)

        rendered_relationship = render_callable(
            "relationship", repr(relationship.target.name), kwargs=kwargs
        )

        relationship_type: str
        if relationship.type == RelationshipType.ONE_TO_MANY:
            relationship_type = f"list['{relationship.target.name}']"
        elif relationship.type in (
            RelationshipType.ONE_TO_ONE,
            RelationshipType.MANY_TO_ONE,
        ):
            relationship_type = f"'{relationship.target.name}'"
            if relationship.constraint and any(
                col.nullable for col in relationship.constraint.columns
            ):
                self.add_literal_import("typing", "Optional")
                relationship_type = f"Optional[{relationship_type}]"
        elif relationship.type == RelationshipType.MANY_TO_MANY:
            relationship_type = f"list['{relationship.target.name}']"
        else:
            self.add_literal_import("typing", "Any")
            relationship_type = "Any"

        return (
            f"{relationship.name}: Mapped[{relationship_type}] "
            f"= {rendered_relationship}"
        )


class DataclassGenerator(DeclarativeGenerator):
    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
        base_class_name: str = "Base",
        quote_annotations: bool = False,
        metadata_key: str = "sa",
    ):
        super().__init__(
            metadata,
            bind,
            options,
            indentation=indentation,
            base_class_name=base_class_name,
        )
        self.metadata_key: str = metadata_key
        self.quote_annotations: bool = quote_annotations

    def generate_base(self) -> None:
        self.base = Base(
            literal_imports=[
                LiteralImport("sqlalchemy.orm", "DeclarativeBase"),
                LiteralImport("sqlalchemy.orm", "MappedAsDataclass"),
            ],
            declarations=[
                (f"class {self.base_class_name}(MappedAsDataclass, DeclarativeBase):"),
                f"{self.indentation}pass",
            ],
            metadata_ref=f"{self.base_class_name}.metadata",
        )


class SQLModelGenerator(DeclarativeGenerator):
    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
        base_class_name: str = "SQLModel",
    ):
        super().__init__(
            metadata,
            bind,
            options,
            indentation=indentation,
            base_class_name=base_class_name,
        )

    @property
    def views_supported(self) -> bool:
        return False

    def render_column_callable(self, is_table: bool, *args: Any, **kwargs: Any) -> str:
        self.add_import(Column)
        return render_callable("Column", *args, kwargs=kwargs)

    def generate_base(self) -> None:
        self.base = Base(
            literal_imports=[],
            declarations=[],
            metadata_ref="",
        )

    def collect_imports(self, models: Iterable[Model]) -> None:
        super(DeclarativeGenerator, self).collect_imports(models)
        if any(isinstance(model, ModelClass) for model in models):
            self.remove_literal_import("sqlalchemy", "MetaData")
            self.add_literal_import("sqlmodel", "SQLModel")
            self.add_literal_import("sqlmodel", "Field")

    def collect_imports_for_model(self, model: Model) -> None:
        super(DeclarativeGenerator, self).collect_imports_for_model(model)
        if isinstance(model, ModelClass):
            for column_attr in model.columns:
                if column_attr.column.nullable:
                    self.add_literal_import("typing", "Optional")
                    break

            if model.relationships:
                self.add_literal_import("sqlmodel", "Relationship")

    def render_module_variables(self, models: list[Model]) -> str:
        declarations: list[str] = []
        if any(not isinstance(model, ModelClass) for model in models):
            if self.base.table_metadata_declaration is not None:
                declarations.append(self.base.table_metadata_declaration)

        return "\n".join(declarations)

    def render_class_declaration(self, model: ModelClass) -> str:
        if model.parent_class:
            parent = model.parent_class.name
        else:
            parent = self.base_class_name

        superclass_part = f"({parent}, table=True)"
        return f"class {model.name}{superclass_part}:"

    def render_class_variables(self, model: ModelClass) -> str:
        variables = []

        if model.table.name != model.name.lower():
            variables.append(f"__tablename__ = {model.table.name!r}")

        # Render constraints and indexes as __table_args__
        table_args = self.render_table_args(model.table)
        if table_args:
            variables.append(f"__table_args__ = {table_args}")

        return "\n".join(variables)

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        rendered_column = self.render_column(column, True)
        rendered_column_python_type = self.render_column_python_type(column)

        kwargs: dict[str, Any] = {}
        if column.nullable:
            kwargs["default"] = None
        kwargs["sa_column"] = f"{rendered_column}"

        rendered_field = render_callable("Field", kwargs=kwargs)

        return f"{column_attr.name}: {rendered_column_python_type} = {rendered_field}"

    def render_relationship(self, relationship: RelationshipAttribute) -> str:
        rendered = super().render_relationship(relationship).partition(" = ")[2]
        args = self.render_relationship_args(rendered)
        kwargs: dict[str, Any] = {}
        annotation = repr(relationship.target.name)

        if relationship.type in (
            RelationshipType.ONE_TO_MANY,
            RelationshipType.MANY_TO_MANY,
        ):
            annotation = f"list[{annotation}]"
        else:
            self.add_literal_import("typing", "Optional")
            annotation = f"Optional[{annotation}]"

        rendered_field = render_callable("Relationship", *args, kwargs=kwargs)
        return f"{relationship.name}: {annotation} = {rendered_field}"

    def render_relationship_args(self, arguments: str) -> list[str]:
        argument_list = arguments.split(",")
        # delete ')' and ' ' from args
        argument_list[-1] = argument_list[-1][:-1]
        argument_list = [argument[1:] for argument in argument_list]

        rendered_args: list[str] = []
        for arg in argument_list:
            if "back_populates" in arg:
                rendered_args.append(arg)
            if "uselist=False" in arg:
                rendered_args.append("sa_relationship_kwargs={'uselist': False}")

        return rendered_args
