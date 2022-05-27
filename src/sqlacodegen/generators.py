from __future__ import annotations

import inspect
import re
import sys
from abc import ABCMeta, abstractmethod
from collections import ChainMap, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import field
from functools import partial
from inspect import Parameter
from textwrap import indent
from typing import Any, ClassVar, Optional

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
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import CompileError
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql.elements import TextClause

from .models import (
    Attribute,
    CallableModel,
    ClassModel,
    ColumnAttribute,
    ColumnTypeModel,
    Import,
    JoinModel,
    LambdaReference,
    ListModel,
    Reference,
    RelationshipAttribute,
    ReprReference,
    TableArgsModel,
    TableModel, Lambda,
)
from .utils import (
    convert_to_valid_identifier,
    decode_postgresql_sequence,
    get_column_names,
    get_common_fk_constraints,
    get_compiled_expression,
    get_constraint_sort_key,
    get_fk_options,
    next_alias,
    qualified_table_name,
    uses_default_name,
)

if sys.version_info < (3, 10):
    from importlib_metadata import version
else:
    from importlib.metadata import version

_sqla_version = tuple(int(x) for x in version("sqlalchemy").split(".")[:2])
_re_boolean_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \(0, 1\)")
_re_column_name = re.compile(r'(?:(["`]?).*\1\.)?(["`]?)(.*)\2')
_re_enum_check_constraint = re.compile(r"(?:.*?\.)?(.*?) IN \((.+)\)")
_re_enum_item = re.compile(r"'(.*?)(?<!\\)'")


class CodeGenerator(metaclass=ABCMeta):
    valid_options: ClassVar[set[str]] = {"noindexes", "noconstraints", "nocomments"}

    def __init__(
        self,
        metadata: MetaData,
        bind: Connection | Engine,
        options: Sequence[str],
        *,
        indentation: str = "    ",
    ):
        self.metadata: MetaData = metadata
        self.bind: Connection | Engine = bind
        self.options: set[str] = set(options)
        self.indentation = indentation
        self.imports: set[Import] = set()
        self.module_variables: list[Attribute] = []
        self.models: list[TableModel | ClassModel] = []

        # Validate options
        invalid_options = {opt for opt in options if opt not in self.valid_options}
        if invalid_options:
            raise ValueError("Unrecognized options: " + ", ".join(invalid_options))

    def generate(self) -> str:
        """Generate the code for the given metadata."""
        self.adjust_metadata()
        self.generate_models()
        self.collect_imports()
        self.fix_name_conflicts()
        return self.render()

    def should_ignore_table(self, table: Table) -> bool:
        """
        Determine if the given table should be excluded from code generation.

        This is chiefly to support Alembic and sqlalchemy-migrate whose schema version
        tables should not be included in the results.

        :param table: the table object
        :return: ``True`` if the table should be excluded, ``False`` if not

        """
        return table.name in ("alembic_version", "migrate_version")

    def adjust_metadata(self) -> None:
        """
        Perform post-reflection adjustments on the metadata.

        This method is called before the actual model generation.

        """
        # Remove unwanted elements from the metadata
        for table in list(self.metadata.tables.values()):
            if self.should_ignore_table(table):
                self.metadata.remove(table)
                continue

            self.adjust_table(table)

        # Perform table specific adjustments
        for table in self.metadata.tables.values():
            self.adjust_table(table)

    def adjust_table(self, table: Table) -> None:
        """Perform post-reflection adjustments on a table."""

        # Remove indexes if so requested
        if "noindexes" in self.options:
            table.indexes.clear()

        # Remove constraints if so requested
        if "noconstraints" in self.options:
            table.constraints.clear()

        # Remove table comment if so requested
        if "nocomments" in self.options:
            table.comment = None

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

        # Perform column-specific adjustments
        for column in table.columns:
            self.adjust_column(column)

    def adjust_column(self, column: Column) -> None:
        """Perform post-reflection adjustments on a table column."""

        # Replace the dialect specific type with the most appropriate generic type
        try:
            column.type = self.get_adapted_type(column.type)
        except CompileError:
            pass

        # Remove column comment if so requested
        if "nocomments" in self.options:
            column.comment = None

        # PostgreSQL specific fix: detect sequences from server_default
        if column.server_default and self.bind.dialect.name == "postgresql":
            if isinstance(column.server_default, DefaultClause) and isinstance(
                column.server_default.arg, TextClause
            ):
                schema, seqname = decode_postgresql_sequence(column.server_default.arg)
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
                # Hack to fix adaptation of the Enum class which is broken since
                # SQLAlchemy 1.2
                kw = {}
                if supercls is Enum:
                    kw["name"] = coltype.name

                try:
                    new_coltype = coltype.adapt(supercls)
                except TypeError:
                    # If the adaptation fails, don't try again
                    break

                for key, value in kw.items():
                    setattr(new_coltype, key, value)

                if isinstance(coltype, ARRAY):
                    new_coltype.item_type = self.get_adapted_type(new_coltype.item_type)
                elif isinstance(coltype, JSONB):
                    new_coltype.astext_type = self.get_adapted_type(
                        new_coltype.astext_type
                    )

                try:
                    # If the adapted column type does not render the same as the
                    # original, don't substitute it
                    if new_coltype.compile(self.bind.engine.dialect) != compiled_type:
                        # Make an exception to the rule for Float and arrays of Float,
                        # since at least on PostgreSQL, Float can accurately represent
                        # both REAL and DOUBLE_PRECISION
                        if not isinstance(new_coltype, Float) and not (
                            isinstance(new_coltype, ARRAY)
                            and isinstance(new_coltype.item_type, Float)
                        ):
                            break
                except CompileError:
                    # If the adapted column type can't be compiled, don't substitute it
                    break

                # Stop on the first valid non-uppercase column type class
                coltype = new_coltype
                if supercls.__name__ != supercls.__name__.upper():
                    break

        return coltype

    @abstractmethod
    def generate_models(self) -> None:
        """
        Generate the abstract models from the adjusted metadata.

        This method should fill in the ``module_variables``, ``table_models`` and
        ``class_models`` lists, as appropriate.
        """

    def generate_table_model(self, table: Table, metadata: Attribute) -> TableModel:
        args: list[Any] = [repr(table.name), Reference(metadata)]
        kwargs: dict[str, Any] = {}

        # Generate models for columns
        column_models = {
            col.name: self.generate_column_model(col) for col in table.columns
        }
        args.extend(column_models.values())

        # Generate models for constraints
        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            # Remove nullable=False from columns involved in a single-column primary key
            if isinstance(constraint, PrimaryKeyConstraint) and len(constraint.columns) == 1:
                for col in constraint.columns:
                    column_models[col.name].kwargs.pop("nullable", None)

            if uses_default_name(constraint):
                if isinstance(constraint, PrimaryKeyConstraint):
                    for col in constraint.columns:
                        column_models[col.name].kwargs = {"primary_key": True, **column_models[col.name].kwargs}

                    continue
                elif (
                    isinstance(constraint, ForeignKeyConstraint)
                    and len(constraint.columns) == 1
                ):
                    remote_column = constraint.elements[0].target_fullname
                    column_model = column_models[constraint.columns[0].name]
                    column_model.args.append(
                        CallableModel(
                            ForeignKey,
                            [repr(remote_column)],
                            get_fk_options(constraint),
                        )
                    )

                    # Remove the column type unless the foreign key points to the column
                    # itself
                    local_column_name = (
                        f"{qualified_table_name(table)}.{column_model.args[0][1:-1]}"
                    )
                    if remote_column != local_column_name:
                        column_model.args = [
                            arg
                            for arg in column_model.args
                            if not isinstance(arg, ColumnTypeModel)
                        ]

                    continue
                elif (
                    isinstance(constraint, UniqueConstraint)
                    and len(constraint.columns) == 1
                ):
                    column_models[constraint.columns[0].name].kwargs["unique"] = True
                    continue

            args.append(self.generate_constraint_model(constraint))

        # Generate models for indexes
        for index in sorted(table.indexes, key=lambda i: i.name):
            # One-column indexes should be rendered as index=True on columns
            if len(index.columns) > 1 or not uses_default_name(index):
                args.append(self.generate_index_model(index))
            else:
                column_models[index.columns[0].name].kwargs["index"] = True
                if index.unique:
                    column_models[index.columns[0].name].kwargs["unique"] = True

        # Add the schema name as keyword argument
        if table.schema:
            kwargs["schema"] = repr(table.schema)

        # Add the table comment as keyword argument
        table_comment = getattr(table, "comment", None)
        if table_comment:
            kwargs["comment"] = repr(table.comment)

        return TableModel(self.generate_table_name(table), args, kwargs, table)

    def generate_table_name(self, table: Table) -> str:
        return convert_to_valid_identifier(f"t_{table.name}")

    def generate_column_model(self, column: Column) -> CallableModel:
        args = [repr(column.name), self.generate_column_type_model(column.type)]
        kwargs: dict[str, Any] = {}

        if column.default:
            args.append(column.default)

        if column.key != column.name:
            kwargs["key"] = column.key

        if not column.nullable:
            kwargs["nullable"] = False

        if isinstance(column.server_default, DefaultClause):
            kwargs["server_default"] = CallableModel(
                text, [repr(column.server_default.arg.text)]
            )
        elif isinstance(column.server_default, Computed):
            expression = str(column.server_default.sqltext)

            computed_kwargs = {}
            if column.server_default.persisted is not None:
                computed_kwargs["persisted"] = column.server_default.persisted

            args.append(CallableModel(Computed, [repr(expression)], computed_kwargs))
        elif isinstance(column.server_default, Identity):
            args.append(column.server_default)
        elif column.server_default:
            kwargs["server_default"] = column.server_default

        comment = getattr(column, "comment", None)
        if comment:
            kwargs["comment"] = repr(comment)

        return CallableModel(Column, args, kwargs)

    def generate_column_type_model(self, coltype: object) -> ColumnTypeModel:
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
                continue

            value = getattr(coltype, param.name, missing)
            default = defaults.get(param.name, missing)
            if value is missing or value == default:
                use_kwargs = True
            elif use_kwargs:
                kwargs[param.name] = value
            else:
                args.append(value)

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

        if isinstance(coltype, Enum) and coltype.name is not None:
            kwargs["name"] = repr(coltype.name)

        if isinstance(coltype, JSONB):
            # Remove astext_type if it's the default
            if (
                isinstance(coltype.astext_type, Text)
                and coltype.astext_type.length is None
            ):
                del kwargs["astext_type"]
            else:
                kwargs["astext_type"] = self.generate_column_type_model(
                    kwargs["astext_type"]
                )
        elif isinstance(coltype, ARRAY):
            args[0] = self.generate_column_type_model(args[0])

        return ColumnTypeModel(coltype.__class__, args, kwargs)

    def generate_constraint_model(
        self, constraint: Constraint | ForeignKey
    ) -> CallableModel:
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
                f"Cannot process constraint of type {constraint.__class__.__name__}"
            )

        if isinstance(constraint, Constraint) and not uses_default_name(constraint):
            kwargs["name"] = repr(constraint.name)

        return CallableModel(constraint.__class__, args, kwargs)

    def generate_index_model(self, index: Index) -> CallableModel:
        args = [repr(index.name)] + [repr(col.name) for col in index.columns]
        kwargs = {}
        if index.unique:
            kwargs["unique"] = True

        return CallableModel(Index, args, kwargs)

    def collect_imports(self) -> None:
        """
        Collect and store any imports from the generated models.

        This method should fill in the ``imports`` list.

        """
        Import.reset_cache()

        for var in self.module_variables:
            self.imports.update(var.collect_imports())

        for model in self.models:
            self.imports.update(model.collect_imports())

        # Remove imports of builtins
        for import_ in list(self.imports):
            if import_.from_ == "builtins":
                self.imports.remove(import_)

    def fix_name_conflicts(self) -> None:
        """Ensure that module names, imports and attribute names are not in conflict."""
        imports = {i.visible_name: i for i in self.imports}
        variable_names = {a.name for a in self.module_variables}
        model_names = {}
        global_names_to_avoid: ChainMap[str, Any] = ChainMap(
            variable_names, model_names
        )
        for model in self.models:
            if model.name in global_names_to_avoid:
                model.name = next_alias(model.name, global_names_to_avoid)

            model_names[model.name] = model

            if isinstance(model, ClassModel):
                local_names_to_avoid = {}
                all_names_to_avoid = ChainMap(
                    local_names_to_avoid, global_names_to_avoid
                )
                for attr in model.attributes:
                    # "metadata" is not an allowable attribute name
                    if attr.name == "metadata":
                        attr.name = next_alias(attr.name, local_names_to_avoid)
                        attr.value.args.insert(0, repr("metadata"))

                    # Check for conflicts with attributes defined earlier
                    if attr.name in local_names_to_avoid:
                        if isinstance(attr, ColumnAttribute) and not isinstance(
                            attr.value.args[0], str
                        ):
                            attr.value.args.insert(0, repr(attr.name))

                        attr.name = next_alias(attr.name, local_names_to_avoid)

                    # Add the name to the list of local names to avoid
                    local_names_to_avoid[attr.name] = attr

                    # Alias any conflicting import
                    while attr.name in imports:
                        import_ = imports[attr.name]
                        old_name = import_.visible_name
                        new_name = import_.alias = next_alias(
                            old_name, all_names_to_avoid
                        )
                        del imports[old_name]
                        imports[new_name] = import_

    def render(self) -> str:
        """
        Render the imports, module variables and models into a string.

        :return: the rendered module contents

        """
        return (
            self.render_imports()
            + "\n\n"
            + self.render_module_variables()
            + "\n\n\n"
            + self.render_models()
        )

    def render_imports(self) -> str:
        """
        Render the collected imports (``self.imports``).

        :return: the rendered imports

        """
        # Group the imports by origin
        future_imports: list[Import] = []
        stdlib_imports: list[Import] = []
        thirdparty_imports: list[Import] = []
        for import_ in self.imports:
            package = import_.module.split(".", 1)[0]
            collection = thirdparty_imports
            if package == "__future__":
                collection = future_imports
            elif package in sys.builtin_module_names:
                collection = stdlib_imports
            elif package in sys.modules:
                if "site-packages" not in (sys.modules[package].__file__ or ""):
                    collection = stdlib_imports

            collection.append(import_)

        # Divide imports by module
        rendered_groups: list[str] = []
        for group in future_imports, stdlib_imports, thirdparty_imports:
            imports_by_module: defaultdict[str, list[Import]] = defaultdict(list)
            for import_ in group:
                imports_by_module[import_.from_].append(import_)

            # Render imports from each module as a separate line
            sections = []
            for modulename in sorted(imports_by_module):
                sorted_imports = sorted(
                    imports_by_module[modulename], key=lambda i: i.name
                )
                names = [
                    f"{i.name} as {i.alias}" if i.alias else i.name
                    for i in sorted_imports
                ]
                sections.append(f"from {modulename} import " + ", ".join(names))

            rendered_groups.append("\n".join(sections))

        return "\n\n".join(grp for grp in rendered_groups if grp)

    def render_object(self, obj: object) -> str:
        if isinstance(obj, TableModel):
            return self.render_table(obj)
        elif isinstance(obj, ClassModel):
            return self.render_class(obj)
        elif isinstance(obj, ListModel):
            return "[" + ", ".join(map(self.render_object, obj.values)) + "]"
        elif isinstance(obj, TableArgsModel):
            rendered_kwargs = ", ".join(
                f"{key!r}: {self.render_object(value)}"
                for key, value in obj.kwargs.items()
            )
            if obj.kwargs and not obj.args:
                # If only keyword arguments are present, render as a dict
                return f"{{\n{indent(rendered_kwargs, self.indentation)}\n}}"
            else:
                args = obj.args.copy()
                if obj.kwargs:
                    args.append(f"{{{rendered_kwargs}}}")

                rendered_args = indent(
                    ",\n".join(self.render_object(arg) for arg in args),
                    self.indentation,
                )
                if len(args) == 1:
                    rendered_args += ","

                return f"(\n{rendered_args}\n)"
        elif isinstance(obj, Attribute):
            rendered = obj.name
            if obj.annotation:
                rendered += f": {obj.annotation}"

            return f"{rendered} = " + self.render_object(obj.value)
        elif isinstance(obj, CallableModel):
            func_import = Import.from_object(obj.func)
            if isinstance(obj, ColumnTypeModel) and not obj.args and not obj.kwargs:
                return func_import.visible_name

            rendered_args = [self.render_object(arg) for arg in obj.args] + [
                f"{key}={self.render_object(value)}"
                for key, value in obj.kwargs.items()
            ]
            joined_rendered_args = ", ".join(rendered_args)
            return f"{func_import.visible_name}({joined_rendered_args})"
        elif isinstance(obj, Reference):
            rendered = ".".join(target.name for target in obj.targets)
            if isinstance(obj, LambdaReference):
                return "lambda: " + rendered
            elif isinstance(obj, ReprReference):
                return repr(rendered)
            else:
                return rendered
        elif isinstance(obj, JoinModel):
            rendered_joins = " and ".join(
                [
                    f"{obj.source_class.name}.{col_attr.name} "
                    f"== {obj.target_table.name}.c.{target_colname}"
                    for col_attr, target_colname in obj.joins
                ]
            )
            return f"lambda: {rendered_joins}"
        elif isinstance(obj, Lambda):
            return "lambda: " + self.render_object(obj.value)
        else:
            return str(obj)

    def render_module_variables(self) -> str:
        rendered_variables: list[str] = [
            self.render_object(var) for var in self.module_variables
        ]
        return "\n".join(rendered_variables)

    def render_models(self) -> str:
        rendered_models: list[str] = [
            self.render_object(model) for model in self.models
        ]
        return "\n\n".join(rendered_models)

    def render_table(self, model: TableModel) -> str:
        args = [self.render_object(arg) for arg in model.args]
        args += [
            f"{key}={self.render_object(value)}" for key, value in model.kwargs.items()
        ]
        rendered_args = indent(
            ",\n".join(self.render_object(arg) for arg in args), self.indentation
        )
        return f"{model.name} = {model.func.__name__}(\n{rendered_args}\n)\n"

    def render_class(self, model: ClassModel) -> str:
        # Render the class name and its bases
        bases = ", ".join(attr.name for attr in model.bases)
        rendered = f"class {model.name}({bases}):\n"
        sections: list[str] = []

        # Render the docstring, if present
        if model.docstring:
            sections.append(self.render_docstring(model.docstring))

        # Render attributes
        rendered_attributes: dict[int, list[str]] = defaultdict(list)
        for attr in model.attributes:
            rendered_attr = self.render_object(attr)
            index = self.get_class_attribute_section_index(attr)
            if index is not None:
                rendered_attributes[index].append(rendered_attr)

        sections.extend(
            "\n".join(rendered_attributes[i]) for i in sorted(rendered_attributes)
        )

        # Join all rendered sections together
        if sections:
            rendered += indent("\n\n".join(sections), self.indentation)
        else:
            rendered += f"{self.indentation}pass"

        return rendered + "\n"

    def render_docstring(self, docstring: str) -> str:
        return f'"""\n{docstring}\n"""'

    def get_class_attribute_section_index(self, attr: Attribute) -> int | None:
        if attr.name.startswith("__") and attr.name.endswith("__"):
            return 0
        elif isinstance(attr, ColumnAttribute):
            return 1
        else:
            return 2


class TablesGenerator(CodeGenerator):
    def generate_models(self) -> None:
        metadata = Attribute("metadata", CallableModel(MetaData))
        self.module_variables.append(metadata)
        for table in self.metadata.sorted_tables:
            self.models.append(self.generate_table_model(table, metadata))


class DeclarativeGenerator(CodeGenerator):
    valid_options: ClassVar[set[str]] = TablesGenerator.valid_options | {
        "use_inflect",
        "nojoined",
        "nobidi",
    }
    base_class: Attribute

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

    def generate_models(self) -> None:
        self.base_class = self.generate_base_class()
        metadata = Attribute("metadata", CallableModel(MetaData))

        # Pick association tables from the metadata into their own set
        # (don't process them normally)
        class_models_by_table_name: dict[str, ClassModel] = {}
        association_tables: list[TableModel] = []
        for table in self.metadata.sorted_tables:
            # Link tables have exactly two foreign key constraints and all columns are
            # involved in them
            fk_constraints = sorted(
                table.foreign_key_constraints, key=get_constraint_sort_key
            )
            if len(fk_constraints) == 2 and all(
                col.foreign_keys for col in table.columns
            ):
                link_model = self.generate_table_model(table, metadata)
                self.models.append(link_model)
                association_tables.append(link_model)
                continue

            table_model = self.generate_table_model(table, metadata)
            if table.primary_key:
                model = self.convert_to_class_model(table_model)
                class_models_by_table_name[qualified_table_name(table)] = model
                self.models.append(model)
            else:
                self.models.append(table_model)

        # Generate relationships
        for model in self.models:
            if isinstance(model, ClassModel):
                self.generate_relationships(
                    model, class_models_by_table_name, association_tables
                )

        # Add the base class to the module if at least one class model was generated
        if any(isinstance(model, ClassModel) for model in self.models):
            metadata.value = f"{self.base_class.name}.metadata"
            self.module_variables.append(self.base_class)

        # If at least one Table was generated, add the "metadata" variable
        if any(isinstance(model, TableModel) for model in self.models):
            self.module_variables.append(metadata)

    def generate_base_class(self) -> Attribute:
        return Attribute(self.base_class_name, CallableModel(declarative_base))

    def convert_to_class_model(self, table_model: TableModel) -> ClassModel:
        table_args = TableArgsModel(kwargs=table_model.kwargs)
        attributes: list[Attribute] = [
            Attribute("__tablename__", repr(table_model.table.name))
        ]

        # Generate column attributes
        for arg in table_model.args[2:]:  # skip the table name and metadata arguments
            if isinstance(arg, CallableModel) and arg.func is Column:
                # Remove the explicit column name
                raw_name = arg.args[0][1:-1]
                attr_name = convert_to_valid_identifier(raw_name)
                if attr_name == raw_name:
                    del arg.args[0]

                attributes.append(ColumnAttribute(attr_name, arg))
            else:
                table_args.args.append(arg)

        # Insert __table_args__ if there are any positional or keyword arguments
        if table_args.args or table_args.kwargs:
            attributes.insert(1, Attribute("__table_args__", table_args))

        return ClassModel(
            self.generate_class_name(table_model.args[0]),
            [self.base_class],
            [],
            attributes,
            None,
            table_model.table,
        )

    def generate_class_name(self, table_name: str) -> str:
        name = convert_to_valid_identifier(table_name)
        name = "".join(part[:1].upper() + part[1:] for part in name.split("_"))
        if "use_inflect" in self.options:
            singular_name = self.inflect_engine.singular_noun(name)
            if singular_name:
                name = singular_name

        return name

    def generate_relationships(
        self,
        source_class: ClassModel,
        class_models_by_table_name: dict[str, ClassModel],
        association_tables: list[TableModel],
    ) -> None:
        # Add many-to-one (and one-to-many) relationships
        pk_column_names = {col.name for col in source_class.table.primary_key.columns}
        for constraint in sorted(
            source_class.table.foreign_key_constraints, key=get_constraint_sort_key
        ):
            # Try to find the target class for the relationship; move on if not found
            target_class = class_models_by_table_name.get(
                constraint.referred_table.fullname
            )
            if not target_class:
                continue

            # Pick out joined-class relationships first
            if "nojoined" not in self.options:
                if set(get_column_names(constraint)) == pk_column_names:
                    source_class.bases = [target_class]
                    continue

            # Make "lambda: " references to the target if it appears after the source
            reference_class = (
                LambdaReference
                if self.models.index(source_class) <= self.models.index(target_class)
                else Reference
            )
            relationship_model = CallableModel(
                relationship, [reference_class(target_class)]
            )

            # Generate the initial preferred name
            column_names = get_column_names(constraint)
            if len(column_names) == 1 and column_names[0].endswith("_id"):
                preferred_name = column_names[0][:-3]
            else:
                preferred_name = target_class.table.name

            # For self-referential relationships, remote_side needs to be set
            if source_class is target_class:
                relationship_model.kwargs["remote_side"] = ListModel(
                    Reference(attr)
                    for attr in source_class.attributes
                    if isinstance(attr, ColumnAttribute)
                    and attr.value.kwargs.get("primary_key")
                )

            # If the two tables share more than one foreign key constraint,
            # SQLAlchemy needs an explicit primaryjoin to figure out which column(s)
            # it needs
            common_fk_constraints = get_common_fk_constraints(
                source_class.table, target_class.table
            )
            if len(common_fk_constraints) > 1:
                # Make direct references for self-referential relationships
                if source_class is target_class:
                    make_reference = Reference
                else:
                    make_reference = partial(reference_class, source_class)

                relationship_model.kwargs["foreign_keys"] = ListModel(
                    Reference(attr)
                    for attr in source_class.attributes
                    if isinstance(attr, ColumnAttribute)
                    and attr.column_name in constraint.column_keys
                )

            # Add the relationship attribute to the source class
            relationship_attribute = RelationshipAttribute(
                preferred_name, relationship_model
            )
            source_class.attributes.append(relationship_attribute)

            # Add the reverse relationship to the target class
            if "nobidi" not in self.options:
                if self.models.index(source_class) >= self.models.index(target_class):
                    reverse_ref_class = LambdaReference
                else:
                    reverse_ref_class = Reference

                if source_class is target_class:
                    reverse_preferred_name = preferred_name + "_reverse"
                else:
                    reverse_preferred_name = source_class.table.name

                reverse_relationship_model = CallableModel(
                    relationship, [reverse_ref_class(source_class)]
                )

                # Add uselist=False to reverse side of one-to-one relationships
                if any(
                    isinstance(c, (PrimaryKeyConstraint, UniqueConstraint))
                    and {col.name for col in c.columns} == set(column_names)
                    for c in constraint.table.constraints
                ):
                    reverse_relationship_model.kwargs["uselist"] = False

                # For self-referential relationships, remote_side needs to be set
                if source_class is target_class:
                    reverse_relationship_model.kwargs["remote_side"] = ListModel(
                        Reference(attr)
                        for attr in source_class.attributes
                        if isinstance(attr, ColumnAttribute)
                        and attr.column_name in column_names
                    )

                if len(common_fk_constraints) > 1:
                    # Make direct references for self-referential relationships
                    if source_class is target_class:
                        make_reference = Reference
                    else:
                        make_reference = partial(Reference, source_class)

                    foreign_keys = ListModel(
                        make_reference(attr)
                        for attr in source_class.attributes
                        if isinstance(attr, ColumnAttribute)
                        and attr.column_name in constraint.column_keys
                    )
                    if self.models.index(source_class) > self.models.index(target_class):
                        foreign_keys = Lambda(foreign_keys)

                    reverse_relationship_model.kwargs["foreign_keys"] = foreign_keys

                reverse_relationship_model.kwargs["back_populates"] = ReprReference(
                    relationship_attribute
                )
                reverse_relationship_attribute = RelationshipAttribute(
                    reverse_preferred_name, reverse_relationship_model
                )
                relationship_model.kwargs["back_populates"] = ReprReference(
                    reverse_relationship_attribute
                )
                target_class.attributes.append(reverse_relationship_attribute)

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = sorted(
                association_table.table.foreign_key_constraints,
                key=get_constraint_sort_key,
            )
            if not fk_constraints[0].elements[0].references(source_class.table):
                continue

            target_class = class_models_by_table_name.get(
                fk_constraints[1].referred_table.fullname
            )
            if not target_class:
                continue

            # Make "lambda: " references to the target if it appears after the source
            reference_class = (
                LambdaReference
                if self.models.index(source_class) <= self.models.index(target_class)
                else Reference
            )
            relationship_model = CallableModel(
                relationship,
                [reference_class(target_class)],
                {"secondary": repr(association_table.table.fullname)},
            )

            # Generate primaryjoin and secondaryjoin for self-referential many-to-many
            if source_class is target_class:
                source_column_names = {
                    element.column.name for element in fk_constraints[0].elements
                }
                source_columns = [
                    attr
                    for attr in source_class.attributes
                    if isinstance(attr, ColumnAttribute)
                    and attr.column_name in source_column_names
                ]
                target_column_names = fk_constraints[0].column_keys
                joins = list(zip(source_columns, target_column_names))
                relationship_model.kwargs["primaryjoin"] = JoinModel(
                    source_class, association_table, joins
                )

                source_column_names = {
                    element.column.name for element in fk_constraints[1].elements
                }
                source_columns = [
                    attr
                    for attr in source_class.attributes
                    if isinstance(attr, ColumnAttribute)
                    and attr.column_name in source_column_names
                ]
                target_column_names = fk_constraints[1].column_keys
                joins = list(zip(source_columns, target_column_names))
                relationship_model.kwargs["secondaryjoin"] = JoinModel(
                    source_class, association_table, joins
                )

            # Generate the initial preferred name
            column_names = get_column_names(fk_constraints[1])
            if len(column_names) == 1 and column_names[0].endswith("_id"):
                preferred_name = column_names[0][:-3]
            else:
                preferred_name = target_class.table.name

            # Add the relationship attribute to the source class
            relationship_attribute = RelationshipAttribute(
                preferred_name, relationship_model
            )
            source_class.attributes.append(relationship_attribute)

            # Generate the opposite end of the relationship in the target class
            reverse_relationship_model: CallableModel | None = None
            if "nobidi" not in self.options:
                if self.models.index(source_class) >= self.models.index(target_class):
                    reverse_ref_class = LambdaReference
                else:
                    reverse_ref_class = Reference

                reverse_relationship_model = CallableModel(
                    relationship,
                    [reverse_ref_class(source_class)],
                    {"secondary": repr(association_table.table.fullname)},
                )

                # Generate primaryjoin and secondaryjoin for self-referential
                # many-to-many
                if source_class is target_class:
                    source_column_names = {
                        element.column.name for element in fk_constraints[1].elements
                    }
                    source_columns = [
                        attr
                        for attr in source_class.attributes
                        if isinstance(attr, ColumnAttribute)
                        and attr.column_name in source_column_names
                    ]
                    target_column_names = fk_constraints[1].column_keys
                    joins = list(zip(source_columns, target_column_names))
                    reverse_relationship_model.kwargs["primaryjoin"] = JoinModel(
                        source_class, association_table, joins
                    )

                    source_column_names = {
                        element.column.name for element in fk_constraints[0].elements
                    }
                    source_columns = [
                        attr
                        for attr in source_class.attributes
                        if isinstance(attr, ColumnAttribute)
                        and attr.column_name in source_column_names
                    ]
                    target_columns = fk_constraints[0].column_keys
                    joins = list(zip(source_columns, target_columns))
                    reverse_relationship_model.kwargs["secondaryjoin"] = JoinModel(
                        source_class, association_table, joins
                    )

                # Generate the initial preferred name
                column_names = get_column_names(fk_constraints[0])
                if len(column_names) == 1 and column_names[0].endswith("_id"):
                    reverse_preferred_name = column_names[0][:-3]
                else:
                    reverse_preferred_name = source_class.table.name

                reverse_relationship_model.kwargs["back_populates"] = ReprReference(
                    relationship_attribute
                )
                reverse_relationship_attribute = RelationshipAttribute(
                    reverse_preferred_name, reverse_relationship_model
                )
                relationship_model.kwargs["back_populates"] = ReprReference(
                    reverse_relationship_attribute
                )
                target_class.attributes.append(reverse_relationship_attribute)

    # def generate_relationship_name(
    #     self,
    #     relationship: RelationshipAttribute,
    # ) -> None:
    #     # Self referential reverse relationships
    #     if (
    #         relationship.type
    #         in (RelationshipType.ONE_TO_MANY, RelationshipType.ONE_TO_ONE)
    #         and relationship.source is relationship.target
    #         and relationship.backref
    #         and relationship.backref.name
    #     ):
    #         preferred_name = relationship.backref.name + "_reverse"
    #     else:
    #         preferred_name = relationship.target.table.name
    #
    #         # If there's a constraint with a single column that ends with "_id", use the
    #         # preceding part as the relationship name
    #         if relationship.constraint:
    #             is_source = relationship.source.table is relationship.constraint.table
    #             if is_source or relationship.type not in (
    #                 RelationshipType.ONE_TO_ONE,
    #                 RelationshipType.ONE_TO_MANY,
    #             ):
    #                 column_names = [c.name for c in relationship.constraint.columns]
    #                 if len(column_names) == 1 and column_names[0].endswith("_id"):
    #                     preferred_name = column_names[0][:-3]
    #
    #         if "use_inflect" in self.options:
    #             if relationship.type in (
    #                 RelationshipType.ONE_TO_MANY,
    #                 RelationshipType.MANY_TO_MANY,
    #             ):
    #                 preferred_name = self.inflect_engine.plural_noun(preferred_name)
    #             else:
    #                 preferred_name = self.inflect_engine.singular_noun(preferred_name)
    #
    #     relationship.name = convert_to_valid_identifier(preferred_name)

    #
    # def render_module_variables(self, models: list[Model]) -> str:
    #     if not any(isinstance(model, ModelClass) for model in models):
    #         return super().render_module_variables(models)
    #
    #     declarations = [f"{self.base_class_name} = declarative_base()"]
    #     if any(not isinstance(model, ModelClass) for model in models):
    #         declarations.append(f"metadata = {self.base_class_name}.metadata")
    #
    #     return "\n".join(declarations)
    #
    # def render(self, models: list[Model]) -> str:
    #     rendered = []
    #     for model in models:
    #         if isinstance(model, ModelClass):
    #             rendered.append(self.render_class(model))
    #         else:
    #             rendered.append(f"{model.name} = {self.render_table(model.table)}")
    #
    #     return "\n\n\n".join(rendered)
    #
    # def render_class(self, model: ModelClass) -> str:
    #     sections: list[str] = []
    #
    #     # Render class variables / special declarations
    #     class_vars: str = self.render_class_variables(model)
    #     if class_vars:
    #         sections.append(class_vars)
    #
    #     # Render column attributes
    #     rendered_column_attributes: list[str] = []
    #     for nullable in (False, True):
    #         for column_attr in model.columns:
    #             if column_attr.column.nullable is nullable:
    #                 rendered_column_attributes.append(
    #                     self.render_column_attribute(column_attr)
    #                 )
    #
    #     if rendered_column_attributes:
    #         sections.append("\n".join(rendered_column_attributes))
    #
    #     # Render relationship attributes
    #     rendered_relationship_attributes: list[str] = [
    #         self.render_relationship(relationship)
    #         for relationship in model.relationships
    #     ]
    #
    #     if rendered_relationship_attributes:
    #         sections.append("\n".join(rendered_relationship_attributes))
    #
    #     declaration = self.render_class_declaration(model)
    #     rendered_sections = "\n\n".join(
    #         indent(section, self.indentation) for section in sections
    #     )
    #     return f"{declaration}\n{rendered_sections}"
    #
    # def render_class_declaration(self, model: ModelClass) -> str:
    #     parent_class_name = (
    #         model.parent_class.name if model.parent_class else self.base_class_name
    #     )
    #     return f"class {model.name}({parent_class_name}):"
    #
    # def render_class_variables(self, model: ModelClass) -> str:
    #     variables = [f"__tablename__ = {model.table.name!r}"]
    #
    #     # Render constraints and indexes as __table_args__
    #     table_args = self.render_table_args(model.table)
    #     if table_args:
    #         variables.append(f"__table_args__ = {table_args}")
    #
    #     return "\n".join(variables)
    #
    # def render_table_args(self, table: Table) -> str:
    #     args: list[str] = []
    #     kwargs: dict[str, str] = {}
    #
    #     # Render constraints
    #     for constraint in sorted(table.constraints, key=get_constraint_sort_key):
    #         if uses_default_name(constraint):
    #             if isinstance(constraint, PrimaryKeyConstraint):
    #                 continue
    #             if (
    #                 isinstance(constraint, (ForeignKeyConstraint, UniqueConstraint))
    #                 and len(constraint.columns) == 1
    #             ):
    #                 continue
    #
    #         args.append(self.render_constraint(constraint))
    #
    #     # Render indexes
    #     for index in sorted(table.indexes, key=lambda i: i.name):
    #         if len(index.columns) > 1 or not uses_default_name(index):
    #             args.append(self.render_index(index))
    #
    #     if table.schema:
    #         kwargs["schema"] = table.schema
    #
    #     if table.comment:
    #         kwargs["comment"] = table.comment
    #
    #     if kwargs:
    #         formatted_kwargs = pformat(kwargs)
    #         if not args:
    #             return formatted_kwargs
    #         else:
    #             args.append(formatted_kwargs)
    #
    #     if args:
    #         rendered_args = f",\n{self.indentation}".join(args)
    #         if len(args) == 1:
    #             rendered_args += ","
    #
    #         return f"(\n{self.indentation}{rendered_args}\n)"
    #     else:
    #         return ""
    #
    # def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
    #     column = column_attr.column
    #     rendered_column = self.render_column(column, column_attr.name != column.name)
    #     return f"{column_attr.name} = {rendered_column}"
    #
    # def render_relationship(self, relationship: RelationshipAttribute) -> str:
    #     def render_column_attrs(column_attrs: list[ColumnAttribute]) -> str:
    #         rendered = []
    #         for attr in column_attrs:
    #             if attr.model is relationship.source:
    #                 rendered.append(attr.name)
    #             else:
    #                 rendered.append(repr(f"{attr.model.name}.{attr.name}"))
    #
    #         return "[" + ", ".join(rendered) + "]"
    #
    #     def render_foreign_keys(column_attrs: list[ColumnAttribute]) -> str:
    #         rendered = []
    #         render_as_string = False
    #         # Assume that column_attrs are all in relationship.source or none
    #         for attr in column_attrs:
    #             if attr.model is relationship.source:
    #                 rendered.append(attr.name)
    #             else:
    #                 rendered.append(f"{attr.model.name}.{attr.name}")
    #                 render_as_string = True
    #
    #         if render_as_string:
    #             return "'[" + ", ".join(rendered) + "]'"
    #         else:
    #             return "[" + ", ".join(rendered) + "]"
    #
    #     def render_join(terms: list[JoinType]) -> str:
    #         rendered_joins = []
    #         for source, source_col, target, target_col in terms:
    #             rendered = f"lambda: {source.name}.{source_col} == {target.name}."
    #             if target.__class__ is Model:
    #                 rendered += "c."
    #
    #             rendered += str(target_col)
    #             rendered_joins.append(rendered)
    #
    #         if len(rendered_joins) > 1:
    #             rendered = ", ".join(rendered_joins)
    #             return f"and_({rendered})"
    #         else:
    #             return rendered_joins[0]
    #
    #     # Render keyword arguments
    #     kwargs: dict[str, Any] = {}
    #     if relationship.type is RelationshipType.ONE_TO_ONE and relationship.constraint:
    #         if relationship.constraint.referred_table is relationship.source.table:
    #             kwargs["uselist"] = False
    #
    #     # Add the "secondary" keyword for many-to-many relationships
    #     if relationship.association_table:
    #         table_ref = relationship.association_table.table.name
    #         if relationship.association_table.schema:
    #             table_ref = f"{relationship.association_table.schema}.{table_ref}"
    #
    #         kwargs["secondary"] = repr(table_ref)
    #
    #     if relationship.remote_side:
    #         kwargs["remote_side"] = render_column_attrs(relationship.remote_side)
    #
    #     if relationship.foreign_keys:
    #         kwargs["foreign_keys"] = render_foreign_keys(relationship.foreign_keys)
    #
    #     if relationship.primaryjoin:
    #         kwargs["primaryjoin"] = render_join(relationship.primaryjoin)
    #
    #     if relationship.secondaryjoin:
    #         kwargs["secondaryjoin"] = render_join(relationship.secondaryjoin)
    #
    #     if relationship.backref:
    #         kwargs["back_populates"] = repr(relationship.backref.name)
    #
    #     rendered_relationship = render_callable(
    #         "relationship", repr(relationship.target.name), kwargs=kwargs
    #     )
    #     return f"{relationship.name} = {rendered_relationship}"


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

    def collect_imports(self, models: Iterable[Model]) -> None:
        super().collect_imports(models)
        if not self.quote_annotations:
            self.add_literal_import("__future__", "annotations")

        if any(isinstance(model, ModelClass) for model in models):
            self.remove_literal_import("sqlalchemy.orm", "declarative_base")
            self.add_literal_import("dataclasses", "dataclass")
            self.add_literal_import("dataclasses", "field")
            self.add_literal_import("sqlalchemy.orm", "registry")

    def collect_imports_for_model(self, model: Model) -> None:
        super().collect_imports_for_model(model)
        if isinstance(model, ModelClass):
            for column_attr in model.columns:
                if column_attr.column.nullable:
                    self.add_literal_import("typing", "Optional")
                    break

            for relationship_attr in model.relationships:
                if relationship_attr.type in (
                    RelationshipType.ONE_TO_MANY,
                    RelationshipType.MANY_TO_MANY,
                ):
                    self.add_literal_import("typing", "List")

    def collect_imports_for_column(self, column: Column[Any]) -> None:
        super().collect_imports_for_column(column)
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            pass
        else:
            self.add_import(python_type)

    def render_module_variables(self, models: list[Model]) -> str:
        if not any(isinstance(model, ModelClass) for model in models):
            return super().render_module_variables(models)

        declarations: list[str] = ["mapper_registry = registry()"]
        if any(not isinstance(model, ModelClass) for model in models):
            declarations.append("metadata = mapper_registry.metadata")

        if not self.quote_annotations:
            self.add_literal_import("__future__", "annotations")

        return "\n".join(declarations)

    def render_class_declaration(self, model: ModelClass) -> str:
        superclass_part = f"({model.parent_class.name})" if model.parent_class else ""
        return (
            f"@mapper_registry.mapped\n@dataclass\nclass {model.name}{superclass_part}:"
        )

    # def render_class_variables(self, model: ModelClass) -> str:
    #     variables = [
    #         super().render_class_variables(model),
    #         f"__sa_dataclass_metadata_key__ = {self.metadata_key!r}",
    #     ]
    #     return "\n".join(variables)

    def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
        column = column_attr.column
        try:
            python_type = column.type.python_type
        except NotImplementedError:
            python_type = Any

        kwargs: dict[str, Any] = {}
        if column.autoincrement and column.name in column.table.primary_key:
            kwargs["init"] = False
        elif column.nullable:
            kwargs["default"] = None
            python_type_name = Optional[python_type]

        rendered_column = self.render_column(column, column_attr.name != column.name)
        kwargs["metadata"] = f"{{{self.metadata_key!r}: {rendered_column}}}"
        rendered_field = CallableModel(field, kwargs=kwargs)
        return f"{column_attr.name}: {python_type_name} = {rendered_field}"

    def render_relationship(self, relationship: RelationshipAttribute) -> str:
        rendered = super().render_relationship(relationship).partition(" = ")[2]
        kwargs: dict[str, Any] = {}

        annotation = relationship.target.name
        if self.quote_annotations:
            annotation = repr(relationship.target.name)

        if relationship.type in (
            RelationshipType.ONE_TO_MANY,
            RelationshipType.MANY_TO_MANY,
        ):
            annotation = f"List[{annotation}]"
            kwargs["default_factory"] = "list"
        else:
            kwargs["default"] = "None"
            annotation = f"Optional[{annotation}]"

        kwargs["metadata"] = f"{{{self.metadata_key!r}: {rendered}}}"
        rendered_field = CallableModel(field, kwargs=kwargs)
        return f"{relationship.name}: {annotation} = {rendered_field}"


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

    # def collect_imports(self, models: Iterable[Model]) -> None:
    #     super(DeclarativeGenerator, self).collect_imports(models)
    #     if any(isinstance(model, ModelClass) for model in models):
    #         self.remove_literal_import("sqlalchemy", "MetaData")
    #         self.add_literal_import("sqlmodel", "SQLModel")
    #         self.add_literal_import("sqlmodel", "Field")
    #
    # def collect_imports_for_model(self, model: Model) -> None:
    #     super(DeclarativeGenerator, self).collect_imports_for_model(model)
    #     if isinstance(model, ModelClass):
    #         for column_attr in model.columns:
    #             if column_attr.column.nullable:
    #                 self.add_literal_import("typing", "Optional")
    #                 break
    #
    #         if model.relationships:
    #             self.add_literal_import("sqlmodel", "Relationship")
    #
    #         for relationship_attr in model.relationships:
    #             if relationship_attr.type in (
    #                 RelationshipType.ONE_TO_MANY,
    #                 RelationshipType.MANY_TO_MANY,
    #             ):
    #                 self.add_literal_import("typing", "List")
    #
    # def collect_imports_for_column(self, column: Column[Any]) -> None:
    #     super().collect_imports_for_column(column)
    #     try:
    #         python_type = column.type.python_type
    #     except NotImplementedError:
    #         self.add_literal_import("typing", "Any")
    #     else:
    #         self.add_import(python_type)

    # def render_module_variables(self, models: list[Model]) -> str:
    #     declarations: list[str] = []
    #     if any(not isinstance(model, ModelClass) for model in models):
    #         declarations.append(f"metadata = {self.base_class_name}.metadata")
    #
    #     return "\n".join(declarations)
    #
    # def render_class_declaration(self, model: ModelClass) -> str:
    #     if model.parent_class:
    #         parent = model.parent_class.name
    #     else:
    #         parent = self.base_class_name
    #
    #     superclass_part = f"({parent}, table=True)"
    #     return f"class {model.name}{superclass_part}:"
    #
    # def render_class_variables(self, model: ModelClass) -> str:
    #     # Render constraints and indexes as __table_args__
    #     table_args = self.render_table_args(model.table)
    #     if table_args:
    #         variables = [f"__table_args__ = {table_args}"]
    #         return "".join(variables)
    #
    #     return ""

    # def render_column_attribute(self, column_attr: ColumnAttribute) -> str:
    #     column = column_attr.column
    #     try:
    #         python_type = column.type.python_type
    #     except NotImplementedError:
    #         python_type_name = "Any"
    #     else:
    #         python_type_name = python_type.__name__
    #
    #     kwargs: dict[str, Any] = {}
    #     if (
    #         column.autoincrement and column.name in column.table.primary_key
    #     ) or column.nullable:
    #         self.add_literal_import("typing", "Optional")
    #         kwargs["default"] = None
    #         python_type_name = f"Optional[{python_type_name}]"
    #
    #     rendered_column = self.render_column(column, True)
    #     kwargs["sa_column"] = f"{rendered_column}"
    #     rendered_field = render_callable("Field", kwargs=kwargs)
    #     return f"{column_attr.name}: {python_type_name} = {rendered_field}"

    # def render_relationship(self, relationship: RelationshipAttribute) -> str:
    #     rendered = super().render_relationship(relationship).partition(" = ")[2]
    #     args = self.render_relationship_args(rendered)
    #     kwargs: dict[str, Any] = {}
    #     annotation = repr(relationship.target.name)
    #
    #     if relationship.type in (
    #         RelationshipType.ONE_TO_MANY,
    #         RelationshipType.MANY_TO_MANY,
    #     ):
    #         self.add_literal_import("typing", "List")
    #         annotation = f"List[{annotation}]"
    #     else:
    #         self.add_literal_import("typing", "Optional")
    #         annotation = f"Optional[{annotation}]"
    #
    #     rendered_field = render_callable("Relationship", *args, kwargs=kwargs)
    #     return f"{relationship.name}: {annotation} = {rendered_field}"

    # def render_relationship_args(self, arguments: str) -> list[str]:
    #     argument_list = arguments.split(",")
    #     # delete ')' and ' ' from args
    #     argument_list[-1] = argument_list[-1][:-1]
    #     argument_list = [argument[1:] for argument in argument_list]
    #
    #     rendered_args = []
    #     for arg in argument_list:
    #         if "back_populates" in arg:
    #             rendered_args.append(arg)
    #         if "uselist=False" in arg:
    #             rendered_args.append("sa_relationship_kwargs={'uselist': False}")
    #
    #     return rendered_args
