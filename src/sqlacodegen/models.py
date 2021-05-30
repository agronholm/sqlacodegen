import re
from keyword import iskeyword
from typing import Any, Dict, List, Optional, Set, Union

import sqlalchemy
import sqlalchemy.exc
from inflect import engine
from sqlalchemy import ARRAY, Enum, Float
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.elements import ColumnElement, quoted_name
from sqlalchemy.sql.schema import (
    Column, ForeignKeyConstraint, PrimaryKeyConstraint, Table, UniqueConstraint)
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.util import OrderedDict

from .collector import ImportCollector
from .utils import get_column_names, get_constraint_sort_key

try:
    from sqlalchemy import Computed
except ImportError:
    Computed = None  # type: ignore

AttributeType = Union['ManyToManyRelationship', 'ManyToOneRelationship', ColumnElement]
_re_invalid_identifier = re.compile(r'(?u)\W')


class Relationship:
    def __init__(self, source_cls: str, target_cls: str) -> None:
        super().__init__()
        self.source_cls = source_cls
        self.target_cls = target_cls
        self.kwargs: Dict[str, Any] = OrderedDict()


class ManyToOneRelationship(Relationship):
    def __init__(self, source_cls: str, target_cls: str, constraint: ForeignKeyConstraint,
                 inflect_engine: engine) -> None:
        super().__init__(source_cls, target_cls)

        column_names = get_column_names(constraint)
        colname = column_names[0]
        tablename = constraint.elements[0].column.table.name
        if not colname.endswith('_id'):
            self.preferred_name = inflect_engine.singular_noun(
                tablename) or tablename
        else:
            self.preferred_name = colname[:-3]

        # Add uselist=False to One-to-One relationships
        if any(isinstance(c, (PrimaryKeyConstraint, UniqueConstraint)) and
               set(col.name for col in c.columns) == set(column_names)
               for c in constraint.table.constraints):
            self.kwargs['uselist'] = 'False'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parent' if not colname.endswith(
                '_id') else colname[:-3]
            pk_col_names = [col.name for col in constraint.table.primary_key]
            self.kwargs['remote_side'] = '[{0}]'.format(
                ', '.join(pk_col_names))

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
        super().__init__(source_cls, target_cls)

        prefix = (assocation_table.schema +
                  '.') if assocation_table.schema else ''
        self.kwargs['secondary'] = repr(prefix + assocation_table.name)
        constraints = [c for c in assocation_table.constraints
                       if isinstance(c, ForeignKeyConstraint)]
        constraints.sort(key=get_constraint_sort_key)
        colname = get_column_names(constraints[1])[0]
        tablename = constraints[1].elements[0].column.table.name
        self.preferred_name = tablename if not colname.endswith(
            '_id') else colname[:-3] + 's'

        # Handle self referential relationships
        if source_cls == target_cls:
            self.preferred_name = 'parents' if not colname.endswith(
                '_id') else colname[:-3] + 's'
            pri_pairs = zip(get_column_names(
                constraints[0]), constraints[0].elements)
            sec_pairs = zip(get_column_names(
                constraints[1]), constraints[1].elements)
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


class Model:
    def __init__(self, table: Table) -> None:
        super()
        self.table = table
        self.schema = table.schema

        # Adapt column types to the most reasonable generic types (ie. VARCHAR -> String)
        for column in table.columns:
            if not isinstance(column.type, NullType):
                column.type = self._get_adapted_type(
                    column.type, column.table.bind)

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
                    new_coltype.item_type = self._get_adapted_type(
                        new_coltype.item_type, bind)

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

        for constraint in sorted(self.table.constraints, key=get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                if len(constraint.columns) > 1:
                    collector.add_literal_import(
                        'sqlalchemy', 'ForeignKeyConstraint')
                else:
                    collector.add_literal_import('sqlalchemy', 'ForeignKey')
            elif isinstance(constraint, UniqueConstraint):
                if len(constraint.columns) > 1:
                    collector.add_literal_import(
                        'sqlalchemy', 'UniqueConstraint')
            elif not isinstance(constraint, PrimaryKeyConstraint):
                collector.add_import(constraint)

        for index in self.table.indexes:
            if len(index.columns) > 1:
                collector.add_import(index)

    @staticmethod
    def _convert_to_valid_identifier(name: Union[quoted_name, str]) -> str:
        assert name, 'Identifier cannot be empty'
        if name[0].isdigit() or iskeyword(name):
            name = '_' + name
        elif name == 'metadata':
            name = 'metadata_'

        return _re_invalid_identifier.sub('_', name)


class ModelTable(Model):
    def __init__(self, table: Table) -> None:
        super().__init__(table)
        self.name = self._convert_to_valid_identifier(table.name)

    def add_imports(self, collector: ImportCollector) -> None:
        super().add_imports(collector)
        collector.add_import(Table)


class ModelClass(Model):
    def __init__(self, table: Table, association_tables: List[Table], inflect_engine: engine,
                 detect_joined: bool, base_class_name: Optional[str]) -> None:
        super().__init__(table)
        self.parent_name = base_class_name
        self.name = self._tablename_to_classname(table.name, inflect_engine)
        self.children: List[ModelClass] = []
        self.attributes: Dict[str, AttributeType] = OrderedDict()
        relationship_: Union[ManyToOneRelationship, ManyToManyRelationship]

        # Assign attribute names for columns
        for column in table.columns:
            self._add_attribute(column.name, column)

        # Add many-to-one relationships
        pk_column_names = set(col.name for col in table.primary_key.columns)
        for constraint in sorted(table.constraints, key=get_constraint_sort_key):
            if isinstance(constraint, ForeignKeyConstraint):
                target_cls = self._tablename_to_classname(constraint.elements[0].column.table.name,
                                                          inflect_engine)
                if (detect_joined and self.parent_name == base_class_name and
                        set(get_column_names(constraint)) == pk_column_names):
                    self.parent_name = target_cls
                else:
                    relationship_ = ManyToOneRelationship(self.name, target_cls, constraint,
                                                          inflect_engine)
                    self._add_attribute(
                        relationship_.preferred_name, relationship_)

        # Add many-to-many relationships
        for association_table in association_tables:
            fk_constraints = [c for c in association_table.constraints
                              if isinstance(c, ForeignKeyConstraint)]
            fk_constraints.sort(key=get_constraint_sort_key)
            target_cls = self._tablename_to_classname(
                fk_constraints[1].elements[0].column.table.name, inflect_engine)
            relationship_ = ManyToManyRelationship(
                self.name, target_cls, association_table)
            self._add_attribute(relationship_.preferred_name, relationship_)

    @classmethod
    def _tablename_to_classname(cls, tablename: str, inflect_engine: engine) -> str:
        tablename = cls._convert_to_valid_identifier(tablename)
        camel_case_name = ''.join(part[:1].upper() + part[1:]
                                  for part in tablename.split('_'))
        return inflect_engine.singular_noun(camel_case_name) or camel_case_name

    def _add_attribute(self, attrname: Union[quoted_name, str], value: AttributeType) -> str:
        attrname = tempname = self._convert_to_valid_identifier(attrname)
        counter = 1
        while tempname in self.attributes:
            tempname = attrname + str(counter)
            counter += 1

        self.attributes[tempname] = value
        return tempname

    def add_imports(self, collector: ImportCollector) -> None:
        super().add_imports(collector)

        if any(isinstance(value, Relationship) for value in self.attributes.values()):
            collector.add_literal_import('sqlalchemy.orm', 'relationship')

        for child in self.children:
            child.add_imports(collector)
