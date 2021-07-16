from typing import List, Set

from sqlalchemy import CheckConstraint
from sqlalchemy.engine import Connectable
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import (
    ColumnCollectionConstraint, Constraint, ForeignKeyConstraint, Table)


def get_column_names(constraint: ColumnCollectionConstraint) -> List[str]:
    return list(constraint.columns.keys())


def get_constraint_sort_key(constraint: Constraint) -> str:
    if isinstance(constraint, CheckConstraint):
        return f'C{constraint.sqltext}'
    elif isinstance(constraint, ColumnCollectionConstraint):
        return constraint.__class__.__name__[0] + repr(get_column_names(constraint))
    else:
        return str(constraint)


def get_compiled_expression(statement: ClauseElement, bind: Connectable) -> str:
    """Return the statement in a form where any placeholders have been filled in."""
    return str(statement.compile(bind, compile_kwargs={"literal_binds": True}))


def get_common_fk_constraints(table1: Table, table2: Table) -> Set[ForeignKeyConstraint]:
    """Return a set of foreign key constraints the two tables have against each other."""
    c1 = set(c for c in table1.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table2)
    c2 = set(c for c in table2.constraints if isinstance(c, ForeignKeyConstraint) and
             c.elements[0].column.table == table1)
    return c1.union(c2)
