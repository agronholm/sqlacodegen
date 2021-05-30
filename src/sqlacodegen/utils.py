from typing import List

from sqlalchemy import CheckConstraint
from sqlalchemy.sql.schema import ColumnCollectionConstraint, Constraint


def get_column_names(constraint: ColumnCollectionConstraint) -> List[str]:
    return list(constraint.columns.keys())


def get_constraint_sort_key(constraint: Constraint) -> str:
    if isinstance(constraint, CheckConstraint):
        return 'C{0}'.format(constraint.sqltext)
    elif isinstance(constraint, ColumnCollectionConstraint):
        return constraint.__class__.__name__[0] + repr(get_column_names(constraint))
    else:
        return str(constraint)
