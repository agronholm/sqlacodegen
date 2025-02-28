from typing import Any, Callable

from sqlalchemy.dialects import mysql
from sqlalchemy.sql import sqltypes


def __adapt_mysql_dobule(mysql_double: mysql.DOUBLE, sa_double: Any) -> Any:
    return sa_double(
        precision=mysql_double.precision,
        decimal_return_scale=mysql_double.scale,
        asdecimal=mysql_double.asdecimal,
    )


__adapters: dict[tuple[Any, Any], Callable[[Any, Any], Any]] = {
    (mysql.DOUBLE, sqltypes.DOUBLE): __adapt_mysql_dobule,  # type: ignore[attr-defined]
}


def adapt_column_type(coltype: Any, supercls: Any) -> Any:
    adapter = __adapters.get((coltype.__class__, supercls), lambda c, s: c.adapt(s))
    return adapter(coltype, supercls)
