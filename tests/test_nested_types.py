from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.orm import clear_mappers, configure_mappers
from sqlalchemy.types import TypeEngine, UserDefinedType

from sqlacodegen.generators import (
    DataclassGenerator,
    DeclarativeGenerator,
    SQLModelGenerator,
    TablesGenerator,
)


class WrappedType(UserDefinedType[list[Any]]):
    """Like ClickHouse Array, retain either a type class or an instance."""

    def __init__(self, item_type: TypeEngine[Any] | type[TypeEngine[Any]]) -> None:
        self.item_type = item_type

    @property
    def python_type(self) -> type[list[Any]]:
        return list

    def get_col_spec(self, **kwargs: Any) -> str:
        return "WRAPPED"


class TupleType(UserDefinedType[tuple[Any, ...]]):
    def __init__(self, *types: TypeEngine[Any] | type[TypeEngine[Any]]) -> None:
        self.types = types

    @property
    def python_type(self) -> type[tuple[Any, ...]]:
        return tuple

    def get_col_spec(self, **kwargs: Any) -> str:
        return f"TUPLE({', '.join(str(type_) for type_ in self.types)})"


@pytest.mark.parametrize(
    "generator_class",
    [TablesGenerator, DeclarativeGenerator, DataclassGenerator, SQLModelGenerator],
)
@pytest.mark.parametrize("options", [[], ["keep_dialect_types"]])
def test_nested_type_arguments(
    generator_class: type[TablesGenerator], options: list[str]
) -> None:
    metadata = MetaData()
    Table(
        "nested_types",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("type_class", WrappedType(String), nullable=False),
        Column("instance", WrappedType(String()), nullable=False),
        Column("parameterized", WrappedType(String(24)), nullable=False),
        Column("nested", WrappedType(WrappedType(String)), nullable=False),
        Column("variadic", TupleType(Integer, String(12)), nullable=False),
    )
    engine = create_engine("sqlite://")
    namespace: dict[str, Any] = {}
    table = None
    try:
        code = generator_class(metadata, engine, options).generate()
        exec(compile(code, "<generated>", "exec", dont_inherit=True), namespace)
        if generator_class is TablesGenerator:
            table = namespace["t_nested_types"]
        else:
            table = namespace["NestedTypes"].__table__

        configure_mappers()

        assert table.c.type_class.type.item_type is String
        assert isinstance(table.c.instance.type.item_type, String)
        assert table.c.instance.type.item_type.length is None
        assert table.c.parameterized.type.item_type.length == 24
        assert isinstance(table.c.nested.type.item_type, WrappedType)
        assert table.c.nested.type.item_type.item_type is String
        assert table.c.variadic.type.types[0] is Integer
        assert isinstance(table.c.variadic.type.types[1], String)
        assert table.c.variadic.type.types[1].length == 12
    finally:
        clear_mappers()
        if generator_class is SQLModelGenerator and table is not None:
            table.metadata.remove(table)
        engine.dispose()
