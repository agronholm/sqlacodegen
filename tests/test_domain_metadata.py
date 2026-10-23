from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    create_engine,
    create_mock_engine,
)
from sqlalchemy.dialects.postgresql import DOMAIN
from sqlalchemy.orm import clear_mappers, configure_mappers
from sqlmodel import SQLModel

from sqlacodegen.generators import (
    DataclassGenerator,
    DeclarativeGenerator,
    SQLModelGenerator,
    TablesGenerator,
)


@pytest.mark.parametrize(
    "generator_class",
    [TablesGenerator, DeclarativeGenerator, DataclassGenerator, SQLModelGenerator],
)
@pytest.mark.parametrize("options", [[], ["keep_dialect_types"]])
@pytest.mark.parametrize("schema", [None, "domain_types"])
def test_domain_metadata(
    generator_class: type[TablesGenerator],
    options: list[str],
    schema: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Isolate named-type DDL listeners as well as tables between SQLModel cases.
    if generator_class is SQLModelGenerator:
        monkeypatch.setattr(SQLModel, "metadata", MetaData())
    metadata = MetaData()
    domain = DOMAIN(
        "positive_number", Integer, metadata=metadata, schema=schema, check="VALUE > 0"
    )
    for name in ("first_item", "second_item"):
        Table(
            name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", domain, nullable=False),
            schema="items",
        )

    statements: list[str] = []

    def execute(statement: Any, *args: Any, **kwargs: Any) -> None:
        statements.append(str(statement.compile(dialect=engine.dialect)).strip())

    engine = create_mock_engine("postgresql://", execute)
    bind = create_engine("postgresql+psycopg://")
    namespace: dict[str, Any] = {}
    generated_metadata = None
    try:
        code = generator_class(metadata, bind, options).generate()
        exec(compile(code, "<generated>", "exec", dont_inherit=True), namespace)
        if generator_class is TablesGenerator:
            generated_metadata = namespace["metadata"]
        elif generator_class is SQLModelGenerator:
            generated_metadata = namespace["SQLModel"].metadata
        else:
            generated_metadata = namespace["Base"].metadata

        configure_mappers()
        for table in generated_metadata.tables.values():
            generated_domain = table.c.value.type
            assert generated_domain.metadata is generated_metadata
            assert generated_domain.schema == schema
            assert str(generated_domain.check) == "VALUE > 0"

        generated_metadata.create_all(engine, checkfirst=False)
        assert sum(s.startswith("CREATE DOMAIN") for s in statements) == 1
        assert sum(s.startswith("CREATE TABLE") for s in statements) == 2
        statements.clear()
        generated_metadata.drop_all(engine, checkfirst=False)
        assert sum(s.startswith("DROP DOMAIN") for s in statements) == 1
        assert sum(s.startswith("DROP TABLE") for s in statements) == 2
    finally:
        clear_mappers()
        bind.dispose()
        if generator_class is SQLModelGenerator and generated_metadata is not None:
            for table in list(generated_metadata.tables.values()):
                generated_metadata.remove(table)
