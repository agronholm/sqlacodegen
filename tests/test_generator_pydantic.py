import pytest
from _pytest.fixtures import FixtureRequest
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine
from sqlalchemy.schema import Column, MetaData, Table

from sqlacodegen.generators import CodeGenerator, PydanticGenerator

from .conftest import validate_code


@pytest.fixture
def generator(
    request: FixtureRequest, metadata: MetaData, engine: Engine
) -> CodeGenerator:
    options = getattr(request, "param", [])
    return PydanticGenerator(metadata, engine, options)


@pytest.mark.parametrize("engine", ["mysql"], indirect=["engine"])
def test_mysql_column_types(generator: CodeGenerator) -> None:
    Table(
        "simple_items",
        generator.metadata,
        Column("id", mysql.INTEGER),
        Column("name", mysql.VARCHAR(255)),
        Column("text", mysql.TEXT),
    )

    validate_code(
        generator.generate(),
        """\
        from typing import Optional

        from pydantic import BaseModel, ConfigDict, StringConstraints
        from typing_extensions import Annotated

        class SimpleItems(BaseModel):
            model_config = ConfigDict(from_attributes=True)

            id: Optional[int] = None
            name: Optional[Annotated[str, StringConstraints(max_length=255)]] = None
            text: Optional[str] = None
        """,
    )
