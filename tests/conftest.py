from textwrap import dedent

import pytest
from pytest import FixtureRequest
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import clear_mappers, configure_mappers
from sqlalchemy.schema import MetaData


@pytest.fixture
def engine(request: FixtureRequest) -> Engine:
    dialect = getattr(request, "param", None)
    if dialect == "postgresql":
        return create_engine("postgresql:///testdb")
    elif dialect == "mysql":
        return create_engine("mysql+mysqlconnector://testdb")
    else:
        return create_engine("sqlite:///:memory:")


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


def validate_code(generated_code: str, expected_code: str) -> None:
    expected_code = dedent(expected_code)
    assert generated_code == expected_code
    try:
        exec(generated_code, {})
        configure_mappers()
    finally:
        clear_mappers()
