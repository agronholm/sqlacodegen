from textwrap import dedent

import pytest
from pytest import FixtureRequest
from sqlalchemy import Dialect
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.orm import clear_mappers, configure_mappers
from sqlalchemy.schema import MetaData


@pytest.fixture
def dialect(request: FixtureRequest) -> Dialect:
    dialect_name = getattr(request, "param", None)
    if dialect_name == "postgresql":
        return postgresql.dialect()
    elif dialect_name == "mysql":
        return mysql.mysqlconnector.dialect()
    else:
        return sqlite.dialect()


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
