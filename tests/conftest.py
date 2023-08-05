import pytest
from pytest import FixtureRequest
from sqlalchemy.engine import Engine, create_engine
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
