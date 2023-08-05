from textwrap import dedent

import pytest
from sqlalchemy.orm import clear_mappers, configure_mappers

from sqlacodegen.generators import _sqla_version

requires_sqlalchemy_1_4 = pytest.mark.skipif(
    _sqla_version >= (2, 0), reason="Test requires SQLAlchemy 1.4.x "
)

requires_sqlalchemy_2_0 = pytest.mark.skipif(
    _sqla_version < (2, 0), reason="Test requires SQLAlchemy 2.0.x or newer"
)


def validate_code(generated_code: str, expected_code: str) -> None:
    expected_code = dedent(expected_code)
    assert generated_code == expected_code
    try:
        exec(generated_code, {})
        configure_mappers()
    finally:
        clear_mappers()
