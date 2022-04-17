from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from sqlacodegen.generators import _sqla_version

if sys.version_info < (3, 8):
    from importlib_metadata import version
else:
    from importlib.metadata import version

future_imports = "from __future__ import annotations\n\n"

if _sqla_version < (1, 4):
    declarative_package = "sqlalchemy.ext.declarative"
else:
    declarative_package = "sqlalchemy.orm"


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.db"
    with sqlite3.connect(str(path)) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE foo (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)"
        )

    return path


def test_cli_tables(db_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "outfile"
    subprocess.run(
        [
            "sqlacodegen",
            f"sqlite:///{db_path}",
            "--generator",
            "tables",
            "--outfile",
            str(output_path),
        ],
        check=True,
    )

    assert (
        output_path.read_text()
        == """\
from sqlalchemy import Column, Integer, MetaData, Table, Text

metadata = MetaData()


t_foo = Table(
    'foo', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', Text, nullable=False)
)
"""
    )


def test_cli_declarative(db_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "outfile"
    subprocess.run(
        [
            "sqlacodegen",
            f"sqlite:///{db_path}",
            "--generator",
            "declarative",
            "--outfile",
            str(output_path),
        ],
        check=True,
    )

    assert (
        output_path.read_text()
        == f"""\
from sqlalchemy import Column, Integer, Text
from {declarative_package} import declarative_base

Base = declarative_base()


class Foo(Base):
    __tablename__ = 'foo'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
"""
    )


def test_cli_dataclass(db_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "outfile"
    subprocess.run(
        [
            "sqlacodegen",
            f"sqlite:///{db_path}",
            "--generator",
            "dataclasses",
            "--outfile",
            str(output_path),
        ],
        check=True,
    )

    assert (
        output_path.read_text()
        == f"""\
{future_imports}from dataclasses import dataclass, field

from sqlalchemy import Column, Integer, Text
from sqlalchemy.orm import registry

mapper_registry = registry()


@mapper_registry.mapped
@dataclass
class Foo:
    __tablename__ = 'foo'
    __sa_dataclass_metadata_key__ = 'sa'

    id: int = field(init=False, metadata={{'sa': Column(Integer, primary_key=True)}})
    name: str = field(metadata={{'sa': Column(Text, nullable=False)}})
"""
    )


def test_main() -> None:
    expected_version = version("sqlacodegen")
    completed = subprocess.run(
        [sys.executable, "-m", "sqlacodegen", "--version"],
        stdout=subprocess.PIPE,
        check=True,
    )
    assert completed.stdout.decode().strip() == expected_version
