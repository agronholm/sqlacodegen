from __future__ import annotations

import sqlite3
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest

from sqlacodegen.cli import create_parser

future_imports = "from __future__ import annotations\n\n"


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
        == """\
from sqlalchemy import Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


class Foo(Base):
    __tablename__ = 'foo'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
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
        == """\
from sqlalchemy import Integer, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column

class Base(MappedAsDataclass, DeclarativeBase):
    pass


class Foo(Base):
    __tablename__ = 'foo'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
"""
    )


def test_cli_sqlmodels(db_path: Path, tmp_path: Path) -> None:
    output_path = tmp_path / "outfile"
    subprocess.run(
        [
            "sqlacodegen",
            f"sqlite:///{db_path}",
            "--generator",
            "sqlmodels",
            "--outfile",
            str(output_path),
        ],
        check=True,
    )

    assert (
        output_path.read_text()
        == """\
from typing import Optional

from sqlalchemy import Column, Integer, Text
from sqlmodel import Field, SQLModel

class Foo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, sa_column=Column('id', Integer, \
primary_key=True))
    name: str = Field(sa_column=Column('name', Text))
"""
    )


@pytest.mark.parametrize(
    "cli_args, expected",
    [
        ([], None),
        (["--thickmode"], True),
        (["--thickmode", "true"], True),
        (["--thickmode", "false"], False),
        (
            ["--thickmode", '{"lib_dir": "/foo", "driver_name": "v1"}'],
            {"lib_dir": "/foo", "driver_name": "v1"},
        ),
    ],
)
def test_cli_thickmode(
    cli_args: list[str],
    expected: None | bool | dict[Any, Any],
) -> None:
    parser = create_parser()
    args = parser.parse_args(cli_args)
    assert args.thickmode == expected


def test_main() -> None:
    expected_version = version("sqlacodegen")
    completed = subprocess.run(
        [sys.executable, "-m", "sqlacodegen", "--version"],
        stdout=subprocess.PIPE,
        check=True,
    )
    assert completed.stdout.decode().strip() == expected_version
