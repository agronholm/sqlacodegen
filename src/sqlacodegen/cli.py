from __future__ import annotations

import argparse
import ast
import sys
from contextlib import ExitStack
from typing import Any, TextIO

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData

try:
    import citext
except ImportError:
    citext = None

try:
    import geoalchemy2
except ImportError:
    geoalchemy2 = None

try:
    import pgvector.sqlalchemy
except ImportError:
    pgvector = None

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points, version
else:
    from importlib.metadata import entry_points, version


def _parse_engine_arg(arg_str: str) -> tuple[str, Any]:
    if "=" not in arg_str:
        raise argparse.ArgumentTypeError("engine-arg must be in key=value format")

    key, value = arg_str.split("=", 1)
    try:
        value = ast.literal_eval(value)
    except Exception:
        pass  # Leave as string if literal_eval fails

    return key, value


def _parse_engine_args(arg_list: list[str]) -> dict[str, Any]:
    result = {}
    for arg in arg_list or []:
        key, value = _parse_engine_arg(arg)
        result[key] = value

    return result


def main() -> None:
    generators = {ep.name: ep for ep in entry_points(group="sqlacodegen.generators")}
    parser = argparse.ArgumentParser(
        description="Generates SQLAlchemy model code from an existing database."
    )
    parser.add_argument("url", nargs="?", help="SQLAlchemy url to the database")
    parser.add_argument(
        "--options", help="options (comma-delimited) passed to the generator class"
    )
    parser.add_argument(
        "--version", action="store_true", help="print the version number and exit"
    )
    parser.add_argument(
        "--schemas", help="load tables from the given schemas (comma-delimited)"
    )
    parser.add_argument(
        "--generator",
        choices=generators,
        default="declarative",
        help="generator class to use",
    )
    parser.add_argument(
        "--tables", help="tables to process (comma-delimited, default: all)"
    )
    parser.add_argument(
        "--noviews",
        action="store_true",
        help="ignore views (always true for sqlmodels generator)",
    )
    parser.add_argument(
        "--engine-arg",
        action="append",
        help=(
            "engine arguments in key=value format, e.g., "
            '--engine-arg=connect_args=\'{"user": "scott"}\' '
            "--engine-arg thick_mode=true or "
            '--engine-arg thick_mode=\'{"lib_dir": "/path"}\' '
            "(values are parsed with ast.literal_eval)"
        ),
    )
    parser.add_argument("--outfile", help="file to write output to (default: stdout)")
    args = parser.parse_args()

    if args.version:
        print(version("sqlacodegen"))
        return

    if not args.url:
        print("You must supply a url\n", file=sys.stderr)
        parser.print_help()
        return

    if citext:
        print(f"Using sqlalchemy-citext {version('sqlalchemy-citext')}")

    if geoalchemy2:
        print(f"Using geoalchemy2 {version('geoalchemy2')}")

    if pgvector:
        print(f"Using pgvector {version('pgvector')}")

    # Use reflection to fill in the metadata
    engine_args = _parse_engine_args(args.engine_arg)
    engine = create_engine(args.url, **engine_args)
    metadata = MetaData()
    tables = args.tables.split(",") if args.tables else None
    schemas = args.schemas.split(",") if args.schemas else [None]
    options = set(args.options.split(",")) if args.options else set()

    # Instantiate the generator
    generator_class = generators[args.generator].load()
    generator = generator_class(metadata, engine, options)

    if not generator.views_supported:
        name = generator_class.__name__
        print(
            f"VIEW models will not be generated when using the '{name}' generator",
            file=sys.stderr,
        )

    for schema in schemas:
        metadata.reflect(
            engine, schema, (generator.views_supported and not args.noviews), tables
        )

    # Open the target file (if given)
    with ExitStack() as stack:
        outfile: TextIO
        if args.outfile:
            outfile = open(args.outfile, "w", encoding="utf-8")
            stack.enter_context(outfile)
        else:
            outfile = sys.stdout

        # Write the generated model code to the specified file or standard output
        outfile.write(generator.generate())
