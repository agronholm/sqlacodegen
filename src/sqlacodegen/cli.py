from __future__ import annotations

import argparse
import re
import sys
from contextlib import ExitStack
from typing import TextIO

from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData

try:
    import citext
except ImportError:
    citext = None

try:
    import geoalchemy2
except ImportError:
    geoalchemy2 = None

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points, version
else:
    from importlib.metadata import entry_points, version


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
        "--tables",
        help="tables to process (comma-delimited strings or regexp, default: all)",
    )
    parser.add_argument(
        "--exclude-tables",
        help="tables to exclude (comma-delimited strings or regexp, default: none)",
    )
    parser.add_argument("--noviews", action="store_true", help="ignore views")
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
        print(f"Using sqlalchemy-citext {citext.__version__}")

    if geoalchemy2:
        print(f"Using geoalchemy2 {geoalchemy2.__version__}")

    # Use reflection to fill in the metadata
    engine = create_engine(args.url)
    metadata = MetaData()
    try:
        # sa 1.4
        tables = engine.table_names()
    except AttributeError:
        # sa 2.0
        inspection = inspect(engine)
        tables = inspection.get_tables_names()

    if args.tables:
        # only keep the tables defined in args.tables
        filter = re.compile(args.tables.replace(",", "|"))
        tables = [t for t in tables if filter.match(t)]
    if args.exclude_tables:
        # exclude the tables defined in args.exclude_tables
        filter = re.compile(args.exclude_tables.replace(",", "|"))
        tables = [t for t in tables if not filter.match(t)]
    tables = args.tables.split(",") if args.tables else None
    schemas = args.schemas.split(",") if args.schemas else [None]
    options = set(args.options.split(",")) if args.options else set()
    for schema in schemas:
        metadata.reflect(engine, schema, not args.noviews, tables)

    # Instantiate the generator
    generator_class = generators[args.generator].load()
    generator = generator_class(metadata, engine, options)

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
