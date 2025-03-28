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
    from importlib_metadata import entry_points, version, EntryPoint
else:
    from importlib.metadata import entry_points, version, EntryPoint


_generators_cache = None


def get_generators() -> dict[str, EntryPoint]:
    global _generators_cache
    if _generators_cache is None:
        _generators_cache = {
            ep.name: ep for ep in entry_points(group="sqlacodegen.generators")
        }
    return _generators_cache


def _parse_flag_or_dict(value: str) -> bool | dict[Any, Any]:
    if value is None or value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        parsed = ast.literal_eval(value)
        if not isinstance(parsed, dict):
            raise ValueError
        return parsed
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            "Value must be 'true' or a valid dict string."
        ) from exc


def create_parser() -> argparse.ArgumentParser:
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
        choices=get_generators(),
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
        "--thickmode",
        nargs="?",
        const=True,
        type=_parse_flag_or_dict,
        help="""(Oracle) Set to True or provide a dict string like: --thickmode '{"lib_dir": "/path", "config_dir": "/config", "driver_name": "app : 1.0.0"}'""",
    )
    parser.add_argument("--outfile", help="file to write output to (default: stdout)")

    return parser


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    if args.version:
        print(version("sqlacodegen"))
        return

    if not args.url:
        print("You must supply a url\n", file=sys.stderr)
        parser.print_help()
        return

    if citext:
        print(f"Using sqlalchemy-citext {version('citext')}")

    if geoalchemy2:
        print(f"Using geoalchemy2 {version('geoalchemy2')}")

    if pgvector:
        print(f"Using pgvector {version('pgvector')}")

    # Use reflection to fill in the metadata
    kwargs = {}
    if args.thickmode is not None:
        kwargs["thick_mode"] = args.thickmode
    engine = create_engine(args.url, **kwargs)
    metadata = MetaData()
    tables = args.tables.split(",") if args.tables else None
    schemas = args.schemas.split(",") if args.schemas else [None]
    options = set(args.options.split(",")) if args.options else set()

    # Instantiate the generator
    generator_class = get_generators()[args.generator].load()
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
