""" """
from __future__ import unicode_literals, division, print_function, absolute_import
import argparse
import sys

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData

from sqlcodegen.codegen import generate_model_code


def main():
    parser = argparse.ArgumentParser(description='Generates SQLAlchemy model code from an existing database.')
    parser.add_argument('url', help='SQLAlchemy url to the database')
    parser.add_argument('--schema', help='load tables from an alternate schema')
    parser.add_argument('--tables', help='tables to process (comma-separated, default: all)')
    parser.add_argument('--noviews', action='store_true', help="don't generate models for views")
    parser.add_argument('--noindexes', action='store_true', help="don't generate models for views")
    parser.add_argument('--outfile', type=argparse.FileType('w'), default=sys.stdout,
                        help='file to write output to (default: stdout)')
    args = parser.parse_args()

    engine = create_engine(args.url)
    metadata = MetaData(engine)
    tables = args.tables.split(',') if args.tables else None
    metadata.reflect(engine, args.schema, not args.noviews, tables)
    print(generate_model_code(metadata), file=args.outfile)
