from __future__ import unicode_literals, division, print_function, \
    absolute_import
import argparse
import codecs
import sys

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData
import pkg_resources

from sqlacodegen.codegen import CodeGenerator


def parser_factory():
    '''
    A factory method to generate a parser with standard args
    :return: ArgumentParser
    '''
    parser = argparse.ArgumentParser(
        description='Generates SQLAlchemy model '
                    'code from an existing database.')
    parser.add_argument('url', nargs='?',
                        help='SQLAlchemy url to the database')
    parser.add_argument('--version', action='store_true',
                        help="print the version number and exit")
    parser.add_argument('--schema',
                        help='load tables from an alternate schema')
    parser.add_argument('--tables',
                        help='tables to process '
                             '(comma-separated, default: all)')
    parser.add_argument('--skiptables', help='tables to skip ('
                                             'comma-separated, default: none)')
    parser.add_argument('--noviews', action='store_true', help="ignore views")
    parser.add_argument('--noindexes', action='store_true',
                        help='ignore indexes')
    parser.add_argument('--noconstraints', action='store_true',
                        help='ignore constraints')
    parser.add_argument('--nojoined', action='store_true',
                        help="don't autodetect joined table inheritance")
    parser.add_argument('--noinflect', action='store_true',
                        help="don't try to convert "
                             "tables names to singular form")
    parser.add_argument('--noclasses', action='store_true',
                        help="don't generate classes, only tables")
    parser.add_argument('--outfile',
                        help='file to write output to (default: stdout)')
    return parser


def main():
    parser = parser_factory()
    args = parser.parse_args()

    if args.version:
        version = pkg_resources.get_distribution('sqlacodegen').parsed_version
        print(version.public)
        return
    if not args.url:
        print('You must supply a url\n', file=sys.stderr)
        parser.print_help()
        return

    engine = create_engine(args.url)
    metadata = MetaData(engine)
    def _filter_tables_list(table_name, metadata_obj):
        '''
        A callable function passed to MetaData.reflect which returns boolean predicates on args entered
        First, return False for all tables in skiptables
        Second, if tables arg is non-empty, only return tables specified; Else return all tables
        :param table_name: string
        :param metadata_obj: sqlalchemy.MetaData
        :return: bool
        '''
        if table_name in args.skiptables:
            return False
        if args.tables:
            if table_name in args.tables:
                return True
            else:
                return False
        else:
            return True

    metadata.reflect(bind=engine, schema=args.schema, views=args.noviews,
                         only=_filter_tables_list)
    outfile = codecs.open(args.outfile, 'w',
                          encoding='utf-8') if args.outfile else sys.stdout
    generator = CodeGenerator(metadata, args.noindexes, args.noconstraints,
                              args.nojoined, args.noinflect,
                              args.noclasses)
    generator.render(outfile)
