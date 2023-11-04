#!/usr/bin/env python3

def main():
    import argparse
    from . import converters
    from .converters.reader import read_new, ALL_DESCRIPTIONS
    import os
    parser = argparse.ArgumentParser(description='Process reBabel annotation files')
    subs = parser.add_subparsers()
    imp_parser = subs.add_parser('import', help='Convert files from other format into reBabel')
    imp_parser.add_argument('mode', choices=sorted(ALL_DESCRIPTIONS.keys()),
                            help='File type to read')
    imp_parser.add_argument('db', action='store', help='Path to write the reBabel database to')
    imp_parser.add_argument('infiles', nargs='+', help='Files to convert')
    usr = os.environ.get('USER', 'import-script')
    imp_parser.add_argument('--user', '-u', action='store', default=usr,
                            help=f'Username for imported data (default: {usr})')
    imp_parser.set_defaults(action='import')
    args = parser.parse_args()
    if args.action == 'import':
        read_new(args.infiles, args.db, args.mode, args.user)
    print('hi', ALL_DESCRIPTIONS)
