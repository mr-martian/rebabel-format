#!/usr/bin/env python3

def main():
    import argparse
    from . import db
    from . import converters
    from .converters.reader import read_new, ALL_DESCRIPTIONS
    import os
    from collections import defaultdict
    parser = argparse.ArgumentParser(
        description='Process reBabel annotation files')
    parser.set_defaults(action='')
    subs = parser.add_subparsers()

    imp_parser = subs.add_parser(
        'import', help='Convert files from other format into reBabel')
    imp_parser.add_argument('mode', choices=sorted(ALL_DESCRIPTIONS.keys()),
                            help='File type to read')
    imp_parser.add_argument('db', action='store',
                            help='Path to write the reBabel database to')
    imp_parser.add_argument('infiles', nargs='+', help='Files to convert')
    usr = os.environ.get('USER', 'import-script')
    imp_parser.add_argument('--user', '-u', action='store', default=usr,
                            help=f'Username for imported data (default: {usr})')
    imp_parser.set_defaults(action='import')

    insp_parser = subs.add_parser(
        'inspect', help='Get information about the structure of a reBabel file')
    insp_parser.add_argument('db', action='store', help='reBabel file to read')
    insp_parser.add_argument(
        '--schema', action='store_true',
        help='generate a schema of types and features in the file')
    insp_parser.set_defaults(action='inspect')

    args = parser.parse_args()
    if args.action == 'import':
        read_new(args.infiles, args.db, args.mode, args.user)
    elif args.action == 'inspect':
        f = db.RBBLFile(args.db)
        if args.schema:
            feats = f.get_all_features()
            fd = defaultdict(lambda: defaultdict(list))
            for fid, tier, name, utype, vtype in feats:
                fd[utype][tier].append((name, vtype))
            for utype, d1 in sorted(fd.items()):
                print(utype)
                for tier, d2 in sorted(d1.items()):
                    if tier == 'meta' and len(d2) == 1:
                        continue
                    print('\t' + tier)
                    for feat, vtype in sorted(d2):
                        if tier == 'meta' and feat == 'active':
                            continue
                        print(f'\t\t{feat}: {vtype}')
                print('')
