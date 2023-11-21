#!/usr/bin/env python3

def main():
    import argparse
    from . import db
    from . import query
    from . import converters
    from .converters.reader import read_new, ALL_DESCRIPTIONS
    from .converters.writer import write
    from . import config
    import os
    from collections import defaultdict
    parser = argparse.ArgumentParser(
        description='Process reBabel annotation files')
    parser.add_argument('action', choices=['import', 'inspect', 'export', 'query'])
    parser.add_argument('config', action='store', help='TOML configuration file')

    args = parser.parse_args()
    conf = config.read_config(args.config)

    if args.action == 'import':
        read_new(conf)
    elif args.action == 'inspect':
        f = db.RBBLFile(config.get_single_param(conf, 'inspect', 'db'))
        if config.get_single_param(conf, 'inspect', 'schema'):
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
    elif args.action == 'export':
        write(conf)
    elif args.action == 'query':
        query.search_conf(conf)
