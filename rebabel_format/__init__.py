#!/usr/bin/env python3

def load_plugins(plugin_type: str) -> None:
    import sys
    if sys.version_info < (3, 10):
        from importlib_metadata import entry_points
    else:
        from importlib.metadata import entry_points
    for ep in entry_points(group='rebabel.'+plugin_type):
        ep.load()

def import_directory(dirname: str) -> None:
    import os
    import glob
    import importlib.util
    import sys
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), dirname)
    for fname in glob.glob(os.path.join(path, '*.py')):
        if fname in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location('blah', fname)
        module = importlib.util.module_from_spec(spec)
        sys.modules[fname] = module
        spec.loader.exec_module(module)

def load_processes(plugins: bool) -> None:
    import_directory('processes')
    if plugins:
        load_plugins('processes')

def load_readers(plugins: bool) -> None:
    import_directory('converters')
    if plugins:
        load_plugins('converters')
        load_plugins('readers')

def load_writers(plugins: bool) -> None:
    import_directory('converters')
    if plugins:
        load_plugins('converters')
        load_plugins('writers')

def get_process(name: str):
    from rebabel_format.process import ALL_PROCESSES
    return ALL_PROCESSES[name]

def main():
    import argparse
    from rebabel_format.process import ALL_PROCESSES
    from rebabel_format import config
    import logging

    load_processes(True)

    actions = sorted(ALL_PROCESSES.keys()) + ['help']

    join = '\n- '
    epilog = f'''Available Actions:
- {join.join(actions)}

run `rebabel-format help [ACTION]` for a longer description.'''

    parser = argparse.ArgumentParser(
        description='Process reBabel annotation files',
        epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('action', choices=actions,
                        metavar='ACTION', help='Action to perform')
    parser.add_argument('config', action='store', help='TOML configuration file')
    parser.add_argument('--log-level', '-l', action='store',
                        default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        metavar='LEVEL')

    args = parser.parse_args()

    if args.action == 'help':
        if args.config in ALL_PROCESSES:
            print(ALL_PROCESSES[args.config].help_text())
        elif args.config.startswith('import.'):
            load_readers(True)
            from rebabel_format.reader import ALL_READERS
            _, reader = args.config.split('.', 1)
            if reader in ALL_READERS:
                print(ALL_READERS[reader].help_text())
            else:
                print(f'Unknown import format {reader}.')
        else:
            print(f'Unknown process {args.config}.')
    else:
        if args.action == 'import':
            load_readers(True)
        elif args.action == 'export':
            load_writers(True)
        conf = config.read_config(args.config)
        logging.basicConfig(level=args.log_level)
        import cProfile
        with cProfile.Profile() as pr:
            proc = ALL_PROCESSES[args.action](conf)
            proc.run()
            pr.dump_stats('/tmp/stats.prof')
