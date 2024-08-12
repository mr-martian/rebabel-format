#!/usr/bin/env python3

def main():
    import argparse
    from . import processes
    from .processes.process import ALL_PROCESSES
    from . import config
    import logging

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
        else:
            print(f'Unknown process {args.config}.')
    else:
        conf = config.read_config(args.config)
        logging.basicConfig(level=args.log_level)
        import cProfile
        with cProfile.Profile() as pr:
            proc = ALL_PROCESSES[args.action](conf)
            proc.run()
            pr.dump_stats('/tmp/stats.prof')
