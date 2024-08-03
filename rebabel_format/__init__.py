#!/usr/bin/env python3

def main():
    import argparse
    from . import processes
    from .processes.process import ALL_PROCESSES
    from . import config
    import logging

    epilog = []
    for name, proc in sorted(ALL_PROCESSES.items()):
        epilog.append(proc.help_text())

    parser = argparse.ArgumentParser(
        description='Process reBabel annotation files',
        epilog='Available Actions:\n\n' + '\n\n'.join(epilog),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('action', choices=sorted(ALL_PROCESSES.keys()),
                        metavar='ACTION', help='Action to perform (see below)')
    parser.add_argument('config', action='store', help='TOML configuration file')
    parser.add_argument('--log-level', '-l', action='store',
                        default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        metavar='LEVEL')

    args = parser.parse_args()
    conf = config.read_config(args.config)
    logging.basicConfig(level=args.log_level)

    import cProfile
    with cProfile.Profile() as pr:
        proc = ALL_PROCESSES[args.action](conf)
        proc.run()
        pr.dump_stats('/tmp/stats.prof')
