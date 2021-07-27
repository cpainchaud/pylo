import os
import sys
import argparse

# in case user wants to run this utility while having a version of pylo already installed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import pylo

from .commands import workload_export
from .commands import workload_relabeler
from .commands import workload_used_in_rule_finder


def run():
    parser = argparse.ArgumentParser(description='TODO LATER')
    parser.add_argument('--pce', type=str, required=True,
                        help='hostname of the PCE')
    parser.add_argument('--debug', type=str, required=False, default=False,
                        help='Enables extra debugging output in PYLO framework')
    parser.add_argument('--use-cache', action='store_true',
                        help='For developers only')

    sub_parsers = parser.add_subparsers(dest='command', required=True)

    for command in pylo.cli.commands.available_commands.values():
        command.fill_parser(sub_parsers.add_parser(command.name, help=''))

    # ruleset_export.fill_parser(sub_parsers.add_parser('rule-export', help=''))
    workload_export.fill_parser(sub_parsers.add_parser('workload-export', help=''))
    workload_relabeler.fill_parser(sub_parsers.add_parser('workload-relabeler', help=''))
    workload_used_in_rule_finder.fill_parser(sub_parsers.add_parser('workload-used-in-rule-finder', help=''))

    args = vars(parser.parse_args())

    if args['debug']:
        pylo.log_set_debug()

    hostname = args['pce']
    settings_use_cache = args['use_cache']

    org = pylo.Organization(1)

    if settings_use_cache:
        print(" * Loading objects from cached PCE '{}' data... ".format(hostname), end="", flush=True)
        org.load_from_cached_file(hostname)
    else:
        print(" * Loading objects from PCE '{}' via API... ".format(hostname), end="", flush=True)
        org.load_from_saved_credentials(hostname, include_deleted_workloads=True, prompt_for_api_key=True)
    print("OK!\n")

    print(" * PCE statistics: ")
    print(org.stats_to_str(padding='    '))

    print()

    if args['command'] in commands.available_commands:
        commands.available_commands[args['command']].main(args, org)
    elif args['command'] == 'workload-export':
        workload_export.run(args, org)
    elif args['command'] == 'workload-relabeler':
        workload_relabeler.run(args, org)
    elif args['command'] == 'workload-used-in-rule-finder':
        workload_used_in_rule_finder.run(args, org)


if __name__ == "__main__":
    run()
