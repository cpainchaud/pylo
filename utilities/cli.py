import os
import sys
from typing import Dict
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo

import commands.ruleset_export
import commands.workload_export

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--debug', type=str, required=False, default=False,
                    help='Enables extra debugging output in PYLO framework')
parser.add_argument('--use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')

sub_parsers = parser.add_subparsers(dest='command', title='command', help='sub-command help')
sub_parsers.required = True

commands.ruleset_export.fill_parser(sub_parsers.add_parser('rule-export', help=''))
commands.workload_export.fill_parser(sub_parsers.add_parser('workload-export', help=''))

args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()

hostname = args['pce']
settings_use_cache = args['use_cache']


org = pylo.Organization(1)

if settings_use_cache:
    print(" * Loading objects from cached PCE '{}' or cached file... ".format(hostname), end="", flush=True)
    org.load_from_cached_file(hostname)
else:
    org.load_from_saved_credentials(hostname, include_deleted_workloads=True, prompt_for_api_key=True)
print("OK!\n")

print(" * PCE statistics: ")
print(org.stats_to_str(padding='    '))

print()

if args['command'] == 'rule-export':
    commands.ruleset_export.run(args, org)
elif args['command'] == 'workload-export':
    commands.workload_export.run(args, org)






