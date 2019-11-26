import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo



parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', 'd', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')

parser.add_argument('--input-file', '-i', type=bool, nargs='?', required=True,
                    help='CSV or Excel input filename')
parser.add_argument('--ignore-label-case-collisions', 'd', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Use this option if you want allow labels with same name but different case (Illumio PCE allows it but its definitely a bad practice!)')

args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['host']
use_cached_config = args['dev_use_cache']
input_file = args['input_file']
ignore_label_case_collisions = args['ignore_label_case_collisions']


org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()

if use_cached_config:
    org.load_from_cache_or_saved_credentials(hostname)
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading Workloads/Agents listing from the PCE... ", end="", flush=True)
    fake_config['workloads'] = connector.objects_workload_get()
    print("OK!")

    print(" * Downloading Labels listing from the PCE... ", end="", flush=True)
    fake_config['labels'] = connector.objects_label_get()
    print("OK!")

    print(" * Parsing PCE data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.load_from_json(fake_config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))

