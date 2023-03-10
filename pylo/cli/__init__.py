import os
from typing import Optional

import sys
import argparse

# in case user wants to run this utility while having a version of pylo already installed
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import pylo
from pylo.cli import commands


def run(forced_command_name: Optional[str] = None):
    parser = argparse.ArgumentParser(description='TODO LATER')
    parser.add_argument('--pce', type=str, required=True,
                        help='hostname of the PCE')
    parser.add_argument('--debug', action='store_true',
                        help='Enables extra debugging output in PYLO framework')
    parser.add_argument('--use-cache', action='store_true',
                        help='For developers only')

    sub_parsers = parser.add_subparsers(dest='command', required=True)

    for command in commands.available_commands.values():
        if(forced_command_name is not None and command.name != forced_command_name):
            continue
        command.fill_parser(sub_parsers.add_parser(command.name, help=''))

    args = vars(parser.parse_args())

    if args['debug']:
        pylo.log_set_debug()

    hostname = args['pce']
    settings_use_cache = args['use_cache']

    # We are getting the command object associated to the command name
    selected_command = commands.available_commands[args['command']]
    if selected_command is None:
        raise pylo.PyloEx("Cannot find command named '{}'".format(args['command']))

    org = pylo.Organization(1)
    connector: Optional[pylo.APIConnector] = None
    config_data = None

    if settings_use_cache:
        print(" * Loading objects from cached PCE '{}' data... ".format(hostname), end="", flush=True)
        org.load_from_cached_file(hostname)
        print("OK!")
    else:
        print(" * Looking for PCE '{}' credentials... ".format(hostname), end="", flush=True)
        connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
        org.connector = connector
        print("OK!")

        print(" * Downloading PCE objects from API... ".format(hostname), end="", flush=True)
        config_data = connector.get_pce_objects(list_of_objects_to_load=selected_command.load_specific_objects_only)
        print("OK!")

        if not selected_command.skip_pce_config_loading:
            print(" * Loading objects from PCE '{}' via API... ".format(hostname), end="", flush=True)
            org.pce_version = connector.getSoftwareVersion()
            org.load_from_json(config_data, list_of_objects_to_load=selected_command.load_specific_objects_only)
            print("OK!")

    print()
    if not selected_command.skip_pce_config_loading:
        print(" * PCE statistics: ")
        print(org.stats_to_str(padding='    '))

        print(flush=True)

    print("**** {} UTILITY ****".format(selected_command.name.upper()), flush=True)
    commands.available_commands[args['command']].main(args, org, config_data=config_data, connector=connector)
    print("**** END OF {} UTILITY ****".format(selected_command.name.upper()))
    print()


if __name__ == "__main__":
    run()
