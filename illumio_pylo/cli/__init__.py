import os
from typing import Optional, Dict
import time
import datetime
import sys
import argparse
from .NativeParsers import BaseParser

# in case user wants to run this utility while having a version of pylo already installed
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import illumio_pylo as pylo
from illumio_pylo.cli import commands


def run(forced_command_name: Optional[str] = None):

    cli_start_time = datetime.datetime.now()

    def add_native_parser_to_argparse(parser: argparse.ArgumentParser, native_parsers: object):
        # each property of the native parser is an extension of BaseParser, we need to iterate over them and add them to the argparse parser
        for attr_name in dir(native_parsers):
            attr = getattr(native_parsers, attr_name)
            if isinstance(attr, BaseParser):
                attr.fill_parser(parser)

    def execute_native_parsers(args: Dict, org: pylo.Organization, native_parsers: object):
        first_native_parser_found = False

        for attr_name in dir(native_parsers):
            attr = getattr(native_parsers, attr_name)
            if isinstance(attr, BaseParser):
                if first_native_parser_found is False:
                    first_native_parser_found = True
                    print(" * Native CLI arguments parsing...")
                attr.execute(args[attr.get_arg_name()], org, padding='    ')

    parser = argparse.ArgumentParser(description='PYLO-CLI: Illumio API&More Command Line Interface')
    parser.add_argument('--pce', type=str, required=False,
                        help='hostname of the PCE')
    parser.add_argument('--force-async-mode', action='store_true',
                        help='Forces the command to run async API queries when required (large PCEs which timeout on specific queries)')
    parser.add_argument('--debug', action='store_true',
                        help='Enables extra debugging output in Pylo framework')
    parser.add_argument('--use-cache', action='store_true',
                        help='For developers only')
    parser.add_argument('--version', action='store_true', help='Prints the version of the Pylo CLI')

    selected_command = None

    if forced_command_name is None:
        sub_parsers = parser.add_subparsers(dest='command', required=True)
        for command in commands.available_commands.values():
            sub_parser = sub_parsers.add_parser(command.name, help='')
            command.fill_parser(sub_parser)
            if command.native_parsers is not None:
                add_native_parser_to_argparse(sub_parser, command.native_parsers)
    else:
        for command in commands.available_commands.values():
            if forced_command_name is not None and command.name != forced_command_name:
                continue
            command.fill_parser(parser)
            if command.native_parsers is not None:
                add_native_parser_to_argparse(parser, command.native_parsers)
            selected_command = command


    # version is a special command that does not require a PCE
    # if first argument is --version, we print the version and exit
    if len(sys.argv) > 1:
        arg_lower = sys.argv[1].lower()
        if arg_lower == '--version' or arg_lower == '-v' or arg_lower == 'version':
            print("Pylo CLI version {}".format(pylo.__version__))
            return

    args = vars(parser.parse_args())

    if args['debug']:
        pylo.log_set_debug()

    credential_profile_name = args['pce']
    settings_use_cache = args['use_cache']
    org: Optional[pylo.Organization] = None

    # We are getting the command object associated to the command name if it was not already set (via forced_command_name)
    if selected_command is None:
        selected_command = commands.available_commands[args['command']]
        if selected_command is None:
            raise pylo.PyloEx("Cannot find command named '{}'".format(args['command']))

    connector: Optional[pylo.APIConnector] = None
    config_data = None
    
    print("* Started Pylo CLI version {}".format(pylo.__version__))


    if not selected_command.credentials_manager_mode:
        timer_start = time.perf_counter()
        # credential_profile_name is required for all commands except the credential manager
        if credential_profile_name is None:
            raise pylo.PyloEx("The --pce argument is required for this command")
        if settings_use_cache:
            print(" * Loading objects from cached PCE '{}' data... ".format(credential_profile_name), end="", flush=True)
            org = pylo.Organization.get_from_cache_file(credential_profile_name)
            print("OK! (execution time: {:.2f} seconds)".format(time.perf_counter() - timer_start))
            connector = pylo.APIConnector.create_from_credentials_in_file(credential_profile_name, request_if_missing=False)
            if connector is not None:
                org.connector = connector
        else:
            print(" * Looking for PCE/profile '{}' credentials... ".format(credential_profile_name), end="", flush=True)
            connector = pylo.APIConnector.create_from_credentials_in_file(credential_profile_name, request_if_missing=True)
            print("OK!")

            print(" * Downloading PCE objects from API... ".format(credential_profile_name), end="", flush=True)
            config_data = connector.get_pce_objects(list_of_objects_to_load=selected_command.load_specific_objects_only, force_async_mode=args['force_async_mode'])
            timer_download_finished = time.perf_counter()
            print("OK! (execution time: {:.2f} seconds)".format(timer_download_finished - timer_start))

            org = pylo.Organization(1)
            org.connector = connector

            if not selected_command.skip_pce_config_loading:
                print(" * Loading objects from PCE '{}' via API... ".format(credential_profile_name), end="", flush=True)
                org.pce_version = connector.get_software_version()
                org.load_from_json(config_data, list_of_objects_to_load=selected_command.load_specific_objects_only)
                print("OK! (execution time: {:.2f} seconds)".format(time.perf_counter() - timer_download_finished))

        print()
        if not selected_command.skip_pce_config_loading:
            print(" * PCE statistics: ")
            print(org.stats_to_str(padding='    '))

            print(flush=True)

    print("**** {} UTILITY ****".format(selected_command.name.upper()), flush=True)
    command_execution_time_start = time.perf_counter()
    if selected_command.native_parsers is None:
        native_parsers = None
    else:
        native_parsers = selected_command.native_parsers
        execute_native_parsers(args, org, native_parsers)

    if native_parsers is not None:
        commands.available_commands[selected_command.name].main(args, org=org, config_data=config_data, connector=connector, native_parsers=native_parsers, pce_cache_was_used=settings_use_cache)
    else:
        commands.available_commands[selected_command.name].main(args, org=org, config_data=config_data, connector=connector, pce_cache_was_used=settings_use_cache)

    print()
    cli_end_time = datetime.datetime.now()
    print("**** END OF {} UTILITY ****".format(selected_command.name.upper()))
    print("Command Specific Execution time: {:.2f} seconds".format(time.perf_counter() - command_execution_time_start))
    print("CLI started at {} and finished at {}".format(cli_start_time, cli_end_time))
    print("CLI Total Execution time: {}".format(cli_end_time - cli_start_time))
    print()


if __name__ == "__main__":
    run()
