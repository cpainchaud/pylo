import argparse
import os
import sys
from dataclasses import dataclass
from typing import Union, Optional, Dict, List, Any, Set

# this line is only needed for dev_playground examples as the developer may not have installed the library, remove it in your own code
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
import illumio_pylo.cli as cli
import illumio_pylo.cli.commands as commands
from illumio_pylo.cli.NativeParsers import LabelParser

command_name = 'my-first-command-show-workloads'

objects_load_filter = ['workloads', 'labels']  # optional, if you want to load only a subset of objects to speedup API calls
# objects_load_filter = None  # optional, if you want to load all objects

@dataclass
class MyBuiltInParser:  # optional, if you want to use built-in parsers
    filter_env_label = LabelParser('--filter-env-label', action_short_name='-env',
                                   label_type='env',  # optional, it will ensure that selected labels are of a specified type
                                   is_required=False, allow_multiple=True)

def fill_parser(parser: argparse.ArgumentParser):
    """ This function will be called by the CLI to fill the parser with the arguments of your command """
    parser.add_argument('--sort-by-name', '-s', action='store_true',
                        help='Filter workloads by environment labels (separated by commas)')


def __main(args, org: pylo.Organization,
           native_parsers: MyBuiltInParser, # optional, if you want to use built-in parsers
           **kwargs):
    """ This is the main function of the command, it will be called by the CLI when the command is executed
    :param args: the arguments passed to the command, as returned by the argparse parser
    :param org: the Organization object from illumio_pylo library, ready to consume
    :param native_parsers: the native parsers object, if you used them
    """

    workloads = native_parsers.filter_env_label.filter_workloads_matching_labels(org.WorkloadStore.workloads)
    print(f" * Now listing the {len(workloads)} Workload(s) found in PCE '{org.connector.hostname}' which are matching the filters:")

    if args['sort_by_name']:
        workloads = sorted(workloads, key=lambda x: x.name)

    for workload in workloads:
        print(f"  - {workload.name} ({workload.href})")


# it's time to inject the properties of your first cli extension to Pylo!
command_object = commands.Command(name=command_name, main_func=__main, parser_func=fill_parser,
                                  load_specific_objects_only=objects_load_filter,
                                  skip_pce_config_loading=False,  # if you want to skip the PCE config loading, set this to True and do your own thing!
                                  native_parsers_as_class=MyBuiltInParser()  # optional, if you want to use your own built-in parsers
                                  )

# and now you can run it!
cli.run(forced_command_name=command_name)  # forced_command_name is optional it will use CLI framework but won't give the option to use other built-in commands


