import argparse
import os
import sys
from typing import Union, Optional, Dict, List, Any, Set

# this line is only needed for dev_playground examples as the developer may not have install the library, remove it in your own code
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pylo
import pylo.cli as cli
import pylo.cli.commands as commands


command_name = 'my-first-command-show-workloads'

objects_load_filter = ['workloads', 'labels']  # optional, if you want to load only a subset of objects to speedup API calls
# objects_load_filter = None  # optional, if you want to load all objects


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                        help='Filter workloads by environment labels (separated by commas)')


def __main(args, org: pylo.Organization, **kwargs):
    """ This is the main function of the command, it will be called by the CLI when the command is executed
    :param args: the arguments passed to the command, as returned by the argparse parser
    :param org: the Organization object from pylo library, ready to consume
    """

    # let's check if user has input any environment label filter
    env_labels: Optional[List[pylo.Label]] = None
    if args['filter_env_label'] is not None:
        env_labels_strings = args['filter_env_label'].split(',')
        print(f" * Environment label filter specified, will display workloads with environment labels: {env_labels_strings}")
        env_labels = []
        # check if labels exist and put them in a list or exit with raise an error
        for label_name in env_labels_strings:
            label = org.LabelStore.find_label_by_name_and_type(label_name, 'env')
            if label is None:
                raise Exception(f"Label '{label_name}' does not exist")
            env_labels.append(label)
    else:
        print(" * No environment label filter specified, will display all workloads")

    print(f"Workloads found in PCE '{org.connector.hostname}' and matching filters (if any):")
    for workload in org.WorkloadStore.itemsByHRef.values():
        if env_labels is not None and workload.env_label in env_labels:
            print(f" - {workload.name} ({workload.href})")


# it's time to inject the properties of your first cli extension to PYLO!
command_object = commands.Command(name=command_name, main_func=__main, parser_func=fill_parser,
                                  load_specific_objects_only=objects_load_filter,
                                  skip_pce_config_loading=False,  # if you want to skip the PCE config loading, set this to True and do your own thing!
                                  )

# and now you can run it!
cli.run(forced_command_name=command_name)  # forced_command_name is optional it will use CLI framework but won't give the option to use other built-in commands


