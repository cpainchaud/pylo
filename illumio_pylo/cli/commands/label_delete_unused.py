import argparse
from typing import Optional, List

import illumio_pylo as pylo
import json
import hashlib

from illumio_pylo import log
from . import Command
from illumio_pylo.API.JsonPayloadTypes import LabelObjectJsonStructure

command_name = "label-delete-unused"
objects_load_filter = []  # No need to load any objects from PCE


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--confirm', action='store_true',
                        help='No change will be implemented in the PCE until you use this function to confirm you\'re good with them after review')
    parser.add_argument('--limit', type=int, required=False, default=None,
                        help='Maximum number of unused labels to delete (default: all found unused labels)')


def __main(args, org: pylo.Organization = None, connector: pylo.APIConnector = None, config_data=None, **kwargs):

    settings_confirmed_changes: bool = args['confirm']
    settings_limit_deletions: Optional[int] = args['limit']

    print("Fetching all Labels from the PCE... ", end='', flush=True)
    # pylo.log_set_debug()
    labels_json = connector.objects_label_get(max_results=199000, get_usage=True, async_mode=False)
    print("OK!")

    print(f"Analyzing {len(labels_json)} labels to find unused ones... ")
    unused_labels: List[LabelObjectJsonStructure] = []

    for label_json in labels_json:
        usage_json = label_json.get('usage', {})
        label_is_used = False

        for usage_type, usage_confirmed in usage_json.items():
            if usage_confirmed:
                label_is_used = True
                print(f"Label '{label_json.get('value')}' is used in '{usage_type}', skipping deletion.")
                break

        if not label_is_used:
            print(f"Label '{label_json.get('value')}' is unused, marking for deletion.")
            unused_labels.append(label_json)

    print()
    print(f"Found {len(unused_labels)} unused labels vs total of {len(labels_json)} labels.")

    if len(unused_labels) > 0:
        if not settings_confirmed_changes:
            print("No change will be implemented in the PCE until you use the '--confirm' flag to confirm you're good with them after review.")
        else:
            print()
            print(f"Proceeding to delete unused labels up to the limit of '{settings_limit_deletions if settings_limit_deletions is not None else 'all'}'...")
            tracker = connector.new_tracker_for_label_multi_deletion()

            if settings_limit_deletions is not None:
                unused_labels = unused_labels[:settings_limit_deletions]

            for label_json in unused_labels:
                tracker.add_label(label_json['href'])

            tracker.execute_deletion()
            errors_count = tracker.get_errors_count()
            success_count = len(unused_labels) - errors_count

            for label_json in unused_labels:
                error = tracker.get_error(label_json['href'])
                if error is not None:
                    print(f" - ERROR deleting label '{label_json.get('value')}': {error}")
                else:
                    print(f" - SUCCESS deleting label '{label_json.get('value')}'")

            print()
            print(f"Deletion completed: {success_count} labels deleted successfully, {errors_count} errors encountered.")


command_object = Command(command_name, __main, fill_parser, skip_pce_config_loading=True, load_specific_objects_only=objects_load_filter)