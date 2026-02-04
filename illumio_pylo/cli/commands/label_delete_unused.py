"""
Usage documentation for this command can be found in docs/cli/label-delete-unused.md
"""

import argparse
import os
from typing import Optional, List, Literal

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader

from . import Command
from .utils.misc import make_filename_with_timestamp
from illumio_pylo.API.JsonPayloadTypes import LabelObjectJsonStructure

command_name = "label-delete-unused"
objects_load_filter = []  # No need to load any objects from PCE


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--confirm', action='store_true',
                        help='No change will be implemented in the PCE until you use this function to confirm you\'re good with them after review')
    parser.add_argument('--limit', type=int, required=False, default=None,
                        help='Maximum number of unused labels to delete (default: all found unused labels)')
    parser.add_argument('--report-format', '-rf', action='append', type=str, choices=['csv', 'xlsx'], default=None,
                        help='Which report formats you want to produce (repeat option to have several)')
    parser.add_argument('--output-dir', '-o', type=str, required=False, default="output",
                        help='Directory where to write the report file(s)')
    parser.add_argument('--output-filename', type=str, default=None,
                        help='Write report to the specified file (or basename) instead of using the default timestamped filename. If multiple formats are requested, the provided path\'s extension will be replaced/added per format.')


def __main(args, org: pylo.Organization = None, connector: pylo.APIConnector = None, config_data=None, **kwargs):

    settings_confirmed_changes: bool = args['confirm']
    settings_limit_deletions: Optional[int] = args['limit']

    report_wanted_format: List[Literal['csv', 'xlsx']] = args['report_format']
    if report_wanted_format is None:
        report_wanted_format = ['csv']

    arg_report_output_dir: str = args['output_dir']
    arg_output_filename: Optional[str] = args.get('output_filename')

    if arg_output_filename is None:
        output_file_prefix = make_filename_with_timestamp('label-delete-unused_', arg_report_output_dir)
    else:
        output_file_prefix = None

    # Initialize report structure
    report_headers = pylo.ExcelHeaderSet([
        ExcelHeader(name='key', max_width=25, wrap_text=False),
        ExcelHeader(name='value', max_width=40),
        ExcelHeader(name='type', max_width=15, wrap_text=False),
        ExcelHeader(name='created_at', max_width=20, wrap_text=False),
        ExcelHeader(name='updated_at', max_width=20, wrap_text=False),
        ExcelHeader(name='external_data_set', max_width=30, wrap_text=False),
        ExcelHeader(name='external_data_reference', max_width=30, wrap_text=False),
        ExcelHeader(name='usage_list', max_width=60),
        'action',
        ExcelHeader(name='error_message', max_width=50),
        ExcelHeader(name='link_to_pce', max_width=15, wrap_text=False, url_text='See in PCE', is_url=True),
        ExcelHeader(name='href', max_width=60, wrap_text=False)
    ])

    report = pylo.ArraysToExcel()
    sheet: pylo.ArraysToExcel.Sheet = report.create_sheet('unused_labels', report_headers, force_all_wrap_text=True, multivalues_cell_delimiter=',')

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
            for label_json in unused_labels:
                add_label_to_report(label_json, sheet, connector, "TO BE DELETED (no confirm option used)")
        else:
            print()
            print(f"Proceeding to delete unused labels up to the limit of '{settings_limit_deletions if settings_limit_deletions is not None else 'all'}'...")
            tracker = connector.new_tracker_for_label_multi_deletion()

            if settings_limit_deletions is not None:
                # Add labels beyond the limit to the report as ignored
                for label_json in unused_labels[settings_limit_deletions:]:
                    add_label_to_report(label_json, sheet, connector, "ignored (limit reached)")
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
                    add_label_to_report(label_json, sheet, connector, "API error", error)
                else:
                    print(f" - SUCCESS deleting label '{label_json.get('value')}'")
                    add_label_to_report(label_json, sheet, connector, "deleted")

            print()
            print(f"Deletion completed: {success_count} labels deleted successfully, {errors_count} errors encountered.")
    else:
        print("\n** WARNING: no unused labels found !\n")

    # Write report to disk
    sheet.reorder_lines(['type', 'value'])  # sort by type and value for better readability
    for report_format in report_wanted_format:
        # Choose output filename depending on whether user provided --output-filename
        if arg_output_filename is None:
            output_filename = output_file_prefix + '.' + report_format
        else:
            # If only one format requested, use the provided filename as-is
            if len(report_wanted_format) == 1:
                output_filename = arg_output_filename
            else:
                base = os.path.splitext(arg_output_filename)[0]
                output_filename = base + '.' + report_format

        # Ensure parent directory exists
        output_directory = os.path.dirname(output_filename)
        if output_directory:
            os.makedirs(output_directory, exist_ok=True)

        print(f" * Writing report file '{output_filename}' ... ", end='', flush=True)
        if report_format == 'csv':
            sheet.write_to_csv(output_filename)
        elif report_format == 'xlsx':
            report.write_to_excel(output_filename)
        else:
            raise pylo.PyloEx(f"Unknown format for report: '{report_format}'")
        print("DONE")


def add_label_to_report(label_json: LabelObjectJsonStructure, sheet: pylo.ArraysToExcel.Sheet,
                        connector: pylo.APIConnector, action: str, error_message: str = ''):
    """
    Add a label to the report sheet.

    :param label_json: The label JSON structure from the API
    :param sheet: The Excel sheet to add the label to
    :param connector: The API connector to get PCE information
    :param action: The action taken on the label
    :param error_message: Optional error message if deletion failed
    """
    # Build PCE UI URL for the label
    pce_hostname = connector.pce_hostname
    pce_port = connector.pce_port
    org_id = connector.org_id
    label_href = label_json.get('href', '')

    if pce_port == 443:
        url_link_to_pce = f"https://{pce_hostname}/orgs/{org_id}{label_href}"
    else:
        url_link_to_pce = f"https://{pce_hostname}:{pce_port}/orgs/{org_id}{label_href}"

    # Generate usage list from usage dictionary
    usage_list = ', '.join([k for k, v in label_json.get('usage', {}).items() if v])

    new_row = {
        'key': label_json.get('key', ''),
        'value': label_json.get('value', ''),
        'type': label_json.get('key', ''),
        'created_at': label_json.get('created_at', ''),
        'updated_at': label_json.get('updated_at', ''),
        'external_data_set': label_json.get('external_data_set', ''),
        'external_data_reference': label_json.get('external_data_reference', ''),
        'usage_list': usage_list,
        'action': action,
        'error_message': error_message,
        'href': label_href,
        'link_to_pce': url_link_to_pce
    }

    sheet.add_line_from_object(new_row)


command_object = Command(command_name, __main, fill_parser, skip_pce_config_loading=True, load_specific_objects_only=objects_load_filter)