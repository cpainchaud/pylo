"""
Usage documentation for this command can be found in docs/cli/label-delete-unused.md
"""

import argparse
from typing import Optional, List, Tuple, Dict

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader
from illumio_pylo.API.JsonPayloadTypes import LabelObjectJsonStructure
from . import Command
from .utils.report_writer import ReportWriter

command_name = "label-delete-unused"
objects_load_filter = []  # No need to load any objects from PCE


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--confirm', action='store_true',
                        help='No change will be implemented in the PCE until you use this function to confirm you\'re good with them after review')
    parser.add_argument('--limit', type=int, required=False, default=None,
                        help='Maximum number of unused labels to delete (default: all found unused labels)')

    # Add standard report arguments (static helper)
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='label-delete-unused',
        default_sheet_name='unused_labels'
    )


def find_unused_labels(labels_json: List[LabelObjectJsonStructure]) -> List[LabelObjectJsonStructure]:
    """
    Identify labels that have no usage.

    Args:
        labels_json: List of label JSON structures from API

    Returns:
        List of unused label JSON structures
    """
    unused_labels = []
    for label_json in labels_json:
        usage_json = label_json.get('usage', {})
        if not any(usage_json.values()):
            unused_labels.append(label_json)
    return unused_labels


def apply_deletion_limit(unused_labels: List[LabelObjectJsonStructure],
                         limit: Optional[int]) -> Tuple[List[LabelObjectJsonStructure], List[LabelObjectJsonStructure]]:
    """
    Split unused labels into to-delete and ignored based on limit.

    Args:
        unused_labels: List of unused labels
        limit: Maximum number to delete, or None for all

    Returns:
        Tuple of (labels_to_delete, labels_ignored)
    """
    if limit is None:
        return unused_labels, []
    return unused_labels[:limit], unused_labels[limit:]


def build_pce_url(connector: pylo.APIConnector, label_href: str) -> str:
    """
    Build PCE UI URL for a label.

    Args:
        connector: API connector with PCE connection info
        label_href: Label href from API

    Returns:
        Full URL to label in PCE UI
    """
    pce_hostname = connector.fqdn
    pce_port = connector.port
    org_id = connector.org_id

    if pce_port == 443:
        return f"https://{pce_hostname}/orgs/{org_id}{label_href}"
    else:
        return f"https://{pce_hostname}:{pce_port}/orgs/{org_id}{label_href}"


def delete_labels_and_collect_results(unused_labels: List[LabelObjectJsonStructure],
                                       connector: pylo.APIConnector) -> Dict:
    """
    Execute label deletion and collect results.

    Args:
        unused_labels: Labels to delete
        connector: API connector

    Returns:
        Dict with 'successful', 'failed', and 'errors' keys
    """
    tracker = connector.new_tracker_for_label_multi_deletion()

    for label_json in unused_labels:
        tracker.add_label(label_json['href'])

    tracker.execute_deletion()

    results = {
        'successful': [],
        'failed': [],
        'errors': {}
    }

    for label_json in unused_labels:
        error = tracker.get_error(label_json['href'])
        if error is not None:
            results['failed'].append(label_json)
            results['errors'][label_json['href']] = error
        else:
            results['successful'].append(label_json)

    return results


def __main(args, org: pylo.Organization = None, connector: pylo.APIConnector = None, config_data=None, **kwargs):

    settings_confirmed_changes: bool = args['confirm']
    settings_limit_deletions: Optional[int] = args['limit']

    # Initialize report structure and writer
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

    report_writer = ReportWriter(headers=report_headers, sheet_name='unused_labels', filename_prefix='label-delete-unused', force_all_wrap_text=True, multivalues_cell_delimiter=',', args=args)

    # ReportWriter initialized from CLI args via constructor
    sheet = report_writer.sheet

    print("Fetching all Labels from the PCE... ", end='', flush=True)
    # pylo.log_set_debug()
    labels_json = connector.objects_label_get(max_results=199000, get_usage=True, async_mode=False)
    print("OK!")

    print(f"Analyzing {len(labels_json)} labels to find unused ones... ")
    unused_labels = find_unused_labels(labels_json)

    # Print debug info for each label
    for label_json in labels_json:
        usage_json = label_json.get('usage', {})
        if any(usage_json.values()):
            for usage_type, usage_confirmed in usage_json.items():
                if usage_confirmed:
                    print(f"Label '{label_json.get('value')}' is used in '{usage_type}', skipping deletion.")
                    break
        else:
            print(f"Label '{label_json.get('value')}' is unused, marking for deletion.")

    print()
    print(f"Found {len(unused_labels)} unused labels vs total of {len(labels_json)} labels.")

    if len(unused_labels) == 0:
        print("No unused labels found.")
    elif not settings_confirmed_changes:
        print("No change will be implemented in the PCE until you use the '--confirm' flag to confirm you're good with them after review.")
        for label_json in unused_labels:
            pce_url = build_pce_url(connector, label_json.get('href', ''))
            add_label_to_report(label_json, sheet, pce_url, "TO BE DELETED (no confirm option used)")
    else:
        # Apply deletion limit
        labels_to_delete, labels_ignored = apply_deletion_limit(unused_labels, settings_limit_deletions)

        # Add ignored labels to report
        for label_json in labels_ignored:
            pce_url = build_pce_url(connector, label_json.get('href', ''))
            add_label_to_report(label_json, sheet, pce_url, "ignored (limit reached)")

        # Execute deletion
        print()
        print(f"Proceeding to delete {len(labels_to_delete)} unused labels...")
        results = delete_labels_and_collect_results(labels_to_delete, connector)

        # Add results to report
        for label_json in results['successful']:
            pce_url = build_pce_url(connector, label_json.get('href', ''))
            add_label_to_report(label_json, sheet, pce_url, "deleted")
            print(f" - SUCCESS deleting label '{label_json.get('value')}'")

        for label_json in results['failed']:
            error = results['errors'][label_json.get('href', '')]
            pce_url = build_pce_url(connector, label_json.get('href', ''))
            add_label_to_report(label_json, sheet, pce_url, "API error", error)
            print(f" - ERROR deleting label '{label_json.get('value')}': {error}")

        print()
        print(f"Deletion completed: {len(results['successful'])} labels deleted successfully, {len(results['failed'])} errors encountered.")

    # Write report to disk (always generate report, even if empty)
    # JSON is generated from the populated sheet inside ReportWriter
    report_writer.write_reports(sort_by=['type', 'value'])


def add_label_to_report(label_json: LabelObjectJsonStructure, sheet: pylo.ArraysToExcel.Sheet,
                        pce_url: str, action: str, error_message: str = ''):
    """
    Add a label to the report sheet.

    Args:
        label_json: The label JSON structure from the API
        sheet: The Excel sheet to add the label to
        pce_url: Pre-built PCE URL for the label
        action: The action taken on the label
        error_message: Optional error message if deletion failed
    """
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
        'href': label_json.get('href', ''),
        'link_to_pce': pce_url
    }

    sheet.add_line_from_object(new_row)


command_object = Command(command_name, __main, fill_parser, skip_pce_config_loading=True, load_specific_objects_only=objects_load_filter)