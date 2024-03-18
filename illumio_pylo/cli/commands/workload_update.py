from typing import Dict, TypedDict, Optional, List, Union
import click
import argparse
import sys

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeader, ExcelHeaderSet
from .utils.LabelCreation import generate_list_of_labels_to_create, create_labels
from .utils.misc import make_filename_with_timestamp
from . import Command


command_name = 'workload-update'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--proceed-with-update', '-p', action='store_true',
                        help="If this flag is not set, the script will not make any changes to the PCE. Useful for dry-runs")
    parser.add_argument('--do-not-ask-for-confirmation', '-y', action='store_true',
                        help="If this flag is not set, the script will ask for confirmation before proceeding. Useful for automations")

    parser.add_argument('--input-file', '-i', type=str, required=True,
                        help='CSV or Excel input filename')
    parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')

    parser.add_argument('--label-type-header-prefix', type=str, required=False, default='label_',
                        help='Prefix for the label type headers in the CSV/Excel file')

    parser.add_argument('--output-dir', '-o', type=str, required=False, default='output',
                        help='Output directory for the report files')

    parser.add_argument('--match-on-hostname', action='store_true',
                        help="In order to be updated, a workload must match a HOSTNAME entry from the CSV file")
    parser.add_argument('--match-on-ip', action='store_true',
                        help="In order to be updated, a workload must match an IP entry from the CSV file")
    parser.add_argument('--match-on-href', action='store_true',
                        help="In order to be updated, a workload must match a HREF entry from the CSV file")

    parser.add_argument('--filter-unmanaged-only', '-fuo', action='store_true',
                        help="If set, only unmanaged workloads will be considered for relabeling")
    parser.add_argument('--filter-managed-only', '-fmo', action='store_true',
                        help="If set, only managed workloads will be considered for relabeling")

    parser.add_argument('--blank-labels-means-remove', action='store_true',
                        help="If a label is blank in the CSV, it will be considered as a request to remove the label from the Workload")

    parser.add_argument('--batch-size', type=int, required=False, default=500,
                        help='Number of Workloads to update per API call')

class ContextSingleton:

    def __init__(self, org: pylo.Organization):
        self.org: pylo.Organization = org
        self.csv_data: List[Dict[str, Union[str,bool,int, None]]] = []
        self.settings_label_type_header_prefix: str = ''
        self.settings_blank_labels_means_remove: bool = False
        self.csv_ip_index: Dict[str, Dict] = {}  # ip -> csv_data
        self.csv_name_index: Dict[str, Dict] = {}  # name -> csv_data
        self.csv_href_index: Dict[str, Dict] = {}  # href -> csv_data
        self.csv_report: Optional[pylo.ArraysToExcel] = None
        self.csv_report_sheet: Optional[pylo.ArraysToExcel.Sheet] = None
        self.ignored_workloads_count = 0
        self.stats_count_csv_entries_with_no_match = 0
        self.workloads_previous_labels: Dict[pylo.Workload,Dict[str, pylo.Label]] = {}
        self.csv_input_missing_label_types: List[str] = []

def __main(args, org: pylo.Organization, **kwargs):
    
    context = ContextSingleton(org=org)

    settings_input_file: str = args['input_file']
    settings_input_file_delimiter: str = args['input_file_delimiter']
    context.settings_label_type_header_prefix = args['label_type_header_prefix']
    settings_output_dir: str = args['output_dir']

    settings_batch_size = args['batch_size']
    settings_proceed_with_update = args['proceed_with_update']
    settings_do_not_ask_for_confirmation = args['do_not_ask_for_confirmation']
    context.settings_blank_labels_means_remove = not args['blank_labels_means_remove']

    settings_filter_unmanaged_only = args['filter_unmanaged_only']
    settings_filter_managed_only = args['filter_managed_only']

    if settings_filter_unmanaged_only and settings_filter_managed_only:
        pylo.log.error('You cannot use both --filter-unmanaged-only and --filter-managed-only at the same time')
        sys.exit(1)

    input_match_on_hostname = args['match_on_hostname']
    input_match_on_ip = args['match_on_ip']
    input_match_on_href = args['match_on_href']

    output_file_prefix = make_filename_with_timestamp('workload-update-results_', settings_output_dir)
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    context.ignored_workloads_count = 0


    csv_report_headers = ExcelHeaderSet(['name', 'hostname'])
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'{context.settings_label_type_header_prefix}{label_type}')
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'new_{label_type}')
    csv_report_headers.extend(['**updated**', '**reason**', 'href'])

    context.csv_report = csv_report = ArraysToExcel()
    context.csv_report_sheet = context.csv_report.create_sheet('Workloads Update Report', csv_report_headers)

    # <editor-fold desc="CSV input file data extraction">
    csv_expected_fields = [
        {'name': 'ip', 'optional': not input_match_on_ip},
        {'name': 'hostname', 'optional': not input_match_on_hostname},
        {'name': 'href', 'optional': not input_match_on_href}
    ]

    # Each label type is also an expected field
    for label_type in org.LabelStore.label_types:
        csv_expected_fields.append({'name': f'{context.settings_label_type_header_prefix}{label_type}', 'optional': True})


    print(" * Loading CSV input file '{}'...".format(settings_input_file), flush=True, end='')
    csv_input_object = pylo.CsvExcelToObject(settings_input_file, expected_headers=csv_expected_fields, csv_delimiter=settings_input_file_delimiter)
    for label_type in org.LabelStore.label_types:
        if f'{context.settings_label_type_header_prefix}{label_type}' not in csv_input_object.headers():
            context.csv_input_missing_label_types.append(label_type)

    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(csv_input_object.count_columns(), csv_input_object.count_lines()))
    # </editor-fold desc="CSV input file data extraction">


    if not input_match_on_ip and not input_match_on_hostname and not input_match_on_href:
        pylo.log.error('You must specify at least one (or several) property to match on for workloads vs input: href, ip or hostname')
        sys.exit(1)

    # <editor-fold desc="CSV input basic checks">
    check_and_sanitize_csv_input_data(context, input_match_on_hostname, input_match_on_href, input_match_on_ip)
    # </editor-fold desc="CSV input basic checks">

    # <editor-fold desc="Filter the list of Workloads to be edited">
    workloads_to_update: Dict[str, pylo.Workload] = org.WorkloadStore.itemsByHRef.copy() # start with a list of all workloads from  the PCE
    print(" * PCE has {} workloads. Now applying requested filters...".format(len(workloads_to_update)))

    context.ignored_workloads_count += filter_pce_workloads(context, workloads_to_update, settings_filter_managed_only,
                                                    settings_filter_unmanaged_only)

    print("  * DONE! {} Workloads remain to be updated".format(len(workloads_to_update)))
    # </editor-fold>

    # <editor-fold desc="Matching between CSV/Excel and Managed Workloads">
    workloads_to_update_match_csv: Dict[pylo.Workload, Dict]  # Workloads from the PCE and CSV data associated to it
    workloads_to_update_match_csv = match_pce_workloads_vs_csv(context,
                                                                input_match_on_hostname,
                                                                input_match_on_href,
                                                                input_match_on_ip,
                                                                workloads_to_update)
    add_unmatched_csv_lines_to_report(context, workloads_to_update_match_csv)
    # </editor-fold>


    # <editor-fold desc="List missing Labels and exclude Workloads which require no changes">
    print(" * Looking for any missing label which need to be created and Workloads which already have the right labels:")
    labels_to_be_created = generate_list_of_labels_to_create(workloads_to_update_match_csv.values(), org, context.settings_label_type_header_prefix)

    if len(labels_to_be_created) > 0:
        print(" * {} Labels need to created before Workloads can be imported, listing:".format(len(labels_to_be_created)), flush=True)
        create_labels(labels_to_be_created, org)
    # </editor-fold>

    # <editor-fold desc="Compare remaining workloads and CSV data to generate update payloads later">
    print(" * Comparing remaining {} Workloads and CSV data to generate update payloads later...".format(len(workloads_to_update)) , flush=True)
    compare_workloads_vs_csv_data_to_generate_changes(context, workloads_to_update, workloads_to_update_match_csv)


    print("  * DONE - {} Workloads remain to be updated".format(len(workloads_to_update_match_csv)))
    # </editor-fold desc="Compare remaining workloads and CSV data to generate update payloads later">


    # <editor-fold desc="Workloads updates Push to API">
    if len(workloads_to_update) == 0:
        print(" * No Workloads to update")
    else:
        workload_update_happened = False
        if settings_proceed_with_update:
            if not settings_do_not_ask_for_confirmation:
                if not click.confirm('Do you want to proceed with the Workloads update (count={})?'.format(len(workloads_to_update))):
                    print("   ** Aborted by user **")
                else:
                    workload_update_happened = True
                    print(" * Updating {} Workloads in batches of {}".format(len(workloads_to_update), settings_batch_size), flush=True)
                    batch_cursor = 0
                    total_created_count = 0
                    total_failed_count = 0

                    update_manager = pylo.WorkloadApiUpdateStackExecutionManager(org)
                    for workload in workloads_to_update.values():
                        update_manager.add_workload(workload)

                    update_manager.push_all(settings_batch_size)

                    for workload in workloads_to_update.values():
                        result = update_manager.get_result_for_workload(workload)
                        if result.successful is True:
                            context.csv_report_sheet.add_line_from_object(workload_to_csv_report(context, workload, True))
                            total_created_count += 1
                        else:
                            context.csv_report_sheet.add_line_from_object(workload_to_csv_report(context, workload, False, result.message))
                            total_failed_count += 1

                        batch_cursor += settings_batch_size
                    print("  * DONE - {} workloads labels updated with success, {} failures and {} ignored. A report was created in {} and {}".
                          format(total_created_count, total_failed_count, context.ignored_workloads_count, output_file_csv, output_file_excel))

                    context.csv_report_sheet.write_to_csv(output_file_csv)
                    context.csv_report.write_to_excel(output_file_excel)

        if not workload_update_happened:
            print("\n*************")
            print(" WARNING!!! --proceed option was not used or confirmation was not given no Workloads were relabeled and no Labels were created")
            print("- {} Managed Workloads were in the queue for relabeling".format(len(workloads_to_update)))
            print("- {} Managed Workloads were marked as Ignored (read reports for details)".format(context.csv_report_sheet.lines_count()))
            print("- {} Labels were found to be missing and to be created".format(len(labels_to_be_created)))
            print("*************")
            for workload in workloads_to_update.values():
                context.csv_report_sheet.add_line_from_object(workload_to_csv_report(context, workload, 'Potentially', reason='No confirmation was given to proceed with the update'))
                #new_labels = workloads_list_changed_labels_for_report[workload]))
    # </editor-fold>

    print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
    context.csv_report_sheet.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    context.csv_report.write_to_excel(output_file_excel)
    print("DONE")


def compare_workloads_vs_csv_data_to_generate_changes(context: ContextSingleton,workloads_to_update, workloads_to_update_match_csv):
    for workload, csv_data in workloads_to_update_match_csv.copy().items():
        workload.api_stacked_updates_start()
        if 'name' in csv_data:
            workload.api_update_forced_name(csv_data['name'])
        if 'hostname' in csv_data and workload.unmanaged:
            workload.api_update_hostname(csv_data['hostname'])

        found_labels = []
        # for each label type that doesn't have a column in CSV, we must include the original label
        for label_type in context.org.LabelStore.label_types:
            if label_type in context.csv_input_missing_label_types:
                current_workload_label = workload.get_label(label_type)
                if current_workload_label is not None:
                    found_labels.append(current_workload_label)

        for label_type in context.org.LabelStore.label_types:
            csv_label_column_name = f'{context.settings_label_type_header_prefix}{label_type}'
            if csv_label_column_name in csv_data:
                if csv_data[csv_label_column_name] is not None and len(csv_data[csv_label_column_name]) > 0:
                    found_label = context.org.LabelStore.find_label_by_name(csv_data[csv_label_column_name], label_type)
                    if found_label is None:
                        raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.
                                          format(csv_data[csv_label_column_name], csv_data['*line*']))
                    found_labels.append(found_label)


        context.workloads_previous_labels[workload] = workload.get_labels_dict()
        workload.api_update_labels(found_labels, missing_label_type_means_no_change=context.settings_blank_labels_means_remove)

        if workload.api_stacked_updates_count() == 0:
            del workloads_to_update_match_csv[workload]
            del workloads_to_update[workload.href]
            context.ignored_workloads_count += 1
            context.csv_report_sheet.add_line_from_object(
                workload_to_csv_report(context, workload, False, 'No changes were needed'))


def check_and_sanitize_csv_input_data(context, input_match_on_hostname, input_match_on_href, input_match_on_ip):
    print(" * Performing basic checks on CSV input:")
    csv_check_failed_count = 0
    if input_match_on_ip:
        for csv_data in context.csv_data:
            ips = csv_data['ip'].rsplit(',')
            csv_data['**ip_array**'] = []

            for ip in ips:
                ip = ip.strip(" \r\n")
                if not pylo.is_valid_ipv4(ip) and not pylo.is_valid_ipv6(ip):
                    print("   - ERROR: CSV line #{} has invalid IP address defined".format(csv_data['*line*']),
                          flush=True)
                    csv_check_failed_count += 1
                    continue

                csv_data['**ip_array**'].append(ip)

                if ip not in context.csv_ip_index:
                    context.csv_ip_index[ip] = csv_data
                    continue

                csv_check_failed_count += 1
                print("   - ERROR: CSV line #{} has a duplicate IP address with line #{}".format(csv_data['*line*'],
                                                                                                 context.csv_ip_index[
                                                                                                     ip]['*line*']),
                      flush=True)

            if len(csv_data['**ip_array**']) < 1:
                print("   - ERROR: CSV line #{} has no valid IP address defined".format(csv_data['*line*']), flush=True)
    if input_match_on_hostname:
        for csv_data in context.csv_data:
            name = csv_data['hostname']
            name = pylo.Workload.static_name_stripped_fqdn(name)
            if name is None or len(name) < 1:
                print("   - ERROR: CSV line #{} has invalid hostname defined: '{}'".format(csv_data['*line*'],
                                                                                           csv_data['hostname']),
                      flush=True)
                csv_check_failed_count += 1
                continue

            if name not in context.csv_name_index:
                context.csv_name_index[name.lower()] = csv_data
                continue

            print("   - ERROR: CSV line #{} has duplicate hostname defined from a previous line: '{}'".format(
                csv_data['*line*'], csv_data['hostname']), flush=True)
            csv_check_failed_count += 1
    if input_match_on_href:
        for csv_data in context.csv_data:
            href = csv_data['href']
            if href is None or len(href) < 1:
                print("   - ERROR: CSV line #{} has invalid href defined: '{}'".format(csv_data['*line*'],
                                                                                       csv_data['href']), flush=True)
                csv_check_failed_count += 1
                continue

            if href not in context.csv_href_index:
                context.csv_name_index[href] = csv_data
                continue

            print("   - ERROR: CSV line #{} has duplicate href defined from a previous line: '{}'".format(
                csv_data['*line*'], csv_data['href']), flush=True)
            csv_check_failed_count += 1
    if csv_check_failed_count > 0:
        pylo.log.error(
            "ERROR! Several ({}) inconsistencies were found in the CSV, please fix them before you continue!".format(
                csv_check_failed_count))
        sys.exit(1)
    print("   * Done")


def match_pce_workloads_vs_csv(context: ContextSingleton,
                               match_on_hostname, match_on_href, match_on_ip,
                               workloads_to_relabel) -> Dict[pylo.Workload, Dict]:

    print(" * Matching remaining Workloads with CSV/Excel input:")

    count = 0
    total_count = len(workloads_to_relabel)

    workloads_to_relabel_match: Dict[pylo.Workload, Dict] = {}  # Workloads and CSV data associated to it
    for workload_href in list(workloads_to_relabel.keys()):
        count += 1
        workload = workloads_to_relabel[workload_href]
        print("  - Workload #{}/{}  named '{}' href '{}' with {} IP addresses".format(count, total_count,
                                                                                      workload.get_name(),
                                                                                      workload.href,
                                                                                      len(workload.interfaces)))
        this_workload_matched_on_ip = None
        this_workload_matched_on_name = None
        this_workload_matched_on_href = None
        this_workload_matched = None  # CSV line match

        if match_on_ip:
            ip_matches = []
            for interface in workload.interfaces:
                print("    - ip {}...".format(interface.ip), flush=True, end='')
                csv_ip_record = context.csv_ip_index.get(interface.ip)
                if csv_ip_record is None:
                    print(" not found in CSV/Excel")
                else:
                    print("found")
                    ip_matches.append(csv_ip_record)
            if len(ip_matches) < 1:
                print("    - No matching IP address found in CSV/Excel, this Workload will not be relabeled")
                del workloads_to_relabel[workload_href]
                context.ignored_workloads_count += 1
                #context.csv_report.add_line_from_object(workload_to_csv_report(context, workload, False,
                #                                                       'No IP match was found in CSV/Excel input'))
                continue
            if len(ip_matches) > 1:
                print("    - Found more than 1 IP matches in CSV/Excel, this Workload will not be relabeled")
                del workloads_to_relabel[workload_href]
                context.ignored_workloads_count += 1
                #context.csv_report.add_line_from_object(workload_to_csv_report(context, workload, False,
                #                                                       'Too many IP matches were found in CSV/Excel input'))
                continue
            this_workload_matched_on_ip = ip_matches[0]
            this_workload_matched = this_workload_matched_on_ip

        if match_on_hostname:
            name_match = context.csv_name_index.get(workload.get_name_stripped_fqdn().lower())
            print("    - match on name '{}'...".format(workload.get_name_stripped_fqdn()), flush=True, end='')
            if name_match is None:
                del workloads_to_relabel[workload_href]
                print("  NOT FOUND")
                context.ignored_workloads_count += 1
                #context.csv_report.add_line_from_object(workload_to_csv_report(context, workload, False,
                #                                                       'No hostname match was found in CSV/Excel input'))
                continue

            print(" FOUND")
            this_workload_matched_on_name = name_match
            this_workload_matched = this_workload_matched_on_name

        if match_on_href:
            href_match = context.csv_name_index.get(workload.href)
            print("    - match on href '{}'...".format(workload.href), flush=True, end='')
            if href_match is None:
                del workloads_to_relabel[workload_href]
                print("  NOT FOUND")
                context.ignored_workloads_count += 1
                #context.csv_report.add_line_from_object(workload_to_csv_report(context, workload, False,
                #                                                       'No href match was found in CSV/Excel input'))
                continue

            print(" FOUND")
            this_workload_matched_on_href = href_match
            this_workload_matched = this_workload_matched_on_href

        if this_workload_matched is not None and \
                (not match_on_ip or match_on_ip and this_workload_matched['*line*'] ==
                 this_workload_matched_on_ip['*line*']) and \
                (not match_on_hostname or match_on_hostname and this_workload_matched['*line*'] ==
                 this_workload_matched_on_name['*line*']) and \
                (not match_on_href or match_on_href and this_workload_matched['*line*'] ==
                 this_workload_matched_on_href['*line*']):
            workloads_to_relabel_match[workload] = this_workload_matched
            print("    - all filters matched, it's in!")

    print("  * Done! After Filtering+CSV Match, {} workloads remain to be relabeled".format(len(workloads_to_relabel)))

    return workloads_to_relabel_match


def filter_pce_workloads(context: ContextSingleton, workloads_to_update: Dict[str, pylo.Workload],
                         filter_managed_only: bool, filter_unmanaged_only: bool) -> int:
    """
    Filter the list of workloads to be relabeled based on the settings
    :param context:
    :param filter_managed_only:
    :param filter_unmanaged_only:
    :param workloads_to_update:  Dict of workloads to be relabeled
    :return: the number of workloads that were ignored
    """
    ignored_workloads_count = 0

    if filter_unmanaged_only:
        print("   - Filtering out Managed Workloads...")
        for workload_href in list(workloads_to_update.keys()):
            workload = workloads_to_update[workload_href]
            if workload.unmanaged:
                del workloads_to_update[workload_href]
                ignored_workloads_count += 1
                context.csv_report_sheet.add_line_from_object(workload_to_csv_report(context, workload, False,
                                                                       'Managed Workload was filtered out'))
    if filter_managed_only:
        print("   - Filtering out Unmanaged Workloads...")
        for workload_href in list(workloads_to_update.keys()):
            workload = workloads_to_update[workload_href]
            if not workload.unmanaged:
                del workloads_to_update[workload_href]
                ignored_workloads_count += 1
                context.csv_report_sheet.add_line_from_object(workload_to_csv_report(context, workload, False,
                                                                       'Unmanaged Workload was filtered out'))

    print("  * DONE! {} Workloads were ignored".format(ignored_workloads_count))
    return ignored_workloads_count


def workload_to_csv_report(context: ContextSingleton, workload: pylo.Workload, updated: Union[bool,str],
                           reason: str = ''):

    record = {
        'name': workload.get_name(),
        'href': workload.href,
        '**updated**': str(updated),
        '**reason**':  reason
    }


    for label_type in context.org.LabelStore.label_types:
        previous_label = context.workloads_previous_labels[workload].get(label_type)
        record[f'{context.settings_label_type_header_prefix}{label_type}'] = previous_label.name if previous_label is not None else ''

    unchanged_str = '*unchanged*'

    for label_type in context.org.LabelStore.label_types:
        previous_label = context.workloads_previous_labels[workload].get(label_type)
        new_label = workload.get_label(label_type)
        if new_label is previous_label:
            record[f'new_{label_type}'] = unchanged_str
        else:
            record[f'new_{label_type}'] = new_label.name if new_label is not None else ''

    return record


command_object = Command(command_name, __main, fill_parser, objects_load_filter)

class ChangedLabelRecord(TypedDict):
    name: Optional[str]
    href: Optional[str]

ChangedLabelRecordCollection = Dict[pylo.Workload, Dict[str,ChangedLabelRecord]]


def add_unmatched_csv_lines_to_report(context: ContextSingleton,
                                      workloads_to_update_match_csv: Dict[pylo.Workload, Dict]):

    workloads_matched_csv_lines = workloads_to_update_match_csv.values()

    # find differences in keys
    for csv_data in context.csv_data:
        # '**line**' is the identifier of the line in the CSV
        csv_data_line = csv_data['*line*']
        found = False
        for workload_csv in workloads_matched_csv_lines:
            workload_csv_line = workload_csv.get('*line*')
            if workload_csv_line is None:
                raise pylo.PyloEx('Workload CSV data does not have a *line* key')
            if csv_data_line == workload_csv_line:
                found = True
                break

        if not found:
            context.stats_count_csv_entries_with_no_match += 1
            new_data = csv_data.copy()
            new_data['**updated**'] = str(False)
            new_data['**reason**'] = 'No matching Workload was found in the PCE'
            context.csv_report_sheet.add_line_from_object(new_data)


    print(" * {} CSV lines were not matched with any Workload. They are added to the report now".
          format(context.stats_count_csv_entries_with_no_match))
