from typing import Dict, TypedDict, Optional

import click

import illumio_pylo as pylo
import argparse
import sys
import math
from .misc import make_filename_with_timestamp
from . import Command
from ...API.JsonPayloadTypes import WorkloadObjectJsonStructure, WorkloadObjectCreateJsonStructure, \
    WorkloadObjectMultiCreateJsonRequestPayload, WorkloadObjectMultiCreateJsonStructure

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

    parser.add_argument('--match-on-hostname', action='store_true',
                        help="In order to be relabeled, a workload must match a HOSTNAME entry from the CSV file")
    parser.add_argument('--match-on-ip', action='store_true',
                        help="In order to be relabeled, a workload must match an IP entry from the CSV file")
    parser.add_argument('--match-on-href', action='store_true',
                        help="In order to be relabeled, a workload must match a HREF entry from the CSV file")

    parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                        help='Filter workloads by environment labels (separated by commas)')
    parser.add_argument('--filter-loc-label', type=str, required=False, default=None,
                        help='Filter workloads by environment labels (separated by commas)')
    parser.add_argument('--filter-app-label', type=str, required=False, default=None,
                        help='Filter workloads by role labels (separated by commas)')
    parser.add_argument('--filter-role-label', type=str, required=False, default=None,
                        help='Filter workloads by role labels (separated by commas)')

    parser.add_argument('--blank-labels-means-remove', action='store_true',
                        help="If a label is blank in the CSV, it will be considered as a request to remove the label from the Workload")

    parser.add_argument('--batch-size', type=int, required=False, default=500,
                        help='Number of Workloads to update per API call')


def __main(args, org: pylo.Organization, **kwargs):

    input_file = args['input_file']
    input_file_delimiter = args['input_file_delimiter']
    batch_size = args['batch_size']
    settings_proceed_with_update = args['proceed_with_update']
    settings_do_not_ask_for_confirmation = args['do_not_ask_for_confirmation']
    settings_blank_labels_means_remove = args['blank_labels_means_remove']

    input_match_on_hostname = args['match_on_hostname']
    input_match_on_ip = args['match_on_ip']
    input_match_on_href = args['match_on_href']

    output_file_prefix = make_filename_with_timestamp('workload-relabeler-results_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    ignored_workloads_count = 0

    csv_expected_fields = [
        {'name': 'ip', 'optional': not input_match_on_ip},
        {'name': 'hostname', 'optional': not input_match_on_hostname},
        {'name': 'href', 'optional': not input_match_on_href}
    ]

    # Each label type is also an expected field
    for label_type in org.LabelStore.label_types:
        csv_expected_fields.append({'name': f'label_{label_type}', 'optional': True})

    if not input_match_on_ip and not input_match_on_hostname and not input_match_on_href:
        pylo.log.error('You must specify at least one (or several) property to match on for workloads vs input: href, ip or hostname')
        sys.exit(1)


    csv_report_headers = ['hostname']
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'label_{label_type}')
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'new_{label_type}')
    csv_report_headers.extend(['**updated**', '**reason**', 'href'])

    csv_report = pylo.ArrayToExport(csv_report_headers)

    print(" * Loading CSV input file '{}'...".format(input_file), flush=True, end='')
    csv_data = pylo.CsvExcelToObject(input_file, expected_headers=csv_expected_fields, csv_delimiter=input_file_delimiter)
    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(csv_data.count_columns(), csv_data.count_lines()))
    # print(pylo.nice_json(CsvData._objects))

    def workload_to_csv_report(workload: pylo.Workload, updated: bool, reason: str = '', new_labels=None):

        labels = workload.get_labels_str_list()

        record = {
            'name': workload.get_name(),
            'href': workload.href,
            '**updated**': str(updated),
            '**reason**':  reason
        }

        for label_type in org.LabelStore.label_types:
            current_label = workload.get_label(label_type)
            record[f'label_{label_type}'] = current_label.name if current_label is not None else ''

        unchanged_str = '*unchanged*'

        if new_labels is not None:
            for label_type in org.LabelStore.label_types:
                if label_type in new_labels:
                    record[f'new_{label_type}'] = new_labels[label_type]['name']
                else:
                    record[f'new_{label_type}'] = unchanged_str

        else:
            for label_type in org.LabelStore.label_types:
                record[f'new_{label_type}'] = unchanged_str

        return record

    # <editor-fold desc="CSV basic checks">
    print(" * Performing basic checks on CSV input:")
    csv_ip_cache = {}
    csv_name_cache = {}
    csv_href_cache = {}
    csv_check_failed = 0

    if input_match_on_ip:
        for csv_object in csv_data.objects():
            ips = csv_object['ip'].rsplit(',')
            csv_object['**ip_array**'] = []

            for ip in ips:
                ip = ip.strip(" \r\n")
                if not pylo.is_valid_ipv4(ip) and not pylo.is_valid_ipv6(ip):
                    print("   - ERROR: CSV line #{} has invalid IP address defined".format(csv_object['*line*']), flush=True)
                    csv_check_failed += 1
                    continue

                csv_object['**ip_array**'].append(ip)

                if ip not in csv_ip_cache:
                    csv_ip_cache[ip] = csv_object
                    continue

                csv_check_failed += 1
                print("   - ERROR: CSV line #{} has a duplicate IP address with line #{}".format(csv_object['*line*'], csv_ip_cache[ip]['*line*']), flush=True)

            if len(csv_object['**ip_array**']) < 1:
                print("   - ERROR: CSV line #{} has no valid IP address defined".format(csv_object['*line*']), flush=True)

    if input_match_on_hostname:
        for csv_object in csv_data.objects():
            name = csv_object['hostname']
            name = pylo.Workload.static_name_stripped_fqdn(name)
            if name is None or len(name) < 1:
                print("   - ERROR: CSV line #{} has invalid hostname defined: '{}'".format(csv_object['*line*'], csv_object['hostname']), flush=True)
                csv_check_failed += 1
                continue

            if name not in csv_name_cache:
                csv_name_cache[name.lower()] = csv_object
                continue

            print("   - ERROR: CSV line #{} has duplicate hostname defined from a previous line: '{}'".format(csv_object['*line*'], csv_object['hostname']), flush=True)
            csv_check_failed += 1

    if input_match_on_href:
        for csv_object in csv_data.objects():
            href = csv_object['href']
            if href is None or len(href) < 1:
                print("   - ERROR: CSV line #{} has invalid href defined: '{}'".format(csv_object['*line*'], csv_object['href']), flush=True)
                csv_check_failed += 1
                continue

            if href not in csv_href_cache:
                csv_name_cache[href] = csv_object
                continue

            print("   - ERROR: CSV line #{} has duplicate href defined from a previous line: '{}'".format(csv_object['*line*'], csv_object['href']), flush=True)
            csv_check_failed += 1

    if csv_check_failed > 0:
        pylo.log.error("ERROR! Several ({}) inconsistencies were found in the CSV, please fix them before you continue!".format(csv_check_failed))
        sys.exit(1)

    print("   * Done")
    # </editor-fold>

    # <editor-fold desc="Parsing Label filters">
    print(" * Parsing filters")
    env_label_list = {}
    if args['filter_env_label'] is not None:
        print("   * Environment Labels specified")
        for raw_label_name in args['filter_env_label'].split(','):
            print("     - label named '{}'".format(raw_label_name), end='', flush=True)
            label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_env)
            if label is None:
                print("NOT FOUND!")
                raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
            else:
                print(" found")
                env_label_list[label] = label

    loc_label_list = {}
    if args['filter_loc_label'] is not None:
        print("   * Location Labels specified")
        for raw_label_name in args['filter_loc_label'].split(','):
            print("     - label named '{}' ".format(raw_label_name), end='', flush=True)
            label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_loc)
            if label is None:
                print("NOT FOUND!")
                raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
            else:
                print(" found")
                loc_label_list[label] = label

    app_label_list = {}
    if args['filter_app_label'] is not None:
        print("   * Application Labels specified")
        for raw_label_name in args['filter_app_label'].split(','):
            print("     - label named '{}' ".format(raw_label_name), end='', flush=True)
            label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_app)
            if label is None:
                print(" NOT FOUND!")
                raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
            else:
                print(" found")
                app_label_list[label] = label

    role_label_list = {}
    if args['filter_role_label'] is not None:
        print("   * Role Labels specified")
        for raw_label_name in args['filter_role_label'].split(','):
            print("     - label named '{}' ".format(raw_label_name), end='', flush=True)
            label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_role)
            if label is None:
                print("NOT FOUND!")
                raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
            else:
                print("found")
                role_label_list[label] = label
    print("  * DONE")
    # </editor-fold>

    # <editor-fold desc="Filter the list of VENs to be relabeled">
    workloads_to_relabel = org.WorkloadStore.itemsByHRef.copy()
    print(" * PCE has {} managed workloads. Now applying requested filters:".format(len(workloads_to_relabel)))
    for workload_href in list(workloads_to_relabel.keys()):
        workload = workloads_to_relabel[workload_href]

        if len(role_label_list) > 0 and (workload.role_label is None or workload.role_label not in role_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Role label did not match filters'))
            continue

        if len(app_label_list) > 0 and (workload.app_label is None or workload.app_label not in app_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Application label did not match filters'))
            continue

        if len(env_label_list) > 0 and (workload.env_label is None or workload.env_label not in env_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Environment label did not match filters'))
            continue

        if len(loc_label_list) > 0 and (workload.loc_label is None or workload.loc_label not in loc_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Location label did not match filters'))
            continue

    print("  * DONE! {} Managed Workloads remain to be relabeled".format(len(workloads_to_relabel)))
    # </editor-fold>

    # <editor-fold desc="Matching between CSV/Excel and Managed Workloads">
    print(" * Matching remaining Managed Workloads with CSV/Excel input:")
    count = 0
    workloads_to_relabel_match: Dict[pylo.Workload,Dict] = {}  # Workloads and CSV data associated to it

    for workload_href in list(workloads_to_relabel.keys()):
        count += 1
        workload = workloads_to_relabel[workload_href]
        print("  - Workload #{}/{}  named '{}' href '{}' with {} IP addresses".format(count, len(workloads_to_relabel), workload.get_name(), workload.href, len(workload.interfaces)))
        this_workload_matched_on_ip = None
        this_workload_matched_on_name = None
        this_workload_matched_on_href = None
        this_workload_matched = None

        if input_match_on_ip:
            ip_matches = []
            for interface in workload.interfaces:
                print("    - ip {}...".format(interface.ip), flush=True, end='')
                csv_ip_record = csv_ip_cache.get(interface.ip)
                if csv_ip_record is None:
                    print(" not found in CSV/Excel")
                else:
                    print("found")
                    ip_matches.append(csv_ip_record)
            if len(ip_matches) < 1:
                print("    - No matching IP address found in CSV/Excel, this Workload will not be relabeled")
                del workloads_to_relabel[workload_href]
                ignored_workloads_count += 1
                csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'No IP match was found in CSV/Excel input'))
                continue
            if len(ip_matches) > 1:
                print("    - Found more than 1 IP matches in CSV/Excel, this Workload will not be relabeled")
                del workloads_to_relabel[workload_href]
                ignored_workloads_count += 1
                csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Too many IP matches were found in CSV/Excel input'))
                continue
            this_workload_matched_on_ip = ip_matches[0]
            this_workload_matched = this_workload_matched_on_ip

        if input_match_on_hostname:
            name_match = csv_name_cache.get(workload.get_name_stripped_fqdn().lower())
            print("    - match on name '{}'...".format(workload.get_name_stripped_fqdn()), flush=True, end='')
            if name_match is None:
                del workloads_to_relabel[workload_href]
                print("  NOT FOUND")
                ignored_workloads_count += 1
                csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'No hostname match was found in CSV/Excel input'))
                continue

            print(" FOUND")
            this_workload_matched_on_name = name_match
            this_workload_matched = this_workload_matched_on_name

        if input_match_on_href:
            href_match = csv_name_cache.get(workload.href)
            print("    - match on href '{}'...".format(workload.href), flush=True, end='')
            if href_match is None:
                del workloads_to_relabel[workload_href]
                print("  NOT FOUND")
                ignored_workloads_count += 1
                csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'No href match was found in CSV/Excel input'))
                continue

            print(" FOUND")
            this_workload_matched_on_href = href_match
            this_workload_matched = this_workload_matched_on_href


        if this_workload_matched is not None and\
                (not input_match_on_ip or input_match_on_ip and this_workload_matched['*line*'] == this_workload_matched_on_ip['*line*']) and\
                (not input_match_on_hostname or input_match_on_hostname and this_workload_matched['*line*'] == this_workload_matched_on_name['*line*']) and \
                (not input_match_on_href or input_match_on_href and this_workload_matched['*line*'] == this_workload_matched_on_href['*line*']):
            workloads_to_relabel_match[workload] = this_workload_matched
            print("    - all filters matched, it's in!")

    print("  * Done! After Filtering+CSV Match, {} workloads remain to be relabeled".format(len(workloads_to_relabel)))
    # </editor-fold>

    # <editor-fold desc="List missing Labels and Workloads which already have the right labels">
    print(" * Looking for any missing label which need to be created and Workloads which already have the right labels:")
    labels_to_be_created = {}
    count_workloads_with_right_labels = 0
    for workload in list(workloads_to_relabel.values()):

        workload_needs_label_change = False

        csv_object = workloads_to_relabel_match[workload]

        def process_label(label_type: str) -> bool:
            change_needed = False

            if csv_object[f'label_{label_type}'] is not None and len(csv_object[f'label_{label_type}']) > 0:
                new_label_name = f'label_{label_type}'
                # look for CSV record's label in the PCE
                label_found = org.LabelStore.find_label_by_name(csv_object[new_label_name], label_type)
                if label_found is None:  # label not found in the PCE
                    change_needed = True
                    temp_label_name = '**{}**{}'.format(label_type, csv_object[new_label_name].lower())
                    # add said label name to be added to the list of labels to be created
                    labels_to_be_created[temp_label_name] = {'name': csv_object[new_label_name], 'type': label_type}
                else:
                    if label_found is not workload.get_label(label_type):
                        change_needed = True
            else:
                if workload.get_label(label_type) is not None:
                    change_needed = True

            return change_needed

        workload_needs_label_change = False
        for label_type in org.LabelStore.label_types:
            workload_needs_label_change = process_label(label_type) or workload_needs_label_change

        if not workload_needs_label_change:
            count_workloads_with_right_labels += 1
            # delete from the list of workloads to be relabeled and their entry in the match list
            del workloads_to_relabel[workload.href]
            del workloads_to_relabel_match[workload]
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Workload already has the right Labels'))
    print()
    print("  * DONE! Found {} missing labels to be created and {} Workloads which need an update".format(len(labels_to_be_created), len(workloads_to_relabel)), flush=True)
    # </editor-fold>

    # <editor-fold desc="Missing Labels creation">
    if len(labels_to_be_created) > 0:
        print(" * {} Labels need to created before Workloads can be imported, listing:".format(len(labels_to_be_created)), flush=True)
        for label_to_create in labels_to_be_created.values():
            print("   - '{}' type {}".format(label_to_create['name'], label_to_create['type']))

        if settings_proceed_with_update:
            if not settings_do_not_ask_for_confirmation:
                if not click.confirm('Do you want to proceed with the creation of the missing labels?'):
                    print("   - Aborted by user")
                    sys.exit(0)
            for label_to_create in labels_to_be_created.values():
                print("   - Pushing '{}' with type '{}' to the PCE... ".format(label_to_create['name'], label_to_create['type']), end='', flush=True)
                org.LabelStore.api_create_label(label_to_create['name'], label_to_create['type'])
                print("OK")
        else:
            # this is only useful to allow the script to continue despite the fact that no changes will be made
            for label_to_create in labels_to_be_created.values():
                org.LabelStore.create_label(label_to_create['name'], label_to_create['type'])

    # </editor-fold>

    # <editor-fold desc="JSON Payloads generation">
    workloads_json_data: WorkloadObjectMultiCreateJsonRequestPayload = []

    if len(workloads_to_relabel) > 0:
        print(' * Preparing Workloads JSON payloads...', flush=True)
    workloads_to_relabel_fixed_index = list(workloads_to_relabel_match.keys())
    workloads_list_changed_labels_for_report: ChangedLabelRecordCollection = {}
    for workload in workloads_to_relabel_fixed_index:
        workload_csv_data = workloads_to_relabel_match[workload]
        new_workload: WorkloadObjectMultiCreateJsonStructure = {'href': workload.href} # future payload for the API
        workloads_json_data.append(new_workload)
        changed_labels: Dict[str,ChangedLabelRecord] = {}
        workloads_list_changed_labels_for_report[workload] = changed_labels
        new_workload['labels'] = []

        def generate_json_payload_for_label_type(label_type: str):
            csv_label_name = f'label_{label_type}'
            if csv_label_name not in workload_csv_data:
                # label not in the CSV, we shall not touch it so original one must be included in the payload
                current_workload_label = workload.get_label(label_type)
                if current_workload_label is not None:
                    new_workload['labels'].append({'href': current_workload_label.href})
            elif workload_csv_data[csv_label_name] is not None and len(workload_csv_data[csv_label_name]) > 0:
                found_label = org.LabelStore.find_label_by_name(workload_csv_data[csv_label_name], label_type)
                if found_label is None:
                    raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.
                                      format(workload_csv_data[csv_label_name], workload_csv_data['*line*']))
                workload_found_label = workload.get_label(label_type)
                if workload_found_label is not found_label:
                    new_workload['labels'].append({'href': found_label.href})
                    changed_labels[label_type] = {'name': found_label.name, 'href': found_label.href}
                else:
                    if workload_found_label is not None:
                        new_workload['labels'].append({'href': workload_found_label.href})
            else:
                # current label for this type is blank in the CSV
                if settings_blank_labels_means_remove:
                    # blank label in the CSV means we want to remove the label from the workload. Nothing to do then.
                    pass
                else: # we shall not touch it so original one must be included in the payload
                    current_workload_label = workload.get_label(label_type)
                    if current_workload_label is not None:
                        new_workload['labels'].append({'href': current_workload_label.href})

        for label_type in org.LabelStore.label_types:
            generate_json_payload_for_label_type(label_type)

        print("  * DONE")
    # </editor-fold>

    # <editor-fold desc="Unmanaged Workloads PUSH to API">
    if len(workloads_to_relabel) == 0:
        print(" * No Workloads to update")
    else:
        workload_update_happened = False
        if settings_proceed_with_update:
            if not settings_do_not_ask_for_confirmation:
                if not click.confirm('Do you want to proceed with the Workloads update (count={})?'.format(len(workloads_to_relabel))):
                    print("   ** Aborted by user **")
                else:
                    workload_update_happened = True
                    print(" * Updating {} Workloads in batches of {}".format(len(workloads_json_data), batch_size), flush=True)
                    batch_cursor = 0
                    total_created_count = 0
                    total_failed_count = 0
                    while batch_cursor <= len(workloads_json_data):
                        print("  - batch #{} of {}".format(math.ceil(batch_cursor/batch_size)+1,
                                                           math.ceil(len(workloads_json_data)/batch_size)), flush=True)
                        batch_json_data = workloads_json_data[batch_cursor:batch_cursor+batch_size-1]
                        results = org.connector.objects_workload_update_bulk(batch_json_data)
                        created_count = 0
                        failed_count = 0

                        # print(results)
                        for i in range(0, batch_size):
                            if i >= len(batch_json_data):
                                break

                            workload = workloads_to_relabel_fixed_index[i + batch_cursor]
                            result = results[i]
                            if result['status'] != 'updated':
                                csv_report.add_line_from_object(workload_to_csv_report(workload, False, result['message']))
                                failed_count += 1
                                total_failed_count += 1
                            else:
                                csv_report.add_line_from_object(workload_to_csv_report(workload, True, new_labels=workloads_list_changed_labels_for_report[workload]))
                                created_count += 1
                                total_created_count += 1

                        print("    - {} updated with success, {} failures (read report to get reasons)".format(created_count, failed_count))
                        csv_report.write_to_csv(output_file_csv)
                        csv_report.write_to_excel(output_file_excel)

                        batch_cursor += batch_size
                    print("  * DONE - {} workloads labels updated with success, {} failures and {} ignored. A report was created in {} and {}".format(total_created_count, total_failed_count, ignored_workloads_count, output_file_csv, output_file_excel))

        if not workload_update_happened:
            print("\n*************")
            print(" WARNING!!! --proceed option was not used or confirmation was not given no Workloads were relabeled and no Labels were created")
            print("- {} Managed Workloads were in the queue for relabeling".format(len(workloads_to_relabel)))
            print("- {} Managed Workloads were marked as Ignored (read reports for details)".format(csv_report.lines_count()))
            print("- {} Labels were found to be missing and to be created".format(len(labels_to_be_created)))
            print("*************")
            for workload in workloads_to_relabel_fixed_index:
                csv_report.add_line_from_object(workload_to_csv_report(workload, True, new_labels=workloads_list_changed_labels_for_report[workload]))
    # </editor-fold>

    print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
    csv_report.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    csv_report.write_to_excel(output_file_excel)
    print("DONE")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)


class ChangedLabelRecord(TypedDict):
    name: Optional[str]
    href: Optional[str]

ChangedLabelRecordCollection = Dict[pylo.Workload, Dict[str,ChangedLabelRecord]]
