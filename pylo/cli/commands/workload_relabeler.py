import pylo
import argparse
import sys
import math
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'workload-relabeler'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--confirm', type=bool, nargs='?', required=False, default=False, const=True,
                        help="No change will be implemented in the PCE until you use this function to confirm you're good with them after review")

    parser.add_argument('--input-file', '-i', type=str, required=True,
                        help='CSV or Excel input filename')
    parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')

    parser.add_argument('--match-on-hostname', type=bool, nargs='?', required=False, default=False, const=True,
                        help="In order to be relabeled, a workload must match a HOSTNAME entry from the CSV file")
    parser.add_argument('--match-on-ip', type=bool, nargs='?', required=False, default=False, const=True,
                        help="In order to be relabeled, a workload must match an IP entry from the CSV file")
    parser.add_argument('--match-on-href', type=bool, nargs='?', required=False, default=False, const=True,
                        help="In order to be relabeled, a workload must match a HREF entry from the CSV file")

    parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                        help='Filter workloads by environment labels (separated by commas)')
    parser.add_argument('--filter-loc-label', type=str, required=False, default=None,
                        help='Filter workloads by environment labels (separated by commas)')
    parser.add_argument('--filter-app-label', type=str, required=False, default=None,
                        help='Filter workloads by role labels (separated by commas)')
    parser.add_argument('--filter-role-label', type=str, required=False, default=None,
                        help='Filter workloads by role labels (separated by commas)')

    parser.add_argument('--batch-size', type=int, required=False, default=500,
                        help='Number of Workloads to update per API call')


def __main(args, org: pylo.Organization):

    hostname = args['pce']
    input_file = args['input_file']
    input_file_delimiter = args['input_file_delimiter']
    batch_size = args['batch_size']
    confirmed_changes = args['confirm']

    input_match_on_hostname = args['match_on_hostname']
    input_match_on_ip = args['match_on_ip']
    input_match_on_href = args['match_on_href']

    output_file_prefix = make_filename_with_timestamp('ven-relabeler-results_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    ignored_workloads_count = 0

    csv_expected_fields = [
        {'name': 'role', 'optional': True},
        {'name': 'app', 'optional': True},
        {'name': 'env', 'optional': True},
        {'name': 'loc', 'optional': True},
        {'name': 'ip', 'optional': not input_match_on_ip},
        {'name': 'hostname', 'optional': not input_match_on_hostname},
        {'name': 'href', 'optional': not input_match_on_href}
    ]

    if not input_match_on_ip and not input_match_on_hostname and not input_match_on_href:
        pylo.log.error('You must specify at least one (or several) property to match on for workloads vs input: href, ip or hostname')
        sys.exit(1)

    csv_report = pylo.ArrayToExport(['name', 'role', 'app', 'env', 'loc', 'new_role', 'new_app', 'new_env', 'new_loc', '**updated**', '**reason**', 'href'])

    print(" * Loading CSV input file '{}'...".format(input_file), flush=True, end='')
    CsvData = pylo.CsvExcelToObject(input_file, expected_headers=csv_expected_fields, csv_delimiter=input_file_delimiter)
    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(CsvData.count_columns(), CsvData.count_lines()))
    # print(pylo.nice_json(CsvData._objects))

    def workload_to_csv_report(workload: pylo.Workload, updated: bool, reason: str = '', new_labels=None):

        labels = workload.get_labels_list()

        record = {
            'name': workload.get_name(),
            'href': workload.href,
            'role': labels[pylo.label_type_role],
            'app': labels[pylo.label_type_app],
            'env': labels[pylo.label_type_env],
            'loc': labels[pylo.label_type_loc],
            '**updated**': str(updated),
            '**reason**':  reason
        }

        unchanged_str = '*unchanged*'

        if new_labels is not None:
            if 'role' in new_labels:
                record['new_role'] = new_labels['role']['name']
            else:
                record['new_role'] = unchanged_str

            if 'app' in new_labels:
                record['new_app'] = new_labels['app']['name']
            else:
                record['new_app'] = unchanged_str

            if 'env' in new_labels:
                record['new_env'] = new_labels['env']['name']
            else:
                record['new_env'] = unchanged_str

            if 'loc' in new_labels:
                record['new_loc'] = new_labels['loc']['name']
            else:
                record['new_loc'] = unchanged_str

        else:
            record['new_role'] = '*unchanged*'
            record['new_app'] = '*unchanged*'
            record['new_env'] = '*unchanged*'
            record['new_loc'] = '*unchanged*'

        return record

    # <editor-fold desc="CSV basic checks">
    print(" * Performing basic checks on CSV input:")
    csv_ip_cache = {}
    csv_name_cache = {}
    csv_href_cache = {}
    csv_check_failed = 0

    if input_match_on_ip:
        for csv_object in CsvData.objects():
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
        for csv_object in CsvData.objects():
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
        for csv_object in CsvData.objects():
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
    workloads_to_relabel = org.WorkloadStore.get_managed_workloads_dict_href()
    print(" * PCE has {} managed workloads. Now applying requested filters:".format(len(workloads_to_relabel)))
    for workload_href in list(workloads_to_relabel.keys()):
        workload = workloads_to_relabel[workload_href]

        if len(role_label_list) > 0 and (workload.roleLabel is None or workload.roleLabel not in role_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Role label did not match filters'))
            continue

        if len(app_label_list) > 0 and (workload.applicationLabel is None or workload.applicationLabel not in app_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Application label did not match filters'))
            continue

        if len(env_label_list) > 0 and (workload.environmentLabel is None or workload.environmentLabel not in env_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Environment label did not match filters'))
            continue

        if len(loc_label_list) > 0 and (workload.locationLabel is None or workload.locationLabel not in loc_label_list):
            del workloads_to_relabel[workload_href]
            ignored_workloads_count += 1
            csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'Location label did not match filters'))
            continue

    print("  * DONE! {} Managed Workloads remain to be relabeled".format(len(workloads_to_relabel)))
    # </editor-fold>

    # <editor-fold desc="Matching between CSV/Excel and Managed Workloads">
    print(" * Matching remaining Managed Workloads with CSV/Excel input:")
    count = 0
    workloads_to_relabel_match = {}  # type: dict[pylo.Workload,dict]

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
                csv_report.add_line_from_object(workload_to_csv_report(workload, False, 'No name match was found in CSV/Excel input'))
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

            if csv_object[label_type] is not None and len(csv_object[label_type]) > 0:
                label_found = org.LabelStore.find_label_by_name_lowercase_and_type(csv_object[label_type], pylo.LabelStore.label_type_str_to_int(label_type))
                if label_found is None:
                    change_needed = True
                    temp_label_name = '**{}**{}'.format(label_type, csv_object[label_type].lower())
                    labels_to_be_created[temp_label_name] = {'name': csv_object[label_type], 'type': label_type}
                else:
                    if label_found is not workload.get_label_by_type_str(label_type):
                        change_needed = True
            else:
                if workload.get_label_by_type_str(label_type) is not None:
                    change_needed = True

            return change_needed

        workload_needs_label_change = process_label('role')
        workload_needs_label_change = process_label('app') or workload_needs_label_change
        workload_needs_label_change = process_label('env') or workload_needs_label_change
        workload_needs_label_change = process_label('loc') or workload_needs_label_change

        if not workload_needs_label_change:
            count_workloads_with_right_labels += 1
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

        if confirmed_changes:
            for label_to_create in labels_to_be_created.values():
                print("   - Pushing '{}' with type '{}' to the PCE... ".format(label_to_create['name'], label_to_create['type']), end='', flush=True)
                org.LabelStore.api_create_label(label_to_create['name'], label_to_create['type'])
                print("OK")
        else:
            for label_to_create in labels_to_be_created.values():
                org.LabelStore.create_label(label_to_create['name'], label_to_create['type'])

    # </editor-fold>

    # <editor-fold desc="JSON Payloads generation">
    print(' * Preparing Workloads JSON payloads...', flush=True)
    workloads_to_relabel_fixed_index = list(workloads_to_relabel_match.keys())
    workloads_list_changed_labels_for_report = {}
    workloads_json_data = []
    for workload in workloads_to_relabel_fixed_index:
        data = workloads_to_relabel_match[workload]
        new_workload = {'href': workload.href}
        workloads_json_data.append(new_workload)
        changed_labels = {}
        workloads_list_changed_labels_for_report[workload] = changed_labels
        new_workload['labels'] = []

        def process_label(label_type: str):
            if data[label_type] is not None and len(data[label_type]) > 0:
                # print(data)
                found_label = org.LabelStore.find_label_by_name_lowercase_and_type(data[label_type], pylo.LabelStore.label_type_str_to_int(label_type))
                if found_label is None:
                    raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.format(data[label_type], data['*line*']))
                workload_found_label = workload.get_label_by_type_str(label_type)
                if workload_found_label is not found_label:
                    new_workload['labels'].append({'href': found_label.href})
                    changed_labels[label_type] = {'name': found_label.name, 'href': found_label.href}
                else:
                    if workload_found_label is not None:
                        new_workload['labels'].append({'href': workload_found_label.href})

        process_label('role')
        process_label('app')
        process_label('env')
        process_label('loc')

    print("  * DONE")
    # </editor-fold>


    if confirmed_changes:
        # <editor-fold desc="Unmanaged Workloads PUSH to API">
        print(" * Updating {} Workloads in batches of {}".format(len(workloads_json_data), batch_size), flush=True)
        batch_cursor = 0
        total_created_count = 0
        total_failed_count = 0
        while batch_cursor <= len(workloads_json_data):
            print("  - batch #{} of {}".format(math.ceil(batch_cursor/batch_size)+1, math.ceil(len(workloads_json_data)/batch_size)), flush=True)
            batch_json_data = workloads_json_data[batch_cursor:batch_cursor+batch_size-1]
            results = connector.objects_workload_update_bulk(batch_json_data)
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
        # </editor-fold>
    else:
        print("\n*************")
        print(" WARNING!!! --confirm option was not used so no Workloads were relabeled and no Labels were created")
        print("- {} Managed Workloads were in the queue for relabeling".format(len(workloads_to_relabel)))
        print("- {} Managed Workloads were marked as Ignored (read reports for details)".format(csv_report.lines_count()))
        print("- {} Labels were found to be missing and to be created".format(len(labels_to_be_created)))
        print("*************")
        for workload in workloads_to_relabel_fixed_index:
            csv_report.add_line_from_object(workload_to_csv_report(workload, True, new_labels=workloads_list_changed_labels_for_report[workload]))

    print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
    csv_report.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    csv_report.write_to_excel(output_file_excel)
    print("DONE")


def run(options, org: pylo.Organization):
    print()
    print("**** {} UTILITY ****".format(command_name.upper()))
    __main(options, org)
    print("**** END OF {} UTILITY ****".format(command_name.upper()))


command_object = Command(command_name, run, fill_parser)