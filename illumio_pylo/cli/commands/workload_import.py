from typing import Dict, List, Any, Union
from dataclasses import dataclass
import sys
import argparse

import click

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeaderSet, ExcelHeader
from .utils.LabelCreation import generate_list_of_labels_to_create, create_labels
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'workload-import'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--input-file', '-i', type=str, required=True,
                        help='CSV or Excel input filename')
    parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')

    parser.add_argument('--output-dir', '-o', type=str, required=False, default='output',
                        help='Directory where the output files will be saved')

    parser.add_argument('--ignore-missing-headers', action='store_true',
                        help='Ignore missing headers in the CSV/Excel file for label types')

    parser.add_argument('--label-type-header-prefix', type=str, required=False, default='label_',
                        help='Prefix for the label type headers in the CSV/Excel file')

    parser.add_argument('--ignore-hostname-collision', action='store_true',
                        help='If a workload with the same hostname exists in the PCE, ignore it and create the unmanaged workload anyway')

    parser.add_argument('--ignore-ip-collision', action='store_true',
                        help='If an IP address is already in use in the PCE or CSV, ignore it and create the unmanaged workload anyway')

    parser.add_argument('--ignore-all-sorts-collisions', action='store_true',
                        help='If names/hostnames/ips collisions are found ignore these CSV/Excel entries')

    parser.add_argument('--ignore-empty-ip-entries', action='store_true',
                        help="if a CSV entry has no IP address it will be ignored as per flag set.")

    parser.add_argument('--batch-size', type=int, required=False, default=500,
                        help='Number of Workloads to create per API call')

    parser.add_argument('--proceed-with-creation', '-p', action='store_true',
                        help='If set, the script will proceed with the creation of the workloads')

    parser.add_argument('--no-confirmation-required', '-n', action='store_true',
                        help='If set, the script will proceed with the creation of the workloads and labels without asking for confirmation')



def __main(args, org: pylo.Organization, **kwargs):
    input_file = args['input_file']
    input_file_delimiter: str = args['input_file_delimiter']
    settings_ignore_hostname_collision: bool = args['ignore_hostname_collision']
    settings_ignore_ip_collision: bool = args['ignore_ip_collision']
    settings_ignore_missing_headers: bool = args['ignore_missing_headers']
    settings_header_label_prefix: str = args['label_type_header_prefix']
    settings_ignore_all_sorts_collisions: bool = args['ignore_all_sorts_collisions']
    settings_ignore_empty_ip_entries: bool = args['ignore_empty_ip_entries']
    settings_proceed_with_creation: bool = args['proceed_with_creation']
    settings_no_confirmation_required: bool = args['no_confirmation_required']
    settings_output_dir: str = args['output_dir']

    batch_size = args['batch_size']

    output_file_prefix = make_filename_with_timestamp('import-umw-results_', settings_output_dir)
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    pylo.file_clean(output_file_csv)
    pylo.file_clean(output_file_excel)

    csv_expected_fields: List[Dict] = [
        {'name': 'name', 'optional': True, 'default': ''},
        {'name': 'hostname', 'optional': False},
        {'name': 'ip', 'optional': False},
        {'name': 'description', 'optional': True, 'default': ''}
    ]

    # each label type/dimension is optional
    for label_type in org.LabelStore.label_types:
        csv_expected_fields.append({'name': f"{settings_header_label_prefix}{label_type}"  , 'optional': True})


    csv_report_headers = ExcelHeaderSet(['name', 'hostname', 'ip', 'description'])
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'label_{label_type}')

    csv_report_headers.append(ExcelHeader(name='**not_created_reason**'))
    csv_report_headers.append(ExcelHeader(name='href', max_width=15))

    csv_report = ArraysToExcel()
    csv_sheet = csv_report.create_sheet('Workloads', csv_report_headers)


    print(" * Loading CSV input file '{}'...".format(input_file), flush=True, end='')
    csv_data = pylo.CsvExcelToObject(input_file, expected_headers=csv_expected_fields, csv_delimiter=input_file_delimiter)
    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(csv_data.count_columns(), csv_data.count_lines()))

    #check if CSV has all headers for each labels types
    if not settings_ignore_missing_headers:
        for label_type in org.LabelStore.label_types:
            header_name = f"{settings_header_label_prefix}{label_type}".lower()
            if header_name not in csv_data.headers():
                raise pylo.PyloEx(f"CSV/Excel file is missing the column '{header_name}' for label type '{label_type}'. "
                                  "If this was intended use --ignore-missing-headers flag")


    detect_workloads_name_collisions(csv_data, org, settings_ignore_all_sorts_collisions, settings_ignore_hostname_collision)

    detect_ip_collisions(csv_data, org, settings_ignore_all_sorts_collisions, settings_ignore_empty_ip_entries, settings_ignore_ip_collision)

    # We need to list of CSV lines which have not been marked with '**not_created_reason**' to create labels
    labels_to_be_created = generate_list_of_labels_to_create([csv_object for csv_object in csv_data.objects() if '**not_created_reason**' not in csv_object],
                                                             org, settings_header_label_prefix)

    # <editor-fold desc="Missing Labels creation">
    if len(labels_to_be_created) > 0:
        print(" * {} Labels need to created before Workloads can be imported, listing:".format(len(labels_to_be_created)))
        for label in labels_to_be_created:
            print("   - Label: {} (type={})".format(label.name, label.type))
        if not settings_no_confirmation_required:
            click.confirm("Do you want to proceed with the creation of these labels?", abort=True)

        create_labels(labels_to_be_created, org)
    # </editor-fold>

    # Listing objects to be created (filtering out inconsistent ones)
    csv_objects_to_create = []
    ignored_objects_count = 0
    for csv_object in csv_data.objects():
        if '**not_created_reason**' not in csv_object:
            csv_objects_to_create.append(csv_object)
        else:
            ignored_objects_count += 1

    # <editor-fold desc="JSON Payloads generation">
    umw_creator_manager = prepare_workload_creation_data(csv_objects_to_create, org, settings_header_label_prefix)
    # </editor-fold>

    # <editor-fold desc="Unmanaged Workloads PUSH to API">
    if umw_creator_manager.count_drafts() < 1:
        print(" * No Workloads to create, all were ignored due to collisions or missing data.")
        # still want to save the CSV/Excel files in the end so don't exit
    else:
        if not settings_proceed_with_creation is True:
            print(" * No workload will be created because the --proceed-with-creation/-p flag was not set. Yet report will be generated")
            for object_to_create in csv_objects_to_create:
                if '**not_created_reason**' not in object_to_create:
                    object_to_create['**not_created_reason**'] = '--proceed-with-creation/-p flag was not set'
        else:
            confirmed = settings_no_confirmation_required
            print(" * Creating {} Unmanaged Workloads in batches of {}".format(umw_creator_manager.count_drafts(), batch_size))
            if not settings_no_confirmation_required:
                confirmed = click.confirm("Do you want to proceed with the creation of these workloads?")

            if not confirmed:
                print(" * No Workloads will be created, user aborted the operation")
                for object_to_create in csv_objects_to_create:
                    if '**not_created_reason**' not in object_to_create:
                        object_to_create['**not_created_reason**'] = 'user aborted the operation'
            else:
                total_created_count = 0
                total_failed_count = 0

                results = umw_creator_manager.create_all_in_pce(amount_created_per_batch=batch_size, retrieve_workloads_after_creation=False)
                for result in results:
                    if result.success:
                        total_created_count += 1
                        result.external_tracker_id['href'] = result.workload_href
                    else:
                        total_failed_count += 1
                        result.external_tracker_id['**not_created_reason**'] = result.message

                print("  * DONE - {} created with success, {} failures and {} ignored.".format(
                    total_created_count, total_failed_count, ignored_objects_count))
    # </editor-fold>

    print()
    print(" * A CSV report was created in {}".format(output_file_csv))
    print(" * An Excel report was created in {}".format(output_file_excel))

    for data in csv_data.objects():
        csv_sheet.add_line_from_object(data)

    csv_sheet.write_to_csv(output_file_csv)
    csv_report.write_to_excel(output_file_excel)


def prepare_workload_creation_data(csv_objects_to_create, org: pylo.Organization, header_label_prefix: str) \
        -> pylo.WorkloadStore.UnmanagedWorkloadDraftMultiCreatorManager:

    umw_creator_manager = org.WorkloadStore.new_unmanaged_workload_multi_creator_manager()
    if len(csv_objects_to_create) > 0:
        print(' * Preparing Workloads JSON payloads...')
        for data in csv_objects_to_create:
            new_workload = umw_creator_manager.new_draft(external_tracker_id=data)

            if len(data['name']) > 0:
                new_workload.name = data['name']

            if len(data['hostname']) < 1:
                raise pylo.PyloEx('Workload at line #{} is missing a hostname in CSV'.format(data['*line*']))
            else:
                new_workload.hostname = data['hostname']

            for label_type in org.LabelStore.label_types:
                label_name = data[f"{header_label_prefix}{label_type}"]  # label_type look for label name in CSV record
                if label_name is not None and len(label_name) > 0:
                    found_label = org.LabelStore.find_label_by_name(label_name, label_type)
                    if found_label is not None:
                        new_workload.set_label(found_label)

            if data['description'] is not None and len(data['description']) > 0:
                new_workload.description = data['description']

            if len(data['**ip_array**']) < 1:
                pylo.log.error('CSV/Excel workload at line #{} has no valid ip address defined'.format(data['*line*']))
                sys.exit(1)

            for ip in data['**ip_array**']:
                new_workload.add_interface(ip)

        print("  * DONE")
    return umw_creator_manager


@dataclass
class WorkloadCollisionItem:
    managed: bool
    from_pce: bool = False
    workload_object: pylo.Workload = None
    csv_object: Dict[str, Any] = None

def detect_workloads_name_collisions(csv_data, org: pylo.Organization, ignore_all_sorts_collisions, ignore_hostname_collision):
    print(" * Checking for name/hostname collisions inside the PCE:", flush=True)
    name_cache: Dict[str, WorkloadCollisionItem] = {}
    for workload in org.WorkloadStore.itemsByHRef.values():
        lower_name = None
        if workload.forced_name is not None and len(workload.forced_name) > 0:
            lower_name = workload.forced_name.lower()
            if lower_name not in name_cache:
                name_cache[lower_name] = WorkloadCollisionItem(from_pce=True, workload_object=workload,
                                                               managed=not workload.unmanaged)
            else:
                pylo.log.warn("    - Warning duplicate found in the PCE for hostname/name: {}".format(workload.get_name()))
        if workload.hostname is not None and len(workload.hostname) > 0:
            lower_hostname = workload.hostname.lower()
            if lower_name != lower_hostname:
                if workload.hostname not in name_cache:
                    name_cache[workload.hostname] = WorkloadCollisionItem(from_pce=True, workload_object=workload,
                                                                          managed=not workload.unmanaged)
                else:
                    print("  - Warning duplicate found in the PCE for hostname/name: {}".format(workload.hostname))

    print(" * Checking for name/hostname collisions inside the CSV file:", flush=True)
    for csv_object in csv_data.objects():
        if '**not_created_reason**' in csv_object:
            continue

        lower_name = None
        if csv_object['name'] is not None and len(csv_object['name']) > 0:
            lower_name = csv_object['name'].lower()
            if lower_name not in name_cache:
                name_cache[lower_name] = WorkloadCollisionItem(from_pce=False, csv_object=csv_object, managed=False)
            else:
                if 'csv' in name_cache[lower_name]:
                    raise pylo.PyloEx('CSV contains workloads with duplicates name/hostname: {}'.format(lower_name))
                else:
                    if not ignore_all_sorts_collisions and not ignore_hostname_collision:
                        csv_object['**not_created_reason**'] = 'Found duplicated name/hostname in PCE'
                    else:
                        print(
                            "  - WARNING: CSV has an entry for workload name '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(
                            lower_name, csv_object['*line*']))

        if csv_object['hostname'] is not None and len(csv_object['hostname']) > 0:
            lower_hostname = csv_object['hostname'].lower()
            if lower_name != lower_hostname:
                if csv_object['hostname'] not in name_cache:
                    name_cache[csv_object['hostname']] = WorkloadCollisionItem(from_pce=False, csv_object=csv_object,
                                                                               managed=False)
                else:
                    if not name_cache[lower_hostname].from_pce:
                        raise pylo.PyloEx('CSV contains workloads with duplicates name/hostname: {}'.format(lower_name))
                    else:
                        if not ignore_all_sorts_collisions and not ignore_hostname_collision:
                            csv_object['**not_created_reason**'] = 'Found duplicated name/hostname in PCE'
                        else:
                            print(
                                "  - WARNING: CSV has an entry for workload hostname '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(
                                lower_hostname, csv_object['*line*']))
    print("  * DONE")


def detect_ip_collisions(csv_data, org: pylo.Organization, ignore_all_sorts_collisions, settings_ignore_empty_ip_entries, settings_ignore_ip_collision):
    print(" * Checking for IP addresses collisions:")
    indent = "    "
    ip_cache: Dict[str, WorkloadCollisionItem] = {}
    count_duplicate_ip_addresses_in_csv = 0
    for workload in org.WorkloadStore.itemsByHRef.values():
        for interface in workload.interfaces:
            if interface.ip not in ip_cache:
                ip_cache[interface.ip] = WorkloadCollisionItem(from_pce=True, workload_object=workload,
                                                               managed=not workload.unmanaged)
            else:
                pylo.log.warn(indent+"- Warning duplicate IPs found in the PCE between 2 workloads ({} and {}) for IP: {}".format(
                    workload.get_name(), ip_cache[interface.ip].workload_object.get_name(), interface.ip))

    for csv_object in csv_data.objects():
        if '**not_created_reason**' in csv_object:
            continue

        ips = csv_object['ip']
        if ips is not None:
            ips.strip(" \r\n")

        if ips is None or len(ips) == 0:
            if not settings_ignore_empty_ip_entries:
                pylo.log.error("CSV/Excel at line #{}: workload has empty IP address".format(csv_object['*line*']))
                sys.exit(1)
            else:
                csv_object['**not_created_reason**'] = "Empty IP address provided"
                continue

        ips = ips.rsplit(',')

        csv_object['**ip_array**'] = []

        for ip in ips:
            ip = ip.strip(" \r\n")

            if not pylo.is_valid_ipv4(ip) and not pylo.is_valid_ipv6(ip):
                pylo.log.error("CSV/Excel at line #{} contains invalid IP addresses: '{}'".format(csv_object['*line*'],
                                                                                                  csv_object['ip']))
                sys.exit(1)

            csv_object['**ip_array**'].append(ip)

            if ip not in ip_cache:
                ip_cache[ip] = WorkloadCollisionItem(from_pce=False, csv_object=csv_object, managed=False)
            else:
                count_duplicate_ip_addresses_in_csv += 1
                if not ignore_all_sorts_collisions and not settings_ignore_ip_collision:
                    pylo.log.warn(indent+"Duplicate IP address {} found in the PCE and CSV/Excel at line #{} (name={}  hostname={}). "
                          "(look for --options to bypass this if you know what you are doing)"
                          .format(ip, csv_object['*line*'], csv_object['name'], csv_object['hostname']))
                    csv_object['**not_created_reason**'] = "Duplicate IP address {} found in the PCE".format(ip)

    if ignore_all_sorts_collisions or settings_ignore_ip_collision:
        print(indent + "- Found {} colliding IP addresses from CSV/Excel, they will still be imported override flags were set.".format(
            count_duplicate_ip_addresses_in_csv))
    else:
        print(indent + "- Found {} colliding IP addresses from CSV/Excel, they won't be imported".format(
            count_duplicate_ip_addresses_in_csv))
    print("  * DONE")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)
