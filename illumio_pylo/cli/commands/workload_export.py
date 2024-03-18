from typing import List, Dict
import argparse
import sys
from datetime import datetime

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeader, ExcelHeaderSet
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'workload-export'


class ExtraColumn:
    class ColumnDescription:
        def __init__(self, name: str, nice_name: str):
            self.name = name
            self.nice_name = nice_name

    def __init__(self):
        extra_columns.append(self)

    def column_description(self) -> ColumnDescription:
        raise NotImplementedError()

    def get_value(self, workload: pylo.Workload, org: pylo.Organization) -> str:
        raise NotImplementedError()

    def apply_cli_args(self, parser: argparse.ArgumentParser):
        raise NotImplementedError()

    def post_process_cli_args(self, args: Dict[str, any], org: pylo.Organization):
        raise NotImplementedError()


extra_columns: List[ExtraColumn] = []


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--output-dir', required=False, default='output')

    parser.add_argument('--verbose', '-v', action='store_true',
                        help='')

    parser.add_argument('--filter-file', '-i', type=str, required=False, default=None,
                        help='CSV or Excel input filename')
    parser.add_argument('--filter-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')
    parser.add_argument('--filter-fields', type=str, required=False, default=None, choices=['hostname', 'app', 'ip'], nargs="+",
                        help='Fields on which you want to filter on')
    parser.add_argument('--keep-filters-in-report', action='store_true',
                        help='If you want to keep filters information in the export file (to do a table joint for example)')
    parser.add_argument('--save-location', type=str, required=False, default='./',
                        help='The folder where this script will save generated Excel report')
    parser.add_argument('--csv-output-only', action='store_true',
                        help='Generate only the CSV output file, no Excel file')
    parser.add_argument('--excel-output-only', action='store_true',
                        help='Generate only the Excel output file, no CSV file')

    for extra_column in extra_columns:
        extra_column.apply_cli_args(parser)

def __main(args, org: pylo.Organization, **kwargs):

    filter_file = args['filter_file']
    filter_file_delimiter = args['filter_file_delimiter']
    filter_fields = args['filter_fields']
    filter_keep_in_report = args['keep_filters_in_report']
    settings_output_dir = args['output_dir']
    verbose = args['verbose']
    csv_output_only = args['csv_output_only']
    excel_output_only = args['excel_output_only']
    # print(args['filter_fields'])

    output_file_prefix = make_filename_with_timestamp('workload_export_', output_directory=settings_output_dir)
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_report_headers = ExcelHeaderSet(['name', 'hostname'])
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'label_{label_type}')

    csv_report_headers.extend([
        'online', 'managed', 'status', 'agent.last_heartbeat',
        'agent.sec_policy_sync_state', 'agent.sec_policy_applied_at',
        ExcelHeader(name='link_to_pce', wrap_text=False, url_text='See in PCE', is_url=True),
        'href', 'agent.href'])

    for extra_column in extra_columns:
        csv_report_headers.append(extra_column.column_description().name)
        print(" - adding extra column from external plugin: " + extra_column.column_description().name)

    filter_csv_expected_fields = []
    filter_data = None

    if filter_file is not None:
        if filter_fields is None:
            pylo.log.error("A filter file was provided but you didn't specify on which fields they should apply")
            sys.exit(1)
        if len(filter_fields) < 1:
            pylo.log.error("A filter file was provided but you specified an empty filter-fields option")
            sys.exit(1)
        for field in filter_fields:
            filter_csv_expected_fields.append({'name': field, 'optional': False})

        print(" * Loading filterCSV input file '{}'...".format(filter_file), flush=True, end='')
        filter_data = pylo.CsvExcelToObject(filter_file, expected_headers=filter_csv_expected_fields, csv_delimiter=filter_file_delimiter)
        print('OK')
        print("   - CSV has {} columns and {} lines (headers don't count)".format(filter_data.count_columns(), filter_data.count_lines()))

    if filter_keep_in_report:
        for field in filter_data._detected_headers:
            csv_report_headers.append('_' + field)

    csv_report = ArraysToExcel()
    csv_sheet = csv_report.create_sheet('workloads', csv_report_headers, force_all_wrap_text=True)

    all_workloads = org.WorkloadStore.itemsByHRef.copy()
    used_filters = {}

    def add_workload_to_report(wkl: pylo.Workload = None, filter=None, filter_append_prefix='_'):
        labels = workload.get_labels_str_list()

        def none_or_date(date):
            if date is None:
                return None
            return datetime.strftime(date, '%Y-%m-%d %H:%M:%S')

        if wkl is not None:
            new_row = {
                'name': wkl.forced_name,
                'hostname': wkl.hostname,
                'href': wkl.href,
                'online': wkl.online,
                'managed': not wkl.unmanaged,
                'status': wkl.get_status_string(),
                'link_to_pce': wkl.href,
            }
            for label_type in org.LabelStore.label_types:
                new_row[f'label_{label_type}'] = wkl.get_label_name(label_type)

            if wkl.ven_agent is not None:
                new_row['agent.href'] = wkl.ven_agent.href
                new_row['agent.sec_policy_sync_state'] = wkl.ven_agent.get_status_security_policy_sync_state()
                new_row['agent.last_heartbeat'] = none_or_date(wkl.ven_agent.get_last_heartbeat_date())
                new_row['agent.sec_policy_applied_at'] = none_or_date(wkl.ven_agent.get_status_security_policy_applied_at())

            for extra_column in extra_columns:
                new_row[extra_column.column_description().name] = extra_column.get_value(wkl, org)
        else:
            new_row = {}

        if filter is not None:
            used_filters[filter['*line*']] = True
            for field in filter:
                new_row[filter_append_prefix + field] = filter[field]

        csv_sheet.add_line_from_object(new_row)

    print(" * Listing and Filtering ({}) workloads now".format(len(all_workloads)))

    for workload in all_workloads.values():
        if verbose:
            print("  - Processing Wkl {}|{}".format(workload.hostname, workload.href))
        if filter_data is not None:
            matched_filters = 0

            for filter_data_row in filter_data.objects():
                # print("   - trying filter : ")
                # print(filter_data_row)
                filter_all_columns_matched = True
                for filter_field_from_csv in filter_data_row:
                    if not filter_all_columns_matched:
                        break

                    if filter_field_from_csv not in filter_fields:
                        continue

                    # print(" field filter={} will be evaluated".format(filter_field_from_csv))

                    if filter_field_from_csv not in filter_data_row:
                        pylo.log.error("Filter field '{}' not found in CSV file".format(filter_field_from_csv))
                        sys.exit(1)

                    current_filter = filter_data_row[filter_field_from_csv]
                    if current_filter is None:
                        # print('    it was empty!')
                        continue

                    if filter_field_from_csv == 'hostname':
                        hostname_in_csv = pylo.hostname_from_fqdn(current_filter).lower()
                        workload_hostname = pylo.hostname_from_fqdn(workload.hostname).lower()
                        # print("   : comparing filter entry {} with wkl entry {}".format(hostname_in_csv, workload_hostname))
                        if hostname_in_csv != workload_hostname:
                            filter_all_columns_matched = False
                            break
                    elif filter_field_from_csv == 'app':
                        if current_filter is None or current_filter == '':
                            continue
                        else:
                            if workload.app_label is None or workload.app_label.name.lower() != current_filter.lower():
                                filter_all_columns_matched = False
                                break
                    elif filter_field_from_csv == 'ip':
                        found_ip = False
                        for interface in workload.interfaces:
                            if current_filter == interface.ip:
                                found_ip = True
                                break
                        if not found_ip:
                            filter_all_columns_matched = False
                            break
                    else:
                        # we don't support this filter type so we exit with an error
                        pylo.log.error("Filter field '{}' is not supported".format(filter_field_from_csv))
                        sys.exit(1)

                if filter_all_columns_matched:
                    add_workload_to_report(workload, filter_data_row)
                    matched_filters += 1

            if matched_filters > 0:
                if verbose:
                    print("  - matched {} filters".format(matched_filters))

        else:
            add_workload_to_report(workload)

    print("  ** All workloads have been processed, {} were added in the report".format(csv_sheet.lines_count()))

    if filter_keep_in_report:
        print(" * Adding unmatched filters back into the report as request...", flush=True, end='')
        count_unused_filters = 0
        for filter_data_row in filter_data.objects():
            if filter_data_row['*line*'] not in used_filters:
                count_unused_filters += 1
                add_workload_to_report(wkl=None, filter=filter_data_row)
        print(" DONE! ({} found)".format(count_unused_filters))

    print()
    print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
    if not excel_output_only:
        csv_sheet.write_to_csv(output_file_csv)
        print("DONE")
    else:
        print("SKIPPED (use --csv-output-only to write CSV file or no option for both)")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    if not csv_output_only:
        csv_report.write_to_excel(output_file_excel)
        print("DONE")
    else:
        print("SKIPPED (use --excel-output-only to write Excel file or no option for both)")

    if csv_sheet.lines_count() < 1:
        print("\n** WARNING: no entry matched your filters so reports are empty !\n")


command_object = Command(command_name, __main, fill_parser)
