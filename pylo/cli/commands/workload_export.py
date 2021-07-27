import pylo
import argparse
import sys
from datetime import datetime
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'workload-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--output', required=False, default='.')

    parser.add_argument('--verbose', '-v', type=bool, nargs='?', required=False, default=False, const=True,
                        help='')

    parser.add_argument('--filter-file', '-i', type=str, required=False, default=None,
                        help='CSV or Excel input filename')
    parser.add_argument('--filter-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')
    parser.add_argument('--filter-fields', type=str, required=False, default=None, choices=['hostname'], nargs="+",
                        help='Fields on which you want to filter on')
    parser.add_argument('--keep-filters-in-report', type=str, required=False, default=False, const=True, nargs='?',
                        help='If you want to keep filters information in the export file (to do a table joint for example)')


def __main(args, org: pylo.Organization):

    filter_file = args['filter_file']
    filter_file_delimiter = args['filter_file_delimiter']
    filter_fields = args['filter_fields']
    filter_keep_in_report = args['keep_filters_in_report']
    verbose = args['verbose']
    # print(args['filter_fields'])

    output_file_prefix = make_filename_with_timestamp('workload_export_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

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

    csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'online', 'managed',
                          'status', 'agent.last_heartbeat', 'agent.sec_policy_sync_state', 'agent.sec_policy_applied_at',
                          'href', 'agent.href']
    if filter_keep_in_report:
        for field in filter_data._detected_headers:
            csv_report_headers.append('_' + field)
    csv_report = pylo.ArrayToExport(csv_report_headers)

    all_workloads = org.WorkloadStore.itemsByHRef.copy()
    used_filters = {}

    def add_workload_to_report(wkl: pylo.Workload = None, filter=None, filter_append_prefix='_'):
        labels = workload.get_labels_list()

        def none_or_date(date):
            if date is None:
                return None
            return datetime.strftime(date, '%Y-%m-%d %H:%M:%S')

        if wkl is not None:
            new_row = {
                'name': wkl.name,
                'hostname': wkl.hostname,
                'role': labels[0],
                'app': labels[1],
                'env': labels[2],
                'loc': labels[3],
                'href': wkl.href,
                'online': wkl.online,
                'managed': not wkl.unmanaged,
                'status': wkl.get_status_string(),
            }
            if wkl.ven_agent is not None:
                new_row['agent.href'] = wkl.ven_agent.href
                new_row['agent.sec_policy_sync_state'] = wkl.ven_agent.get_status_security_policy_sync_state()
                new_row['agent.last_heartbeat'] = none_or_date(wkl.ven_agent.get_last_heartbeat_date())
                new_row['agent.sec_policy_applied_at'] = none_or_date(wkl.ven_agent.get_status_security_policy_applied_at())
        else:
            new_row = {}

        if filter is not None:
            used_filters[filter['*line*']] = True
            for field in filter:
                new_row[filter_append_prefix + field] = filter[field]

        csv_report.add_line_from_object(new_row)

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

                if filter_all_columns_matched:
                    add_workload_to_report(workload, filter_data_row)
                    print("test\n")
                    matched_filters += 1

            if matched_filters > 0:
                if verbose:
                    print("  - matched {} filters".format(matched_filters))

        else:
            add_workload_to_report(workload)

    print("  ** All workloads have been processed, {} were added in the report".format(csv_report.lines_count()))

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
    csv_report.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    csv_report.write_to_excel(output_file_excel)
    print("DONE")

    if csv_report.lines_count() < 1:
        print("\n** WARNING: no entry matched your filters so reports are empty !\n")


def run(options, org: pylo.Organization):
    print()
    print("**** {} UTILITY ****".format(command_name.upper()))
    __main(options, org)
    print("**** END OF {} UTILITY ****".format(command_name.upper()))


command_object = Command(command_name, run, fill_parser)
