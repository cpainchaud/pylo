import argparse
import sys
from datetime import datetime
from typing import List, Dict, Optional, Any

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader, ExcelHeaderSet
from illumio_pylo.FilterQuery import FilterQuery, get_workload_filter_registry
from . import Command
from .utils.report_writer import ReportWriter

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

    def post_process_cli_args(self, args: Dict[str, Any], org: pylo.Organization):
        raise NotImplementedError()


extra_columns: List[ExtraColumn] = []


class ExtraColumnRegistry:
    """Registry for managing extra columns in a testable way."""

    def __init__(self):
        self._columns: List[ExtraColumn] = []

    def register(self, column: ExtraColumn) -> None:
        """Register a new extra column."""
        self._columns.append(column)

    def clear(self) -> None:
        """Clear all registered columns."""
        self._columns.clear()

    def get_all(self) -> List[ExtraColumn]:
        """Get all registered columns."""
        return self._columns.copy()

    def get_column_descriptions(self) -> List[ExtraColumn.ColumnDescription]:
        """Get column descriptions for all registered columns."""
        return [col.column_description() for col in self._columns]


# Global registry instance for backward compatibility
_extra_column_registry = ExtraColumnRegistry()

# Constants for filter field names
FILTER_FIELD_HOSTNAME = 'hostname'
FILTER_FIELD_APP = 'app'
FILTER_FIELD_IP = 'ip'


def format_date_or_none(date: Optional[datetime]) -> Optional[str]:
    """Convert a date to string format, or return None if date is None."""
    if date is None:
        return None
    return datetime.strftime(date, '%Y-%m-%d %H:%M:%S')


def build_workload_row(workload: pylo.Workload, org: pylo.Organization) -> Dict[str, Any]:
    """Build a report row dictionary from a workload object."""
    new_row = {
        'name': workload.forced_name,
        'hostname': workload.hostname,
        'href': workload.href,
        'online': workload.online,
        'managed': not workload.unmanaged,
        'status': workload.get_status_string(),
        'link_to_pce': workload.href,
    }

    for label_type in org.LabelStore.label_types:
        new_row[f'label_{label_type}'] = workload.get_label_name(label_type)

    if workload.ven_agent is not None:
        new_row['agent.href'] = workload.ven_agent.href
        new_row['agent.sec_policy_sync_state'] = workload.ven_agent.get_status_security_policy_sync_state()
        new_row['agent.last_heartbeat'] = format_date_or_none(workload.ven_agent.get_last_heartbeat_date())
        new_row['agent.sec_policy_applied_at'] = format_date_or_none(workload.ven_agent.get_status_security_policy_applied_at())

    for extra_column in extra_columns:
        new_row[extra_column.column_description().name] = extra_column.get_value(workload, org)

    return new_row


def match_filter_row_against_workload(workload: pylo.Workload, filter_data_row: Dict[str, Any], filter_fields: List[str]) -> bool:
    """Check if a workload matches all criteria in a filter row."""
    for filter_field_from_csv in filter_data_row:
        if filter_field_from_csv not in filter_fields:
            continue

        current_filter = filter_data_row[filter_field_from_csv]
        if current_filter is None:
            continue

        if filter_field_from_csv == FILTER_FIELD_HOSTNAME:
            hostname_in_csv = pylo.hostname_from_fqdn(current_filter).lower()
            workload_hostname = pylo.hostname_from_fqdn(workload.hostname).lower()
            if hostname_in_csv != workload_hostname:
                return False
        elif filter_field_from_csv == FILTER_FIELD_APP:
            if current_filter is None or current_filter == '':
                continue
            else:
                if workload.app_label is None or workload.app_label.name.lower() != current_filter.lower():
                    return False
        elif filter_field_from_csv == FILTER_FIELD_IP:
            found_ip = False
            for interface in workload.interfaces:
                if current_filter == interface.ip:
                    found_ip = True
                    break
            if not found_ip:
                return False
        else:
            # we don't support this filter type so we raise an error
            raise ValueError(f"Filter field '{filter_field_from_csv}' is not supported")

    return True


def build_report_headers(org: pylo.Organization, include_extra_columns: bool = True) -> ExcelHeaderSet:
    """Build the Excel header set for the workload export report."""
    csv_report_headers = ExcelHeaderSet(['name', 'hostname'])
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(f'label_{label_type}')

    csv_report_headers.extend([
        'online', 'managed', 'status', 'agent.last_heartbeat',
        'agent.sec_policy_sync_state', 'agent.sec_policy_applied_at',
        ExcelHeader(name='link_to_pce', wrap_text=False, url_text='See in PCE', is_url=True),
        'href', 'agent.href'])

    if include_extra_columns:
        for extra_column in extra_columns:
            csv_report_headers.append(extra_column.column_description().name)

    return csv_report_headers


def find_matching_filters_for_workload(workload: pylo.Workload, filter_data, filter_fields: List[str]) -> List[Dict[str, Any]]:
    """Find all filter rows that match a given workload."""
    matching_filters = []

    for filter_data_row in filter_data.objects():
        try:
            if match_filter_row_against_workload(workload, filter_data_row, filter_fields):
                matching_filters.append(filter_data_row)
        except ValueError as e:
            raise pylo.PyloEx(str(e))

    return matching_filters


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='')

    parser.add_argument('--filter-query', '-q', type=str, required=False, default=None,
                        help='Filter workloads using a SQL-like query expression. '
                             'Supports: ==, !=, <, >, <=, >=, contains, matches (regex), and/or/not, parentheses. '
                             'Available fields: name, hostname, description, online, managed, deleted, '
                             'ip_address, last_heartbeat, mode, env, app, role, loc, os_id, etc. '
                             'Examples: "name contains \'prod\'" or "env == \'Production\' and online == true" '
                             'or "(name == \'srv1\' or name == \'srv2\') and last_heartbeat <= \'2024-01-01\'"')

    parser.add_argument('--filter-file', '-i', type=str, required=False, default=None,
                        help='CSV or Excel input filename')
    parser.add_argument('--filter-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')
    parser.add_argument('--filter-fields', type=str, required=False, default=None, choices=[FILTER_FIELD_HOSTNAME, FILTER_FIELD_APP, FILTER_FIELD_IP], nargs="+",
                        help='Fields on which you want to filter on')
    parser.add_argument('--keep-filters-in-report', action='store_true',
                        help='If you want to keep filters information in the export file (to do a table joint for example)')

    # Add standard report arguments (static helper)
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='workload-export',
        default_sheet_name='workloads'
    )

    for extra_column in extra_columns:
        extra_column.apply_cli_args(parser)


def __main(args: Dict[str, Any], org: pylo.Organization, logger=None, **kwargs) -> None:
    if logger is None:
        logger = pylo.log

    filter_query_string = args['filter_query']
    filter_file = args['filter_file']
    filter_file_delimiter = args['filter_file_delimiter']
    filter_fields = args['filter_fields']
    filter_keep_in_report = args['keep_filters_in_report']
    verbose = args['verbose']

    # Create headers first
    csv_report_headers = build_report_headers(org, include_extra_columns=True)

    for extra_column in extra_columns:
        print(" - adding extra column from external plugin: " + extra_column.column_description().name)

    filter_csv_expected_fields = []
    filter_data = None

    if filter_file is not None:
        if filter_fields is None:
            logger.error("A filter file was provided but you didn't specify on which fields they should apply")
            sys.exit(1)
        if len(filter_fields) < 1:
            logger.error("A filter file was provided but you specified an empty filter-fields option")
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

    # Create ReportWriter AFTER all headers have been finalized
    report_writer = ReportWriter(headers=csv_report_headers, sheet_name='workloads', filename_prefix='workload-export', force_all_wrap_text=True, args=args)
    # ReportWriter initialized from CLI args via constructor
    csv_report = report_writer.excel_workbook
    csv_sheet = report_writer.sheet

    all_workloads = org.WorkloadStore.itemsByHRef.copy()

    # Apply filter query if provided
    if filter_query_string is not None:
        print(" * Applying filter query: '{}'".format(filter_query_string))
        try:
            # Pass org to get registry with all configured label types for this PCE
            registry = get_workload_filter_registry(org)
            filter_query = FilterQuery(registry)

            matching_workloads = filter_query.execute(filter_query_string, list(all_workloads.values()))

            # Convert back to dict by href
            all_workloads = {w.href: w for w in matching_workloads}
            print("   - Filter query matched {} workload(s)".format(len(all_workloads)))
        except pylo.PyloEx as e:
            logger.error("Filter query error: {}".format(e))
            sys.exit(1)

    used_filters = {}

    def add_workload_to_report(wkl: pylo.Workload = None, filter=None, filter_append_prefix='_'):
        if wkl is not None:
            new_row = build_workload_row(wkl, org)
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
            try:
                matching_filters = find_matching_filters_for_workload(workload, filter_data, filter_fields)

                for filter_data_row in matching_filters:
                    add_workload_to_report(workload, filter_data_row)

                if len(matching_filters) > 0 and verbose:
                    print("  - matched {} filters".format(len(matching_filters)))
            except pylo.PyloEx as e:
                logger.error(str(e))
                sys.exit(1)

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

    if csv_sheet.lines_count() < 1:
        print("\n** WARNING: no workload matched your filters !\n")

    # Always write report (even if empty)
    # JSON is generated from the populated sheet inside ReportWriter
    print()
    report_writer.write_reports()


command_object = Command(command_name, __main, fill_parser)
