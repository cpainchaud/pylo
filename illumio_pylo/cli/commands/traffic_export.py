"""
Usage documentation for this command can be found in docs/cli/traffic-export.md
"""

import argparse
from datetime import datetime
import os
from typing import Dict, List, Literal
from zoneinfo import ZoneInfo

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeader, ExplorerResultV2
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'traffic-export'
objects_load_filter: List[pylo.ObjectTypes] = ['labels', 'labelgroups', 'iplists', 'services']

# Base column definitions for traffic export
BASE_COLUMNS = ['src_ip', 'src_iplist', 'src_workload']
DST_BASE_COLUMNS = ['dst_ip', 'dst_iplist', 'dst_workload']
SERVICE_COLUMNS = ['protocol', 'port']
POLICY_COLUMNS = ['policy_decision', 'draft_policy_decision']
TIME_COLUMNS = ['first_detected', 'last_detected']


def _generate_omit_columns_help() -> str:
    """Generate help text for --omit-columns with all available base columns."""
    base_cols = ', '.join(BASE_COLUMNS + DST_BASE_COLUMNS + SERVICE_COLUMNS + POLICY_COLUMNS + TIME_COLUMNS)
    return (f'Column names to omit from the export (e.g., protocol, port). '
            f'Base columns: {base_cols}. '
            f'Label columns are added dynamically based on label types (e.g., src_app, dst_env, etc.). '
            f'When --consolidate-labels is used, src_labels and dst_labels are available instead of individual label columns.')


def fill_parser(parser: argparse.ArgumentParser):
    parser.description = "Export traffic records from the PCE based on specified filters and settings."

    parser.add_argument('--format', '-f', required=False, default='excel', choices=['csv', 'excel'],
                        help='Output file format')
    parser.add_argument('--output-dir', '-o', required=False, default='output',
                        help='Directory where to save the output file')

    parser.add_argument('--source-filters', '-sf', required=False, type=str, nargs='+', default=None,
                        help='Source filters to apply (e.g. label:Web, iplist:Private_Networks)')
    parser.add_argument('--destination-filters', '-df', required=False, type=str, nargs='*', default=None,
                        help='Destination filters to apply (e.g. label:DB, iplist:Public_NATed)')

    parser.add_argument('--since-timestamp', '-st', required=False, type=str, default=None,
                        help='Export traffic records since this timestamp (ISO 8601 format)')
    parser.add_argument('--until-timestamp', '-ut', required=False, type=str, default=None,
                        help='Export traffic records until this timestamp (ISO 8601 format)')
    parser.add_argument('--timeframe-hours', '-tfh', required=False, type=int, default=None,
                        help='Export traffic records from the last X hours (overrides --since-timestamp and --until-timestamp)')
    parser.add_argument('--records-count-limit', '-rl', required=False, type=int, default=10000,
                        help='Maximum number of records to export')

    parser.add_argument('--draft-mode-enabled', '-dme', action='store_true', required=False, default=False,
                        help='Enable draft mode to recalculate policy decisions based on draft rules')
    parser.add_argument('--protocol-names', '-pn', action='store_true', required=False, default=False,
                        help='Translate common protocol numbers to names (e.g., 6 -> TCP) before export')
    parser.add_argument('--timezone', '-tz', required=False, type=str, default=None,
                        help='Convert timestamps to this timezone (e.g., America/New_York, Europe/Paris). If not specified, timestamps remain in UTC.')
    parser.add_argument('--consolidate-labels', '-cl', action='store_true', required=False, default=False,
                        help='Consolidate all workload labels into a single column (src_labels, dst_labels) as comma-separated values, ordered by label types')
    parser.add_argument('--label-separator', '-ls', required=False, type=str, default=',',
                        help='Separator to use when consolidating labels (default: ","). Only applies when --consolidate-labels is enabled. Examples: ", ", " ", "|", ";"')
    parser.add_argument('--disable-wrap-text', '-dwt', action='store_true', required=False, default=False,
                        help='Disable text wrapping for all report columns (enabled by default)')
    parser.add_argument('--omit-columns', '-oc', required=False, type=str, nargs='+', default=None,
                        help=_generate_omit_columns_help())


def __main(args: Dict, org: pylo.Organization, **kwargs):
    settings_output_file_format: Literal['csv', 'excel'] = args['format']
    settings_output_dir: str = args['output_dir']
    settings_source_filters: List[str] | None = args['source_filters']
    settings_destination_filters: List[str] | None = args['destination_filters']
    settings_since_timestamp: str | None = args['since_timestamp']
    settings_until_timestamp: str | None = args['until_timestamp']
    settings_timeframe_hours: int | None = args['timeframe_hours']
    settings_records_count_limit: int = args['records_count_limit']
    settings_draft_mode_enabled: bool = args['draft_mode_enabled']
    settings_protocol_names: bool = args['protocol_names']
    settings_timezone: str | None = args['timezone']
    settings_consolidate_labels: bool = args['consolidate_labels']
    settings_label_separator: str = args['label_separator']
    settings_disable_wrap_text: bool = args['disable_wrap_text']
    settings_omit_columns: List[str] | None = args['omit_columns']

    explorer_query = org.connector.new_explorer_query_v2(max_results=settings_records_count_limit, draft_mode_enabled=settings_draft_mode_enabled)

    def _apply_filters(filter_values: List[str] | None, filter_set: pylo.ExplorerFilterSetV2, descriptor: Literal['source', 'destination']):
        valid_filter_prefixes = ['label:', 'iplist:']
        if filter_values is None:
            return

        for filter_value in filter_values:
            # a single filter may be made of multiple comma-separated values which will be processed individually
            value_parts = [part.strip() for part in filter_value.split(',') if part.strip() != '']
            if descriptor == 'source':
                filter = filter_set.new_source_filter()
            else:
                filter = filter_set.new_destination_filter()

            for filter_item_string in value_parts:
                if filter_item_string.startswith('label:'):
                    label_name = filter_item_string[len('label:'):]
                    label_search_result: List[pylo.Label] | None = org.LabelStore.find_label_by_name(label_name,
                                                                                           raise_exception_if_not_found=False,
                                                                                           case_sensitive=False)
                    if len(label_search_result) == 0:
                        raise pylo.PyloEx(f"Label '{label_name}' not found in PCE!")
                    elif len(label_search_result) > 1:
                        raise pylo.PyloEx(f"Multiple labels found for name '{label_name}', please use a more specific name or enable case sensitivity!")

                    filter.add_label(label_search_result[0])

                elif filter_item_string.startswith('iplist:'):
                    iplist_name = filter_item_string[len('iplist:'):]
                    iplist_obj = org.IPListStore.find_by_name(iplist_name)
                    if iplist_obj is None:
                        raise pylo.PyloEx(f"IPList '{iplist_name}' not found in PCE!")
                    filter.add_iplist(iplist_obj)
                else:
                    raise pylo.PyloEx(f"Invalid {descriptor} filter format: '{filter_item_string}', valid prefixes are: {valid_filter_prefixes}")

    # Processing time filters
    if settings_timeframe_hours is not None:
        if settings_since_timestamp is not None or settings_until_timestamp is not None:
            raise pylo.PyloEx("--timeframe-hours cannot be used together with --since-timestamp or --until-timestamp")
        explorer_query.filters.set_time_from_x_seconds_ago(settings_timeframe_hours * 3600)
    else:
        if settings_since_timestamp is not None:
            try:
                explorer_query.filters.set_time_from(datetime.fromisoformat(settings_since_timestamp))
            except ValueError:
                raise pylo.PyloEx("Invalid --since-timestamp format, please use ISO 8601 format")
        else:
            raise pylo.PyloEx("Either --since-timestamp or --timeframe-hours must be provided")

        if settings_until_timestamp is not None:
            try:
                explorer_query.filters.set_time_to(datetime.fromisoformat(settings_until_timestamp))
            except ValueError:
                raise pylo.PyloEx("Invalid --until-timestamp format, please use ISO 8601 format")

    # Processing source filters
    _apply_filters(settings_source_filters, explorer_query.filters, 'source')

    # Processing destination filters
    _apply_filters(settings_destination_filters, explorer_query.filters, 'destination')

    print("Executing and downloading traffic export query... ", flush=True, end='')
    query_results = explorer_query.execute()
    print("DONE")

    print("Processing traffic records... ", flush=True, end='')
    records: List[ExplorerResultV2] = query_results.get_all_records()
    print(f"DONE - {len(records)} records retrieved")

    # Get label types from the organization
    label_types = org.LabelStore.label_types

    # Define base columns and dynamically add label columns
    if settings_consolidate_labels:
        # Use consolidated label columns
        src_label_columns = ['src_labels']
        dst_label_columns = ['dst_labels']
    else:
        # Use individual label columns
        src_label_columns = [f'src_{label_type}' for label_type in label_types]
        dst_label_columns = [f'dst_{label_type}' for label_type in label_types]

    # Build policy columns, excluding draft_policy_decision if draft mode is not enabled
    policy_columns = POLICY_COLUMNS.copy()
    if not settings_draft_mode_enabled:
        policy_columns = [col for col in policy_columns if col != 'draft_policy_decision']

    # Construct all columns in the correct order:
    # src_ip, src_workload, src_labels, dst_ip, dst_workload, dst_labels, protocol, port, policy_decision, [draft_policy_decision], first_detected, last_detected
    all_columns = (BASE_COLUMNS + src_label_columns + DST_BASE_COLUMNS + dst_label_columns +
                   SERVICE_COLUMNS + policy_columns + TIME_COLUMNS)

    # Process omit-columns setting
    columns_to_include = all_columns.copy()
    if settings_omit_columns is not None:
        # Validate column names
        omit_columns_lower = [col.lower() for col in settings_omit_columns]
        invalid_columns = [col for col in omit_columns_lower if col not in all_columns]
        if invalid_columns:
            raise pylo.PyloEx(f"Invalid column names in --omit-columns: {invalid_columns}. Available columns: {all_columns}")

        # Remove omitted columns
        columns_to_include = [col for col in all_columns if col not in omit_columns_lower]

        # Ensure at least one column remains
        if len(columns_to_include) == 0:
            raise pylo.PyloEx("Cannot omit all columns. At least one column must be included in the export.")

    # Validate timezone if provided
    target_timezone = None
    if settings_timezone is not None:
        try:
            target_timezone = ZoneInfo(settings_timezone)
        except Exception as e:
            raise pylo.PyloEx(f"Invalid timezone '{settings_timezone}': {e}")

    # Build headers based on columns to include
    header_definitions = {
        'src_ip': ExcelHeader(name='src_ip', max_width=18),
        'src_iplist': ExcelHeader(name='src_iplist', max_width=40),
        'src_workload': ExcelHeader(name='src_workload', max_width=30),
        'dst_ip': ExcelHeader(name='dst_ip', max_width=18),
        'dst_iplist': ExcelHeader(name='dst_iplist', max_width=40),
        'dst_workload': ExcelHeader(name='dst_workload', max_width=30),
        'protocol': ExcelHeader(name='protocol', max_width=18),
        'port': ExcelHeader(name='port', max_width=12),
        'policy_decision': ExcelHeader(name='policy_decision', max_width=20),
        'first_detected': ExcelHeader(name='first_detected', max_width=22),
        'last_detected': ExcelHeader(name='last_detected', max_width=22),
    }

    # Add a draft_policy_decision header only if draft mode is enabled
    if settings_draft_mode_enabled:
        header_definitions['draft_policy_decision'] = ExcelHeader(name='draft_policy_decision', max_width=25)

    # Add dynamic label column headers
    if settings_consolidate_labels:
        header_definitions['src_labels'] = ExcelHeader(name='src_labels', max_width=50)
        header_definitions['dst_labels'] = ExcelHeader(name='dst_labels', max_width=50)
    else:
        for label_type in label_types:
            header_definitions[f'src_{label_type}'] = ExcelHeader(name=f'src_{label_type}', max_width=25)
            header_definitions[f'dst_{label_type}'] = ExcelHeader(name=f'dst_{label_type}', max_width=25)

    csv_report_headers = pylo.ExcelHeaderSet([
        header_definitions[col] for col in columns_to_include
    ])
    csv_report = ArraysToExcel()
    sheet = csv_report.create_sheet(
        'traffic',
        csv_report_headers,
        force_all_wrap_text=not settings_disable_wrap_text,
        multivalues_cell_delimiter=','
    )

    def _protocol_display(proto: str | int | None) -> str | int | None:
        """Return a human-readable protocol name when known; otherwise the original value."""
        if proto is None:
            return None
        # Accept ints or numeric strings; fallback to original value on conversion issues.
        try:
            proto_int = int(proto)
        except (ValueError, TypeError):
            return proto

        common_protocols = {
            1: 'ICMP',
            6: 'TCP',
            17: 'UDP',
            50: 'ESP',
            51: 'AH',
            132: 'SCTP'
        }
        return common_protocols.get(proto_int, proto)

    def _convert_timestamp(timestamp_str: str | None, target_tz: ZoneInfo | None) -> str | None:
        """Convert UTC ISO 8601 timestamp to target timezone if specified, otherwise return as-is."""
        if timestamp_str is None or target_tz is None:
            return timestamp_str

        try:
            # Parse the UTC timestamp
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # Convert to the target timezone
            dt_converted = dt.astimezone(target_tz)
            # Return as ISO 8601 string
            return dt_converted.isoformat()
        except Exception:
            # If conversion fails, return original
            return timestamp_str

    def _format_iplists(iplists: Dict[str, pylo.IPList]) -> str | None:
        if not iplists:
            return None
        names: List[str] = []
        for iplist in iplists.values():
            if iplist.name:
                names.append(iplist.name)
            else:
                names.append(iplist.href)
        if not names:
            return None
        return ','.join(sorted(set(names), key=str.lower))

    for record in records:
        # Build a full record with all columns
        full_record_to_export = {
            'src_ip': record.source_ip,
            'src_iplist': _format_iplists(record.get_source_iplists(org)),
            'src_workload': record.source_workload_hostname,
            'dst_ip': record.destination_ip,
            'dst_iplist': _format_iplists(record.get_destination_iplists(org)),
            'dst_workload': record.destination_workload_hostname,
            'protocol': _protocol_display(record.service_protocol) if settings_protocol_names else record.service_protocol,
            'port': record.service_port,
            'policy_decision': record.policy_decision_string,
            'draft_policy_decision': record.draft_mode_policy_decision_to_str() if settings_draft_mode_enabled else None,
            'first_detected': _convert_timestamp(record.first_detected, target_timezone),
            'last_detected': _convert_timestamp(record.last_detected, target_timezone),
        }

        # Add source workload labels
        if settings_consolidate_labels:
            # Consolidate all labels into a single comma-separated string, ordered by label types
            if record.source_workload_href:
                src_label_values = [record.source_workload_labels_by_type.get(label_type) for label_type in label_types]
                src_label_values = [lv for lv in src_label_values if lv is not None]
                full_record_to_export['src_labels'] = settings_label_separator.join(src_label_values) if src_label_values else None
            else:
                full_record_to_export['src_labels'] = None
        else:
            for label_type in label_types:
                full_record_to_export[f'src_{label_type}'] = record.source_workload_labels_by_type.get(label_type) if record.source_workload_href else None

        # Add destination workload labels
        if settings_consolidate_labels:
            # Consolidate all labels into a single comma-separated string, ordered by label types
            if record.destination_workload_href:
                dst_label_values = [record.destination_workload_labels_by_type.get(label_type) for label_type in label_types]
                dst_label_values = [lv for lv in dst_label_values if lv is not None]
                full_record_to_export['dst_labels'] = settings_label_separator.join(dst_label_values) if dst_label_values else None
            else:
                full_record_to_export['dst_labels'] = None
        else:
            for label_type in label_types:
                full_record_to_export[f'dst_{label_type}'] = record.destination_workload_labels_by_type.get(label_type) if record.destination_workload_href else None

        # Filter to include only selected columns
        csv_record = {col: full_record_to_export[col] for col in columns_to_include}
        sheet.add_line_from_object(csv_record)

    if sheet.lines_count() < 1:
        print("No traffic records matched the filters; nothing to export.")
        return

    os.makedirs(settings_output_dir, exist_ok=True)
    output_filename_base = make_filename_with_timestamp('traffic-export_', settings_output_dir)

    if settings_output_file_format == 'csv':
        output_filename = output_filename_base + '.csv'
        print(f"Writing CSV report to '{output_filename}' ... ", end='', flush=True)
        sheet.write_to_csv(output_filename)
    else:
        output_filename = output_filename_base + '.xlsx'
        print(f"Writing Excel report to '{output_filename}' ... ", end='', flush=True)
        csv_report.write_to_excel(output_filename)
    print("DONE")


command_object = Command(command_name, __main, fill_parser, load_specific_objects_only=objects_load_filter)
