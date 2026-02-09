import argparse
from typing import Dict, List

from prettytable import PrettyTable

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader, ExcelHeaderSet
from . import Command
from .utils.report_writer import ReportWriter

command_name = "ven-compatibility-report-export"
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--filter-label', '-fl', action='append',
                        help='Only look at workloads matching specified labels')
    parser.add_argument('--limit', '-l', type=int, required=False, default=None,
                        help='Limit the number of workloads/agents to process')

    # Add standard report arguments (similar to traffic_export)
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='ven-compatibility-reports',
        default_sheet_name='duplicates',
        format_help='Report format to generate (csv, xlsx, or json). Can be repeated for multiple formats. Default: csv'
    )


def __main(args, org: pylo.Organization, **kwargs):

    settings_output_dir: str = args.get('output_dir')
    settings_filter_labels: List[str] = args.get('filter_label')
    settings_limit: int = args.get('limit')

    # <editor-fold desc="Prepare the output files and CSV/Excel Object">
    # Build headers
    csv_report_headers = ExcelHeaderSet([
        ExcelHeader(name='name', max_width=40),
        ExcelHeader(name='hostname', max_width=40)
    ])

    # insert all label dimensions
    for label_type in org.LabelStore.label_types:
        csv_report_headers.append(ExcelHeader(name='label_' + label_type, wrap_text=False))

    csv_report_headers.extend([
        'operating_system',
        'report_failed',
        'details',
        ExcelHeader(name='link_to_pce', max_width=15, wrap_text=False, url_text='See in PCE', is_url=True),
        ExcelHeader(name='href', max_width=15, wrap_text=False)
    ])

    # Create a ReportWriter instead of raw ArraysToExcel
    report_writer = ReportWriter(
        headers=csv_report_headers,
        sheet_name='duplicates',
        filename_prefix='ven-compatibility-reports',
        force_all_wrap_text=True,
        multivalues_cell_delimiter=','
    )

    # Initialize from CLI args (will set formats, output_dir, output_filename, etc.)
    report_writer.initialize_from_args(args)
    sheet: pylo.ArraysToExcel.Sheet = report_writer.sheet
    # </editor-fold desc="Prepare the output files and CSV/Excel Object">

    agents: Dict[str, pylo.VENAgent] = {}
    for agent in org.AgentStore.items_by_href.values():
        if agent.mode == 'idle':
            agents[agent.href] = agent
    print(" * Found {} IDLE Agents".format(len(agents)))

    filter_labels: List[pylo.Label] = []  # the list of labels to filter the workloads against
    if settings_filter_labels is not None and len(settings_filter_labels) > 0:
        print(" * Parsing Labels filter...", flush=True)
        if args['filter_label'] is not None:
            for label_name in args['filter_label']:
                label = org.LabelStore.find_label_by_name(label_name)
                if label is None:
                    raise pylo.PyloEx("Cannot find label '{}' in the PCE".format(label_name))
                print("   - Adding label '{}' to the filter".format(label_name))
                filter_labels.append(label)

    print(" * Applying filters to the list of Agents...", flush=True, end='')
    for agent_href in list(agents.keys()):
        agent = agents[agent_href]
        workload = agent.workload

        if workload.get_label('loc') is not None and workload.get_label('loc').name == 'CN':
            print("hello")

        if len(filter_labels) > 0:
            if not workload.uses_all_labels(filter_labels):
                pylo.log.info(" - Removing Agent '{}' because it does not match the filter".format(workload.get_name()))
                del agents[agent_href]
                continue

    # limit the number of workloads to process
    if settings_limit is not None:
        agents = dict(list(agents.items())[:settings_limit])

    print("OK! {} Agents left after filtering".format(len(agents)))

    if len(agents) > 0:
        print()
        print(" ** Request Compatibility Report for each Agent in IDLE mode **", flush=True)

        stats_agent_count = 0
        stats_agent_green_count = 0
        stats_agent_mode_changed_count = 0
        stats_agent_skipped_not_online = 0
        stats_agent_has_no_report_count = 0
        stats_agent_report_failed_count = 0

        for agent in agents.values():
            stats_agent_count += 1

            print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(stats_agent_count, len(agents), agent.workload.get_name(),
                                                                            agent.workload.href,
                                                                            agent.workload.get_labels_str())
                  )
            if not agent.workload.online:
                print("    - Agent is not ONLINE so we're skipping it")
                stats_agent_skipped_not_online += 1
                continue

            export_row = {
                'name': agent.workload.get_name(),
                'hostname': agent.workload.hostname,
                'operating_system': agent.workload.os_id,
                'link_to_pce': agent.workload.get_pce_ui_url(),
                'href': agent.workload.href
            }

            for label_type in org.LabelStore.label_types:
                label = agent.workload.get_label(label_type)
                export_row['label_'+label_type] = label.name if label else ''

            print("    - Downloading report (it may be delayed by API flood protection)...", flush=True, end='')
            report = org.connector.agent_get_compatibility_report(agent_href=agent.href, return_raw_json=False)
            print('OK')

            if report.empty:
                print("    - Report does not exist")
                stats_agent_has_no_report_count += 1
                export_row['report_failed'] = 'not-available'
            else:
                print("    - Report status is '{}'".format(report.global_status))
                if report.global_status == 'green':
                    stats_agent_green_count += 1
                    export_row['report_failed'] = 'no'
                else:
                    export_row['report_failed'] = 'yes'
                    failed_items_texts = []
                    for failed_item in report.get_failed_items().values():
                        if failed_item.extra_debug_message is None:
                            failed_items_texts.append(failed_item.name)
                        else:
                            failed_items_texts.append('{}({})'.format(failed_item.name, failed_item.extra_debug_message))
                    failed_items = pylo.string_list_to_text(failed_items_texts)
                    stats_agent_report_failed_count += 1
                    export_row['details'] = failed_items

            sheet.add_line_from_object(export_row)

        print("\n\n*** Statistics ***\n")
        table = PrettyTable()
        table.field_names = ["item", "Value"]
        table.align["item"] = "l"
        table.align["Value"] = "r"

        table.add_row(["IDLE Agents count", stats_agent_count])
        table.add_row(["Agents with successful report count", stats_agent_green_count])
        table.add_row(["SKIPPED because not online count", stats_agent_skipped_not_online])
        table.add_row(["SKIPPED because report was not found", stats_agent_has_no_report_count])
        table.add_row(["Agents with failed reports", stats_agent_report_failed_count])
        print(table)

    report_writer.write_reports()
    print("OK!")

    print()


command_object = Command(
                            name=command_name,
                            main_func=__main,
                            parser_func=fill_parser,
                            load_specific_objects_only=objects_load_filter
                         )
