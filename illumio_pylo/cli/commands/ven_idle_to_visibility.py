from typing import Dict, List, Any, Union
from dataclasses import dataclass
import sys
import argparse
import math
import illumio_pylo as pylo
from .utils.misc import make_filename_with_timestamp
from . import Command
from ..NativeParsers import LabelParser

command_name = 'ven-idle-to-visibility'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--filter-on-href-from-file', type=str, required=False, default=None,
                        help='Filter agents on workload href found in specific csv file')

    parser.add_argument('--ignore-incompatibilities', type=str, nargs='+', required=False, default=None,
                        help="Ignore specific incompatibilities and force mode switch!")

    parser.add_argument('--ignore-all-incompatibilities', action='store_true',
                        help="Don't check compatibility report and just do the change!")

    parser.add_argument('-c', '--confirm', action='store_true',
                        help='Request upgrade of the Agents')

    parser.add_argument('-m', '--mode', type=str.lower, required=True, choices=['build', 'test'],
                        help='Select if you want to switch from IDLE to BUILD or TEST')


@dataclass
class MyBuiltInParser:
    filter_env_label = LabelParser('--filter-env-label', '-env', 'env', is_required=False, allow_multiple=True)
    filter_app_label = LabelParser('--filter-app-label', '-app', 'app', is_required=False, allow_multiple=True)
    filter_role_label = LabelParser('--filter-role-label', '-role', 'role', is_required=False, allow_multiple=True)
    filter_loc_label = LabelParser('--filter-loc-label', '-loc', 'loc', is_required=False, allow_multiple=True)


def __main(args, org: pylo.Organization, native_parsers: MyBuiltInParser, **kwargs):
    confirmed_updates = args['confirm']
    switch_to_mode = args['mode']
    href_filter_file = args['filter_on_href_from_file']
    options_ignore_all_incompatibilities = args['ignore_all_incompatibilities']
    option_ignore_incompatibilities: Union[None, Dict[str, bool]] = None
    if args['ignore_incompatibilities'] is not None:
        option_ignore_incompatibilities = {}
        for entry in args['ignore_incompatibilities']:
            option_ignore_incompatibilities[entry] = True

    minimum_supported_version = pylo.SoftwareVersion("18.2.0-0")

    output_file_prefix = make_filename_with_timestamp('ven-mode-update-results_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'changed_mode', 'details', 'href']
    csv_report = pylo.ArrayToExport(csv_report_headers)

    def add_workload_to_report(wkl: pylo.Workload, changed_mode: str, details: str):
        labels = workload.get_labels_str_list()
        new_row = {
            'hostname': wkl.hostname,
            'role': labels[0],
            'app': labels[1],
            'env': labels[2],
            'loc': labels[3],
            'href': wkl.href,
            'status': wkl.get_status_string(),
            'changed_mode': changed_mode,
            'details': details
        }

        csv_report.add_line_from_object(new_row)

    href_filter_data = None
    if href_filter_file is not None:
        print(" * Loading CSV input file '{}'...".format(href_filter_file), flush=True, end='')
        href_filter_data = pylo.CsvExcelToObject(href_filter_file,
                                                 expected_headers=[{'name': 'href', 'optional': False}])
        print('OK')
        print("   - CSV has {} columns and {} lines (headers don't count)".format(href_filter_data.count_columns(),
                                                                                  href_filter_data.count_lines()),
              flush=True)

    agents = {}
    for agent in org.AgentStore.items_by_href.values():
        if agent.mode == 'idle':
            agents[agent.href] = agent
    print(" * Found {} IDLE Agents".format(len(agents)))
    count_idle_agents_total = len(agents)

    print(" * Parsing filters")

    env_label_list = native_parsers.filter_env_label.results
    if env_label_list is None:
        print("   * No Environment Labels specified")
    else:
        print("   * Environment Labels specified")
        for label in env_label_list:
            print("     - label named '{}'".format(label.name))

    loc_label_list = native_parsers.filter_loc_label.results
    if loc_label_list is None:
        print("   * No Location Labels specified")
    else:
        print("   * Location Labels specified")
        for label in loc_label_list:
            print("     - label named '{}'".format(label.name))

    app_label_list = native_parsers.filter_app_label.results
    if app_label_list is None:
        print("   * No Application Labels specified")
    else:
        print("   * Application Labels specified")
        for label in app_label_list:
            print("     - label named '{}'".format(label.name))

    role_label_list = native_parsers.filter_role_label.results
    if role_label_list is None:
        print("   * No Role Labels specified")
    else:
        print("   * Role Labels specified")
        for label in role_label_list:
            print("     - label named '{}'".format(label.name))

    print("  * DONE")

    print(" * Applying filters to the list of Agents...", flush=True, end='')

    for agent_href in list(agents.keys()):
        agent = agents[agent_href]
        workload = agent.workload

        if env_label_list is not None and (workload.env_label is None or workload.env_label not in env_label_list):
            del agents[agent_href]
            continue
        if loc_label_list is not None and (workload.loc_label is None or workload.loc_label not in loc_label_list):
            del agents[agent_href]
            continue
        if app_label_list is not None and (workload.app_label is None or workload.app_label not in app_label_list):
            del agents[agent_href]
            continue
        if role_label_list is not None and (workload.role_label is None or workload.role_label not in role_label_list):
            del agents[agent_href]
            continue

        if href_filter_data is not None:
            workload_href_found = False
            for href_entry in href_filter_data.objects():
                workload_href = href_entry['href']
                if workload_href is not None and workload_href == workload.href:
                    workload_href_found = True
                    break
            if not workload_href_found:
                del agents[agent_href]
                continue

    print("OK! {} VENs are matching filters (from initial list of {} IDLE VENs).".format(len(agents), count_idle_agents_total))

    print()
    print(" ** Request Compatibility Report for each Agent in IDLE mode **")

    agent_count = 0
    agent_green_count = 0
    agent_mode_changed_count = 0
    agent_skipped_not_online = 0
    agent_has_no_report_count = 0
    agent_report_failed_count = 0

    try:
        for agent in agents.values():
            agent_count += 1
            print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(agent_count, len(agents), agent.workload.get_name(),
                                                                            agent.workload.href,
                                                                            agent.workload.get_labels_str())
                  )
            if not agent.workload.online:
                print("    - Agent is not ONLINE so we're skipping it")
                agent_skipped_not_online += 1
                add_workload_to_report(agent.workload, 'no', 'VEN is not online')
                continue

            if options_ignore_all_incompatibilities:
                if not confirmed_updates:
                    print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                    add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                    continue
                print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
                org.connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
                print("OK")
                agent_mode_changed_count += 1
                add_workload_to_report(agent.workload, 'yes', '')
                continue

            print("    - Downloading report...", flush=True, end='')
            report = org.connector.agent_get_compatibility_report(agent_href=agent.href, return_raw_json=False)
            print('OK')
            if report.empty:
                print("    - ** SKIPPING : Report does not exist")
                agent_has_no_report_count += 1
                add_workload_to_report(agent.workload, 'no', 'Compatibility report does not exist')
                continue
            print("    - Report status is '{}'".format(report.global_status))
            if report.global_status == 'green':
                agent_green_count += 1
                if not confirmed_updates:
                    print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                    add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                    continue
                print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
                org.connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
                print("OK")
                agent_mode_changed_count += 1
                add_workload_to_report(agent.workload, 'yes', '')
            else:
                print("       - the following issues were found in the report:", flush=True)
                failed_items = report.get_failed_items()
                issues_remaining = False
                for failed_item in failed_items:
                    if option_ignore_incompatibilities is not None and failed_item in option_ignore_incompatibilities:
                        print("         -{} (ignored because it's part of --ignore-incompatibilities list)".format(failed_item))
                    else:
                        print("         -{}".format(failed_item))
                        issues_remaining = True

                if not issues_remaining:
                    agent_green_count += 1
                    if not confirmed_updates:
                        print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                        add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                        continue
                    print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
                    org.connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
                    print("OK")
                    agent_mode_changed_count += 1
                    add_workload_to_report(agent.workload, 'yes', '')
                    continue

                add_workload_to_report(agent.workload, 'no',
                                       'compatibility report has reported issues: {}'.format(pylo.string_list_to_text(failed_items.keys()))
                                       )
                agent_report_failed_count += 1

    except:
        pylo.log.error("An unexpected error happened, an intermediate report will be written and original traceback displayed")
        pylo.log.error(" * Writing report file '{}' ... ".format(output_file_csv))
        csv_report.write_to_csv(output_file_csv)
        pylo.log.error("DONE")
        pylo.log.error(" * Writing report file '{}' ... ".format(output_file_excel))
        csv_report.write_to_excel(output_file_excel)
        pylo.log.error("DONE")

        raise

    def myformat(name, value):
        return "{:<42} {:>6}".format(name, value)
        # return "{:<18} {:>6}".format(name, "${:.2f}".format(value))

    print("\n\n*** Statistics ***")
    print(myformat(" - IDLE Agents count (after filters):", agent_count))
    if confirmed_updates:
        print(myformat(" - Agents mode changed count:", agent_mode_changed_count))
    else:
        print(myformat(" - Agents with successful report count:", agent_green_count))
    print(myformat(" - SKIPPED because not online count:", agent_skipped_not_online))
    print(myformat(" - SKIPPED because report was not found:", agent_has_no_report_count))
    print(myformat(" - Agents with failed reports:", agent_report_failed_count ))

    print()
    print(" * Writing report file '{}' ... ".format(output_file_csv), end='', flush=True)
    csv_report.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing report file '{}' ... ".format(output_file_excel), end='', flush=True)
    csv_report.write_to_excel(output_file_excel)
    print("DONE")

    if not confirmed_updates:
        print()
        print(" ***** No Agent was switched to Illumination because --confirm option was not used *****")
        print()


command_object = Command(command_name, __main, fill_parser,
                         load_specific_objects_only=objects_load_filter,
                         native_parsers_as_class=MyBuiltInParser()
                         )