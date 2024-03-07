import os
import sys
import argparse
from datetime import datetime
from typing import Union,Dict,List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')

parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')
parser.add_argument('--filter-loc-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')
parser.add_argument('--filter-app-label', type=str, required=False, default=None,
                    help='Filter agents by role labels (separated by commas)')
parser.add_argument('--filter-role-label', type=str, required=False, default=None,
                    help='Filter agents by role labels (separated by commas)')

parser.add_argument('--filter-on-href-from-file', type=str, required=False, default=None,
                    help='Filter agents on workload href found in specific csv file')

parser.add_argument('--ignore-incompatibilities', type=str, nargs='+', required=False, default=None,
                    help="Ignore specific incompatibilities and force mode switch!")

parser.add_argument('--ignore-all-incompatibilities', action='store_true',
                    help="Don't check compatibility report and just do the change!")


parser.add_argument('--confirm', action='store_true',
                    help='Request upgrade of the Agents')

parser.add_argument('--debug', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages')

parser.add_argument('--mode', type=str.lower, required=True, choices=['build', 'test'],
                    help='Select if you want to switch from IDLE to BUILD or TEST')


args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['pce']
print(args)
use_cached_config = args['dev_use_cache']
request_upgrades = args['confirm']
switch_to_mode = args['mode']
href_filter_file = args['filter_on_href_from_file']
options_ignore_all_incompatibilities = args['ignore_all_incompatibilities']
option_ignore_incompatibilities: Union[None, Dict[str, bool]] = None
if args['ignore_incompatibilities'] is not None:
    option_ignore_incompatibilities = {}
    for entry in args['ignore_incompatibilities']:
        option_ignore_incompatibilities[entry] = True


minimum_supported_version = pylo.SoftwareVersion("18.2.0-0")

org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()

now = datetime.now()
report_file = 'ven-idle-to-illumination-results_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
report_file_excel = 'ven-idle-to-illumination-results_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))

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


if use_cached_config:
    org.load_from_cache_or_saved_credentials(hostname)
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading Workloads/Agents listing from the PCE... ", end="", flush=True)
    fake_config['workloads'] = connector.objects_workload_get()
    print("OK!")

    print(" * Downloading Labels listing from the PCE... ", end="", flush=True)
    fake_config['labels'] = connector.objects_label_get()
    print("OK!")

    print(" * Parsing PCE data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.load_from_json(fake_config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))

href_filter_data = None
if href_filter_file is not None:
    print(" * Loading CSV input file '{}'...".format(href_filter_file), flush=True, end='')
    href_filter_data = pylo.CsvExcelToObject(href_filter_file, expected_headers=[{'name': 'href', 'optional': False}])
    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(href_filter_data.count_columns(), href_filter_data.count_lines()), flush=True)

agents = {}
for agent in org.AgentStore.items_by_href.values():
    if agent.mode == 'idle':
        agents[agent.href] = agent
print(" * Found {} IDLE Agents".format(len(agents)))
count_idle_agents_total = len(agents)

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

print(" * Applying filters to the list of Agents...", flush=True, end='')

for agent_href in list(agents.keys()):
    agent = agents[agent_href]
    workload = agent.workload

    if len(env_label_list) > 0 and (workload.env_label is None or workload.env_label not in env_label_list):
        del agents[agent_href]
        continue
    if len(loc_label_list) > 0 and (workload.loc_label is None or workload.loc_label not in loc_label_list):
        del agents[agent_href]
        continue
    if len(app_label_list) > 0 and (workload.app_label is None or workload.app_label not in app_label_list):
        del agents[agent_href]
        continue
    if len(role_label_list) > 0 and (workload.role_label is None or workload.role_label not in role_label_list):
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
            if not request_upgrades:
                print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                continue
            print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
            connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
            print("OK")
            agent_mode_changed_count += 1
            add_workload_to_report(agent.workload, 'yes', '')
            continue

        print("    - Downloading report...", flush=True, end='')
        report = connector.agent_get_compatibility_report(agent_href=agent.href, return_raw_json=False)
        print('OK')
        if report.empty:
            print("    - ** SKIPPING : Report does not exist")
            agent_has_no_report_count += 1
            add_workload_to_report(agent.workload, 'no', 'Compatibility report does not exist')
            continue
        print("    - Report status is '{}'".format(report.global_status))
        if report.global_status == 'green':
            agent_green_count += 1
            if not request_upgrades:
                print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                continue
            print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
            connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
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
                if not request_upgrades:
                    print("    - ** SKIPPING Agent mode change process as option '--confirm' was not used")
                    add_workload_to_report(agent.workload, 'no', '--confirm option was not used')
                    continue
                print("    - Request Agent mode switch to BUILD/TEST...", end='', flush=True)
                connector.objects_agent_change_mode(agent.workload.href, switch_to_mode)
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
    pylo.log.error(" * Writing report file '{}' ... ".format(report_file))
    csv_report.write_to_csv(report_file)
    pylo.log.error("DONE")
    pylo.log.error(" * Writing report file '{}' ... ".format(report_file_excel))
    csv_report.write_to_excel(report_file_excel)
    pylo.log.error("DONE")

    raise


def myformat(name, value):
    return "{:<42} {:>6}".format(name, value)
    # return "{:<18} {:>6}".format(name, "${:.2f}".format(value))


print("\n\n*** Statistics ***")
print(myformat(" - IDLE Agents count (after filters):", agent_count))
if request_upgrades:
    print(myformat(" - Agents mode changed count:", agent_mode_changed_count))
else:
    print(myformat(" - Agents with successful report count:", agent_green_count))
print(myformat(" - SKIPPED because not online count:", agent_skipped_not_online))
print(myformat(" - SKIPPED because report was not found:", agent_has_no_report_count))
print(myformat(" - Agents with failed reports:", agent_report_failed_count ))

print()
print(" * Writing report file '{}' ... ".format(report_file), end='', flush=True)
csv_report.write_to_csv(report_file)
print("DONE")
print(" * Writing report file '{}' ... ".format(report_file_excel), end='', flush=True)
csv_report.write_to_excel(report_file_excel)
print("DONE")


if not request_upgrades:
    print()
    print(" ***** No Agent was switched to Illumination because --confirm option was not used *****")
    print()
