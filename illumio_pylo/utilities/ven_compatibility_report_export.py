import os
import sys
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo


parser = argparse.ArgumentParser(description='Get compatibility reports from all your IDLE VEN so you can review and remediate them')
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


parser.add_argument('--debug', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages')

args = vars(parser.parse_args())

if args['debug']:
        pylo.log_set_debug()


hostname = args['pce']
use_cached_config = args['dev_use_cache']
now = datetime.now()
output_filename_csv = 'compatibility-reports_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
output_filename_xls = 'compatibility-reports_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))


pylo.file_clean(output_filename_csv)
pylo.file_clean(output_filename_xls)


minimum_supported_version = pylo.SoftwareVersion("18.2.0-0")

org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()

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

agents = {}
for agent in org.AgentStore.items_by_href.values():
    if agent.mode == 'idle':
        agents[agent.href] = agent
print(" * Found {} IDLE Agents".format(len(agents)))

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
            print(" FOUND")
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
            print("FOUND")
            loc_label_list[label] = label

app_label_list = {}
if args['filter_app_label'] is not None:
    print("   * Application Labels specified")
    for raw_label_name in args['filter_app_label'].split(','):
        print("     - label named '{}' ".format(raw_label_name), end='', flush=True)
        label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_app)
        if label is None:
            print("NOT FOUND!")
            raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
        else:
            print("FOUND")
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
            print("FOUND")
            role_label_list[label] = label


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
print("OK!")

print()
print(" ** Request Compatibility Report for each Agent in IDLE mode **", flush=True)

agent_count = 0
agent_green_count = 0
agent_mode_changed_count = 0
agent_skipped_not_online = 0
agent_has_no_report_count = 0
agent_report_failed_count = 0

export_report = pylo.Helpers.ArrayToExport(['hostname', 'role', 'app', 'env', 'loc', 'operating_system', 'report_failed', 'details', 'href'])

for agent in agents.values():
    agent_count += 1
    export_row = []
    print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(agent_count, len(agents), agent.workload.get_name(),
                                                                    agent.workload.href,
                                                                    agent.workload.get_labels_str())
          )
    if not agent.workload.online:
        print("    - Agent is not ONLINE so we're skipping it")
        agent_skipped_not_online += 1
        continue

    export_row.append(agent.workload.get_name())
    labels = agent.workload.get_labels_str_list()
    export_row.append(labels[0])
    export_row.append(labels[1])
    export_row.append(labels[2])
    export_row.append(labels[3])
    export_row.append(agent.workload.os_id)

    print("    - Downloading report...", flush=True, end='')
    report = connector.agent_get_compatibility_report(agent_href=agent.href, return_raw_json=False)
    print('OK')

    if report.empty:
        print("    - Report does not exist")
        agent_has_no_report_count += 1
        export_row.append('not-available')
        export_row.append('')
    else:
        print("    - Report status is '{}'".format(report.global_status))
        if report.global_status == 'green':
            agent_green_count += 1
            export_row.append('no')
            export_row.append('')
        else:
            export_row.append('yes')
            failed_items_texts = []
            for failed_item in report.get_failed_items().values():
                if failed_item.extra_debug_message is None:
                    failed_items_texts.append(failed_item.name)
                else:
                    failed_items_texts.append('{}({})'.format(failed_item.name, failed_item.extra_debug_message))
            failed_items = pylo.string_list_to_text(failed_items_texts)
            agent_report_failed_count += 1
            export_row.append(failed_items)

    export_row.append(agent.workload.href)

    export_report.add_line_from_list(export_row)


print("\n**** Saving Compatibility Reports to '{}' ****".format(output_filename_csv), end='', flush=True)
export_report.write_to_csv(output_filename_csv)
print("OK!")
print("\n**** Saving Compatibility Reports to '{}' ****".format(output_filename_xls), end='', flush=True)
export_report.write_to_excel(output_filename_xls, 'Workloads')
print("OK!")


def myformat(name, value):
    return "{:<42} {:>6}".format(name, value)
    # return "{:<18} {:>6}".format(name, "${:.2f}".format(value))


print("\n\n*** Statistics ***")
print(myformat(" - IDLE Agents count:", agent_count))
print(myformat(" - Agents with successful report count:", agent_green_count))
print(myformat(" - SKIPPED because not online count:", agent_skipped_not_online))
print(myformat(" - SKIPPED because report was not found:", agent_has_no_report_count))
print(myformat(" - Agents with failed reports:", agent_report_failed_count ))

print()
