import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo



parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developpers only')

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


hostname = args['host']
use_cached_config = args['dev_use_cache']
output_filename_csv = 'reports.csv'


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
for agent in org.AgentStore.itemsByHRef.values():
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

    if len(env_label_list) > 0 and (workload.environmentLabel is None or workload.environmentLabel not in env_label_list):
        del agents[agent_href]
        continue
    if len(loc_label_list) > 0 and (workload.locationLabel is None or workload.locationLabel not in loc_label_list):
        del agents[agent_href]
        continue
    if len(app_label_list) > 0 and (workload.applicationLabel is None or workload.applicationLabel not in app_label_list):
        del agents[agent_href]
        continue

    if len(role_label_list) > 0 and (workload.roleLabel is None or workload.roleLabel not in role_label_list):
        del agents[agent_href]
        continue
print("OK!")

print()
print(" ** Request Compatibility Report for each Agent in IDLE mode **")

agent_count = 0
agent_green_count = 0
agent_mode_changed_count = 0
agent_skipped_not_online = 0
agent_has_no_report_count = 0
agent_report_failed_count = 0

export_report = pylo.Helpers.ArrayToExport(['hostname', 'labels', 'operating_system', 'report_failed', 'details', 'href'])

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
    export_row.append(agent.workload.get_labels_str())
    export_row.append(agent.workload.os_family)

    print("    - Downloading report...", flush=True, end='')
    report = connector.agent_get_compatibility_report(agent_href=agent.href, return_raw_json=False)
    print('OK')

    if report.empty:
        print("    - ** SKIPPING : Report does not exist")
        agent_has_no_report_count += 1
        continue

    print("    - Report status is '{}'".format(report.global_status))
    if report.global_status == 'green':
        agent_green_count += 1
        export_row.append('no')
        export_row.append('')
    else:
        export_row.append('yes')
        failed_items = report.get_failed_items()
        agent_report_failed_count += 1
        export_row.append(failed_items)

    export_row.append(agent.workload.href)

    export_report.add_line_from_list(export_row)


print("\n**** Saving Compatibility Reports to '{}' ****".format(output_filename_csv), end='', flush=True)
export_report.write_to_csv(output_filename_csv)
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
