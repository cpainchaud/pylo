import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo


# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')

parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')
parser.add_argument('--filter-loc-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')
parser.add_argument('--filter-app-label', type=str, required=False, default=None,
                    help='Filter agents by role labels (separated by commas)')
parser.add_argument('--filter-role-label', type=str, required=False, default=None,
                    help='Filter agents by role labels (separated by commas)')

parser.add_argument('--confirm', action='store_true',
                    help='Confirm reassignment request')
parser.add_argument('--stop-after', type=int, nargs='?', required=False, default=False, const=True,
                    help='Stop reassigning agents after X number of agents have already been processed')

parser.add_argument('--target-pce', type=str, required=True,
                    help='the new PCE these VENs should report to')

args = vars(parser.parse_args())
# </editor-fold>

if args['debug']:
    pylo.log_set_debug()

hostname = args['pce']
use_cached_config = args['dev_use_cache']
request_upgrades = args['confirm']
target_pce_string = args['target_pce']
stop_after_x_agents = args['stop_after']

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
    org.connector = connector
    org.load_from_json(fake_config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))

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


agents = org.AgentStore.items_by_href.copy()

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

    if 'active_pce_fqdn' in agent.raw_json and agent.raw_json['active_pce_fqdn'] == target_pce_string:
        del agents[agent_href]
        continue

print("")


print("\n *** Now Requesting Agents Reassignment to the new PCE '{}' ***".format(target_pce_string))
processed_agent_count = 0
for agent in agents.values():
    if stop_after_x_agents is not False and processed_agent_count >= stop_after_x_agents:
        print("\n ** Reassignment Processed stopped after {} agents as requested while there is {} more to process **".format(stop_after_x_agents, len(agents)-stop_after_x_agents))
        break

    print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(processed_agent_count, len(agents), agent.workload.get_name(),
                                                                    agent.workload.href,
                                                                    agent.workload.get_labels_str())
          )
    if agent.workload.online is not True:
        print("   * SKIPPED because Workload is not online")
        continue
    if not request_upgrades:
        print("   * SKIPPED Reassign Request process as option '--confirm' was not used")
        continue
    if use_cached_config:
        print("   * SKIPPED Upgrade process as --dev-use-cache option was used!")
        continue

    connector.objects_agent_reassign_pce(agent.href, target_pce_string)
    processed_agent_count += 1


print("\n ** {} Agents were reassigned to PCE '{}' **".format(processed_agent_count, target_pce_string))


