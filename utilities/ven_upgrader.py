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
                    help='Filter agents by location labels (separated by commas)')
parser.add_argument('--filter-app-label', type=str, required=False, default=None,
                    help='Filter agents by application labels (separated by commas)')
parser.add_argument('--filter-role-label', type=str, required=False, default=None,
                    help='Filter agents by role labels (separated by commas)')

parser.add_argument('--filter-ven-versions', nargs='+', type=str, required=False, default=None,
                    help='Filter agents by versions (separated by spaces)')

parser.add_argument('--confirm', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Request upgrade of the Agents')

parser.add_argument('--target-version', type=str, required=True,
                    help='Request upgrade of the Agents')

parser.add_argument('--debug-pylo', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Request upgrade of the Agents')

args = vars(parser.parse_args())
# print(args)

hostname = args['host']
use_cached_config = args['dev_use_cache']
request_upgrades = args['confirm']

if args['debug_pylo']:
    pylo.log_set_debug()

minimum_supported_version = pylo.SoftwareVersion("18.3.0-0")

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

print(" * Listing VEN Agents TOTAL count per version:")
version_count = {}
for agent in org.AgentStore.itemsByHRef.values():
    if agent.software_version.version_string in version_count:
        version_count[agent.software_version.version_string] += 1
    else:
        version_count[agent.software_version.version_string] = 1


for version_string in sorted(version_count.keys()):
    count = version_count[version_string]
    if minimum_supported_version.is_greater_than(pylo.SoftwareVersion(version_string)):
        print("   - {}: {}    *NOT SUPPORTED*".format(version_string.ljust(12, ' '), count))
    else:
        print("   - {}: {}".format(version_string.ljust(12, ' '), count))
print("    - TOTAL: {} Agents".format(len(org.AgentStore.itemsByHRef)))


target_version_string = args['target_version']
print(" * Parsing target version '{}'".format(target_version_string))
target_version = pylo.SoftwareVersion(target_version_string)

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
            print("found")
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
            print("found")
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
            print("found")
            app_label_list[label] = label

role_label_list = {}
if args['filter_role_label'] is not None:
    print("   * Role Labels specified")
    for raw_label_name in args['filter_role_label']:
        print("     - label named '{}' ".format(raw_label_name), end='', flush=True)
        label = org.LabelStore.find_label_by_name_and_type(raw_label_name, pylo.label_type_role)
        if label is None:
            print("NOT FOUND!")
            raise pylo.PyloEx("Cannot find label named '{}'".format(raw_label_name))
        else:
            print("found")
            role_label_list[label] = label

filter_versions = {}
if args['filter_ven_versions'] is not None:
    print("   * VEN versions specified")
    for raw_version_name in args['filter_ven_versions'].split(','):
        if len(raw_version_name) < 0:
            pylo.log.error("Unsupported version provided: '{}'".format(raw_version_name))
            sys.exit(1)
        parsed_version = pylo.SoftwareVersion(raw_version_name)
        print("     - version '{}' ".format(raw_version_name), end='', flush=True)
        filter_versions[raw_version_name] = parsed_version


print(" * Filter out VEN Agents which aren't matching filters:")
agents = org.AgentStore.itemsByHRef.copy()

for agent_href in list(agents.keys()):
    agent = agents[agent_href]
    workload = agent.workload

    if len(env_label_list) > 0 and (workload.environmentLabel is None or workload.environmentLabel not in env_label_list):
        pylo.log.debug(" - workload '{}' does not match env_label filters, it's out!".format(workload.get_name()))
        del agents[agent_href]
        continue
    if len(loc_label_list) > 0 and (workload.locationLabel is None or workload.locationLabel not in loc_label_list):
        pylo.log.debug(" - workload '{}' does not match loc_label filters, it's out!".format(workload.get_name()))
        del agents[agent_href]
        continue
    if len(app_label_list) > 0 and (workload.applicationLabel is None or workload.applicationLabel not in app_label_list):
        pylo.log.debug(" - workload '{}' does not match app_label filters, it's out!".format(workload.get_name()))
        del agents[agent_href]
        continue
    if len(role_label_list) > 0 and (workload.roleLabel is None or workload.roleLabel not in role_label_list):
        pylo.log.debug(" - workload '{}' does not match role_label filters, it's out!".format(workload.get_name()))
        del agents[agent_href]
        continue

    if agent.software_version.version_string not in filter_versions:
        pylo.log.debug(" - workload '{}' does not match the version filter, it's out!".format(workload.get_name()))
        del agents[agent_href]
        continue

    # Hiding versions which are higher than the one requested for upgrade
    if agent.software_version > target_version:
        pylo.log.debug(" - workload '{}' has a higher version of VEN ({}) than requested, it's out".format(workload.get_name(), workload.ven_agent.software_version.version_string))
        del agents[agent_href]
        continue

    # Hiding unsupported versions
    if agent.software_version.is_lower_than(minimum_supported_version):
        pylo.log.debug(" - workload '{}' has incompatible version of VEN ({}), it's out".format(workload.get_name(), workload.ven_agent.software_version.version_string))
        del agents[agent_href]
        continue

print("\n  * DONE\n")

print(" * Listing VEN Agents FILTERED count per version:")
version_count = {}
for agent in agents.values():
    if agent.software_version.version_string in version_count:
        version_count[agent.software_version.version_string] += 1
    else:
        version_count[agent.software_version.version_string] = 1


for version_string in sorted(version_count.keys()):
    count = version_count[version_string]
    print("   - {}: {}".format(version_string.ljust(12, ' '), count))
print("    - TOTAL: {} Agents".format(len(agents)))


if use_cached_config:
    print("\n\n *** SKIPPING Upgrade process as --dev-use-cache option was used!")
    sys.exit(0)

if not request_upgrades:
    print("\n\n *** SKIPPING Upgrade process as option '--confirm' was not used")
    sys.exit(0)

if len(agents) < 1:
    print("\n\n *** After filtering there no Agent left for the upgrade process")
    sys.exit(0)

print("\n *** Now Requesting Agents Upgrades from the PCE ***")
agent_count = 0
for agent in agents.values():
    agent_count += 1
    print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(agent_count, len(agents), agent.workload.get_name(),
                                                                    agent.workload.href,
                                                                    agent.workload.get_labels_str())
          )
    connector.objects_workload_agent_upgrade(agent.workload.href, target_version_string)


print("\n \n** All Agents Upgraded **\n")


