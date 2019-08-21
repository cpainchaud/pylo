
import pylo
import sys
import argparse
import re


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developpers only')

parser.add_argument('--filter-env-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')
parser.add_argument('--filter-loc-label', type=str, required=False, default=None,
                    help='Filter agents by environment labels (separated by commas)')

parser.add_argument('--confirm', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Request upgrade of the Agents')

parser.add_argument('--target-version', type=str, required=True,
                    help='Request upgrade of the Agents')

args = vars(parser.parse_args())

hostname = args['host']
print(args)
use_cached_config = args['dev_use_cache']
request_upgrades = args['confirm']

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
    if minimum_supported_version < pylo.SoftwareVersion(version_string):
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



print(" * Listing VEN Agents FILTERED count per version:")
agents = org.AgentStore.itemsByHRef.copy()

for agent_href in [*agents.keys()]:
    agent = agents[agent_href]
    workload = agent.workload

    if len(env_label_list) > 0 and (workload.environmentLabel is None or workload.environmentLabel not in env_label_list):
        del agents[agent_href]
        continue
    if len(loc_label_list) > 0 and (workload.locationLabel is None or workload.locationLabel not in loc_label_list):
        del agents[agent_href]
        continue

    # Hiding versions which are higher than the one request for upgrade
    if agent.software_version < target_version:
        del agents[agent_href]
        continue


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
    exit(0)

if not request_upgrades:
    print("\n\n *** SKIPPING Upgrade process as option '--confirm' was not used")
    exit(0)

print("\n *** Now Requesting Agents Upgrades from the PCE ***")
agent_count = 0
for agent in agents.values():
    agent_count += 1
    print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(agent_count, len(agents), agent.workload.get_name(),
                                                                    agent.workload.href,
                                                                    agent.workload.get_labels_str())
          )
    connector.objects_workload_agent_upgrade(agent.workload.href, target_version_string)


