from typing import Dict, List, Any
import sys
import argparse
import illumio_pylo as pylo
from .utils.misc import make_filename_with_timestamp
from . import Command


command_name = 'ven-upgrade'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--filter-label', '-fl', action='append',
                        help='Only look at workloads matching specified labels')

    parser.add_argument('--filter-ven-versions', nargs='+', type=str, required=False, default=None,
                        help='Filter agents by versions (separated by spaces)')

    parser.add_argument('--filter-on-href-from-file', type=str, required=False, default=None,
                        help='Filter agents on workload href found in specific csv file')

    parser.add_argument('--confirm', action='store_true',
                        help='Request upgrade of the Agents')

    parser.add_argument('--target-version', type=str, required=True,
                        help='Request upgrade of the Agents')


def __main(args, org: pylo.Organization, **kwargs):
    settings_href_filter_file = args['filter_on_href_from_file']
    request_upgrades = args['confirm']

    minimum_supported_version = pylo.SoftwareVersion("18.3.0-0")

    href_filter_data = None
    if settings_href_filter_file is not None:
        print(" * Loading CSV input file '{}'...".format(settings_href_filter_file), flush=True, end='')
        href_filter_data = pylo.CsvExcelToObject(settings_href_filter_file, expected_headers=[{'name': 'href', 'optional': False}])
        print('OK')
        print("   - CSV has {} columns and {} lines (headers don't count)".format(href_filter_data.count_columns(), href_filter_data.count_lines()), flush=True)

    print(" * Listing VEN Agents TOTAL count per version:")
    version_count = {}
    for agent in org.AgentStore.items_by_href.values():
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
    print("    - TOTAL: {} Agents".format(len(org.AgentStore.items_by_href)))

    target_version_string = args['target_version']
    print(" * Parsing target version '{}'".format(target_version_string))
    target_version = pylo.SoftwareVersion(target_version_string)

    print(" * Parsing filters")
    filter_labels: List[pylo.Label] = []  # the list of labels to filter the workloads against
    if args['filter_label'] is not None:
        for label_name in args['filter_label']:
            label = org.LabelStore.find_label_by_name(label_name)
            if label is None:
                raise pylo.PyloEx("Cannot find label '{}' in the PCE".format(label_name))
            filter_labels.append(label)

    filter_versions = {}
    if args['filter_ven_versions'] is not None:
        print("   * VEN versions specified")
        for raw_version_name in args['filter_ven_versions']:
            if len(raw_version_name) < 0:
                raise pylo.PyloEx("Unsupported version provided: '{}'".format(raw_version_name))
            parsed_version = pylo.SoftwareVersion(raw_version_name)
            print("     - version '{}' ".format(raw_version_name), end='', flush=True)
            filter_versions[raw_version_name] = parsed_version

    print(" * Filter out VEN Agents which aren't matching filters:")
    agents = org.AgentStore.items_by_href.copy()

    for agent_href in list(agents.keys()):
        agent = agents[agent_href]
        workload = agent.workload

        if href_filter_data is not None:
            workload_href_found = False
            for href_entry in href_filter_data.objects():
                workload_href = href_entry['href']
                if workload_href is not None and workload_href == workload.href:
                    workload_href_found = True
                    break
            if not workload_href_found:
                pylo.log.debug(" - workload '{}' is not listed the CSV/Excel file".format(workload.get_name()))
                del agents[agent_href]
                continue

        if workload.uses_all_labels(filter_labels) is False:
            pylo.log.debug(" - workload '{}' is not matching the labels filter".format(workload.get_name()))
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

    if args['use_cache']:
        print("\n\n *** SKIPPING Upgrade process as --use-cache option was used!")
        sys.exit(0)

    if not request_upgrades:
        print("\n\n *** SKIPPING Upgrade process as option '--confirm' was not used")
        sys.exit(0)

    if len(agents) < 1:
        print("\n\n *** After filtering there is no Agent left for the upgrade process")
        sys.exit(0)

    print("\n *** Now Requesting Agents Upgrades from the PCE ***")
    agent_count = 0
    for agent in agents.values():
        agent_count += 1
        print(" - Agent #{}/{}: wkl NAME:'{}' HREF:{} Labels:{}".format(agent_count, len(agents), agent.workload.get_name(),
                                                                        agent.workload.href,
                                                                        agent.workload.get_labels_str())
              )
        org.connector.objects_workload_agent_upgrade(agent.workload.href, target_version_string)

    print("\n \n** All Agents Upgraded **\n")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)
