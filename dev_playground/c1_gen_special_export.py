import os
import sys
import io
import argparse
import shlex
from datetime import datetime
import socket
import time
from typing import Union, Optional, Dict, List, Any
import c1_shared
from pylo import Workload

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo

# makes unbuffered output
sys.stdout = io.TextIOWrapper(open(sys.stdout.fileno(), 'wb', 0), write_through=True)
sys.stderr = io.TextIOWrapper(open(sys.stderr.fileno(), 'wb', 0), write_through=True)

# <editor-fold desc="Handling of arguments provided on stdin">
parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--args-from-input', type=bool, required=False, default=False, const=True, nargs='?')
parser.add_argument('--use-config-file', type=str, required=False, default=None,
                    help='')
parser.add_argument('--full-help', type=bool, required=False, default=False, const=True, nargs='?')
args, unknown = parser.parse_known_args()

input_args = None
if args.args_from_input:
    if args.full_help:
        input_str = ' --help'
    else:
        input_str = input("Please enter arguments now: ")
        if args.use_config_file is not None:
            input_str += ' --use-config-file {}'.format(args.use_config_file)

    input_args = shlex.split(input_str)
# </editor-fold>

# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', '--host',type=str, required=True,
                    help='FQDN or alias name of the PCE')

parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for maintainers')

parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')

parser.add_argument('--env', type=str, required=False, default=None,
                    help='Environment Label name associated to the environment you are investigating for')
parser.add_argument('--loc', type=str, required=False, default=None,
                    help='Location Label name associated to the environment you are investigating for')
parser.add_argument('--app', type=str, required=False, default=None,
                    help='Application Label name associated to the environment you are investigating for')
parser.add_argument('--cs-label', type=str, required=True, default=None,
                    help='Common Services Label/Label Group used to make the distinction between application flows and core services flows')

parser.add_argument('--opposite-ip-filter-include', type=str, required=False, default=None,
                    help='Filter on IP network for the "other" side. Use this to narrow down your investigation to only this network (CIDR notation)')

parser.add_argument('--use-config-file', type=str, required=False, default=None,
                    help='')

parser.add_argument('--skip-dns-resolution', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Traffic logs going to or from an IP address will trigger a DNS resolution of said IP address. This option will turn it off to lower processing time')

parser.add_argument('--include-reported-allowed-traffic', type=bool, nargs='?', required=False, default=False, const=True,
                    help='By default only blocked (in reported logs) traffic will be evaluated in the report, use this option to evaluate allowed traffic as well')

default_settings_logs_history_days = 30
parser.add_argument('--up-to-x-days-ago', type=int, required=False, default=default_settings_logs_history_days,
                    help='Report will extracts logs back to number of days you requested with this option. By default logs up to {} days will be extracted.'.format(default_settings_logs_history_days))

parser.add_argument('--traffic-max-results', type=int, required=False, default=100000,
                    help='Limit the number of traffic logs return by Explorer API')

parser.add_argument('--save-location', type=str, required=True, default=None,
                    help='The folder where this script will save generated Excel report')

if args.args_from_input:
    args = vars(parser.parse_args(input_args))
else:
    args = vars(parser.parse_args())

# </editor-fold>

debug = args['debug']

if debug:
    pylo.log_set_debug()
    print(args)

hostname = args['pce']
save_location = args['save_location']
org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()
use_cached_config = args['dev_use_cache']
traffic_max_results = args['traffic_max_results']
settings_skip_dns_resolution = args['skip_dns_resolution']
settings_override_config_file = args['use_config_file']
settings_include_reported_allowed_traffic = args['include_reported_allowed_traffic']

print(" * Looking for local configuration file to populate default settings...", end='')
if settings_override_config_file is not None:
    c1_config_file_loaded = c1_shared.load_config_file(filename=settings_override_config_file)
else:
    c1_config_file_loaded = c1_shared.load_config_file()

print("OK")
if c1_config_file_loaded:
    c1_shared.print_stats()



settings_opposite_ip: Optional[pylo.IP4Map] = None
if args['opposite_ip_filter_include'] is not None:
    print(" * 'Opposite IP' was provided, now parsing... ", end='')
    settings_opposite_ip = pylo.IP4Map()
    settings_opposite_ip.add_from_text(args['opposite_ip_filter_include'])
    print("OK")

if use_cached_config:
    print(" * Loading PCE Database from cached file or API if not available... ", end='', flush=True)
    if hostname.lower() in c1_shared.pce_listing:
        connector = c1_shared.pce_listing[hostname.lower()]
        if not org.load_from_cached_file(connector.hostname.lower(), no_exception_if_file_does_not_exist=True):
            print('(cache not found)... ', end='')
            org.load_from_api(connector)
    else:
        connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
        if not org.load_from_cached_file(hostname.lower(), no_exception_if_file_does_not_exist=True):
            print('(cache not found)... ', end='')
            org.load_from_api(connector)

    org.connector = connector
    print("OK!")
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    if hostname.lower() in c1_shared.pce_listing:
        connector = c1_shared.pce_listing[hostname.lower()]
    else:
        connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading and Parsing PCE Data... ", end="", flush=True)
    org.load_from_api(connector)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))

print("")
print(" - Looking for APP label '{}' in PCE database... ".format(args['app']), end='')
app_label: Optional[pylo.Label] = None
if args['app'] is not None:
    app_label = org.LabelStore.find_label_by_name_and_type(args['app'], pylo.label_type_app)
    if app_label is None:
        pylo.log.error("NOT FOUND!")
        exit(1)
    print("OK!")
else:
    print("SKIPPED")

print(" - Looking for ENV label '{}' in PCE database... ".format(args['env']), end='')
env_label: Optional[pylo.Label] = None
if args['env'] is not None:
    env_label = org.LabelStore.find_label_by_name_and_type(args['env'], pylo.label_type_env)
    if env_label is None:
        pylo.log.error("NOT FOUND!")
        exit(1)
    print("OK!")
else:
    print("SKIPPED")

print(" - Looking for LOC label '{}' in PCE database... ".format(args['loc']), end='')
loc_label: Optional[pylo.Label] = None
if args['loc'] is not None:
    loc_label = org.LabelStore.find_label_by_name_and_type(args['loc'], pylo.label_type_loc)
    if loc_label is None:
        pylo.log.error("NOT FOUND!")
        exit(1)
    print("OK!")
else:
    print("SKIPPED")

print(" - Looking for CoreServices label '{}' in PCE database...".format(args['cs_label']), end='')
find_cs_label = org.LabelStore.find_label_by_name_whatever_type(args['cs_label'])
if find_cs_label is None:
    pylo.log.error("NOT FOUND!\n\n** Please check that you didn't misspell the name (case-sensitive)**")
    exit(1)
print(" OK!")

cs_labels: Dict[str, pylo.Label] = {}
if not find_cs_label.is_group():
    cs_labels = {find_cs_label.href: find_cs_label}
else:
    for label in find_cs_label.expand_nested_to_array():
        cs_labels[label.href] = label


log_filter_from_x_days = args['up_to_x_days_ago']
print(" - Report will look for traffic data over the past {} days".format(log_filter_from_x_days))


now = datetime.now()
output_filename_traffic_inbound = '{}/report_traffic_inbound_{}-{}-{}_{}.csv'.format(save_location,
                                                          'All' if app_label is None else app_label.name,
                                                          'All' if env_label is None else env_label.name,
                                                          'All' if loc_label is None else loc_label.name,
                                                          now.strftime("%Y%m%d-%H%M%S"))

output_filename_traffic_outbound = '{}/report_traffic_outbound_{}-{}-{}_{}.csv'.format(save_location,
                                                                                     'All' if app_label is None else app_label.name,
                                                                                     'All' if env_label is None else env_label.name,
                                                                                     'All' if loc_label is None else loc_label.name,
                                                                                     now.strftime("%Y%m%d-%H%M%S"))

output_filename_estate = '{}/report_estate_{}.csv'.format(save_location,
                                                       now.strftime("%Y%m%d-%H%M%S"))

output_filename_rulesets = '{}/report_rulesets_{}.csv'.format(save_location,
                                                                             now.strftime("%Y%m%d-%H%M%S"))

output_filename_labels = '{}/report_labels_{}.csv'.format(save_location,
                                                                    now.strftime("%Y%m%d-%H%M%S"))

output_filename_labelgroups = '{}/report_labelgroups_{}.csv'.format(save_location,
                                                          now.strftime("%Y%m%d-%H%M%S"))


output_filename_iplists = '{}/report_iplists_{}.csv'.format(save_location,
                                                                 now.strftime("%Y%m%d-%H%M%S"))


output_filename_services = '{}/report_services_{}.csv'.format(save_location,
                                                            now.strftime("%Y%m%d-%H%M%S"))



# <editor-fold desc="Workload export to Excel">
csv_file = pylo.Helpers.ArrayToExport(['hostname', 'role', 'application', 'environment', 'location',
                                       'mode', 'interfaces', 'ven version', 'os', 'os_detail', 'href'])

print()
workload_for_report = org.WorkloadStore.find_workloads_matching_all_labels([app_label, env_label, loc_label])
print(" * Processing {} workloads data... ".format(len(workload_for_report)), end='')
for workload in workload_for_report.values():
    data = {'hostname': workload.hostname,
            'href': workload.href}

    if workload.roleLabel is not None:
        data['role'] = workload.roleLabel.href

    if workload.applicationLabel is not None:
        data['application'] = workload.applicationLabel.href

    if workload.environmentLabel is not None:
        data['environment'] = workload.environmentLabel.href

    if workload.locationLabel is not None:
        data['location'] = workload.locationLabel.href

    data['interfaces'] = workload.interfaces_to_string(separator=',', show_ignored=False)

    
    if not workload.unmanaged:
        data['ven version'] = workload.ven_agent.software_version.version_string
        data['mode'] = workload.ven_agent.mode
        if data['mode'] == 'test':
            data['mode'] = 'monitoring'
        data['os'] = workload.os_id
        data['os_detail'] = workload.os_detail
    else:
        data['ven version'] = 'not applicable'
        data['mode'] = 'unmanaged'

    csv_file.add_line_from_object(data)
csv_file.write_to_csv(output_filename_estate)
print("OK!")
# </editor-fold>

# <editor-fold desc="Labels export">
csv_file = pylo.Helpers.ArrayToExport(['name', 'href', 'type'])
print()
labels = org.LabelStore.get_labels_no_groups().values()
print(" * Processing {} Labels data... ".format(len(workload_for_report)), end='')
for label in labels:
    data = {'name': label.name,
            'href': label.href,
            'type': label.type_string()
            }

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_labels)
print("OK!")
# </editor-fold>

# <editor-fold desc="Label Groups export">
csv_file = pylo.Helpers.ArrayToExport(['name', 'href', 'type', 'members'])
print()
labels = org.LabelStore.get_label_groups().values()
print(" * Processing {} Label Groups data... ".format(len(workload_for_report)), end='')
for label in labels:
    data = {'name': label.name,
            'href': label.href,
            'type': label.type_string(),
            'members': pylo.string_list_to_text(label.get_members().keys())
            }

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_labelgroups)
print("OK!")
# </editor-fold>

# <editor-fold desc="IPLists export">
csv_file = pylo.Helpers.ArrayToExport(['name', 'href', 'members'])
print()
iplists = org.IPListStore.itemsByHRef.values()
print(" * Processing {} IPLIsts data... ".format(len(workload_for_report)), end='')
for iplist in iplists:
    data = {'name': iplist.name,
            'href': iplist.href,
            'members': iplist.get_raw_entries_as_string_list()
            }

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_iplists)
print("OK!")
# </editor-fold>

# <editor-fold desc="Services export">
csv_file = pylo.Helpers.ArrayToExport(['name', 'href', 'members'])
print()
services = org.ServiceStore.itemsByHRef.values()
print(" * Processing {} Services data... ".format(len(workload_for_report)), end='')
for service in services:
    data = {'name': service.name,
            'href': service.href,
            'members': pylo.string_list_to_text(service.get_entries_str_list(procolFirst=False))
            }

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_services)
print("OK!")
# </editor-fold>

# <editor-fold desc="Rulesets Export">
print(" * Requesting matching Rules&Rulesets from PCE... ", end='')


rules_query = connector.new_RuleSearchQuery()

rules_results = rules_query.execute_and_resolve(organization=org)
print("OK!  (received {} rules)".format(rules_results.count_results()))

csv_file = pylo.Helpers.ArrayToExport(['ruleset_name',
                                       'ruleset_href',
                                       'rule_href',
                                       'scopes',
                                       'consumers',
                                       'providers',
                                       'services'
                                       ])


def rule_actors_to_str(container: 'pylo.RuleHostContainer') -> str:
    result = ''

    if container.contains_all_workloads():
        if len(result) > 1:
            result += ","
        result += "All Workloads"

    labels = container.get_role_labels()
    if len(labels) > 0:
        if len(result) > 1:
            result += ","
        result += pylo.obj_with_href_list_to_text(labels)

    labels = container.get_app_labels()
    if len(labels) > 0:
        if len(result) > 1:
            result += ","
        result += pylo.obj_with_href_list_to_text(labels)

    labels = container.get_env_labels()
    if len(labels) > 0:
        if len(result) > 1:
            result += ","
        result += pylo.obj_with_href_list_to_text(labels)

    labels = container.get_loc_labels()
    if len(labels) > 0:
        if len(result) > 1:
            result += ","
        result += pylo.obj_with_href_list_to_text(labels)

    iplists = container.get_iplists()
    if len(iplists) > 0:
        if len(result) > 1:
            result += ","
        result += pylo.obj_with_href_list_to_text(iplists)

    return result


def rule_services_to_str(container: 'pylo.RuleServiceContainer') -> str:
    result = ''

    directs = container.get_direct_services()
    if len(directs) > 0:
        if len(result) > 1:
            result += ","

        directs_str_list = []
        for direct in directs:
            directs_str_list.append(direct.to_string_standard(protocol_first=False))

        result += pylo.string_list_to_text(directs_str_list)

    services = container.get_services()
    if len(services) > 0:
        for service in services:
            if len(result) > 1:
                result += ','
            result += service.href

    return result


for ruleset, rules in rules_results.rules_per_ruleset.items():

    if ruleset.description.startswith('COMMON SERVICES'):
        continue

    for rule in rules.values():
        data = {
            'ruleset_name': ruleset.name,
            'ruleset_href': ruleset.href,
            'rule_href': rule.href,
            'scopes': ruleset.scopes.get_all_scopes_str(label_separator='|', scope_separator=",", use_href=True),
            'extra_scope': rule.is_extra_scope(),
            'consumers': rule_actors_to_str(rule.consumers),
            'providers': rule_actors_to_str(rule.providers),
            'services': rule_services_to_str(rule.services)
        }
        csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_rulesets)
# </editor-fold>


# <editor-fold desc="Inbound traffic handling">
csv_file = pylo.Helpers.ArrayToExport([ 'src_href', 'src_ip', 'src_hostname', 'src_role', 'src_application', 'src_environment', 'src_location', 'src_iplists',
                                        'dst_href', 'dst_ip', 'dst_hostname', 'dst_role', 'dst_application', 'dst_environment', 'dst_location', 'dst_iplists',
                                        'dst_port', 'count', 'pd', 'last_seen', 'first_seen',
                                        'process_name', 'username'])


print(" * Requesting 'Inbound' traffic records from PCE... ", end='')
explorer_inbound_filters = connector.ExplorerFilterSetV1(max_results=traffic_max_results)
if app_label is not None:
    explorer_inbound_filters.provider_include_label(app_label)
if env_label is not None:
    explorer_inbound_filters.provider_include_label(env_label)
if loc_label is not None:
    explorer_inbound_filters.provider_include_label(loc_label)
explorer_inbound_filters.set_time_from_x_days_ago(log_filter_from_x_days)
# if not settings_include_reported_allowed_traffic:
#    explorer_inbound_filters.filter_on_policy_decision_all_blocked()
explorer_inbound_filters.set_exclude_broadcast(exclude=True)
# explorer_inbound_filters.consumer_exclude_ip4map(c1_shared.excluded_ranges)
# explorer_inbound_filters.provider_exclude_ip4map(c1_shared.excluded_broadcast)
# if settings_opposite_ip is not None:
#     explorer_inbound_filters.consumer_include_ip4map(settings_opposite_ip)
# for service in c1_shared.excluded_direct_services:
#     explorer_inbound_filters.service_exclude_add(service)
# for process in c1_shared.excluded_processes:
#     explorer_inbound_filters.process_exclude_add(process, emulate_on_client=True)
inbound_results = connector.explorer_search(explorer_inbound_filters)
print("OK!  (received {} rows)".format(inbound_results.count_results()))

all_records = inbound_results.get_all_records(draft_mode=True)

pylo.clock_start('inbound_log_process')
print(" * Processing 'Inbound' traffic logs... ", end='')
count_dns_resolutions = 0
for record in all_records:

    if record.draft_mode_policy_decision_is_allowed():
        continue

    src_workload: Optional[Workload] = record.get_source_workload(org)
    dst_workload: Optional[Workload] = record.get_destination_workload(org)

    if dst_workload is None:
        continue

    data = None
    if src_workload is not None:
        data = {
                'src_href': src_workload.href,
                'src_ip': record.source_ip,
                'src_hostname': src_workload.get_name(),
                'src_role': src_workload.get_label_href_by_type('role'),
                'src_application': src_workload.get_label_href_by_type('app'),
                'src_environment': src_workload.get_label_href_by_type('env'),
                'src_location': src_workload.get_label_href_by_type('loc'),
                'dst_href': record.destination_workload_href,
                'dst_ip': record.destination_ip,
                'dst_hostname': dst_workload.get_name(),
                'dst_role': dst_workload.get_label_href_by_type('role'),
                'dst_application': dst_workload.get_label_href_by_type('app'),
                'dst_environment': dst_workload.get_label_href_by_type('env'),
                'dst_location': dst_workload.get_label_href_by_type('loc'),
                'dst_port': record.service_to_str(protocol_first=False),
                'count': record.num_connections,
                'pd': record.policy_decision_string,
                'last_seen': record.last_detected,
                'first_seen': record.first_detected,
                'process_name': record.process_name,
                'username': record.username
                }

    else:
        reverse_dns = None

        data = {
            'src_ip': record.source_ip,
            # 'src_name': reverse_dns,
            'src_iplists': pylo.obj_with_href_list_to_text(record.get_source_iplists(org).values(), ","),
            'dst_href': record.destination_workload_href,
            'dst_ip': record.destination_ip,
            'dst_hostname': dst_workload.get_name(),
            'dst_role': dst_workload.get_label_href_by_type('role'),
            'dst_application': dst_workload.get_label_href_by_type('app'),
            'dst_environment': dst_workload.get_label_href_by_type('env'),
            'dst_location': dst_workload.get_label_href_by_type('loc'),
            'dst_port': record.service_to_str(protocol_first=False),
            'count': record.num_connections,
            'pd': record.policy_decision_string,
            'last_seen': record.last_detected,
            'first_seen': record.first_detected,
            'process_name': record.process_name,
            'username': record.username}

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_traffic_inbound)
print("OK! (exec_time:{}, dns_count:{})".format(pylo.clock_elapsed_str('inbound_log_process'), count_dns_resolutions))
# </editor-fold>


# <editor-fold desc="Outbound traffic handling">
csv_file = pylo.Helpers.ArrayToExport(['src_href', 'src_ip', 'src_hostname', 'src_role', 'src_application', 'src_environment', 'src_location', 'src_iplists',
                                       'dst_href', 'dst_ip', 'dst_hostname', 'dst_role', 'dst_application', 'dst_environment', 'dst_location', 'dst_iplists',
                                       'dst_port', 'count', 'pd', 'last_seen', 'first_seen',
                                       'process_name', 'username'])


print(" * Requesting 'Outbound' traffic records from PCE... ", end='')
explorer_outbound_filters = connector.ExplorerFilterSetV1(max_results=traffic_max_results)
if app_label is not None:
    explorer_outbound_filters.consumer_include_label(app_label)
if env_label is not None:
    explorer_outbound_filters.consumer_include_label(env_label)
if loc_label is not None:
    explorer_outbound_filters.consumer_include_label(loc_label)
explorer_outbound_filters.set_time_from_x_days_ago(log_filter_from_x_days)
#if not settings_include_reported_allowed_traffic:
#    explorer_outbound_filters.filter_on_policy_decision_all_blocked()
explorer_outbound_filters.set_exclude_broadcast(exclude=True)
# explorer_outbound_filters.provider_exclude_ip4map(c1_shared.excluded_ranges)
# explorer_outbound_filters.provider_exclude_ip4map(c1_shared.excluded_broadcast)
if settings_opposite_ip is not None:
    explorer_outbound_filters.provider_include_ip4map(settings_opposite_ip)
# for service in c1_shared.excluded_direct_services:
#     explorer_outbound_filters.service_exclude_add(service)
# for process in c1_shared.excluded_processes:
#     explorer_outbound_filters.process_exclude_add(process, emulate_on_client=True)
outbound_results = connector.explorer_search(explorer_outbound_filters)
print("OK!  (received {} rows)".format(outbound_results.count_results()))

all_records = inbound_results.get_all_records(draft_mode=True)

pylo.clock_start('outbound_log_process')
print(" * Processing 'Outbound' traffic logs... ", end='')
count_dns_resolutions = 0
for record in all_records:

    if record.draft_mode_policy_decision_is_allowed():
        continue

    src_workload: Optional[Workload] = record.get_source_workload(org)
    dst_workload: Optional[Workload] = record.get_destination_workload(org)

    if src_workload is None:
        continue

    data = None
    if dst_workload is not None:

        data = {
            'src_href': record.source_workload_href,
            'src_ip': record.source_ip,
            'src_hostname': src_workload.get_name(),
            'src_role': src_workload.get_label_href_by_type('role'),
            'src_application': src_workload.get_label_href_by_type('app'),
            'src_environment': src_workload.get_label_href_by_type('env'),
            'src_location': src_workload.get_label_href_by_type('loc'),
            'dst_href': record.source_workload_href,
            'dst_ip': record.destination_ip,
            'dst_hostname': dst_workload.get_name(),
            'dst_role': dst_workload.get_label_href_by_type('role'),
            'dst_application': dst_workload.get_label_href_by_type('app'),
            'dst_environment': dst_workload.get_label_href_by_type('env'),
            'dst_location': dst_workload.get_label_str_by_type('loc'),
            'dst_port': record.service_to_str(protocol_first=False),
            'count': record.num_connections,
            'pd': record.policy_decision_string,
            'last_seen': record.last_detected,
            'first_seen': record.first_detected,
            'process_name': record.process_name,
            'username': record.username,
        }

    else:

        data = {
            'src_href': record.source_workload_href,
            'src_ip': record.source_ip,
            'src_hostname': src_workload.get_name(),
            'src_role': src_workload.get_label_href_by_type('role'),
            'src_application': src_workload.get_label_href_by_type('app'),
            'src_environment': src_workload.get_label_href_by_type('env'),
            'src_location': src_workload.get_label_href_by_type('loc'),
            'dst_iplists': pylo.obj_with_href_list_to_text(record.get_destination_iplists(org).values(), ','),
            'dst_ip': record.destination_ip,
            # 'dst_name': reverse_dns,
            'dst_port': record.service_to_str(protocol_first=False),
            'count': record.num_connections,
            'pd': record.policy_decision_string,
            'last_seen': record.last_detected,
            'first_seen': record.first_detected,
            'process_name': record.process_name,
            'username': record.username
        }

    csv_file.add_line_from_object(data)

csv_file.write_to_csv(output_filename_traffic_outbound)
print("OK! (exec_time:{}, dns_count:{})".format(pylo.clock_elapsed_str('outbound_log_process'), count_dns_resolutions))
# </editor-fold>


print("OK! ****")

