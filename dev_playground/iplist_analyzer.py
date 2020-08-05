import os
import sys
import argparse
import math
from datetime import datetime
from typing import Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo


# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')
parser.add_argument('--verbose', '-v', type=bool, nargs='?', required=False, default=False, const=True,
                    help='')

# </editor-fold>


args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['host']
verbose = args['verbose']
use_cached_config = args['dev_use_cache']


now = datetime.now()
report_file = 'iplist-analyzer_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
report_file_excel = 'iplist-analyzer_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))

filter_csv_expected_fields = []
filter_data = None



csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'online', 'managed',
                      'status', 'agent.last_heartbeat', 'agent.sec_policy_sync_state', 'agent.sec_policy_applied_at',
                      'href', 'agent.href']

csv_report = pylo.ArrayToExport(csv_report_headers)


# <editor-fold desc="PCE Configuration Download and Parsing">
org = pylo.Organization(1)
if use_cached_config:
    print(" * Loading cached PCE data from disk...")
    org.load_from_cache_or_saved_credentials(hostname)
    print("OK!")
else:
    fake_config = pylo.Organization.create_fake_empty_config()

    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading Workloads/Agents listing from the PCE... ", end="", flush=True)
    fake_config['workloads'] = connector.objects_workload_get()
    print("OK!")

    print(" * Downloading Labels listing from the PCE... ", end="", flush=True)
    fake_config['labels'] = connector.objects_label_get()
    print("OK!")

    print(" * Downloading IPLists listing from the PCE... ", end="", flush=True)
    fake_config['iplists'] = connector.objects_iplist_get()
    print("OK!")

    print(" * Parsing PCE data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.connector = connector
    org.load_from_json(fake_config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))
# </editor-fold>


def add_workload_to_report(wkl: pylo.Workload = None, filter=None, filter_append_prefix='_'):
    labels = workload.get_labels_list()

    def none_or_date(date):
        if date is None:
            return None
        return datetime.strftime(date, '%Y-%m-%d %H:%M:%S')

    if wkl is not None:
        new_row = {
            'name': wkl.name,
            'hostname': wkl.hostname,
            'role': labels[0],
            'app': labels[1],
            'env': labels[2],
            'loc': labels[3],
            'href': wkl.href,
            'online': wkl.online,
            'managed': not wkl.unmanaged,
            'status': wkl.get_status_string(),
        }
        if wkl.ven_agent is not None:
            new_row['agent.href'] = wkl.ven_agent.href
            new_row['agent.sec_policy_sync_state'] = wkl.ven_agent.get_status_security_policy_sync_state()
            new_row['agent.last_heartbeat'] = none_or_date(wkl.ven_agent.get_last_heartbeat_date())
            new_row['agent.sec_policy_applied_at'] = none_or_date(wkl.ven_agent.get_status_security_policy_applied_at())
    else:
        new_row = {}

    if filter is not None:
        used_filters[filter['*line*']] = True
        for field in filter:
            new_row[filter_append_prefix + field] = filter[field]

    csv_report.add_line_from_object(new_row)


# <editor-fold desc="Building Workloads ip4 Cache">
workloads_ip4maps_cache: Dict[pylo.Workload, pylo.IP4Map] = {}
print(" * Building Workloads IP4 mapping... ", end='')
for workload in org.WorkloadStore.get_managed_workloads_list():
    ip_map = workload.get_ip4map_from_interfaces()
    workloads_ip4maps_cache[workload] = ip_map
print("OK")
# </editor-fold>

# <editor-fold desc="Building IPLists ip4 Cache">
iplists_ip4maps_cache: Dict[pylo.IPList, pylo.IP4Map] = {}
print(" * Building IPLists IP4 mapping... ", end='')
for iplist in org.IPListStore.itemsByHRef.values():
    ip_map = iplist.get_ip4map_from_interfaces()
    iplists_ip4maps_cache[iplist] = ip_map
print("OK")
# </editor-fold>



print()
print(" * Writing report file '{}' ... ".format(report_file), end='', flush=True)
csv_report.write_to_csv(report_file)
print("DONE")
print(" * Writing report file '{}' ... ".format(report_file_excel), end='', flush=True)
csv_report.write_to_excel(report_file_excel)
print("DONE")

if csv_report.lines_count() < 1:
    print("\n** WARNING: no entry matched your filters so reports are empty !\n")


