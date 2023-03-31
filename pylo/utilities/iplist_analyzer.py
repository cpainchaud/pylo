import os
import sys
import argparse
import math
from datetime import datetime
from typing import Dict, List, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import pylo


# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')
parser.add_argument('--verbose', '-v', action='store_true',
                    help='')

# </editor-fold>


args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['pce']
verbose = args['verbose']
use_cached_config = args['dev_use_cache']


now = datetime.now()
report_file = 'iplist-analyzer_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
report_file_excel = 'iplist-analyzer_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))

filter_csv_expected_fields = []
filter_data = None


csv_report_headers = ['name', 'members', 'ip4_mapping', 'ip4_count', 'ip4_uncovered_count', 'covered_workloads_count',
                      'covered_workloads_list', 'covered_workloads_appgroups', 'href']

csv_report = pylo.ArrayToExport(csv_report_headers)


# <editor-fold desc="PCE Configuration Download and Parsing">
org = pylo.Organization(1)
if use_cached_config:
    print(" * Loading cached PCE data from disk...")
    org.load_from_cache_or_saved_credentials(hostname)
    print("OK!")
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading objects data from the PCE... ", end="", flush=True)
    config = connector.get_pce_objects()
    print("OK!")

    print(" * Parsing PCE objects data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.connector = connector
    org.load_from_json(config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))
# </editor-fold>


def add_iplist_to_report(iplist: pylo.IPList):

    appgroup_tracker: Dict[str, bool] = {}

    print("  - {}/{}".format(iplist.name, iplist.href))

    ip_map = iplist.get_ip4map()
    #print(ip_map.print_to_std(padding="     "))

    new_row = {
        'name': iplist.name,
        'href': iplist.href,
        'members': iplist.get_raw_entries_as_string_list(separator="\n"),
        'ip4_mapping': ip_map.to_string_list(),
        'ip4_count': ip_map.count_ips()
    }

    matched_workloads: List[pylo.Workload] = []

    for workload, wkl_map in workloads_ip4maps_cache.items():
        affected_rows = ip_map.substract(wkl_map)
        if affected_rows > 0:
            print("matched workload   {}".format(workload.get_name()))
            matched_workloads.append(workload)
            appgroup_tracker[workload.get_appgroup_str()] = True
        #print(ip_map.print_to_std(header="after substraction", padding="     "))


    new_row['ip4_uncovered_count'] = ip_map.count_ips()
    new_row['covered_workloads_count'] = len(matched_workloads)
    new_row['covered_workloads_list'] = pylo.string_list_to_text(matched_workloads, "\n")
    new_row['covered_workloads_appgroups'] = pylo.string_list_to_text(appgroup_tracker.keys(), "\n")

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
for iplist in org.IPListStore.items_by_href.values():
    ip_map = iplist.get_ip4map()
    iplists_ip4maps_cache[iplist] = ip_map
print("OK")
# </editor-fold>


print(" * Now analyzing IPLists:", flush=True)
for (iplist, ip_map) in iplists_ip4maps_cache.items():
    add_iplist_to_report(iplist)

print(" ** DONE **")

print()
print(" * Writing report file '{}' ... ".format(report_file), end='', flush=True)
csv_report.write_to_csv(report_file)
print("DONE")
print(" * Writing report file '{}' ... ".format(report_file_excel), end='', flush=True)
csv_report.write_to_excel(report_file_excel)
print("DONE")

if csv_report.lines_count() < 1:
    print("\n** WARNING: no entry matched your filters so reports are empty !\n")


