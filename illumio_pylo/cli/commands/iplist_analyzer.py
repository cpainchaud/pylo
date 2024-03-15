import argparse
from datetime import datetime
from typing import Dict, List

import illumio_pylo as pylo
from . import Command

command_name = "iplist-analyzer"
objects_load_filter = ['iplists', 'workloads']

def fill_parser(parser: argparse.ArgumentParser):
    pass

def __main(args, org: pylo.Organization, **kwargs):
    now = datetime.now()
    report_file = 'iplist-analyzer_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
    report_file_excel = 'iplist-analyzer_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))

    filter_csv_expected_fields = []
    filter_data = None

    csv_report_headers = ['name', 'members', 'ip4_mapping', 'ip4_count', 'ip4_uncovered_count', 'covered_workloads_count',
                          'covered_workloads_list', 'covered_workloads_appgroups', 'href']

    csv_report: pylo.ArrayToExport = pylo.ArrayToExport(csv_report_headers)


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
        add_iplist_to_report(iplist, workloads_ip4maps_cache, csv_report)

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



command_object = Command(command_name, __main, fill_parser, objects_load_filter)



def add_iplist_to_report(iplist: pylo.IPList, workloads_ip4maps_cache: Dict[pylo.Workload, pylo.IP4Map],
                         csv_report: pylo.ArrayToExport):

    appgroup_tracker: Dict[str, bool] = {}

    print("  - {}/{}".format(iplist.name, iplist.href))

    ip_map = iplist.get_ip4map()

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
        #print(ip_map.print_to_std(header="after subtraction", padding="     "))


    new_row['ip4_uncovered_count'] = ip_map.count_ips()
    new_row['covered_workloads_count'] = len(matched_workloads)
    new_row['covered_workloads_list'] = pylo.string_list_to_text(matched_workloads, "\n")
    new_row['covered_workloads_appgroups'] = pylo.string_list_to_text(appgroup_tracker.keys(), "\n")

    csv_report.add_line_from_object(new_row)


