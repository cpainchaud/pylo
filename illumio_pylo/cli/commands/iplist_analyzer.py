import argparse
from typing import Dict, List

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader
from . import Command
from .utils.report_writer import ReportWriter

command_name = "iplist-analyzer"
objects_load_filter = ['iplists', 'workloads']


def fill_parser(parser: argparse.ArgumentParser):
    # Add standard report arguments (static helper)
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='iplist-analyzer',
        default_sheet_name='iplist_analysis'
    )


def build_workloads_ip4_cache(org: pylo.Organization) -> Dict[pylo.Workload, pylo.IP4Map]:
    """
    Build IP4 mapping cache for all managed workloads.

    Args:
        org: Organization with workload store

    Returns:
        Dict mapping workloads to their IP4Maps
    """
    workloads_ip4maps_cache: Dict[pylo.Workload, pylo.IP4Map] = {}
    for workload in org.WorkloadStore.get_managed_workloads_list():
        ip_map = workload.get_ip4map_from_interfaces()
        workloads_ip4maps_cache[workload] = ip_map
    return workloads_ip4maps_cache


def build_iplists_ip4_cache(org: pylo.Organization) -> Dict[pylo.IPList, pylo.IP4Map]:
    """
    Build IP4 mapping cache for all IPLists.

    Args:
        org: Organization with IPList store

    Returns:
        Dict mapping IPLists to their IP4Maps
    """
    iplists_ip4maps_cache: Dict[pylo.IPList, pylo.IP4Map] = {}
    for iplist in org.IPListStore.items_by_href.values():
        ip_map = iplist.get_ip4map()
        iplists_ip4maps_cache[iplist] = ip_map
    return iplists_ip4maps_cache


def analyze_iplist_coverage(iplist: pylo.IPList, workloads_ip4maps_cache: Dict[pylo.Workload, pylo.IP4Map]) -> Dict:
    """
    Analyze which workloads are covered by an IPList.

    Args:
        iplist: The IPList to analyze
        workloads_ip4maps_cache: Cache of workload IP4 maps

    Returns:
        Dict with analysis results including matched workloads and coverage stats
    """
    ip_map = iplist.get_ip4map()
    ip_map_copy = iplist.get_ip4map()  # Get fresh copy for subtraction
    matched_workloads: List[pylo.Workload] = []
    appgroup_tracker: Dict[str, bool] = {}

    for workload, wkl_map in workloads_ip4maps_cache.items():
        affected_rows = ip_map_copy.substract(wkl_map)
        if affected_rows > 0:
            matched_workloads.append(workload)
            appgroup_tracker[workload.get_appgroup_str()] = True

    return {
        'iplist': iplist,
        'ip_map': ip_map,
        'ip_map_after_substraction': ip_map_copy,
        'matched_workloads': matched_workloads,
        'appgroup_tracker': appgroup_tracker
    }


def __main(args, org: pylo.Organization, **kwargs):
    # Initialize report structure and writer
    report_headers = pylo.ExcelHeaderSet([
        ExcelHeader(name='name', max_width=30, wrap_text=False),
        ExcelHeader(name='members', max_width=60),
        ExcelHeader(name='ip4_mapping', max_width=60),
        ExcelHeader(name='ip4_count', max_width=15, wrap_text=False),
        ExcelHeader(name='ip4_uncovered_count', max_width=20, wrap_text=False),
        ExcelHeader(name='covered_workloads_count', max_width=25, wrap_text=False),
        ExcelHeader(name='covered_workloads_list', max_width=80),
        ExcelHeader(name='covered_workloads_appgroups', max_width=60),
        ExcelHeader(name='href', max_width=60, wrap_text=False)
    ])

    report_writer = ReportWriter(
        headers=report_headers,
        sheet_name='iplist_analysis',
        filename_prefix='iplist-analyzer',
        force_all_wrap_text=True,
        multivalues_cell_delimiter='\n',
        args=args
    )

    # ReportWriter initialized from CLI args via constructor
    sheet = report_writer.sheet

    # Build caches
    print(" * Building Workloads IP4 mapping... ", end='', flush=True)
    workloads_ip4maps_cache = build_workloads_ip4_cache(org)
    print("OK")

    print(" * Building IPLists IP4 mapping... ", end='', flush=True)
    iplists_ip4maps_cache = build_iplists_ip4_cache(org)
    print("OK")

    # Analyze IPLists
    print(" * Now analyzing IPLists:", flush=True)
    for iplist in iplists_ip4maps_cache.keys():
        print("  - {}/{}".format(iplist.name, iplist.href))
        analysis_result = analyze_iplist_coverage(iplist, workloads_ip4maps_cache)

        # Print matched workloads
        for workload in analysis_result['matched_workloads']:
            print("matched workload   {}".format(workload.get_name()))

        add_iplist_analysis_to_report(analysis_result, sheet)

    print(" ** DONE **")
    print()

    # Write report to disk (always generate report, even if empty)
    report_writer.write_reports()

    if sheet.lines_count() < 1:
        print("\n** WARNING: no entry matched your filters so reports are empty !\n")


command_object = Command(command_name, __main, fill_parser, objects_load_filter)


def add_iplist_analysis_to_report(analysis_result: Dict, sheet: pylo.ArraysToExcel.Sheet):
    """
    Add IPList analysis results to the report sheet.

    Args:
        analysis_result: Dict containing analysis results from analyze_iplist_coverage()
        sheet: The Excel sheet to add the row to
    """
    iplist = analysis_result['iplist']
    ip_map = analysis_result['ip_map']
    ip_map_after = analysis_result['ip_map_after_substraction']
    matched_workloads = analysis_result['matched_workloads']
    appgroup_tracker = analysis_result['appgroup_tracker']

    new_row = {
        'name': iplist.name,
        'href': iplist.href,
        'members': iplist.get_raw_entries_as_string_list(separator="\n"),
        'ip4_mapping': ip_map.to_string_list(),
        'ip4_count': ip_map.count_ips(),
        'ip4_uncovered_count': ip_map_after.count_ips(),
        'covered_workloads_count': len(matched_workloads),
        'covered_workloads_list': pylo.string_list_to_text(matched_workloads, "\n"),
        'covered_workloads_appgroups': pylo.string_list_to_text(appgroup_tracker.keys(), "\n")
    }

    sheet.add_line_from_object(new_row)
