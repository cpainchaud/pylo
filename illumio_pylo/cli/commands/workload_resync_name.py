from typing import List
import illumio_pylo as pylo
import argparse
import math
import colorama
from . import Command

command_name = 'workload-resync-names'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
    parser.description = ("This command looks for Managed workloads that have a forced name that doesn't match their"
                          " hostname and resets the name to be null (which will cause the PCE to recalculate the name"
                          " based on hostname and labels). This is useful for fixing workloads that had their names"
                          " forced in the past but now have a hostname that matches what you want the name to be."
                          " The command will first do an analysis and show you how many workloads would be changed,"
                          " then you can confirm to implement the changes in the PCE.")
    parser.add_argument('--confirm', action='store_true',
                        help="No change will be implemented in the PCE until you use this function to confirm you're good with them after review")
    parser.add_argument('--batch-size', type=int, required=False, default=500,
                        help='Number of Workloads to update per API call')


def __main(args, org: pylo.Organization, **kwargs):
    batch_size = args['batch_size']
    confirmed_changes = args['confirm']

    count_managed_workloads = 0
    count_workloads_with_forced_names = 0
    count_workloads_with_mismatching_names = 0

    workloads_with_forced_names: List[pylo.Workload] = []
    workloads_with_mismatching_names: List[pylo.Workload] = []

    # iterate through each workload
    for wkl in org.WorkloadStore.itemsByHRef.values():
        # only care about Managed workloads
        if wkl.unmanaged:
            continue

        count_managed_workloads += 1
        if wkl.forced_name is not None:
            workloads_with_forced_names.append(wkl)
            short_forced_name = wkl.static_name_stripped_fqdn(wkl.forced_name).lower()
            short_hostname = wkl.static_name_stripped_fqdn(wkl.hostname).lower()
            if short_forced_name != short_hostname:
                workloads_with_mismatching_names.append(wkl)
                print(f"Found mismatching forced name for {wkl.hostname} (hostname={wkl.forced_name})")

    print()
    print(" * Summary of Analysis:")

    print(f" - Found {count_managed_workloads} Managed Workloads")
    print(f" - Found {len(workloads_with_forced_names)} Workloads with Forced Names")
    print(f" - Found {len(workloads_with_mismatching_names)} Workloads with Mismatching Forced Names")

    if not confirmed_changes:
        print(colorama.Fore.YELLOW + "Changes have not been confirmed. Use the --confirm flag to confirm the changes and push to the PCE")
        # reset colorama colors
        print(colorama.Style.RESET_ALL)
        return

    failed_workloads = 0
    # for loop for each batch of workloads
    for i in range(math.ceil(len(workloads_with_mismatching_names) / batch_size)):
        # get the next batch of workloads
        batch = workloads_with_mismatching_names[i * batch_size: (i + 1) * batch_size]
        payload = []
        for wkl in batch:
            stripped_hostname = wkl.static_name_stripped_fqdn(wkl.hostname)
            payload.append({"href": wkl.href, "name": stripped_hostname})
        # debug display
        print(f"Sending payload for batch {i + 1} of {math.ceil(len(workloads_with_mismatching_names) / batch_size)} ({len(payload)} workloads)")

        update_bulk_status = org.connector.objects_workload_update_bulk(payload)
        # check each response entry for success/failure and surface any errors
        if isinstance(update_bulk_status, list):
            batch_failed = False
            for status_entry in update_bulk_status:
                href = status_entry.get('href', 'unknown href')
                status = status_entry.get('status', 'unknown status')
                message = status_entry.get('message', '')
                workload_object = org.WorkloadStore.itemsByHRef.get(href)
                display_name = workload_object.name if workload_object and workload_object.name else status_entry.get('name', 'unknown workload')
                if status == 'updated':
                    print(colorama.Fore.GREEN + f" - Updated {display_name} ({href})" + colorama.Style.RESET_ALL)
                else:
                    batch_failed = True
                    failed_workloads += 1
                    print(colorama.Fore.RED + f" - Failed to update {display_name} ({href}): {status}" + colorama.Style.RESET_ALL)
                    if message:
                        print(f"   {message}")
            if batch_failed:
                print(colorama.Fore.YELLOW + "One or more workloads in this batch failed to update." + colorama.Style.RESET_ALL)
        else:
            print(colorama.Fore.RED + "Bulk update response was not a list, unable to verify each workload." + colorama.Style.RESET_ALL)
    if failed_workloads:
        print(colorama.Fore.YELLOW + f"\nTotal failed workloads: {failed_workloads}" + colorama.Style.RESET_ALL)



command_object = Command(command_name, __main, fill_parser, objects_load_filter)
