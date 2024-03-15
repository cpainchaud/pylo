from typing import List
import illumio_pylo as pylo
import argparse
import sys
import math
import colorama
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'workload-reset-ven-names-to-null'
objects_load_filter = ['workloads', 'labels']


def fill_parser(parser: argparse.ArgumentParser):
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

    #iterate through each workload
    for wkl in org.WorkloadStore.itemsByHRef.values():
        #only care about Managed workloads
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

    # <editor-fold desc="JSON Payloads generation">

    #for each batch of workloads, generate a JSON payload to send to the PCE to reset name to null
    #the payload will be a list of objects with the following structure:
    # {
    #     "href": "string",
    #     "name": null
    # }

    if not confirmed_changes:
        print(colorama.Fore.YELLOW + "Changes have not been confirmed. Use the --confirm flag to confirm the changes and push to the PCE")
        #reset colorama colors
        print(colorama.Style.RESET_ALL)
        return

    # for loop for each batch of workloads
    for i in range(math.ceil(len(workloads_with_mismatching_names) / batch_size)):
        #get the next batch of workloads
        batch = workloads_with_mismatching_names[i * batch_size: (i + 1) * batch_size]
        #create a list of objects with the structure described above
        payload = [{"href": wkl.href, "name": wkl.static_name_stripped_fqdn(wkl.hostname)} for wkl in batch]
        #debug display
        print(f"Sending payload for batch {i + 1} of {math.ceil(len(workloads_with_mismatching_names) / batch_size)} ({len(payload)} workloads)")

        org.connector.objects_workload_update_bulk(payload)

    # </editor-fold>


command_object = Command(command_name, __main, fill_parser, objects_load_filter)