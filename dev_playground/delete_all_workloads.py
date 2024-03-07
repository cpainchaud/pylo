import sys

from illumio_pylo import log
import logging

sys.path.append('..')

import argparse
import illumio_pylo as pylo

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--debug', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Enabled extra debug output')

args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()

targetHostname = args['pce']
numberOfWorkloadsPerBatch = 100

print("Loading Organization's data from API... ", flush=True, end='')
target = pylo.Organization.get_from_api_using_credential_file(targetHostname)
print("done!")

print("Organisation Statistics:\n", target.stats_to_str())

print("\n")

deletedWorkloads = 0

href_list = list(target.WorkloadStore.itemsByHRef.keys())

while deletedWorkloads < len(target.WorkloadStore.itemsByHRef):
    print(" - Deleting workloads " + str(deletedWorkloads+1) + "-" + str(numberOfWorkloadsPerBatch+deletedWorkloads) +
          " of " + str(len(target.WorkloadStore.itemsByHRef)))
    href_to_delete = []
    for i in range(0, numberOfWorkloadsPerBatch):
        if deletedWorkloads+i >= len(target.WorkloadStore.itemsByHRef):
            continue
        href_to_delete.append(href_list[deletedWorkloads+i])

    target.connector.objects_workload_delete_multi(href_to_delete)
    deletedWorkloads += numberOfWorkloadsPerBatch






