
import illumio_pylo as pylo
import sys
import argparse

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

args = vars(parser.parse_args())

hostname = args['pce']


org = pylo.Organization(1)

print("Loading Origin PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
org.load_from_cache_or_saved_credentials(hostname)
print("OK!\n")

# pylo.log_set_debug()

for workload in org.WorkloadStore.itemsByHRef.values():
    if workload.deleted or workload.temporary:
        continue
    print(" - updating wkl '{}' ... ".format(workload.name), end='', flush=True)
    workload.api_update_description('hello')
    print("Done!")


print("\nEND OF SCRIPT\n")

