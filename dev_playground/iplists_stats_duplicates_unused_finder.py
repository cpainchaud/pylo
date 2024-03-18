import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo
import argparse


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

args = vars(parser.parse_args())

hostname = args['pce']


org = pylo.Organization(1)

print("Loading PCE configuration from '{}' or cached file... ".format(hostname), end="", flush=True)
org.load_from_cache_or_saved_credentials(hostname, prompt_for_api_key_if_missing=True)
print("OK!\n")

unique_hashes = {}
unique_ipranges = {}
total_ipranges = 0
unused_iplists = []

for iplist in org.IPListStore.items_by_href.values():
    #print("- handling of iplist '{}' with {} members".format(iplist.name, iplist.count_entries()))
    entries_hash = ""
    for entry in sorted(iplist.raw_entries):
        total_ipranges += 1

        entries_hash += entry + ' '
        if entry not in unique_ipranges:
            unique_ipranges[entry] = [iplist]
        else:
            unique_ipranges[entry].append(iplist)

    #print("  - hash: {}".format(entries_hash))

    if entries_hash in unique_hashes:
        unique_hashes[entries_hash].append(iplist)
    else:
        unique_hashes[entries_hash] = [iplist]

    if iplist.count_references() == 0:
        unused_iplists.append(iplist)

    #print(pylo.nice_json(iplist.raw_json))


print("*** Listing Unused IPLists ***")
for iplist in unused_iplists:
    print(" - '{}' HREF:{}".format(iplist.name, iplist.href))


print("*** Listing Duplicate IPLists ***")
for hash in unique_hashes:
    if len(unique_hashes[hash]) <= 1:
        continue
    print(" - hash {}'".format(hash))
    for iplist in unique_hashes[hash]:
        print("    - {} HREF:{}".format(iplist.name, iplist.href))


print("\nIPlist count: {}\nUnique iplists: {}\nIPRanges count: {}\nUnique IPRanges: {}".format(org.IPListStore.count(),
                                                                                               len(unique_hashes),
                                                                                               total_ipranges,
                                                                                               len(unique_ipranges)
                                                                                               ))
print("Unused IPlists: {}".format(len(unused_iplists)))

print("\nGeneric Config statistics:\n{}".format(org.stats_to_str()))