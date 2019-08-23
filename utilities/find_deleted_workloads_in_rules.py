import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import pylo
import argparse

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')

args = vars(parser.parse_args())

hostname = args['host']
pylo.ignoreWorkloadsWithSameName = True
pylo.log_set_debug()

org = pylo.Organization(1)

print("Loading Origin PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
org.load_from_cache_or_saved_credentials(hostname)
print("OK!\n")

deleted_workloads = []  # type: list[pylo.Workload]

print("Looking for workloads which are marked as temporary or deleted ... ", end="", flush=True)
for workload in org.WorkloadStore.itemsByHRef.values():
    if workload.temporary or workload.deleted:
        deleted_workloads.append(workload)
print("Done!  Found {} over {}\n".format(len(deleted_workloads), len(org.WorkloadStore.itemsByHRef)))


global_count_concerned_workloads = 0

global_concerned_rulesets = {}
global_concerned_rules = {}


for workload in deleted_workloads:
    concerned_rulesets = {}  # type: dict[pylo.Ruleset, dict[pylo.Rule,pylo.Rule]]
    count_concerned_rules = 0
    for referencer in workload.get_references():
        if type(referencer) is pylo.RuleHostContainer:
            concerned_rule = referencer.owner  # type: pylo.Rule
            concerned_ruleset = concerned_rule.owner

            global_concerned_rulesets[concerned_ruleset] = True
            global_concerned_rules[concerned_rule] = True

            if concerned_ruleset not in concerned_rulesets:
                concerned_rulesets[concerned_ruleset] = {concerned_rule: concerned_rule}
                count_concerned_rules += 1
            else:
                concerned_rulesets[concerned_ruleset][concerned_rule] = concerned_rule

    if len(concerned_rulesets) < 1:  # this workload was not used in any ruleset
        continue

    global_count_concerned_workloads += 1

    print(" - Workload {} HREF {} is used {} Rulesets and {} Rules".format(workload.name, workload.href, len(concerned_rulesets), count_concerned_rules))
    for ruleset in concerned_rulesets:
        print("   - in ruleset '{}' HREF:{}".format(ruleset.name, ruleset.href))


def get_name(obj):
    return obj.name


tmp_rulesets = sorted(global_concerned_rulesets.keys(), key=get_name)


print("\n* For convenience here is the list of Rulesets:")
for ruleset in tmp_rulesets:
    print("  - '{}' HREF: {}".format(ruleset.name, ruleset.href))


print("\n** Total: {} deleted Workloads used in {} Rulesets and {} Rules".format(global_count_concerned_workloads,
                                                                               len(global_concerned_rulesets),
                                                                               len(global_concerned_rules)))

print("\nEND OF SCRIPT\n")

