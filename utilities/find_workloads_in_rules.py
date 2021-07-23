import os
import sys
from typing import Dict
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True, help='hostname of the PCE')
parser.add_argument('--only-deleted', type=bool, required=False, nargs='?', const=True, help='only look for deleted workloads')
parser.add_argument('--use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')

args = vars(parser.parse_args())

hostname = args['pce']
settings_only_delete_workloads = args['only_deleted']
settings_use_cache = args['use_cache']

org = pylo.Organization(1)

print(" * Loading PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
if settings_use_cache:
    org.load_from_cache_or_saved_credentials(hostname)
else:
    org.load_from_saved_credentials(hostname, include_deleted_workloads=True)
print("OK!\n")

print(" * PCE statistics: ")
print(org.stats_to_str(padding='    '))

print()
print("**** Now parsing workloads and rules.... ****")

workloads_to_inspect = org.WorkloadStore.itemsByHRef.values()
global_count_concerned_workloads = 0
global_concerned_rulesets = {}
global_concerned_rules = {}


for workload in workloads_to_inspect:

    if settings_only_delete_workloads:
        if workload.temporary is True or workload.deleted is True:
            continue

    concerned_rulesets: Dict[pylo.Ruleset, Dict[pylo.Rule, pylo.Rule]] = {}
    count_concerned_rules = 0
    for referencer in workload.get_references():
        if type(referencer) is pylo.RuleHostContainer:
            concerned_rule = referencer.owner  # type: pylo.Rule
            concerned_ruleset = concerned_rule.owner

            global_concerned_rulesets[concerned_ruleset] = True
            global_concerned_rules[concerned_rule] = True

            if concerned_ruleset not in concerned_rulesets:
                    concerned_rulesets[concerned_ruleset] = {concerned_rule: concerned_rule}
                    count_concerned_rules = count_concerned_rules + 1
            else:
                if concerned_rule not in concerned_rulesets[concerned_ruleset]:
                    concerned_rulesets[concerned_ruleset][concerned_rule] = concerned_rule
                    count_concerned_rules = count_concerned_rules + 1

    if len(concerned_rulesets) < 1:  # this workload was not used in any ruleset
        continue

    global_count_concerned_workloads += 1

    print(" - Workload {} HREF {} is used {} Rulesets and {} Rules".format(workload.name, workload.href, len(concerned_rulesets), count_concerned_rules))
    for ruleset in concerned_rulesets:
        print("   - in ruleset '{}' HREF:{}".format(ruleset.name, ruleset.href))


def get_name(obj):
    return obj.name


tmp_rulesets = sorted(global_concerned_rulesets.keys(), key=get_name)

print("\n* For convenience here is the consolidated list of Rulesets:")
for ruleset in tmp_rulesets:
    ruleset_url = ruleset.get_ruleset_url()
    print("  - '{}' HREF: {} URL: {}".format(ruleset.name, ruleset.href, ruleset_url))


print("\n*****  DONE with workloads & rules parsing  *****")
print("** Total: {} Workloads used in {} Rulesets and {} Rules".format(   global_count_concerned_workloads,
                                                                            len(global_concerned_rulesets),
                                                                            len(global_concerned_rules)))

print("\n**** END OF SCRIPT ****\n")

