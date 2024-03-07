import illumio_pylo as pylo
import argparse
from typing import *
from . import Command

command_name = "workload-used-in-rules-finder"
objects_load_filter = ['workloads', 'rules', 'rulesets']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--only-deleted', action='store_true', help='only look for deleted workloads')


def __main(args, org: pylo.Organization, **kwargs):

    settings_only_delete_workloads = args['only_deleted']

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

        print(" - Workload {} HREF {} is used {} Rulesets and {} Rules".format(workload.get_name(), workload.href, len(concerned_rulesets), count_concerned_rules))
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


command_object = Command(command_name, __main,
                         fill_parser,
                         load_specific_objects_only=objects_load_filter,
                         skip_pce_config_loading=True)
