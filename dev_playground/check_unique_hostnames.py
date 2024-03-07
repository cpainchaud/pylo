
import illumio_pylo as pylo
import sys

originHostname='10.107.3.2'
targetHostname='ilo-emea-poc.xmp.net.intra'

exit_with_error = None


def check_unique_within_single_pce(org: pylo.Organization):

    conflicting_names = {}  # type: dict[str,list[pylo.Workload]]
    unique_names = {}  # type: dict[str,pylo.Workload]

    for workload in org.WorkloadStore.itemsByHRef.values():
        """:type workload: pylo.Workload"""
        wkl_name = workload.name.lower()
        if wkl_name in conflicting_names:
            conflicting_names[wkl_name].append(workload)
        else:
            conflicting_names[wkl_name] = []
            conflicting_names[wkl_name].append(workload)

    for name in conflicting_names.copy().keys():
        workload_array = conflicting_names[name]
        if len(workload_array) < 2:
            conflicting_names.pop(name)

    return conflicting_names


def check_conflicts_between_two_pce(origin: pylo.Organization, target: pylo.Organization):
    names = {}  # type: dict[str, pylo.Workload]
    conflicts = {}  # type: dict[str, str]

    for workload in origin.WorkloadStore.itemsByHRef.values():
        names[workload.name.lower()] = workload

    for workload in target.WorkloadStore.itemsByHRef.values():
        workload_name_lower = workload.name.lower()
        if workload_name_lower in names:
            conflicts[workload_name_lower] = workload_name_lower

    return conflicts


print("Loading Origin PCE configuration from " + originHostname + " or cached file... ", end="", flush=True)
origin = pylo.get_organization_using_credential_file(originHostname)
print("OK!")

print("Loading Target PCE configuration from " + targetHostname + " or cached file ... ", end="", flush=True)
target = pylo.get_organization_using_credential_file(targetHostname)
print("OK!")

conflicting_name_at_origin = check_unique_within_single_pce(origin)
if len(conflicting_name_at_origin) > 0:
    exit_with_error = "Origin does have conflicting hostnames, please fix before you can continue"

    print("** Now showing all Workloads ({}) with same name on Origin PCE {}:".
          format(len(conflicting_name_at_origin),
                 targetHostname))
    for name in conflicting_name_at_origin:
        print(" - '%s'" % name)
else:
    print("** ORIGIN has no conflicting hostnames within itself, good news!!")


conflicting_name_at_target = check_unique_within_single_pce(target)
if len(conflicting_name_at_target) > 0:
    exit_with_error = "Target does have conflicting hostnames, please fix before you can continue"

    print("** Now showing all Workloads ({}) with same name on Target PCE {}:".
          format(   len(conflicting_name_at_target),
                    targetHostname))
    for name in conflicting_name_at_target:
        print(" - '%s' with %i occurences" % (name, len(conflicting_name_at_target[name])))
else:
    print("** ORIGIN has no conflicting hostnames within itself, good news!!")

print()

inter_pce_conflicts = check_conflicts_between_two_pce(origin, target)
if len(inter_pce_conflicts) > 0:
    exit_with_error = "Origin and Target are sharing Workloads with same hostnamesn please fix before you can continue"
    print("** Now showing all Workloads (%i) with same name on ORIGIN:" % len(conflicting_name_at_origin))
    for name in conflicting_name_at_target:
        print(" - '%s'" % name)
else:
    print("** ORIGIN and TARGET do not have conflicts between each other !!!");


if exit_with_error is not None:
    pylo.log.fatal("\nERROR!! %s\n" % exit_with_error)
    sys.exit(-1)

print("You are OK to proceed as no conflict has been detected\n")