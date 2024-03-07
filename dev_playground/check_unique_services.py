
import illumio_pylo as pylo
import sys

originHostname='10.107.3.2'
targetHostname='ilo-emea-poc.xmp.net.intra'




origin = pylo.Organization(1)
target = pylo.Organization(1)

exit_with_error = None


def check_unique_within_single_pce(org: pylo.Organization):

    conflicting_names = {}  # type: dict[str,list[pylo.Service]]
    unique_names = {}  # type: dict[str,pylo.Service]

    for service in org.ServiceStore.itemsByHRef.values():
        svc_name = service.name.lower()
        if svc_name in conflicting_names:
            conflicting_names[svc_name].append(service)
        else:
            conflicting_names[svc_name] = []
            conflicting_names[svc_name].append(service)

    for name in conflicting_names.copy().keys():
        service_array = conflicting_names[name]
        if len(service_array) < 2:
            conflicting_names.pop(name)

    return conflicting_names


def check_conflicts_between_two_pce(origin: pylo.Organization, target: pylo.Organization):
    names = {}  # type: dict[str, pylo.Service]
    conflicts = {}  # type: dict[str, str]

    for service in origin.ServiceStore.itemsByName.values():
        names[service.name.lower()] = service

    for service in target.ServiceStore.itemsByName.values():
        service_name_lower = service.name.lower()
        if service_name_lower in names:
            conflicts[service_name_lower] = service_name_lower

    return conflicts


print("Loading Origin PCE configuration from " + originHostname + " or cached file... ", end="", flush=True)
origin.load_from_cache_or_saved_credentials(originHostname)
print("OK!")

print("Loading Target PCE configuration from " + targetHostname + " or cached file ... ", end="", flush=True)
target.load_from_cache_or_saved_credentials(targetHostname)
print("OK!")

conflicting_name_at_origin = check_unique_within_single_pce(origin)
if len(conflicting_name_at_origin) > 0:
    exit_with_error = "Origin does have conflicting services, please fix before you can continue"

    print("\n** Now showing all Services ({}) with same name on Origin PCE {}:".
          format(len(conflicting_name_at_origin),
                 originHostname))
    for name in conflicting_name_at_origin:
        print(" - '%s'" % name)
        for cf_service in conflicting_name_at_origin[name]:
            print("    - '{}' used {} times".format(cf_service.href, cf_service.count_references()))
else:
    print("** Origin has no conflicting services within itself, good news!!")


conflicting_name_at_target = check_unique_within_single_pce(target)
if len(conflicting_name_at_target) > 0:
    exit_with_error = "Target does have conflicting services, please fix before you can continue"

    print("\n** Now showing all Services ({}) with same name on Target PCE {}:".
          format(   len(conflicting_name_at_target),
                    targetHostname))
    for name in conflicting_name_at_target:
        print(" - '%s' with %i occurences" % (name, len(conflicting_name_at_target[name])))
        for cf_service in conflicting_name_at_target[name]:
            print("    - '{}' used {} times".format(cf_service.href, cf_service.count_references()))
else:
    print("** Target has no conflicting services within itself, good news!!")

print()

inter_pce_conflicts = check_conflicts_between_two_pce(origin, target)
if len(inter_pce_conflicts) > 0:
    exit_with_error = "Origin and Target are sharing Services with same name please fix before you can continue"
    print("\n** Now showing all Services (%i) with same name on both PCEs:" % len(inter_pce_conflicts))
    for name in conflicting_name_at_target:
        print(" - '%s'" % name)
else:
    print("** ORIGIN and TARGET do not have conflicts between each other !!!")


if exit_with_error is not None:
    pylo.log.fatal("\nERROR!! %s\n" % exit_with_error)
    sys.exit(-1)

print("You are OK to proceed as no conflict has been detected\n")