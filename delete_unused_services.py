
import pylo
import sys

hostname='10.107.3.2'
hostname='ilo-emea-poc.xmp.net.intra'



org = pylo.Organization(1)
target = pylo.Organization(1)


print("Loading Origin PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
org.load_from_cache_or_saved_credentials(hostname)
print("OK!\n")


print(' - Looking for unused objects ... ', end='', flush=True)
unused_services = []  # type: list[pylo.Service]
for service in org.ServiceStore.itemsByHRef.values():
    if service.count_references() == 0 and not service.deleted:
        unused_services.append(service)
print('OK!')
print(' - Found {} unused objects'.format(len(unused_services)))

for service in unused_services:
    print("  - deleting Service '{}' : {}' ... ".format(service.name, service.href), flush=True, end='')
    org.connector.objects_service_delete(service)
    print("OK!")


print("\n - Found {} unused Services and they have all been deleted".format(len(unused_services)))
print("\nEND OF SCRIPT\n")





