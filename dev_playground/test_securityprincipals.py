
import illumio_pylo as pylo
import sys
import argparse
import random

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')

args = vars(parser.parse_args())

hostname = args['pce']

print("Loading Origin PCE configuration from " + hostname + " or cached file... ", end="", flush=True)
org  = pylo.get_organization_using_credential_file(hostname)
print("OK!\n")

print("Organization statistics:\n{}\n\n".format(org.stats_to_str()))

# pylo.log_set_debug()

for group in org.SecurityPrincipalStore.itemsByHRef.values():
    print(" - Found User Group '{}' with SID '{}'".format(group.name, group.href))
    print("    + used in '{}' places".format(group.count_references()))


base = 'S-1-5-21-1180699209-877415012-{}-1004'.format(random.randint(1000,999999))
print("\nAbout to create Group SID {}".format(base))

print(
    org.connector.objects_securityprincipal_create('grp-{}'.format(random.randint(10000,999999)), base)
)

print("\nEND OF SCRIPT\n")

