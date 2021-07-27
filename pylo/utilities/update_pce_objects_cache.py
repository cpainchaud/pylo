import os
import sys
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import pylo

from pylo import log


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--include-deleted-workloads', type=bool, required=False, nargs='?', default=False, const=True,
                    help='should deleted workloads be downloaded as well')
parser.add_argument('--debug', type=bool, required=False, nargs='?', default=False, const=True,
                    help='should deleted workloads be downloaded as well')

args = vars(parser.parse_args())

# print(args)

hostname = args['pce']
include_deleted = args['include_deleted_workloads']

if args['debug']:
    log.setLevel(logging.DEBUG)


print("* Getting API credentials from cached credentials for host '%s' ... " % hostname, end="", flush=True)
con = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
print("OK!")

print("* Getting PCE Version... ", end="", flush=True)
con.collect_pce_infos()
print(con.version.generate_str_from_numbers())

org = pylo.Organization(1)
print("* Downloading PCE objects and settings from '{}'".format(hostname), end="", flush=True)
(file_name, file_size) = org.make_cache_file_from_api(con, include_deleted_workloads=include_deleted)
print("OK!")

print("\nPCE objects and settings were saved to file '%s' with a size of %iKB" % (file_name, int(file_size/1024)))

print()
