import os
import sys
import argparse
import math
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo


# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--debug', '-d', action='store_true',
                    help='extra debugging messages for developers')

parser.add_argument('--consumer-labels', '-cl', type=str, nargs='+', required=False, default=False,
                    help='extra debugging messages for developers')
# </editor-fold>


args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['pce']


now = datetime.now()
report_file = 'explorer-results_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
report_file_excel = 'explorer-results_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))


csv_report_headers = ['name', 'hostname', 'role', 'app', 'env', 'loc', 'href']
csv_report = pylo.ArrayToExport(csv_report_headers)


# <editor-fold desc="PCE Configuration Download and Parsing">
org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()

print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
print("OK!")

print(" * Downloading Labels listing from the PCE... ", end="", flush=True)
fake_config['labels'] = connector.objects_label_get()
print("OK!")

print(" * Parsing PCE data ... ", end="", flush=True)
org.pce_version = connector.version
org.connector = connector
org.load_from_json(fake_config)
print("OK!")

# </editor-fold>


print(" - Now building query parameters:")
if args['consumer_labels'] is not False:
    print("   - Consumer labels were provided:")
else:
    print("   - No Consumer label provided")

explorer_filters = connector.ExplorerFilterSetV1(max_results=2)

print(" - Querying PCE...", end='', flush=True)
search_results = connector.explorer_search(explorer_filters)
print(" OK!")
print(pylo.nice_json(search_results))


print()
print(" * Writing report file '{}' ... ".format(report_file), end='', flush=True)
csv_report.write_to_csv(report_file)
print("DONE")
print(" * Writing report file '{}' ... ".format(report_file_excel), end='', flush=True)
csv_report.write_to_excel(report_file_excel)
print("DONE")

if csv_report.lines_count() < 1:
    print("\n** WARNING: no entry matched your filters so reports are empty !\n")

