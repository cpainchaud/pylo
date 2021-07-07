import os
import sys
import argparse
import math
from datetime import datetime


sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')

parser.add_argument('--input-file', '-i', type=str, required=True,
                    help='CSV or Excel input filename')
parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                    help='CSV field delimiter')

parser.add_argument('--ignore-if-iplist-exists', type=bool, required=False, default=False, nargs='?', const=True,
                    help='If an IPList with same same exists, ignore CSV entry')

parser.add_argument('--network-delimiter', type=str, required=False, default=',', nargs='?', const=True,
                    help='If an IPList with same same exists, ignore CSV entry')


args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['pce']
use_cached_config = args['dev_use_cache']
input_file = args['input_file']
input_file_delimiter = args['input_file_delimiter']
ignore_if_iplist_exists = args['ignore_if_iplist_exists']
network_delimiter = args['network_delimiter']

now = datetime.now()
report_file = 'import-iplists-results_{}.csv'.format(now.strftime("%Y%m%d-%H%M%S"))
report_file_excel = 'import-iplists-results_{}.xlsx'.format(now.strftime("%Y%m%d-%H%M%S"))

csv_expected_fields = [
    {'name': 'name', 'optional': False},
    {'name': 'description', 'optional': True},
    {'name': 'networks', 'optional': False, 'type': 'array_of_strings'}
]

csv_created_fields = csv_expected_fields.copy()
csv_created_fields.append({'name': 'href'})
csv_created_fields.append({'name': '**not_created_reason**'})

pylo.file_clean(report_file)
pylo.file_clean(report_file_excel)

print(" * Loading CSV input file '{}'...".format(input_file), flush=True, end='')
CsvData = pylo.CsvExcelToObject(input_file, expected_headers=csv_expected_fields, csv_delimiter=input_file_delimiter)
print('OK')
print("   - CSV has {} columns and {} lines (headers don't count)".format(CsvData.count_columns(), CsvData.count_lines()))
# print(pylo.nice_json(CsvData._objects))


org = pylo.Organization(1)
fake_config = pylo.Organization.create_fake_empty_config()

if use_cached_config:
    org.load_from_cache_or_saved_credentials(hostname)
else:
    print(" * Looking for credentials for PCE '{}'... ".format(hostname), end="", flush=True)
    connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
    print("OK!")

    print(" * Downloading objects data from the PCE... ", end="", flush=True)
    config = connector.get_pce_objects()
    print("OK!")

    print(" * Parsing PCE objects data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.connector = connector
    org.load_from_json(config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))


print(" * Checking for iplist name collisions:", flush=True)
name_cache = {}
for iplist in org.IPListStore.itemsByHRef.values():
    lower_name = None
    if iplist.name is not None and len(iplist.name) > 0:
        lower_name = iplist.name.lower()
        if lower_name not in name_cache:
            name_cache[lower_name] = {'pce': True }
        else:
            print("  - Warning duplicate found in the PCE for IPList name: {}".format(iplist.name))

for csv_object in CsvData.objects():
    lower_name = None
    if csv_object['name'] is not None and len(csv_object['name']) > 0:
        lower_name = csv_object['name'].lower()
        if lower_name not in name_cache:
            name_cache[lower_name] = {'csv': True}
        else:
            if 'csv' in name_cache[lower_name]:
                pylo.log.error('CSV contains iplists with duplicates name: {}'.format(lower_name))
                exit(1)
            else:
                csv_object['**not_created_reason**'] = 'Found duplicated name in PCE'
                if not ignore_if_iplist_exists:
                    pylo.log.error("PCE contains iplists with duplicates name from CSV: '{}' at line #{}. Please fix CSV or look for --options to ignore it".format(lower_name, csv_object['*line*']))
                    exit(1)
                print("  - WARNING: CSV has an entry for iplist name '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(lower_name, csv_object['*line*']))

del name_cache
print("  * DONE", flush=True)


# Listing objects to be created (filtering out inconsistent ones)
csv_objects_to_create = []
ignored_objects_count = 0
for csv_object in CsvData.objects():
    if '**not_created_reason**' not in csv_object:
        csv_objects_to_create.append(csv_object)
    else:
        ignored_objects_count += 1


print(' * Preparing Iplist JSON data...')
iplists_json_data = []
for data in csv_objects_to_create:
    new_iplist = {}
    iplists_json_data.append(new_iplist)

    if len(data['name']) < 1:
        raise pylo.PyloEx('Iplist at line #{} is missing a name in CSV'.format(data['*line*']))
    else:
        new_iplist['name'] = data['name']

    if len(data['description']) > 0:
        new_iplist['description'] = data['description']

    if len(data['networks']) < 1:
        print('Iplist at line #{} has empty networks list'.format(data['*line*']))
        exit(1)

    network_delimiter = network_delimiter.replace("\\n", "\n")
    networks_strings = data['networks'].rsplit(network_delimiter)
    ip_ranges = []
    new_iplist['ip_ranges'] = ip_ranges

    for network_string in data['networks'].rsplit(network_delimiter):
        network_string = network_string.strip(" \r\n")  # cleanup trailing characters

        exclusion = False
        if network_string.find('!') == 0:
            exclusion = True
            network_string = network_string[1:]

        split_dash = network_string.split('-')
        if len(split_dash) > 2:
            pylo.log.error('Iplist at line #{} has invalid network entry: {}'.format(data['*line*'], network_string))
            exit(1)
        if len(split_dash) == 2:
            ip_ranges.append({'from_ip': split_dash[0], 'to_ip': split_dash[1], 'exclusion': exclusion})
            continue

        split_slash = network_string.split('/')
        if len(split_slash) > 2:
            pylo.log.error('Iplist at line #{} has invalid network entry: {}'.format(data['*line*'], network_string))
            exit(1)
        if len(split_slash) == 2:
            if len(split_slash[1]) > 2:
                pylo.log.error('Iplist at line #{} has invalid network mask in CIDR {}'.format(data['*line*'], network_string))
                exit(1)
            ip_ranges.append({'from_ip': split_slash[0]+'/'+split_slash[1], 'exclusion': exclusion})
            continue
        else:
            is_ip4 = pylo.is_valid_ipv4(network_string)
            is_ip6 = pylo.is_valid_ipv6(network_string)

            if not is_ip4 and not is_ip6:
                pylo.log.error('Iplist at line #{} has invalid address format: {}'.format(data['*line*'], network_string))
                exit(1)
            ip_ranges.append({'from_ip': network_string, 'exclusion': exclusion})
            continue

    if len(ip_ranges) == 0:
        pylo.log.error('Iplist at line #{} has no network entry'.format(data['*line*']))
        exit(1)

    print("  - iplist '{}' extracted with {} with networks".format(new_iplist['name'], len(new_iplist['ip_ranges'])))
    # print(new_iplist)

print("  * DONE")


print(" * Creating {} IPLists".format(len(iplists_json_data)))
total_created_count = 0
total_failed_count = 0

for index in range(0, len(iplists_json_data)):
    json_blob = iplists_json_data[index]
    print("  - Pushing new iplist '{}' to PCE (#{} of {})... ".format(json_blob['name'], index+1, len(iplists_json_data)), end='', flush=True)
    result = connector.objects_iplist_create(json_blob)
    print("OK")

    href = result.get('href')
    if href is None:
        raise pylo.PyloEx('API returned unexpected response which is missing a HREF:', result)

    total_created_count += 1
    csv_objects_to_create[index]['href'] = href

    CsvData.save_to_csv(report_file, csv_created_fields)
    CsvData.save_to_excel(report_file_excel, csv_created_fields)


CsvData.save_to_csv(report_file, csv_created_fields)
CsvData.save_to_excel(report_file_excel, csv_created_fields)

print("  * DONE - {} created with success, {} failures and {} ignored. A report was created in {} and {}".format(total_created_count, total_failed_count, ignored_objects_count, report_file, report_file_excel))

