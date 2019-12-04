import os
import sys
import argparse
import math

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--host', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--dev-use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='For developers only')
parser.add_argument('--debug', '-d', type=bool, nargs='?', required=False, default=False, const=True,
                    help='extra debugging messages for developers')

parser.add_argument('--input-file', '-i', type=str, required=True,
                    help='CSV or Excel input filename')
parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                    help='CSV field delimiter')

parser.add_argument('--ignore-if-managed-workload-exists', type=bool, required=False, default=False, nargs='?', const=True,
                    help='If a Managed Workload with same same exists, ignore CSV entry')
# parser.add_argument('--ignore-label-case-collisions', type=bool, nargs='?', required=False, default=False, const=True,
#                     help='Use this option if you want allow Workloads to be created with labels with same name but different case (Illumio PCE allows it but its definitely a bad practice!)')

parser.add_argument('--batch-size', type=int, nargs='?', required=False, default=500, const=True,
                    help='extra debugging messages for developers')

args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()


hostname = args['host']
use_cached_config = args['dev_use_cache']
input_file = args['input_file']
input_file_delimiter = args['input_file_delimiter']
ignore_if_managed_workload_exists = args['ignore_if_managed_workload_exists']
#ignore_label_case_collisions = args['ignore_label_case_collisions']
batch_size = args['batch_size']
report_file = 'results.csv'
report_file_excel = 'results.xlsx'

csv_expected_fields = [
    {'name': 'name', 'optional': False},
    {'name': 'hostname', 'optional': False},
    {'name': 'role', 'optional': True},
    {'name': 'app', 'optional': True},
    {'name': 'env', 'optional': True},
    {'name': 'loc', 'optional': True},
    {'name': 'ip', 'optional': False},
    {'name': 'description', 'optional': True, 'default': ''}
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

    print(" * Downloading Workloads/Agents listing from the PCE... ", end="", flush=True)
    fake_config['workloads'] = connector.objects_workload_get()
    print("OK!")

    print(" * Downloading Labels listing from the PCE... ", end="", flush=True)
    fake_config['labels'] = connector.objects_label_get()
    print("OK!")

    print(" * Parsing PCE data ... ", end="", flush=True)
    org.pce_version = connector.version
    org.connector = connector
    org.load_from_json(fake_config)
    print("OK!")

print(" * PCE data statistics:\n{}".format(org.stats_to_str(padding='    ')))


print(" * Checking for name/hostname collisions:")
name_cache = {}
for workload in org.WorkloadStore.itemsByHRef.values():
    lower_name = None
    if workload.name is not None and len(workload.name) > 0:
        lower_name = workload.name.lower()
        if lower_name not in name_cache:
            name_cache[lower_name] = {'pce': True, 'managed': not workload.unmanaged}
        else:
            print("  - Warning duplicate found in the PCE for hostname/name: {}".format(workload.name))
    if workload.hostname is not None and len(workload.hostname) > 0:
        lower_hostname = workload.hostname.lower()
        if lower_name != lower_hostname:
            if workload.hostname not in name_cache:
                name_cache[workload.hostname] = {'pce': True, 'managed': not workload.unmanaged}
            else:
                print("  - Warning duplicate found in the PCE for hostname/name: {}".format(workload.hostname))

for csv_object in CsvData.objects():
    lower_name = None
    if csv_object['name'] is not None and len(csv_object['name']) > 0:
        lower_name = csv_object['name'].lower()
        if lower_name not in name_cache:
            name_cache[lower_name] = {'csv': True}
        else:
            if 'csv' in name_cache[lower_name]:
                raise pylo.PyloEx('CSV contains workloads with duplicates name/hostname: {}'.format(lower_name))
            else:
                csv_object['**not_created_reason**'] = 'Found duplicated name/hostname in PCE'
                if not name_cache[lower_name]['managed'] or not ignore_if_managed_workload_exists:
                    raise pylo.PyloEx("PCE contains workloads with duplicates name/hostname from CSV: '{}' at line #{}".format(lower_name, csv_object['*line*']))
                print("  - WARNING: CSV has an entry for workload name '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(lower_name, csv_object['*line*']))

    if csv_object['hostname'] is not None and len(csv_object['hostname']) > 0:
        lower_hostname = csv_object['hostname'].lower()
        if lower_name != lower_hostname:
            if csv_object['hostname'] not in name_cache:
                name_cache[csv_object['hostname']] = {'csv': True}
            else:
                if 'csv' in name_cache[lower_name]:
                    raise pylo.PyloEx('CSV contains workloads with duplicates name/hostname: {}'.format(lower_name))
                else:
                    csv_object['**not_created_reason**'] = 'Found duplicated name/hostname in PCE'
                    if not name_cache[lower_name]['managed'] or not ignore_if_managed_workload_exists:
                        raise pylo.PyloEx("PCE contains workloads with duplicates name/hostname from CSV: '{}' at line #{}".format(lower_name, csv_object['*line*']))
                    print("  - WARNING: CSV has an entry for workload hostname '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(lower_name, csv_object['*line*']))

del name_cache
print("  * DONE")


print(" * Checking for Labels case collisions and missing ones to be created:")
name_cache = {}
for label in org.LabelStore.itemsByHRef.values():
    lower_name = None
    if label.name is not None:
        lower_name = label.name.lower()
        if lower_name not in name_cache:
            name_cache[lower_name] = {'pce': True, 'realcase': label.name}
        else:
            print("  - Warning duplicate found in the PCE for Label: {}".format(label.name))

for csv_object in CsvData.objects():
    role_label = csv_object['role']
    role_label_lower = role_label.lower()
    app_label = csv_object['app']
    app_label_lower = app_label.lower()
    env_label = csv_object['env']
    env_label_lower = env_label.lower()
    loc_label = csv_object['loc']
    loc_label_lower = loc_label.lower()

    if len(role_label_lower) < 1:
        raise pylo.PyloEx("CSV Line #{} has no Role label defined".format(csv_object['*line*']))
    if len(app_label_lower) < 1:
        raise pylo.PyloEx("CSV Line #{} has no App label defined".format(csv_object['*line*']))
    if len(env_label_lower) < 1:
        raise pylo.PyloEx("CSV Line #{} has no Env label defined".format(csv_object['*line*']))
    if len(loc_label_lower) < 1:
        raise pylo.PyloEx("CSV Line #{} has no Loc label defined".format(csv_object['*line*']))

    if role_label_lower not in name_cache:
        name_cache[role_label_lower] = {'csv': True, 'realcase': role_label, 'type': 'role'}
    elif name_cache[role_label_lower]['realcase'] != role_label:
        if 'csv' in name_cache[role_label_lower]:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case within the CSV".format(role_label))
        else:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case between CSV and PCE".format(role_label))

    if app_label_lower not in name_cache:
        name_cache[app_label_lower] = {'csv': True, 'realcase': app_label, 'type': 'app'}
    elif name_cache[app_label_lower]['realcase'] != app_label:
        if 'csv' in name_cache[app_label_lower]:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case within the CSV".format(app_label))
        else:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case between CSV and PCE".format(app_label))

    if env_label_lower not in name_cache:
        name_cache[env_label_lower] = {'csv': True, 'realcase': env_label, 'type': 'env'}
    elif name_cache[env_label_lower]['realcase'] != env_label:
        if 'csv' in name_cache[env_label_lower]:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case within the CSV".format(env_label))
        else:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case between CSV and PCE".format(env_label))

    if loc_label_lower not in name_cache:
        name_cache[loc_label_lower] = {'csv': True, 'realcase': loc_label, 'type': 'loc'}
    elif name_cache[loc_label_lower]['realcase'] != loc_label:
        if 'csv' in name_cache[loc_label_lower]:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case within the CSV".format(loc_label))
        else:
            raise pylo.PyloEx("Found duplicate label with name '{}' but different case between CSV and PCE".format(loc_label))

labels_to_be_created = []
for label_entry in name_cache.values():
    if 'csv' in label_entry:
        labels_to_be_created.append({'name': label_entry['realcase'], 'type': label_entry['type']})

del name_cache
print("  * DONE")

if len(labels_to_be_created) > 0:
    print(" * {} Labels need to created before Workloads can be imported, listing:".format(len(labels_to_be_created)))
    for label_to_create in labels_to_be_created:
        print("   - {} type {}".format(label_to_create['name'], label_to_create['type']))

    print("  ** Proceed and create all the {} Labels? (yes/no):  ".format(len(labels_to_be_created)), flush=True, end='')
    while True:
        keyboard_input = input()
        keyboard_input = keyboard_input.lower()
        if keyboard_input == 'yes' or keyboard_input == 'y':
            break
        if keyboard_input == 'no' or keyboard_input == 'n':
            exit(0)
    for label_to_create in labels_to_be_created:
        print("   - Pushing '{}' with type '{}' to the PCE... ".format(label_to_create['name'], label_to_create['type']), end='', flush=True)
        org.LabelStore.api_create_label(label_to_create['name'], label_to_create['type'])
        print("OK")

# Listing objects to be created (filtering out inconsistent ones)
csv_objects_to_create = []
ignored_objects_count = 0
for csv_object in CsvData.objects():
    if '**not_created_reason**' not in csv_object:
        csv_objects_to_create.append(csv_object)
    else:
        ignored_objects_count += 1


print(' * Preparing Workload JSON data...')
workloads_json_data = []
for data in csv_objects_to_create:
    new_workload = {}
    workloads_json_data.append(new_workload)

    if len(data['name']) < 1:
        raise pylo.PyloEx('Workload at line #{} is missing a name in CSV'.format(data['*line*']))
    else:
        new_workload['name'] = data['name']

    if len(data['hostname']) < 1:
        raise pylo.PyloEx('Workload at line #{} is missing a hostname in CSV'.format(data['*line*']))
    else:
        new_workload['hostname'] = data['hostname']

    found_role_label = org.LabelStore.find_label_by_name_and_type(data['role'], pylo.label_type_role)
    if found_role_label is None:
        raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.format(data['role'], data['*line*']))
    found_app_label = org.LabelStore.find_label_by_name_and_type(data['app'], pylo.label_type_app)
    if found_app_label is None:
        raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.format(data['app'], data['*line*']))
    found_env_label = org.LabelStore.find_label_by_name_and_type(data['env'], pylo.label_type_env)
    if found_env_label is None:
        raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.format(data['env'], data['*line*']))
    found_loc_label = org.LabelStore.find_label_by_name_and_type(data['loc'], pylo.label_type_loc)
    if found_loc_label is None:
        raise pylo.PyloEx('Cannot find a Label named "{}" in the PCE for CSV line #{}'.format(data['loc'], data['*line*']))

    new_workload['labels'] = [{'href': found_role_label.href},
                              {'href': found_app_label.href},
                              {'href': found_env_label.href},
                              {'href': found_loc_label.href}]

    if len(data['description']) > 0:
        new_workload['description'] = data['description']

    new_workload['public_ip'] = data['ip']
    new_workload['interfaces'] = [{"name": "eth0", "address": data['ip']}]

print("  * DONE")


print(" * Creating {} Unmanaged Workloads in batches of {}".format(len(workloads_json_data), batch_size))
batch_cursor = 0
total_created_count = 0
total_failed_count = 0
while batch_cursor <= len(workloads_json_data):
    print("  - batch #{} of {}".format(math.ceil(batch_cursor/batch_size)+1, math.ceil(len(workloads_json_data)/batch_size)))
    batch_json_data = workloads_json_data[batch_cursor:batch_cursor+batch_size-1]
    results = connector.objects_workload_create_bulk_unmanaged(batch_json_data)
    created_count = 0
    failed_count = 0

    for i in range(0, batch_size):
        if i >= len(batch_json_data):
            break
        result = results[i]
        if result['status'] != 'created':
            csv_objects_to_create[i + batch_cursor]['**not_created_reason**'] = result['message']
            failed_count += 1
            total_failed_count += 1
        else:
            csv_objects_to_create[i + batch_cursor]['href'] = result['href']
            created_count += 1
            total_created_count += 1

    print("    - {} created with success, {} failures (read report to get reasons)".format(created_count, failed_count))
    CsvData.save_to_csv(report_file, csv_created_fields)
    CsvData.save_to_excel(report_file_excel, csv_created_fields)

    batch_cursor += batch_size
print("  * DONE - {} created with success, {} failures and {} ignored. A report was created in {} and {}".format(total_created_count, total_failed_count, ignored_objects_count, report_file, report_file_excel))

