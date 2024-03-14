import illumio_pylo as pylo
import argparse
import sys
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'iplist-import'
objects_load_filter = ['iplists']


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--input-file', '-i', type=str, required=True,
                        help='CSV or Excel input filename')
    parser.add_argument('--input-file-delimiter', type=str, required=False, default=',',
                        help='CSV field delimiter')

    parser.add_argument('--ignore-if-iplist-exists', action='store_true',
                        help='If an IPList with same same exists, ignore CSV entry')

    parser.add_argument('--network-delimiter', type=str, required=False, default=',', nargs='?', const=True,
                        help='If an IPList with same same exists, ignore CSV entry')


def __main(args, org: pylo.Organization, **kwargs):
    input_file = args['input_file']
    input_file_delimiter = args['input_file_delimiter']
    ignore_if_iplist_exists = args['ignore_if_iplist_exists']
    network_delimiter = args['network_delimiter']

    output_file_prefix = make_filename_with_timestamp('import-iplists-results_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_expected_fields = [
        {'name': 'name', 'optional': False},
        {'name': 'description', 'optional': True},
        {'name': 'networks', 'optional': False, 'type': 'array_of_strings'}
    ]

    csv_created_fields = csv_expected_fields.copy()
    csv_created_fields.append({'name': 'href'})
    csv_created_fields.append({'name': '**not_created_reason**'})

    pylo.file_clean(output_file_csv)
    pylo.file_clean(output_file_excel)

    print(" * Loading CSV input file '{}'...".format(input_file), flush=True, end='')
    csv_data = pylo.CsvExcelToObject(input_file, expected_headers=csv_expected_fields, csv_delimiter=input_file_delimiter)
    print('OK')
    print("   - CSV has {} columns and {} lines (headers don't count)".format(csv_data.count_columns(), csv_data.count_lines()))
    # print(pylo.nice_json(csv_data._objects))

    print(" * Checking for iplist name collisions:", flush=True)
    name_cache = {}
    for iplist in org.IPListStore.items_by_href.values():
        if iplist.name is not None and len(iplist.name) > 0:
            lower_name = iplist.name.lower()
            if lower_name not in name_cache:
                name_cache[lower_name] = {'pce': True}
            else:
                print("  - Warning duplicate found in the PCE for IPList name: {}".format(iplist.name))

    for csv_object in csv_data.objects():
        if csv_object['name'] is not None and len(csv_object['name']) > 0:
            lower_name = csv_object['name'].lower()
            if lower_name not in name_cache:
                name_cache[lower_name] = {'csv': True}
            else:
                if 'csv' in name_cache[lower_name]:
                    pylo.log.error('CSV contains iplists with duplicates name: {}'.format(lower_name))
                    sys.exit(1)
                else:
                    csv_object['**not_created_reason**'] = 'Found duplicated name in PCE'
                    if not ignore_if_iplist_exists:
                        pylo.log.error("PCE contains iplists with duplicates name from CSV: '{}' at line #{}. Please fix CSV or look for --options to ignore it".format(lower_name, csv_object['*line*']))
                        sys.exit(1)
                    print("  - WARNING: CSV has an entry for iplist name '{}' at line #{} but it exists already in the PCE. It will be ignored.".format(lower_name, csv_object['*line*']))

    del name_cache
    print("  * DONE", flush=True)

    # Listing objects to be created (filtering out inconsistent ones)
    csv_objects_to_create = []
    ignored_objects_count = 0
    for csv_object in csv_data.objects():
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
            sys.exit(1)

        network_delimiter = network_delimiter.replace("\\n", "\n")
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
                sys.exit(1)
            if len(split_dash) == 2:
                ip_ranges.append({'from_ip': split_dash[0], 'to_ip': split_dash[1], 'exclusion': exclusion})
                continue

            split_slash = network_string.split('/')
            if len(split_slash) > 2:
                pylo.log.error('Iplist at line #{} has invalid network entry: {}'.format(data['*line*'], network_string))
                sys.exit(1)
            if len(split_slash) == 2:
                if len(split_slash[1]) > 2:
                    pylo.log.error('Iplist at line #{} has invalid network mask in CIDR {}'.format(data['*line*'], network_string))
                    sys.exit(1)
                ip_ranges.append({'from_ip': split_slash[0]+'/'+split_slash[1], 'exclusion': exclusion})
                continue
            else:
                is_ip4 = pylo.is_valid_ipv4(network_string)
                is_ip6 = pylo.is_valid_ipv6(network_string)

                if not is_ip4 and not is_ip6:
                    pylo.log.error('Iplist at line #{} has invalid address format: {}'.format(data['*line*'], network_string))
                    sys.exit(1)
                ip_ranges.append({'from_ip': network_string, 'exclusion': exclusion})
                continue

        if len(ip_ranges) == 0:
            pylo.log.error('Iplist at line #{} has no network entry'.format(data['*line*']))
            sys.exit(1)

        print("  - iplist '{}' extracted with {} with networks".format(new_iplist['name'], len(new_iplist['ip_ranges'])))
        # print(new_iplist)

    print("  * DONE")

    print(" * Creating {} IPLists".format(len(iplists_json_data)))
    total_created_count = 0
    total_failed_count = 0

    for index in range(0, len(iplists_json_data)):
        json_blob = iplists_json_data[index]
        print("  - Pushing new iplist '{}' to PCE (#{} of {})... ".format(json_blob['name'], index+1, len(iplists_json_data)), end='', flush=True)
        result = org.connector.objects_iplist_create(json_blob)
        print("OK")

        href = result.get('href')
        if href is None:
            raise pylo.PyloEx('API returned unexpected response which is missing a HREF:', result)

        total_created_count += 1
        csv_objects_to_create[index]['href'] = href

        csv_data.save_to_csv(output_file_csv, csv_created_fields)
        csv_data.save_to_excel(output_file_excel, csv_created_fields)

    csv_data.save_to_csv(output_file_csv, csv_created_fields)
    csv_data.save_to_excel(output_file_excel, csv_created_fields)

    print("  * DONE - {} created with success, {} failures and {} ignored. A report was created in {} and {}".format(total_created_count, total_failed_count, ignored_objects_count, output_file_csv, output_file_excel))


command_object = Command(command_name, __main, fill_parser, load_specific_objects_only=objects_load_filter)

