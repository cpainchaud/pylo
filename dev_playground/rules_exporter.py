import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
import argparse
import time
import json
import csv
import xlsxwriter

output_dir = 'output'
rules_json_file = output_dir + '/rules.json'
rules_csv_file = output_dir + '/rules.csv'
rules_xls_file = output_dir + '/rules.xlsx'

log = pylo.get_logger()

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--use-cache', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Use cached configuration on local filesystem if it exists')
parser.add_argument('--debug', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Enabled extra debug output')

args = vars(parser.parse_args())

hostname = args['pce']


if args['debug']:
    pylo.log_set_debug()


if os.path.exists(output_dir):
    if not os.path.isdir(output_dir):
        raise("Folder/file '{}' was found but it's not a folder".format(output_dir))
else:
    print("* Creating folder '{}' to store output files... ".format(output_dir), end='', flush=True)
    os.mkdir(output_dir)
    print("OK!")


def file_clean(path):
    if not os.path.exists(path):
        return
    print("* Cleaning file '{}' from previous runs... ".format(path), end='', flush=True)
    os.remove(path)
    print("OK!")


file_clean(rules_json_file)
file_clean(rules_csv_file)
file_clean(rules_xls_file)

if args['use_cache']:
    print("Loading PCE configuration cached file... ", end="", flush=True)
    org = pylo.Organization.get_from_cache_file(hostname)
    print("OK!\n")
else:
    print("Loading PCE configuration from '{}'... ".format(hostname), end="", flush=True)
    org = pylo.Organization.get_from_api_using_credential_file(hostname)
    print("OK!\n")

print(org.stats_to_str())
print()


rules_json_output = []



# <editor-fold desc="RuleSets Export">
start_time = time.time()
print("*** Generating Rules ***")

csv_rules_headers = ['ruleset_name', 'src', 'dst', 'svc', 'rule_description', 'ruleset_description', 'ruleset_href', 'ruleset_href']
csv_rules_rows = []

xls_rules_headers = ['ruleset_name', 'src', 'dst', 'svc', 'rule_description', 'ruleset_url', 'ruleset_href', 'ruleset_href']
xls_rules_rows = []
xls_column_width = {}
for header in xls_rules_headers:
    xls_column_width[header] = 0


for ruleset in org.RulesetStore.rulesets:

    log.debug(" - Handling ruleset '{}'".format(ruleset.name))

    for rule in ruleset.rules_by_href.values():
        log.debug("   - Handling rule '{}'".format(rule.href))

        scope_type = 'intra'
        if rule.unscoped_consumers:
            scope_type = 'extra'


        # <editor-fold desc="Handling of Services">
        local_services_json = []
        csv_svc_members = []
        for service in rule.services._items.values():
            local_services_json.append({'type': 'service', 'name': service.name, 'href': service.href})
            csv_svc_members.append(service.name)

        for service in rule.services._direct_services:
            local_services_json.append({'type': 'direct_in_rule', 'name': service.to_string_standard()})
            csv_svc_members.append(service.to_string_standard())

        for member in csv_svc_members:
            if len(member) > xls_column_width['svc']:
                xls_column_width['svc'] = len(member)

        xls_svc_members = pylo.string_list_to_text(csv_svc_members, "\n")
        csv_svc_members = pylo.string_list_to_text(csv_svc_members)
        # </editor-fold">

        rule_json = {'href': rule.href, 'type': scope_type, 'description': rule.description, 'src': [], 'dst': [], 'svc': local_services_json}
        rules_json_output.append(rule_json)

        # <editor-fold desc="Handling of Consumers">
        csv_src_members = []

        if rule.consumers._hasAllWorkloads:
            rule_json['src'].append({'type': 'special', 'name': 'All Workloads'})
            csv_src_members.append('All Workloads')

        for entry in rule.consumers._items.values():
            if isinstance(entry, pylo.IPList):
                rule_json['src'].append({'type': 'iplist', 'name': entry.name, 'href': entry.href})
                csv_src_members.append('I:'+entry.name)
            elif isinstance(entry, pylo.Workload):
                rule_json['src'].append({'type': 'host', 'name': entry.get_name(), 'href': entry.href})
                csv_src_members.append('W:'+entry.get_name())
            elif isinstance(entry, pylo.Label):
                rule_json['src'].append({'type': 'label', 'name': entry.name, 'href': entry.href})
                csv_src_members.append('L:'+entry.name)
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(entry)))
        # </editor-fold>

        # <editor-fold desc="Handling of Providers">
        csv_dst_members = []

        if rule.providers._hasAllWorkloads:
            rule_json['dst'].append({'type': 'special', 'name': 'All Workloads'})
            csv_dst_members.append('All Workloads')

        for entry in rule.providers._items.values():
            if isinstance(entry, pylo.IPList):
                rule_json['dst'].append({'type': 'iplist', 'name': entry.name, 'href': entry.href})
                csv_dst_members.append('I:'+entry.name)
            elif isinstance(entry, pylo.Workload):
                rule_json['dst'].append({'type': 'host', 'name': entry.get_name(), 'href': entry.href})
                csv_dst_members.append('W:'+entry.get_name())
            elif isinstance(entry, pylo.Label):
                rule_json['dst'].append({'type': 'label', 'name': entry.name, 'href': entry.href})
                csv_dst_members.append('L:'+entry.name)
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(entry)))
        # </editor-fold>

        # <editor-fold desc="CSV making">

        if rule.description is not None:
            sanitized_rule_desc = rule.description.replace("\n", ' ')
        else:
            sanitized_rule_desc = ''
        if ruleset.description is not None:
            sanitized_ruleset_desc = ruleset.description.replace("\n", ' ')
        else:
            sanitized_ruleset_desc = ''

        csv_row = [ruleset.name, pylo.string_list_to_text(csv_src_members),
                   pylo.string_list_to_text(csv_dst_members), csv_svc_members,
                   sanitized_rule_desc,
                   sanitized_ruleset_desc,
                   rule.href,
                   ruleset.href]
        csv_rules_rows.append(csv_row)

        xls_row = [ruleset.name, pylo.string_list_to_text(csv_src_members, "\n"),
                   pylo.string_list_to_text(csv_dst_members, "\n"), xls_svc_members,
                   rule.description,
                   ruleset.get_ruleset_url(org.connector.hostname, org.connector.port),
                   rule.href,
                   ruleset.href]
        xls_rules_rows.append(xls_row)
        for member in csv_src_members:
            if len(member) > xls_column_width['src']:
                xls_column_width['src'] = len(member)
        for member in csv_dst_members:
            if len(member) > xls_column_width['dst']:
                xls_column_width['dst'] = len(member)
        # </editor-fold>


elapsed_time = time.time() - start_time
print('*** Rules generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>

exported_rules_count = len (csv_rules_rows)
print("*** {} rules were exported ***".format(exported_rules_count))

print("\n*** Saving rules to '{}'... ".format(rules_json_file), flush=True, end='')
with open(rules_json_file, 'w') as outfile:
    json.dump(rules_json_output, outfile, indent=True)
print("OK!")


print("*** Saving rules to '{}'... ".format(rules_csv_file), flush=True, end='')
with open(rules_csv_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_rules_headers)
    filewriter.writerows(csv_rules_rows)
print("OK!")

print("*** Saving rules to '{}'... ".format(rules_xls_file), flush=True, end='')
xls_workbook = xlsxwriter.Workbook(rules_xls_file)
cell_format = xls_workbook.add_format()
cell_format.set_text_wrap()
cell_format.set_valign('vcenter')
xls_worksheet = xls_workbook.add_worksheet('rules')
xls_headers = []
header_index = 0
for header in xls_rules_headers:
    if header == 'rule_description':
        xls_column_width[header] = 25
    elif header != 'src' and header != 'dst' and header != 'svc':
        for row in xls_rules_rows:
            if row[header_index] is not None and len(row[header_index]) > xls_column_width[header]:
                xls_column_width[header] = len(row[header_index])

    xls_headers.append({'header': header, 'format': cell_format})
    if xls_column_width[header] != 0:
        xls_worksheet.set_column(header_index, header_index, width=xls_column_width[header])

    header_index += 1
xls_table = xls_worksheet.add_table(0, 0, len(xls_rules_rows), len(xls_rules_headers)-1, {'header_row': True, 'data': xls_rules_rows, 'columns': xls_headers})
xls_workbook.close()
print("OK!")

print("\nEND OF SCRIPT\n")

