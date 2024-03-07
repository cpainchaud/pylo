import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from typing import Dict, Any

import illumio_pylo as pylo
import argparse
import time
import json
import csv
import re

import time
import os
import psutil


def elapsed_since(start):
    return time.strftime("%H:%M:%S", time.gmtime(time.time() - start))


def get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss

def track(func):
    def wrapper(*args, **kwargs):
        mem_before = get_process_memory()
        start = time.time()
        result = func(*args, **kwargs)
        elapsed_time = elapsed_since(start)
        mem_after = get_process_memory()
        print("{}: memory before: {:,}, after: {:,}, consumed: {:,}; exec time: {}".format(
            func.__name__,
            mem_before, mem_after, mem_after - mem_before,
            elapsed_time))
        return result
    return wrapper

output_dir = 'output'
json_dump_file = output_dir + '/dump.json'
csv_rulesets_file = output_dir + '/rulesets.csv'
csv_hosts_file = output_dir + '/hosts.csv'
csv_services_file = output_dir + '/services.csv'
csv_groups_file = output_dir + '/groups.csv'
csv_iplists_file = output_dir + '/iplists.csv'

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

tag_regex = '\[([\-\s\w]+:[,\-\s\w]+)]'


def file_clean(path):
    if not os.path.exists(path):
        return
    print("* Cleaning file '{}' from previous runs... ".format(path), end='', flush=True)
    os.remove(path)
    print("OK!")


file_clean(json_dump_file)
file_clean(csv_rulesets_file)
file_clean(csv_hosts_file)
file_clean(csv_groups_file)
file_clean(csv_services_file)
file_clean(csv_iplists_file)



class ScopeMatrix:
    def __init__(self):
        self.rol_labels = {None: None}  # type: Dict[pylo.Label,pylo.Label]
        self.app_labels = {None: None}  # type: Dict[pylo.Label,pylo.Label]
        self.env_labels = {None: None}  # type: Dict[pylo.Label,pylo.Label]
        self.loc_labels = {None: None}  # type: Dict[pylo.Label,pylo.Label]

        self.rol_labels_by_string = {}  # type: Dict[str,pylo.Label]
        self.app_labels_by_string = {}  # type: Dict[str,pylo.Label]
        self.env_labels_by_string = {}  # type: Dict[str,pylo.Label]
        self.loc_labels_by_string = {}  # type: Dict[str,pylo.Label]

    def is_empty(self):
        if None in self.rol_labels and None in self.app_labels and None in self.env_labels and None in self.loc_labels:
            return True
        return False

    def add_label(self, label: pylo.Label):
        if label.type_is_role():
            if None in self.rol_labels:
                self.rol_labels.pop(None)
            self.rol_labels[label] = label
        elif label.type_is_application():
            if None in self.app_labels:
                self.app_labels.pop(None)
            self.app_labels[label] = label
        elif label.type_is_environment():
            if None in self.env_labels:
                self.env_labels.pop(None)
            self.env_labels[label] = label
        elif label.type_is_location():
            if None in self.loc_labels:
                self.loc_labels.pop(None)
            self.loc_labels[label] = label
        else:
            raise pylo.PyloEx("Unsupported Label type")

    def calculate_scopes_with_filter(self, scope: pylo.RulesetScopeEntry):
        result_scopes = []

        for local_scope in self.generate_scopes():
            if scope.app_label is not None:
                if local_scope['app'] is None:
                    local_scope['app'] = scope.app_label
                else:
                    raise pylo.PyloEx("Unexpected case where Scope APP is specified but Rule has APP defined as well")

            if scope.env_label is not None:
                if local_scope['env'] is None:
                    local_scope['env'] = scope.env_label
                else:
                    raise pylo.PyloEx("Unexpected case where Scope ENV is specified but Rule has ENV defined as well")

            if scope.loc_label is not None:
                if local_scope['loc'] is None:
                    local_scope['loc'] = scope.loc_label
                else:
                    raise pylo.PyloEx("Unexpected case where Scope LOC is specified but Rule has LOC defined as well")

            result_scopes.append(local_scope)

        return result_scopes

    def generate_scopes(self):
        scopes = []

        roles = list(self.rol_labels.keys())
        for role in roles:
            apps = list(self.app_labels.keys())
            for app in apps:
                envs = list(self.env_labels.keys())
                for env in envs:
                    locs = list(self.loc_labels.keys())
                    for loc in locs:
                        scopes.append({'role': role, 'app': app, 'env': env, 'loc': loc})

        return scopes

    def generate_scopes_string(self, separator='_'):
        scopes = []

        roles = list(self.rol_labels.keys())
        for role in roles:
            role_name = 'R-All'
            if role is not None:
                role_name = role.name
            apps = list(self.app_labels.keys())

            for app in apps:
                app_name = 'A-All'
                if app is not None:
                    app_name = app.name
                envs = list(self.env_labels.keys())

                for env in envs:
                    env_name = 'E-All'
                    if env is not None:
                        env_name = env.name
                    locs = list(self.loc_labels.keys())

                    for loc in locs:
                        loc_name = 'L-All'
                        if loc is not None:
                            loc_name = loc.name
                        scopes.append(role_name + separator + app_name + separator + env_name + separator + loc_name)


        return scopes

    def generate_group_strings(self):
        groups = []

        all_string = '-All-'

        roles = list(self.rol_labels_by_string.keys())
        if len(roles) == 0:
            roles.append(all_string)
        for role in roles:
            apps = list(self.app_labels_by_string.keys())
            if len(apps) == 0:
                apps.append(all_string)
            for app in apps:
                envs = list(self.env_labels_by_string.keys())
                if len(envs) == 0:
                    envs.append(all_string)
                for env in envs:
                    locs = list(self.loc_labels_by_string.keys())
                    if len(locs) == 0:
                        locs.append(all_string)
                    for loc in locs:
                        group_name = role + '|' + app + '|' + env + '|' + loc
                        groups.append(group_name)

        return groups


def label_tuple_to_group_name(role: pylo.Label, app: pylo.Label, env: pylo.Label, loc: pylo.Label):
    if role is None:
        string = 'R-All_'
    else:
        string = role.name + '_'

    if app is None:
        string += 'A-All_'
    else:
        string += app.name + '_'

    if env is None:
            string += 'E-All_'
    else:
        string += env.name + '_'

    if loc is None:
        string += 'L-All'
    else:
        string += loc.name


    return string


if args['use_cache']:
    print("Loading Origin PCE configuration from '{}' or cached file... ".format(hostname), end="", flush=True)
    org = pylo.Organization.get_from_cache_file(hostname)
    print("OK!\n")
else:
    print("Loading Origin PCE configuration from '{}'... ".format(hostname), end="", flush=True)
    org = pylo.Organization.get_from_api_using_credential_file(hostname, prompt_for_api_key=True)
    print("OK!\n")

print(org.stats_to_str())
print()

print('*** Generating label_resolution_cache... ', end='', flush=True)
start_time = time.time()
tracker = track(org.LabelStore.generate_label_resolution_cache)
tracker()
elapsed_time = time.time() - start_time
print(' {} seconds ***'.format(round(elapsed_time, 2)))


resolved_labels_groups_to_include: Dict[str, Any] = {}


rulesets_json = []
hosts_json = []
services_json = []
groups_json = []
iplists_json = []

json_output = {'ruleset': rulesets_json,
               'hosts': hosts_json,
               'services': services_json,
               'groups': groups_json,
               'iplists': iplists_json
               }


class TagIndex:
    def __init__(self):
        self.tags_dict = {}

    def get_or_create_tag_index(self, tag_name: str):
        tags_dict = self.tags_dict
        tag_name = tag_name.lower()
        if tag_name is None or len(tag_name) <= 0:
            raise pylo.PyloEx("Tag name is null or")
        index = tags_dict.get(tag_name)
        if index is not None:
            return index
        index = len(tags_dict)
        tags_dict[tag_name] = index
        return index

    def add_tags_name_to_existing_list(self, target_list):
        elements = [None] * len(self.tags_dict)
        for tag, index in self.tags_dict.items():
            elements[index] = tag

        target_list.extend(elements)


# <editor-fold desc="RuleSets Export">
start_time = time.time()
print("*** Generating Rules ***")

csv_rulesets_headers = ['ruleset_name', 'src', 'dst', 'svc', 'rule_description', 'ruleset_description', 'ruleset_href', 'ruleset_href']
csv_rulesets_rows = []

rulesets_tags = TagIndex()

for ruleset in org.RulesetStore.rulesets:
    rules = []
    r_json = {'href': ruleset.href, 'name': ruleset.name, 'description': ruleset.description,'rules': rules}
    rulesets_json.append(r_json)

    log.debug(" - Handling ruleset '{}'".format(ruleset.name))

    for rule in ruleset.rules_by_href.values():
        log.debug("   - Handling rule '{}'".format(rule.href))

        if not rule.enabled:
            log.debug("     - Rule is disabled, skipping!")
            continue

        scope_type = 'intra'
        if rule.unscoped_consumers:
            scope_type = 'extra'
        resulting_rules = []

        rule_json = {'href': rule.href, 'type': scope_type, 'description': rule.description, 'resulting_rules': resulting_rules}
        rules.append(rule_json)

        if not rule.unscoped_consumers:
            # pylo.log.warning("Intra scope rules are not supported, be careful!")
            continue

        # Extracting Tags
        rule_tags = [None] * len(rulesets_tags.tags_dict)
        if rule.description is not None:
            tags = re.findall(tag_regex, rule.description)
            if len(tags) > 0:
                for match in tags:
                    log.debug("     - Found tag regex match: {}".format(match))
                    parts = match.split(':')
                    if len(parts) != 2:
                        raise pylo.PyloEx("The following TAG has wrong syntax [key:value] : '{}'".format(match))
                    if len(parts[0]) <= 0:
                        raise pylo.PyloEx("The following TAG has wrong key syntax [key:value] : '{}'".format(match))
                    key_index = rulesets_tags.get_or_create_tag_index(parts[0])
                    if key_index >= len(rule_tags):
                        rule_tags.append(parts[1])
                    else:
                        rule_tags[key_index] = parts[1]
                    log.debug("     - Tag Key/Value #{} added: {}/{}".format(len(rule_tags), parts[0], parts[1]))


        # <editor-fold desc="Handling of Services">
        local_services_json = []
        csv_svc_members = []
        for service in rule.services._items.values():
            local_services_json.append({'type': 'service', 'name': service.name, 'href': service.href})
            csv_svc_members.append(service.name)

        for service in rule.services._direct_services:
            local_services_json.append({'type': 'direct_in_rule', 'name': service.to_string_standard()})
            csv_svc_members.append(service.to_string_standard())

        csv_svc_members = pylo.string_list_to_text(csv_svc_members)

        # </editor-fold">

        r_rule = {'src': [], 'dst': [], 'svc': local_services_json}
        resulting_rules.append(r_rule)

        # <editor-fold desc="Handling of Consumers">

        csv_src_members = []

        if rule.consumers._hasAllWorkloads:
            r_rule['src'].append({'type': 'special', 'name': 'All Workloads'})
            csv_src_members.append('All Workloads')

        consumer_scope_matrix = ScopeMatrix()

        for entry in rule.consumers._items.values():
            if isinstance(entry, pylo.IPList):
                r_rule['src'].append({'type': 'iplist', 'name': entry.name})
                csv_src_members.append(entry.name)
            elif isinstance(entry, pylo.Workload):
                r_rule['src'].append({'type': 'host', 'name': entry.get_name()})
                csv_src_members.append(entry.get_name())
            elif isinstance(entry, pylo.Label):
                    consumer_scope_matrix.add_label(entry)
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(entry)))

        if not consumer_scope_matrix.is_empty():
            for scope in consumer_scope_matrix.generate_scopes():
                scope_string = label_tuple_to_group_name(scope['role'], scope['app'], scope['env'], scope['loc'])
                resolved_labels_groups_to_include[scope_string] = scope
                r_rule['src'].append({'type': 'group', 'name': scope_string})
                csv_src_members.append(scope_string)

        # </editor-fold>

        # <editor-fold desc="Handling of Providers">
        csv_dst_members = []

        provider_scope_matrix = ScopeMatrix()

        for entry in rule.providers._items.values():
            if isinstance(entry, pylo.IPList):
                r_rule['dst'].append({'type': 'iplist', 'name': entry.name})
                csv_dst_members.append(entry.name)
            elif isinstance(entry, pylo.Workload):
                r_rule['dst'].append({'type': 'host', 'name': entry.get_name()})
                csv_dst_members.append(entry.get_name())
            elif isinstance(entry, pylo.Label):
                provider_scope_matrix.add_label(entry)
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(entry)))

        if not provider_scope_matrix.is_empty() or rule.providers._hasAllWorkloads:
            log.debug("     - Provider is not empty of Labels or 'All Workload':")

            ruleset_scopes = list(ruleset.scopes.scope_entries.values())
            # there is no scope entry for All All All
            if len(ruleset_scopes) == 0:
                ruleset_scopes.append(pylo.RulesetScopeEntry())

            for ruleset_scope in ruleset_scopes:
                log.debug("     - handling Ruleset Scope: {}".format(ruleset_scope.to_string()))
                if ruleset_scope.is_all_all_all() and rule.providers._hasAllWorkloads:
                    r_rule['dst'].append({'type': 'special', 'name': 'All Workloads'})
                    csv_dst_members.append('All Workloads')

                resulting_scopes = provider_scope_matrix.calculate_scopes_with_filter(ruleset_scope)
                for resulting_scope in resulting_scopes:
                    # if All Workloads is used and scope is all/all/all , it was already covered
                    if ruleset_scope.is_all_all_all() and resulting_scope['role'] is None and resulting_scope['app'] is None and resulting_scope['env'] is None and resulting_scope['loc'] is None:
                        log.debug('      - skipped All Workloads in All/All/All')
                        continue
                    resulting_scope_string = label_tuple_to_group_name(resulting_scope['role'], resulting_scope['app'], resulting_scope['env'], resulting_scope['loc'])
                    resolved_labels_groups_to_include[resulting_scope_string] = resulting_scope
                    r_rule['dst'].append({'type': 'group', 'name': resulting_scope_string})
                    csv_dst_members.append(resulting_scope_string)
        # </editor-fold>

        # <editor-fold desc="CSV making">

        if rule.description is not None:
            sanitized_rule_desc = rule.description.replace("\n",' ')
        else:
            sanitized_rule_desc = ''
        if ruleset.description is not None:
            sanitized_ruleset_desc = ruleset.description.replace("\n",' ')
        else:
            sanitized_ruleset_desc = ''

        row = [ruleset.name, pylo.string_list_to_text(csv_src_members),
               pylo.string_list_to_text(csv_dst_members), csv_svc_members,
               sanitized_rule_desc,
               sanitized_ruleset_desc,
               rule.href,
               ruleset.href]
        row.extend(rule_tags)

        csv_rulesets_rows.append(row)
        # </editor-fold>

rulesets_tags.add_tags_name_to_existing_list(csv_rulesets_headers)



elapsed_time = time.time() - start_time
print('*** Rules generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>

# <editor-fold desc="Workloads Export">
print("\n*** Generating Workloads ***")
start_time = time.time()


csv_workload_header = ['name', 'addresses', 'description', 'href']
csv_workload_rows = []

workloads_tags = TagIndex()

for workload in org.WorkloadStore.itemsByHRef.values():
    workload_json = {'name': workload.get_name(), 'href': workload.href, 'description': workload.description}

    # Extracting Tags
    workload_tags = [None] * len(workloads_tags.tags_dict)
    if workload.description is not None:
        tags = re.findall(tag_regex, workload.description)
        if len(tags) > 0:
            for match in tags:
                log.debug("     - Found tag regex match: {}".format(match))
                parts = match.split(':')
                if len(parts) != 2:
                    raise pylo.PyloEx("The following TAG has wrong syntax [key:value] : '{}'".format(match))
                if len(parts[0]) <= 0:
                    raise pylo.PyloEx("The following TAG has wrong key syntax [key:value] : '{}'".format(match))
                key_index = workloads_tags.get_or_create_tag_index(parts[0])
                if key_index >= len(workload_tags):
                    workload_tags.append(parts[1])
                else:
                    workload_tags[key_index] = parts[1]
                log.debug("     - Tag Key/Value #{} added: {}/{}".format(len(workload_tags), parts[0], parts[1]))

    interfaces = []

    for interface in workload.interfaces:
        interfaces.append(interface.ip)

    workload_json['addresses'] = pylo.string_list_to_text(interfaces)

    hosts_json.append(workload_json)
    if workload.description is not None:
        sanitized_workload_desc = workload.description.replace("\n", ' ')
    else:
        sanitized_workload_desc = ''

    row = [workload.get_name(), workload_json['addresses'], sanitized_workload_desc, workload.href]
    row.extend(workload_tags)
    csv_workload_rows.append(row)

workloads_tags.add_tags_name_to_existing_list(csv_workload_header)

elapsed_time = time.time() - start_time
print('*** Workloads generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>

# <editor-fold desc="Services Export">
print("\n*** Generating Services ***")
start_time = time.time()

csv_services_rows = []
csv_services_header = ['name', 'ports', 'description', 'href']

services_tags = TagIndex()

for service in org.ServiceStore.itemsByHRef.values():
    service_json = {'name': service.name, 'href': service.href, 'description': service.description}

    # Extracting Tags
    service_tags = [None] * len(services_tags.tags_dict)
    if service.description is not None:
        tags = re.findall(tag_regex, service.description)
        if len(tags) > 0:
            for match in tags:
                log.debug("     - Found tag regex match: {}".format(match))
                parts = match.split(':')
                if len(parts) != 2:
                    raise pylo.PyloEx("The following TAG has wrong syntax [key:value] : '{}'".format(match))
                if len(parts[0]) <= 0:
                    raise pylo.PyloEx("The following TAG has wrong key syntax [key:value] : '{}'".format(match))
                key_index = services_tags.get_or_create_tag_index(parts[0])
                if key_index >= len(service_tags):
                    service_tags.append(parts[1])
                else:
                    service_tags[key_index] = parts[1]
                log.debug("     - Tag Key/Value #{} added: {}/{}".format(len(service_tags), parts[0], parts[1]))

    ports = []

    for entry in service.entries:
        ports.append(entry.to_string_standard())

    service_json['ports'] = pylo.string_list_to_text(ports)

    services_json.append(service_json)
    if service.description is not None:
        sanitized_service_desc = service.description.replace("\n",' ')
    else:
        sanitized_service_desc = ''

    row = [service.name, service_json['ports'], sanitized_service_desc, service.href]
    row.extend(service_tags)
    csv_services_rows.append(row)
services_tags.add_tags_name_to_existing_list(csv_services_header)


elapsed_time = time.time() - start_time
print('*** Services generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>

# <editor-fold desc="Groups Export">
print("\n*** Generating Groups ***")
start_time = time.time()

csv_groups_rows = []
csv_groups_header = ['name', 'members']

for group_name, label_set in resolved_labels_groups_to_include.items():
    members_json = []
    members_name = []
    workloads = org.LabelStore.get_workloads_by_label_scope(label_set['role'], label_set['app'], label_set['env'], label_set['loc'])

    for workload in workloads:
        members_json.append({'type': 'workload', 'name': workload.get_name(), 'href': workload.href})
        members_name.append(workload.get_name())

    members_name = pylo.string_list_to_text(members_name)
    csv_groups_rows.append([group_name, members_name])

    groups_json.append({'name': group_name, 'members': members_json})



elapsed_time = time.time() - start_time
print('*** Groups generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>

# <editor-fold desc="IPLists Export">
print("\n*** Generating IPLists ***")
start_time = time.time()

csv_iplists_rows = []
csv_iplists_headers = ['name', 'members', 'description', 'href']

iplists_tags = TagIndex()

for iplist in org.IPListStore.items_by_href.values():
    iplist_json = {'name': iplist.name, 'href': iplist.href, 'description': iplist.description}

    # Extracting Tags
    iplist_tags = [None] * len(iplists_tags.tags_dict)
    if iplist.description is not None:
        tags = re.findall(tag_regex, iplist.description)
        if len(tags) > 0:
            for match in tags:
                log.debug("     - Found tag regex match: {}".format(match))
                parts = match.split(':')
                if len(parts) != 2:
                    raise pylo.PyloEx("The following TAG has wrong syntax [key:value] : '{}'".format(match))
                if len(parts[0]) <= 0:
                    raise pylo.PyloEx("The following TAG has wrong key syntax [key:value] : '{}'".format(match))
                key_index = iplists_tags.get_or_create_tag_index(parts[0])
                if key_index >= len(iplist_tags):
                    iplist_tags.append(parts[1])
                else:
                    iplist_tags[key_index] = parts[1]
                log.debug("     - Tag Key/Value #{} added: {}/{}".format(len(iplist_tags), parts[0], parts[1]))

    members = []

    for entry in iplist.raw_entries:
        members.append(entry)

    iplist_json['members'] = pylo.string_list_to_text(members)

    iplists_json.append(iplist_json)
    if iplist.description is not None:
        sanitized_iplist_desc = iplist.description.replace("\n", ' ')
    else:
        sanitized_iplist_desc = ''

    row = [iplist.name, iplist_json['members'], sanitized_iplist_desc, iplist.href]
    row.extend(iplist_tags)
    csv_iplists_rows.append(row)

iplists_tags.add_tags_name_to_existing_list(csv_iplists_headers)

elapsed_time = time.time() - start_time
print('*** IPLists generated in {} seconds ***'.format(round(elapsed_time, 2)))
# </editor-fold>


print("\n*** Saving objects to '{}'... ".format(json_dump_file), flush=True, end='')
with open(json_dump_file, 'w') as outfile:
    json.dump(json_output, outfile, indent=True)
print("OK!")

print("*** Saving workloads to '{}'... ".format(csv_hosts_file), flush=True, end='')
with open(csv_hosts_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_workload_header)
    filewriter.writerows(csv_workload_rows)
print("OK!")

print("*** Saving services to '{}'... ".format(csv_services_file), flush=True, end='')
with open(csv_services_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_services_header)
    filewriter.writerows(csv_services_rows)
print("OK!")

print("*** Saving groups to '{}'... ".format(csv_groups_file), flush=True, end='')
with open(csv_groups_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_groups_header)
    filewriter.writerows(csv_groups_rows)
print("OK!")

print("*** Saving Rulesets to '{}'... ".format(csv_rulesets_file), flush=True, end='')
with open(csv_rulesets_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_rulesets_headers)
    filewriter.writerows(csv_rulesets_rows)
print("OK!")

print("*** Saving IPLists to '{}'... ".format(csv_iplists_file), flush=True, end='')
with open(csv_iplists_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_iplists_headers)
    filewriter.writerows(csv_iplists_rows)
print("OK!")

print("\nEND OF SCRIPT\n")

