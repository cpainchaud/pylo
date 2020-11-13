from typing import Optional, List, Union, Dict

import pylo
from pylo import log, Organization
import re

ruleset_id_extraction_regex = re.compile(r"^/orgs/([0-9]+)/sec_policy/([0-9]+)?(draft)?/rule_sets/(?P<id>[0-9]+)$")


class RulesetScope:
    """
    :type owner: pylo.Ruleset
    """

    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner = owner
        self.scope_entries = {}  # type: dict[pylo.RulesetScopeEntry, pylo.RulesetScopeEntry]

    def load_from_json(self, data):
        for scope_json in data:
            scope_entry = pylo.RulesetScopeEntry(self)
            scope_entry.load_from_json(scope_json)
            self.scope_entries[scope_entry] = scope_entry


    def get_all_scopes_str(self, label_separator='|', scope_separator="\n"):
        result = ''
        for scope in self.scope_entries.keys():
            if len(result) < 1:
                result += scope.to_string(label_separator)
            else:
                result += scope_separator
                result += scope.to_string(label_separator)

        return result


class RulesetScopeEntry:
    """
    :type owner: pylo.RulesetScope
    """

    def __init__(self, owner: 'pylo.RulesetScope'):
        self.owner = owner
        self.loc_label = None  # type: pylo.Label
        self.env_label = None  # type: pylo.Label
        self.app_label = None  # type: pylo.Label

    def load_from_json(self, data):
        self.loc_label = None
        #log.error(pylo.nice_json(data))
        l_store = self.owner.owner.owner.owner.LabelStore
        for label_json in data:
            label_entry = label_json.get('label')
            if label_entry is None:
                label_entry = label_json.get('label_group')
                if label_entry is None:
                    raise pylo.PyloEx("Cannot find 'label' or 'label_group' entry in scope: {}".format(pylo.nice_json(label_json)))
            href_entry = label_entry.get('href')
            if href_entry is None:
                raise pylo.PyloEx("Cannot find 'href' entry in scope: {}".format(pylo.nice_json(data)))

            label = l_store.find_by_href_or_die(href_entry)
            if label.type_is_location():
                self.loc_label = label
            elif label.type_is_environment():
                self.env_label = label
            elif label.type_is_application():
                self.app_label = label
            else:
                raise pylo.PyloEx("Unsupported label type '{}' in scope".format(label.type_string()))

    def to_string(self, label_separator = '|'):
        string = 'All' + label_separator
        if self.app_label is not None:
            string = self.app_label.name + label_separator

        if self.env_label is None:
            string += 'All' + label_separator
        else:
            string += self.env_label.name + label_separator

        if self.loc_label is None:
            string += 'All'
        else:
            string += self.loc_label.name

        return string

    def is_all_all_all(self):
        if self.app_label is None and self.env_label is None and self.loc_label is None:
            return True
        return False



class Ruleset:

    name: str
    description: str

    def __init__(self, owner: 'pylo.RulesetStore'):
        self.owner = owner
        self.href = None  # type: str
        self.name = ''
        self.description = ''
        self.scopes = pylo.RulesetScope(self)
        self.rules_byHref = {}  # type: dict[str,pylo.Rule]

    def load_from_json(self, data):
        if 'name' not in data:
            raise pylo.PyloEx("Cannot find Ruleset name in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.name = data['name']

        if 'href' not in data:
            raise pylo.PyloEx("Cannot find Ruleset href in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.href = data['href']

        if 'scopes' not in data:
            raise pylo.PyloEx("Cannot find Ruleset scope in JSON data: \n" + pylo.Helpers.nice_json(data))

        self.description = data.get('description')
        if self.description is None:
            self.description = ''

        self.scopes.load_from_json(data['scopes'])

        if 'rules' in data:
            for rule_data in data['rules']:
                self.load_single_rule_from_json(rule_data)

    def load_single_rule_from_json(self, rule_data) -> 'pylo.Rule':
        new_rule = pylo.Rule(self)
        new_rule.load_from_json(rule_data)
        self.rules_byHref[new_rule.href] = new_rule
        return new_rule


    def create_rule(self, intra_scope: bool,
                    consumers: List[Union['pylo.IPList', 'pylo.Label', 'pylo.LabelGroup', Dict]],
                    providers: List[Union['pylo.IPList', 'pylo.Label', 'pylo.LabelGroup', Dict]],
                    services: List[Union['pylo.Service', 'pylo.DirectServiceInRule', Dict]],
                    description='', machine_auth=False, secure_connect=False, enabled=True,
                    stateless=False, consuming_security_principals=[],
                    resolve_consumers_as_virtual_services=True, resolve_consumers_as_workloads=True,
                    resolve_providers_as_virtual_services=True, resolve_providers_as_workloads=True) -> 'pylo.Rule':

        new_rule_json = self.owner.owner.connector.objects_rule_create(
            intra_scope=intra_scope, ruleset_href=self.href,
            consumers=consumers, providers=providers, services=services,
            description=description, machine_auth=machine_auth, secure_connect=secure_connect, enabled=enabled,
            stateless=stateless, consuming_security_principals=consuming_security_principals,
            resolve_consumers_as_virtual_services=resolve_providers_as_virtual_services,
            resolve_consumers_as_workloads=resolve_consumers_as_workloads,
            resolve_providers_as_virtual_services=resolve_providers_as_virtual_services,
            resolve_providers_as_workloads=resolve_providers_as_workloads
        )

        return self.load_single_rule_from_json(new_rule_json)

    def count_rules(self):
        return len(self.rules_byHref)

    def extract_id_from_href(self):
        match = ruleset_id_extraction_regex.match(self.href)
        if match is None:
            raise pylo.PyloEx("Cannot extract ruleset_id from href '{}'".format(self.href))

        return match.group("id")

    def get_ruleset_url(self, pce_hostname: str, pce_port):
        return 'https://{}:{}/#/rulesets/{}/draft/rules/'.format(pce_hostname, pce_port, self.extract_id_from_href())


class Rule:
    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner = owner  # type: pylo.Ruleset
        self.description = None  # type: str
        self.services = pylo.RuleServiceContainer(self)
        self.providers = pylo.RuleHostContainer(self, 'providers')
        self.consumers = pylo.RuleHostContainer(self, 'consumers')
        self.consuming_principals = pylo.RuleSecurityPrincipalContainer(self)
        self.href = None
        self.enabled = True
        self.secure_connect = False
        self.unscoped_consumers = False

    def load_from_json(self, data):
        self.href = data['href']

        self.description = data.get('description')

        services = data.get('ingress_services')
        if services is not None:
            self.services.load_from_json(services)

        enabled = data.get('enabled')
        if enabled is not None:
            self.enabled = enabled

        secure_connect = data.get('sec_connect')
        if secure_connect is not None:
            self.secure_connect = secure_connect

        unscoped_consumers = data.get('unscoped_consumers')
        if unscoped_consumers is not None:
            self.unscoped_consumers = unscoped_consumers

        self.providers.load_from_json(data['providers'])
        self.consumers.load_from_json(data['consumers'])
        self.consuming_principals.load_from_json(data['consuming_security_principals'])

    def is_extra_scope(self):
        return self.unscoped_consumers

    def is_intra_scope(self):
        return not self.unscoped_consumers


class RuleSecurityPrincipalContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.SecurityPrincipal, pylo.SecurityPrincipal]

    def load_from_json(self, data):
        ssStore = self.owner.owner.owner.owner.SecurityPrincipalStore
        for item_data in data:
            wanted_href = item_data['href']
            found_object = ssStore.find_by_href_or_die(wanted_href)
            found_object.add_reference(self)
            self._items[found_object] = found_object


class DirectServiceInRule:
    def __init__(self, proto: int, port: int=None, toport: int = None):
        self.protocol = proto
        self.port = port
        self.to_port = toport

    def to_string_standard(self, protocol_first=True):
        if self.protocol == 17:
            if self.to_port is None:
                if protocol_first:
                    return 'udp/' + str(self.port)

                return str(self.port) + '/udp'
            if protocol_first:
                return 'udp/' + str(self.port) + '-' + str(self.to_port)

            return str(self.port) + '-' + str(self.to_port)+ '/udp'
        elif self.protocol == 6:
            if self.to_port is None:
                if protocol_first:
                    return 'tcp/' + str(self.port)
                return str(self.port) + '/tcp'

            if protocol_first:
                return 'tcp/' + str(self.port) + '-' + str(self.to_port)
            return str(self.port) + '-' + str(self.to_port)+ '/tcp'

        if protocol_first:
            return 'proto/' + str(self.protocol)

        return str(self.protocol) + '/proto'

    def get_api_json(self) -> Dict:
        if self.protocol != 17 and self.protocol != 6:
            return {'proto': self.protocol}

        if self.to_port is None:
            return {'proto': self.protocol, 'port': self.port}
        return {'proto': self.protocol, 'port': self.port, 'to_port': self.to_port}

    @staticmethod
    def create_from_text(txt: str, seperator='/', protocol_first=True) -> 'DirectServiceInRule':
        parts = txt.split(seperator)

        if len(parts) != 2:
            lower = txt.lower()
            if lower == 'icmp':
                return pylo.DirectServiceInRule(proto=1)
            raise pylo.PyloEx("Invalid service syntax '{}'".format(txt))

        if protocol_first:
            proto = parts[0]
            port_input = parts[1]
        else:
            proto = parts[1]
            port_input = parts[0]

        if not proto.isdigit():
            proto_lower = proto.lower()
            if proto_lower == 'tcp':
                protocol_int = 6
            elif proto_lower == 'udp':
                protocol_int = 17
            else:
                raise pylo.PyloEx("Invalid protocol provided: {}".format(proto))
        else:
            protocol_int = int(proto)

        port_parts = port_input.split('-')
        if len(port_parts) > 2:
            raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        if len(port_parts) == 2:
            if protocol_int != 17 and protocol_int != 6:
                raise pylo.PyloEx("Only TCP and UDP support port ranges so this service in invalid: '{}'".format(txt))
            from_port_input = port_parts[0]
            to_port_input = port_parts[1]

            if not from_port_input.isdigit():
                raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(from_port_input, txt))
            if not to_port_input.isdigit():
                raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(to_port_input, txt))

            return pylo.DirectServiceInRule(protocol_int, port=int(from_port_input), toport=int(to_port_input))

        if not port_input.isdigit():
            raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        return pylo.DirectServiceInRule(protocol_int, port=int(port_input))



class RuleServiceContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.Service, pylo.Service]
        self._direct_services = []

    def load_from_json_legacy_single(self, data):
        href = data.get('href')
        if href is None:
            raise Exception('Cannot find service HREF')

        find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
        if find_service is None:
            raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

        self._items[find_service] = find_service
        find_service.add_reference(self)

    def load_from_json(self, data_list):
        for data in data_list:
            # print(data)
            href = data.get('href')
            if href is None:
                port = data.get('port')
                if port is None:
                    raise pylo.PyloEx("unsupported service type in rule: {}".format(pylo.nice_json(data)))
                protocol = data.get('proto')
                if protocol is None:
                    raise pylo.PyloEx("Protocol not found in direct service use: {}".format(pylo.nice_json(data)))

                to_port = data.get('to_port')
                direct_port = DirectServiceInRule(protocol, port, to_port)
                self._direct_services.append(direct_port)

                continue

            find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
            if find_service is None:
                raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

            self._items[find_service] = find_service
            find_service.add_reference(self)


    def get_direct_services(self) -> List[DirectServiceInRule]:
        return self._direct_services

    def get_services(self) -> List[pylo.Service]:
        return list(self._items.values())



class RuleHostContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule', name: str):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.Label | pylo.LabelGroup | pylo.Workload, pylo.Label|pylo.LabelGroup|pylo.Workload]
        self.name = name
        self._hasAllWorkloads = False

    def load_from_json(self, data):
        for host_data in data:
            find_object = None
            if 'label' in host_data:
                href = host_data['label'].get('href')
                if href is None:
                    pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find Label with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'label_group' in host_data:
                href = host_data['label_group'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find LabelGroup with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'ip_list' in host_data:
                href = host_data['ip_list'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.IPListStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find IPList with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'workload' in host_data:
                href = host_data['workload'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                # @TODO : better handling of temporary objects
                find_object = self.owner.owner.owner.owner.WorkloadStore.itemsByHRef.get(href)
                if find_object is None:
                    # raise Exception("Cannot find Workload with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, pylo.nice_json(host_data)))
                    find_object = self.owner.owner.owner.owner.WorkloadStore.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'actors' in host_data:
                actor_value = host_data['actors']
                if actor_value is not None and actor_value == 'ams':
                    self._hasAllWorkloads = True
                    continue
                # TODO implement actors
                raise pylo.PyloEx("An actor that is not 'ams' was detected but this library doesn't support it yet", host_data)
            else:
                raise pylo.PyloEx("Unsupported reference type", host_data)

            if find_object is not None:
                self._items[find_object] = find_object
                find_object.add_reference(self)

    def has_workloads(self):
        for item in self._items.values():
            if isinstance(item, pylo.Workload):
                return True
        return False

    def has_labels(self):
        for item in self._items.values():
            if isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup):
                return True
        return False

    def get_labels(self) -> List[Union[pylo.Label,pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup):
                result.append(item)

        return result


    def has_iplists(self):
        for item in self._items.values():
            if isinstance(item, pylo.IPList):
                return True
        return False


    def get_iplists(self) -> List[pylo.IPList]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.IPList):
                result.append(item)

        return result

    def contains_all_workloads(self):
        return self._hasAllWorkloads


class RulesetStore:

    """
    :type itemsByHRef: dict[str,Ruleset]
    :type itemsByName: dict[str,Ruleset]
    """
    owner: pylo.Organization

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}
        self.itemsByName = {}

    def count_rulesets(self):
        return len(self.itemsByHRef)

    def count_rules(self):
        count = 0
        for ruleset in self.itemsByHRef.values():
            count += ruleset.count_rules()

        return count

    def load_rulesets_from_json(self, data):
        for json_item in data:
            self.load_single_ruleset_from_json(json_item)

    def load_single_ruleset_from_json(self, json_item):
        new_item = pylo.Ruleset(self)
        new_item.load_from_json(json_item)

        if new_item.href in self.itemsByHRef:
            raise Exception("A Ruleset with href '%s' already exists in the table, please check your JSON data for consistency. JSON:\n%s"
                            % (new_item.href, pylo.nice_json(json_item)) )

        if new_item.name in self.itemsByName:
            print("The following Ruleset is conflicting (name already exists): '%s' Href: '%s'" % (self.itemsByName[new_item.name].name, self.itemsByName[new_item.name].href), flush=True)
            raise Exception("A Ruleset with name '%s' already exists in the table, please check your JSON data for consistency. JSON:\n%s"
                            % (new_item.name, pylo.nice_json(json_item)))

        self.itemsByHRef[new_item.href] = new_item
        self.itemsByName[new_item.name] = new_item

        log.debug("Found Ruleset '%s' with href '%s'" % (new_item.name, new_item.href) )

        return new_item


    def find_rule_by_href(self, href: str) -> Optional['pylo.Rule']:
        for ruleset in self.itemsByHRef.values():
            rule = ruleset.rules_byHref.get(href)
            if rule is not None:
                return rule

        return None

    def find_ruleset_by_name(self, name: str, case_sensitive=True) -> Optional['pylo.Ruleset']:
        if case_sensitive:
            return self.itemsByName.get(name)

        lower_name = name.lower()

        for ruleset in self.itemsByHRef.values():
            if ruleset.name.lower() == lower_name:
                return ruleset

        return None

    def create_ruleset(self, name: str,
               scope_app: 'pylo.Label' = None,
               scope_env: 'pylo.Label' = None,
               scope_loc: 'pylo.Label' = None,
               description: str = '', enabled: bool = True) -> 'pylo.Ruleset':

        con = self.owner.connector
        json_item = con.objects_ruleset_create(name, scope_app, scope_env, scope_loc, description, enabled)
        return self.load_single_ruleset_from_json(json_item)