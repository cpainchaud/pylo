from typing import Optional, List, Union, Dict

import pylo
from pylo import log, Organization
import re

ruleset_id_extraction_regex = re.compile(r"^/orgs/([0-9]+)/sec_policy/([0-9]+)?(draft)?/rule_sets/(?P<id>[0-9]+)$")


class RulesetScope:

    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner: 'pylo.Ruleset' = owner
        self.scope_entries: Dict['pylo.RulesetScopeEntry', 'pylo.RulesetScopeEntry'] = {}

    def load_from_json(self, data):
        for scope_json in data:
            scope_entry = pylo.RulesetScopeEntry(self)
            scope_entry.load_from_json(scope_json)
            self.scope_entries[scope_entry] = scope_entry

    def get_all_scopes_str(self, label_separator='|', scope_separator="\n", use_href: bool = False):
        result = ''
        for scope in self.scope_entries.keys():
            if len(result) < 1:
                result += scope.to_string(label_separator,use_href=use_href)
            else:
                result += scope_separator
                result += scope.to_string(label_separator,use_href=use_href)

        return result


class RulesetScopeEntry:

    def __init__(self, owner: 'pylo.RulesetScope'):
        self.owner: pylo.RulesetScope = owner
        self.loc_label: Optional['pylo.Label'] = None
        self.env_label: Optional['pylo.Label'] = None
        self.app_label: Optional['pylo.Label'] = None

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
                raise pylo.PyloEx("Unsupported label type '{}' named '{}' in scope of ruleset '{}'/'{}'".format(label.type_string(),
                                                                                                                label.name,
                                                                                                                self.owner.owner.href,
                                                                                                                self.owner.owner.name))

    def to_string(self, label_separator = '|', use_href=False):
        string = 'All' + label_separator
        if self.app_label is not None:
            if use_href:
                string = self.app_label.href + label_separator
            else:
                string = self.app_label.name + label_separator

        if self.env_label is None:
            string += 'All' + label_separator
        else:
            if use_href:
                string += self.env_label.href + label_separator
            else:
                string += self.env_label.name + label_separator

        if self.loc_label is None:
            string += 'All'
        else:
            if use_href:
                string += self.loc_label.href
            else:
                string += self.loc_label.name

        return string

    def is_all_all_all(self):
        if self.app_label is None and self.env_label is None and self.loc_label is None:
            return True
        return False


class Ruleset:

    name: str
    href: Optional[str]
    description: str

    def __init__(self, owner: 'pylo.RulesetStore'):
        self.owner: 'pylo.RulesetStore' = owner
        self.href: Optional[str] = None
        self.name: str = ''
        self.description: str = ''
        self.scopes: 'pylo.RulesetScope' = pylo.RulesetScope(self)
        self.rules_by_href: Dict[str, 'pylo.Rule'] = {}

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
        self.rules_by_href[new_rule.href] = new_rule
        return new_rule

    def api_delete_rule(self, rule: Union[str, 'pylo.Rule']):
        """

        :param rule: should be href string or a Rule object
        """
        href = rule
        if isinstance(rule, pylo.Rule):
            href = rule.href

        find_object = self.rules_by_href.get(href)
        if find_object is None:
            raise pylo.PyloEx("Cannot delete a Rule with href={} which is not part of ruleset {}/{}".format(href, self.name, self.href))

        self.owner.owner.connector.objects_rule_delete(href)
        del self.rules_by_href[href]

    def api_create_rule(self, intra_scope: bool,
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
        return len(self.rules_by_href)

    def extract_id_from_href(self):
        match = ruleset_id_extraction_regex.match(self.href)
        if match is None:
            raise pylo.PyloEx("Cannot extract ruleset_id from href '{}'".format(self.href))

        return match.group("id")

    def get_ruleset_url(self, pce_hostname: str = None, pce_port: int = None):
        if pce_hostname is None or pce_port is None:
            connector = pylo.find_connector_or_die(self)
            if pce_hostname is None:
                pce_hostname = connector.hostname
            if pce_port is None:
                pce_port = connector.port

        return 'https://{}:{}/#/rulesets/{}/draft/rules/'.format(pce_hostname, pce_port, self.extract_id_from_href())

    def api_set_name(self, new_name: str):
        find_collision = self.owner.find_ruleset_by_name(new_name)
        if find_collision is not self:
            raise pylo.PyloEx("A Ruleset with name '{}' already exists".format(new_name))

        self.owner.owner.connector.objects_ruleset_update(self.href, update_data={'name': new_name})
        self.name = new_name

    def api_set_description(self, new_description: str):
        self.owner.owner.connector.objects_ruleset_update(self.href, update_data={'description': new_description})
        self.description = new_description
