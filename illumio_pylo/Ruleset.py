from typing import Optional, List, Union, Dict

import illumio_pylo as pylo
from .API.JsonPayloadTypes import RuleObjectJsonStructure, RulesetObjectJsonStructure, \
    RulesetScopeEntryLineJsonStructure
import re

ruleset_id_extraction_regex = re.compile(r"^/orgs/([0-9]+)/sec_policy/([0-9]+)?(draft)?/rule_sets/(?P<id>[0-9]+)$")


class RulesetScope:

    __slots__ = ['owner', 'scope_entries']

    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner: 'pylo.Ruleset' = owner
        self.scope_entries: Dict['pylo.RulesetScopeEntry', 'pylo.RulesetScopeEntry'] = {}

    def load_from_json(self, data: List[List[RulesetScopeEntryLineJsonStructure]]):
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

    def has_at_least_one_all_all_all(self) -> bool:
        for scope_entry in self.scope_entries:
            if scope_entry.is_all_all_all():
                return True
        return False

class RulesetScopeEntry:

    __slots__ = ['owner', '_labels']

    def __init__(self, owner: 'pylo.RulesetScope'):
        self.owner: pylo.RulesetScope = owner
        self._labels: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}

    def load_from_json(self, data: List[RulesetScopeEntryLineJsonStructure]):
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

            label = l_store.find_by_href(href_entry)
            if label is None:
                raise pylo.PyloEx("Cannot find label with href '{}' in Ruleset '{}' scope: {}".format(href_entry,
                                                                                                      self.owner.owner.name,
                                                                                      pylo.nice_json(data)))

            if label.type not in self.owner.owner.owner.owner.LabelStore.label_types_as_set:
                raise pylo.PyloEx("Unsupported label type '{}' named '{}' in scope of ruleset '{}'/'{}'".format(label.type_string(),
                                                                                                                label.name,
                                                                                                                self.owner.owner.href,
                                                                                                                self.owner.owner.name))
            self._labels[label.type] = label

    @property
    def labels(self) -> List[Union['pylo.Label', 'pylo.LabelGroup']]:
        """
        Return a copy of the labels list
        """
        return list(self._labels.values())

    @property
    def labels_sorted_by_type(self) -> List[Union['pylo.Label', 'pylo.LabelGroup']]:
        """
        Return a copy of the labels list sorted by type which are defined by the LabelStore
        """
        return pylo.LabelStore.Utils.list_sort_by_type(self._labels.values(), self.owner.owner.owner.owner.LabelStore.label_types)

    @property
    def labels_by_type(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """
        Return a copy of the labels dict keyed by label type
        :return:
        """
        return self._labels.copy()

    @property
    def labels_by_href(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """
        Return labels dict keyed by label href
        :return:
        """
        return {label.href: label for label in self._labels.values()}


    def to_string(self, label_separator = '|', use_href=False):
        string = ''
        for label_type in self.owner.owner.owner.owner.LabelStore.label_types:
            label = self._labels.get(label_type)
            if len(string) > 0:
                string += label_separator
            if label is None:
                string += 'All'
            elif use_href:
                string += label.href
            else:
                string += label.name
        return string

    def is_all_all_all(self):
        return len(self._labels) == 0

    @property
    def loc_label(self) -> Optional['pylo.Label']:
        """
        @deprecated
        :return:
        """
        return self._labels.get('loc')

    @property
    def env_label(self) -> Optional['pylo.Label']:
        """
        @deprecated
        :return:
        """
        return self._labels.get('env')

    @property
    def app_label(self) -> Optional['pylo.Label']:
        """
        @deprecated
        :return:
        """
        return self._labels.get('app')


class Ruleset:

    __slots__ = ['owner', 'href', 'name', 'description', 'scopes', '_rules_by_href', '_rules', 'disabled']

    def __init__(self, owner: 'pylo.RulesetStore'):
        self.owner: 'pylo.RulesetStore' = owner
        self.href: Optional[str] = None
        self.name: str = ''
        self.description: str = ''
        self.scopes: 'pylo.RulesetScope' = pylo.RulesetScope(self)
        # must keep an ordered list of rules while the dict by href is there for quick searches
        self._rules_by_href: Dict[str, 'pylo.Rule'] = {}
        self._rules: List['pylo.Rule'] = []
        self.disabled: bool = False

    @property
    def rules(self):
        """
        Return a copy of the rules list
        :return:
        """
        return self._rules.copy()

    @property
    def rules_by_href(self):
        """
        Return a copy of the rules dict keyed by href
        :return:
        """
        return self._rules_by_href.copy()

    @property
    def rules_ordered_by_type(self):
        """
        Return a list of rules ordered by type (Intra Scope First)
        :return:
        """
        rules: List[pylo.Rule] = []
        for rule in self._rules:
            if rule.is_intra_scope():
                rules.append(rule)
        for rule in self._rules:
            if not rule.is_intra_scope():
                rules.append(rule)
        return rules


    def load_from_json(self, data: RulesetObjectJsonStructure):
        if 'name' not in data:
            raise pylo.PyloEx("Cannot find Ruleset name in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.name = data['name']

        if 'href' not in data:
            raise pylo.PyloEx("Cannot find Ruleset href in JSON data: \n" + pylo.Helpers.nice_json(data))
        self.href = data['href']

        if 'enabled' in data:
            self.disabled = not data['enabled']

        scopes_json = data.get('scopes')
        if scopes_json is None:
            raise pylo.PyloEx("Cannot find Ruleset scope in JSON data: \n" + pylo.Helpers.nice_json(data))

        self.description = data.get('description')
        if self.description is None:
            self.description = ''

        self.scopes.load_from_json(scopes_json)

        if 'rules' in data:
            for rule_data in data['rules']:
                self.load_single_rule_from_json(rule_data)

    def load_single_rule_from_json(self, rule_data: RuleObjectJsonStructure) -> 'pylo.Rule':
        new_rule = pylo.Rule(self)
        new_rule.load_from_json(rule_data)
        self._rules_by_href[new_rule.href] = new_rule
        self._rules.append(new_rule)
        return new_rule

    def api_delete_rule(self, rule_or_href: Union[str, 'pylo.Rule']):
        """

        :param rule_or_href: HRef string or a Rule object to be deleted
        """
        href = rule_or_href
        if isinstance(rule_or_href, pylo.Rule):
            href = rule_or_href.href

        find_object = self._rules_by_href.get(href)
        if find_object is None:
            raise pylo.PyloEx("Cannot delete a Rule with href={} which is not part of ruleset {}/{}".format(href, self.name, self.href))

        self.owner.owner.connector.objects_rule_delete(href)
        del self._rules_by_href[href]
        self._rules.remove(find_object)

    def api_create_rule(self, intra_scope: bool,
                        consumers: List['pylo.RuleActorsAcceptableTypes'],
                        providers: List['pylo.RuleActorsAcceptableTypes'],
                        services: List[Union['pylo.Service', 'pylo.DirectServiceInRule']],
                        description='', machine_auth=False, secure_connect=False, enabled=True,
                        stateless=False, consuming_security_principals=None,
                        resolve_consumers_as_virtual_services=True, resolve_consumers_as_workloads=True,
                        resolve_providers_as_virtual_services=True, resolve_providers_as_workloads=True) -> 'pylo.Rule':
        if consuming_security_principals is None:
            consuming_security_principals = []

        new_rule_json = self.owner.owner.connector.objects_rule_create(
            intra_scope=intra_scope, ruleset_href=self.href,
            consumers=consumers, providers=providers, services=services,
            description=description, machine_auth=machine_auth, secure_connect=secure_connect, enabled=enabled,
            stateless=stateless, consuming_security_principals=consuming_security_principals,
            resolve_consumers_as_virtual_services=resolve_consumers_as_virtual_services,
            resolve_consumers_as_workloads=resolve_consumers_as_workloads,
            resolve_providers_as_virtual_services=resolve_providers_as_virtual_services,
            resolve_providers_as_workloads=resolve_providers_as_workloads
        )

        return self.load_single_rule_from_json(new_rule_json)

    def count_rules(self) -> int:
        return len(self._rules)

    def extract_id_from_href(self) -> int:
        match = ruleset_id_extraction_regex.match(self.href)
        if match is None:
            raise pylo.PyloEx("Cannot extract ruleset_id from href '{}'".format(self.href))

        return int(match.group("id"))

    def get_ruleset_url(self, pce_fqdn: str = None, pce_port: int = None) -> str:
        if pce_fqdn is None or pce_port is None:
            connector = pylo.find_connector_or_die(self)
            if pce_fqdn is None:
                pce_fqdn = connector.fqdn
            if pce_port is None:
                pce_port = connector.port

        return 'https://{}:{}/#/rulesets/{}/draft/rules/'.format(pce_fqdn, pce_port, self.extract_id_from_href())

    def api_set_name(self, new_name: str):
        find_collision = self.owner.find_ruleset_by_name(new_name)
        if find_collision is not self:
            raise pylo.PyloEx("A Ruleset with name '{}' already exists".format(new_name))

        self.owner.owner.connector.objects_ruleset_update(self.href, update_data={'name': new_name})
        self.name = new_name

    def api_set_description(self, new_description: str):
        self.owner.owner.connector.objects_ruleset_update(self.href, update_data={'description': new_description})
        self.description = new_description
