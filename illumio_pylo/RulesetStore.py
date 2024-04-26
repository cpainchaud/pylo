from typing import Optional, List, Union, Dict

import illumio_pylo as pylo
from illumio_pylo import log, Organization, PyloEx, Rule, Ruleset
from .Helpers import nice_json


class RulesetStore:

    __slots__ = ['owner', '_items_by_href']

    def __init__(self, owner: 'pylo.Organization'):
        self.owner: pylo.Organization = owner
        self._items_by_href: Dict[str, 'pylo.Ruleset'] = {}

    @property
    def rulesets(self) -> List['pylo.Ruleset']:
        """
        :return: a copy of the list of rulesets
        """
        return list(self._items_by_href.values())

    @property
    def rulesets_dict_by_href(self) -> Dict[str, 'pylo.Ruleset']:
        """
        :return: a copy of the dict of rulesets by href
        :return:
        """
        return self._items_by_href.copy()

    def count_rulesets(self) -> int:
        return len(self._items_by_href)

    def count_rules(self) -> int:
        count = 0
        for ruleset in self._items_by_href.values():
            count += ruleset.count_rules()

        return count

    def load_rulesets_from_json(self, data):
        for json_item in data:
            self.load_single_ruleset_from_json(json_item)

    def load_single_ruleset_from_json(self, json_item):
        new_item = Ruleset(self)
        new_item.load_from_json(json_item)

        if new_item.href in self._items_by_href:
            raise PyloEx(
                "A Ruleset with href '%s' already exists in the table, please check your JSON data for consistency. JSON:\n%s"
                % (new_item.href, nice_json(json_item)))

        self._items_by_href[new_item.href] = new_item

        log.debug("Found Ruleset '%s' with href '%s'" % (new_item.name, new_item.href))

        return new_item

    def find_rule_by_href(self, href: str) -> Optional['pylo.Rule']:
        for ruleset in self._items_by_href.values():
            rule = ruleset.rules_by_href.get(href)
            if rule is not None:
                return rule

        return None

    def find_ruleset_by_name(self, name: str, case_sensitive=True) -> Optional['pylo.Ruleset']:
        used_name = name
        if case_sensitive:
            used_name = name.lower()

        for ruleset in self._items_by_href.values():
            if ruleset.name.lower() == used_name:
                return ruleset

        return None

    def api_create_ruleset(self, name: str,
                           scope_app: 'pylo.Label' = None,
                           scope_env: 'pylo.Label' = None,
                           scope_loc: 'pylo.Label' = None,
                           description: str = '', enabled: bool = True) -> 'pylo.Ruleset':

        con = self.owner.connector
        json_item = con.objects_ruleset_create(name, scope_app, scope_env, scope_loc, description, enabled)
        return self.load_single_ruleset_from_json(json_item)

    def api_delete_ruleset(self, ruleset: Union[str, 'pylo.Ruleset']):
        """

        :param ruleset: should be href string or a Ruleset object
        """
        href = ruleset
        if isinstance(ruleset, Ruleset):
            href = ruleset.href

        find_object = self._items_by_href.get(href)
        if find_object is None:
            raise PyloEx("Cannot delete a Ruleset with href={} which is not part of this RulesetStore".format(href))

        self.owner.connector.objects_ruleset_delete(href)
        del self._items_by_href[href]
