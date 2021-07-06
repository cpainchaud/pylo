from typing import Optional, List, Union, Dict

import pylo
from pylo import log, Organization
import re


class RulesetStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner: pylo.Organization = owner
        self.itemsByHRef: Dict[str, 'pylo.Ruleset'] = {}
        self.itemsByName: Dict[str, 'pylo.Ruleset'] = {}

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