from typing import List, Union, Dict, Optional
import illumio_pylo as pylo


class RuleSearchQueryResolvedResultSet:
    rules: Dict[str, 'pylo.Rule']
    rules_per_ruleset: Dict['pylo.Ruleset', Dict[str, 'pylo.Rule']]

    def count_results(self):
        return len(self.rules)

    def __init__(self, raw_json_data, organization: 'pylo.Organization'):
        self._raw_json = raw_json_data
        self.rules = {}
        self.rules_per_ruleset = {}

        for rule_data in raw_json_data:
            rule_href = rule_data.get('href')
            if rule_href is None:
                raise pylo.PyloEx('Cannot find rule HREF in RuleSearchQuery response', rule_data)
            rule_found = organization.RulesetStore.find_rule_by_href(rule_href)
            if rule_found is None:
                raise pylo.PyloEx("Cannot find rule with HREF '{}' in Organization".format(rule_href), rule_data)

            self.rules[rule_found.href] = rule_found
            ruleset_found = self.rules_per_ruleset.get(rule_found.owner)
            if ruleset_found is None:
                # print("new ruleset")
                self.rules_per_ruleset[rule_found.owner] = {rule_found.href: rule_found}
            else:
                # print("existing rs")
                self.rules_per_ruleset[rule_found.owner][rule_found.href] = rule_found


class RuleSearchQuery:
    _advanced_mode_consumer_labels: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}
    _advanced_mode_provider_labels: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}
    _basic_mode_labels: Dict[str, 'pylo.Label']
    connector: 'pylo.APIConnector'
    max_results: int = 10000

    def __init__(self, connector: 'pylo.APIConnector'):
        self.connector = connector
        self.mode_is_basic = True
        self._basic_mode_labels = {}
        self._advanced_mode_provider_labels = {}
        self._advanced_mode_consumer_labels = {}
        self._exact_matches = True
        self._mode_is_draft = True

    def set_basic_mode(self):
        self.mode_is_basic = True
        self._advanced_mode_provider_labels = {}
        self._advanced_mode_consumer_labels = {}

    def set_advanced_mode(self):
        self.mode_is_basic = False
        self._basic_mode_labels = {}

    def set_draft_mode(self):
        self._mode_is_draft = True

    def set_active_mode(self):
        self._mode_is_draft = False

    def set_max_results(self, max_results: int):
        if max_results < 1:
            raise pylo.PyloEx("max_results must be greater than 0")
        self.max_results = max_results

    def add_label(self, label: 'pylo.Label'):
        if not self.mode_is_basic:
            raise pylo.PyloEx('You can add labels to RuleSearchQuery only in Basic mode. Use consumer/provider counterparts with Advanced mode')
        self._basic_mode_labels[label.href] = label

    def add_consumer_label(self, label: 'pylo.Label'):
        if self.mode_is_basic:
            raise pylo.PyloEx('You can add labels to RuleSearchQuery consumers only in Advanced mode')
        self._advanced_mode_consumer_labels[label.href] = label

    def add_provider_label(self, label: 'pylo.Label'):
        if self.mode_is_basic:
            raise pylo.PyloEx('You can add labels to RuleSearchQuery providers only in Advanced mode')
        self._advanced_mode_provider_labels[label.href] = label

    def use_exact_matches(self):
        self._exact_matches = True

    def use_resolved_matches(self):
        self._exact_matches = False

    def execute(self):
        data = {'max_results': self.max_results}
        if not self._exact_matches:
            data['resolve_actors'] = True

        uri = '/sec_policy/draft/rule_search'
        if not self._mode_is_draft:
            uri = '/sec_policy/active/rule_search'

        if self.mode_is_basic:
            if len(self._basic_mode_labels) > 0:
                data['providers_or_consumers'] = []
                for label_href, label in self._basic_mode_labels.items():
                    if label.is_label():
                        data['providers_or_consumers'].append({'label': {'href': label_href}})
                    else:
                        data['providers_or_consumers'].append({'label_group': {'href': label_href}})
        else:
            if len(self._advanced_mode_provider_labels) > 0:
                data['providers'] = []
                for label_href, label in self._advanced_mode_provider_labels.items():
                    if label.is_label():
                        data['providers'].append({'label': {'href': label_href}})
                    else:
                        data['providers'].append({'label_group': {'href': label_href}})
            if len(self._advanced_mode_consumer_labels) > 0:
                data['consumers'] = []
                for label_href, label in self._advanced_mode_consumer_labels.items():
                    if label.is_label():
                        data['consumers'].append({'label': {'href': label_href}})
                    else:
                        data['consumers'].append({'label_group': {'href': label_href}})

        return self.connector.do_post_call(uri, data)

    def execute_and_resolve(self, organization: 'pylo.Organization'):
        return RuleSearchQueryResolvedResultSet(self.execute(), organization)
