from typing import Dict, Union
import pylo
from typing import *

from pylo import Label


class LabelGroup(pylo.ReferenceTracker, pylo.LabelCommon):

    def __init__(self, name: str, href: str, ltype: int, owner):
        pylo.ReferenceTracker.__init__(self)
        pylo.LabelCommon.__init__(self, name, href, ltype, owner)
        self._members: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}
        self.raw_json = None

    def load_from_json(self):
        # print(self.raw_json)
        if 'labels' in self.raw_json:
            for href_record in self.raw_json['labels']:
                if 'href' in href_record:
                    find_label = self.owner.find_by_href_or_die(href_record['href'])
                    find_label.add_reference(self)
                    self._members[find_label.name] = find_label
                else:
                    raise pylo.PyloEx('LabelGroup member has no HREF')

    def expand_nested_to_array(self):
        results = {}
        for label in self._members.values():
            if isinstance(label, pylo.Label):
                results[label] = label
            elif isinstance(label, pylo.LabelGroup):
                for nested_label in label.expand_nested_to_array():
                    results[nested_label] = nested_label
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(label)))
        return list(results.values())

    def get_api_reference_json(self) -> Dict:
        return {'label_group': {'href': self.href}}

    def get_members(self) -> Dict[str, 'pylo.Label']:
        data = {}
        for label in self._members.values():
            data[label.href] = label
        return data

    def is_group(self) -> bool:
        return True

    def is_label(self) -> bool:
        return False

