from typing import Dict, Union
import pylo
from typing import *

from pylo import Label
from .API.JsonPayloadTypes import LabelGroupObjectJsonStructure


class LabelGroup(pylo.ReferenceTracker, pylo.LabelCommon):

    def __init__(self, name: str, href: str, label_type: str, owner):
        pylo.ReferenceTracker.__init__(self)
        pylo.LabelCommon.__init__(self, name, href, label_type, owner)
        self._members_by_href: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}
        self.raw_json: Optional[LabelGroupObjectJsonStructure] = None

    def load_from_json(self):
        # print(self.raw_json)
        if 'labels' in self.raw_json:
            for href_record in self.raw_json['labels']:
                if 'href' in href_record:
                    find_label = self.owner.find_by_href_or_die(href_record['href'])
                    find_label.add_reference(self)
                    self._members_by_href[find_label.href] = find_label
                else:
                    raise pylo.PyloEx('LabelGroup member has no HREF')

    def expand_nested_to_dict_by_href(self) -> Dict[str, 'pylo.Label']:
        results = {}
        for label in self._members_by_href.values():
            if isinstance(label, pylo.Label):
                results[label.href] = label
            elif isinstance(label, pylo.LabelGroup):
                for nested_label in label.expand_nested_to_dict_by_href().values():
                    results[nested_label.href] = nested_label
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(label)))
        return results

    def expand_nested_to_array(self) -> List['pylo.Label']:
        return list(self.expand_nested_to_dict_by_href().values())

    def get_api_reference_json(self) -> Dict:
        return {'label_group': {'href': self.href}}

    def get_members(self) -> Dict[str, 'pylo.Label']:
        data = {}
        for label in self._members_by_href.values():
            data[label.href] = label
        return data

    def get_members_count(self) -> int:
        return len(self._members_by_href)

    def has_member_with_href(self, href: str) -> bool:
        return href in self._members_by_href

    def has_member_object(self, label: 'pylo.Label') -> bool:
        return label.href in self._members_by_href

    def is_group(self) -> bool:
        return True

    def is_label(self) -> bool:
        return False

