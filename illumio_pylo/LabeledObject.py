from typing import Dict, Optional, List, Union, Iterable
import illumio_pylo as pylo


class LabeledObject:

    __slots__ = ['_labels']

    def __init__(self):
        self._labels: Dict[str, 'pylo.Label'] = {}

    def get_label(self, key: str) -> Optional['pylo.Label']:
        return self._labels.get(key)

    def get_labels_dict(self):
        return self._labels.copy()

    def get_labels(self) -> Iterable['pylo.Label']:
        return self._labels.values()

    def set_label(self, label :'pylo.Label'):
        self._labels[label.type] = label

    def get_label_name(self, key: str, not_found_return_value = None):
        label = self.get_label(key)
        return label.name if label else not_found_return_value

    def uses_label(self, label: Union['pylo.Label', 'pylo.LabelGroup']):
        if isinstance(label, pylo.Label):
            if label.type not in self._labels:
                return False
            return label in self._labels[label.type]

        members = label.get_members().values()
        return self.uses_all_labels(members)

    def uses_all_labels(self, labels: Union[Iterable['pylo.Label'], Dict[str, Iterable['pylo.Label']]]):
        # for each type of label is must match at least one
        # make dict of list of labels by type
        labels_by_type: Dict[str, List['pylo.Label']] = {}

        if isinstance(labels, dict):
            labels_by_type = labels
        else:
            labels_by_type = pylo.LabelStore.Utils.list_to_dict_by_type(labels)

        for label_type, label_list in labels_by_type.items():
            if label_type not in self._labels:
                return False
            if not self._labels[label_type] in label_list:
                return False
        return True

    def is_using_label(self, label: Union['pylo.Label', 'pylo.LabelGroup']) -> bool:
        if isinstance(label, pylo.Label):
            return label.type in self._labels
        else:
            return any([label.type in self._labels for label in label.labels])

