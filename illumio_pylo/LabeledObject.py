from typing import Dict, Optional
import illumio_pylo as pylo


class LabeledObject:

    def __init__(self):
        self._labels: Dict[str, 'pylo.Label'] = {}

    def get_label(self, key: str) -> Optional['pylo.Label']:
        return self._labels.get(key)

    def get_labels_dict(self):
        return self._labels.copy()

    def get_labels(self):
        return self._labels.values()

    def set_label(self, label :'pylo.Label'):
        self._labels[label.type] = label

    def get_label_name(self, key: str, not_found_return_value = None):
        label = self.get_label(key)
        return label.name if label else not_found_return_value

