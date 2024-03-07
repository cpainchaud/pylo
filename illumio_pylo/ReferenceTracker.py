import illumio_pylo as pylo
from illumio_pylo import log
from .Helpers import *


class ReferenceTracker:
    def __init__(self):
        self._references = {}  # type: dict[Referencer, Referencer]

    def add_reference(self, ref: 'pylo.Referencer'):
        self._references[ref] = ref

    def remove_reference(self, ref: 'pylo.Referencer'):
        index = self._references.get(ref)
        if index is None:
            raise Exception('Tried to unreference an object which is not actually referenced')
        self._references.pop(ref)

    def count_references(self):
        return len(self._references)

    def get_references(self):
        return self._references.values()

    def get_references_filter_by_class(self, classes):
        matches = []
        for obj in self._references.values():
            if type(obj) in classes:
                matches.append(obj)
        return matches


class Referencer:
    def reference_name_changed(self):
        raise Exception('not implemented')


class Pathable:
    def __init__(self):
        self.name = ''

