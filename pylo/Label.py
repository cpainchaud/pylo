import pylo


class Label(pylo.ReferenceTracker, pylo.LabelCommon):

    def __init__(self, name, href, ltype, owner):
        pylo.ReferenceTracker.__init__(self)
        pylo.LabelCommon.__init__(self, name, href, ltype, owner)

    def is_group(self):
        return True

    def is_label(self):
        return False

    def reference_obj(self):
        return { "href": self.href, "value": self.name, "key": self.type_to_short_string() }

