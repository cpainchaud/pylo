from .ReferenceTracker import ReferenceTracker
from .LabelCommon import LabelCommon
import pylo



class Label(ReferenceTracker, LabelCommon):

    def __init__(self, name, href, ltype, owner: 'pylo.LabelStore'):
        ReferenceTracker.__init__(self)
        LabelCommon.__init__(self, name, href, ltype, owner)

    def is_group(self):
        return False

    def is_label(self):
        return True

    def reference_obj(self):
        return {"href": self.href,
                "value": self.name,
                "key": self.type_to_short_string()}

    def get_api_reference_json(self):
        return {'label': {'href': self.href}}

