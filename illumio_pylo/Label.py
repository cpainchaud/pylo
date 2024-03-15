from .API.JsonPayloadTypes import LabelHrefRef
from .ReferenceTracker import ReferenceTracker
from .LabelCommon import LabelCommon
from .LabelStore import LabelStore


class Label(ReferenceTracker, LabelCommon):
    def __init__(self, name, href, label_type: str, owner: 'LabelStore'):
        ReferenceTracker.__init__(self)
        LabelCommon.__init__(self, name, href, label_type, owner)

    def __str__(self):
        return f"Label: {self.name} ({self.type})"

    __repr__ = __str__

    def is_group(self) -> bool:
        return False

    def is_label(self) -> bool:
        return True

    def reference_obj(self):
        return {"href": self.href,
                "value": self.name,
                "key": self.type}

    def get_api_reference_json(self) -> LabelHrefRef:
        return {'label': {'href': self.href}}

