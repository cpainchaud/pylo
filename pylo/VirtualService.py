from typing import Optional
from .API.JsonPayloadTypes import VirtualServiceObjectJsonStructure
import pylo


class VirtualService(pylo.ReferenceTracker):
    def __init__(self, name: str, href: str, owner: 'pylo.VirtualServiceStore'):
        pylo.ReferenceTracker.__init__(self)
        self.owner = owner
        self.name: str = name
        self.href: str = href

        self.raw_json: Optional[VirtualServiceObjectJsonStructure] = None

    def load_from_json(self, data: VirtualServiceObjectJsonStructure):
        self.raw_json = data

