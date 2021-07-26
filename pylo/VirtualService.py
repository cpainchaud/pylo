import pylo


class VirtualService(pylo.ReferenceTracker):
    def __init__(self, name: str, href: str, owner: 'pylo.VirtualServiceStore'):
        pylo.ReferenceTracker.__init__(self)
        self.owner = owner
        self.name: str = name
        self.href: str = href

        self.raw_json = None

    def load_from_json(self, data):
        self.raw_json = data

