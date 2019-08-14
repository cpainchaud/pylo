import pylo
from .Helpers import nice_json
from pylo import log


class SecurityPrincipal(pylo.ReferenceTracker):
    def __init__(self, name: str, href: str, owner: 'pylo.SecurityPrincipalStore'):
        pylo.ReferenceTracker.__init__(self)
        self.owner = owner
        self.name = name
        self.href = href
        self.sid = None
        self.deleted = False

        self.raw_json = None

    def load_from_json(self, data):
        self.raw_json = data
        # print(pylo.nice_json(data))

        self.sid = data['sid']
        self.deleted = data['deleted']



class SecurityPrincipalStore:
    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}  # type: dict[str,pylo.SecurityPrincipal]
        self.itemsByName = {}  # type: dict[str,pylo.SecurityPrincipal]

    def load_principals_from_json(self, json_list):
        for json_item in json_list:
            if 'name' not in json_item or 'href' not in json_item:
                raise pylo.PyloEx("Cannot find 'value'/name or href for SecurityPrincipal in JSON:\n" + nice_json(json_item))

            new_item_name = json_item['name']
            new_item_href = json_item['href']

            # SecurityPrincipals's name is None when it's provided by VEN through its hostname until it's manually overwritten
            # (eventually) by someone. In such a case, you need to use hostname instead
            if new_item_name is None:
                if 'hostname' not in json_item:
                    raise pylo.PyloEx("Cannot find 'value'/hostname in JSON:\n" + nice_json(json_item))
                new_item_name = json_item['hostname']

            new_item = pylo.SecurityPrincipal(new_item_name, new_item_href, self)
            new_item.load_from_json(json_item)

            if new_item_href in self.itemsByHRef:
                raise pylo.PyloEx("A SecurityPrincipal with href '%s' already exists in the table", new_item_href)

            if new_item_name in self.itemsByName:
                raise pylo.PyloEx(
                    "A SecurityPrincipal with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (
                        new_item_name, new_item_href, self.itemsByName[new_item_name].href))

            self.itemsByHRef[new_item_href] = new_item
            self.itemsByName[new_item_name] = new_item

            log.debug("Found SecurityPrincipal '%s' with href '%s'", new_item_name, new_item_href)

    def find_by_href_or_die(self, href: str):

        find_object = self.itemsByHRef.get(href)
        if find_object is None:
            raise pylo.PyloEx("Workload with HREF '%s' was not found" % href)

        return find_object


