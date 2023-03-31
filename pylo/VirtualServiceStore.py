from typing import Dict, List, Optional
import pylo
from pylo import log
from .API.JsonPayloadTypes import VirtualServiceObjectJsonStructure


class VirtualServiceStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner: 'pylo.Organization' = owner
        self.itemsByHRef: Dict[str, 'pylo.VirtualService'] = {}
        self.itemsByName: Dict[str, 'pylo.VirtualService'] = {}

    def load_virtualservices_from_json(self, json_list: List[VirtualServiceObjectJsonStructure]):
        for json_item in json_list:
            if 'name' not in json_item or 'href' not in json_item:
                raise pylo.PyloEx(
                    "Cannot find 'value'/name or href for VirtualService in JSON:\n" + pylo.nice_json(json_item))

            new_item_name = json_item['name']
            new_item_href = json_item['href']

            new_item = pylo.VirtualService(new_item_name, new_item_href, self)
            new_item.load_from_json(json_item)

            if new_item_href in self.itemsByHRef:
                raise pylo.PyloEx("A VirtualService with href '%s' already exists in the table", new_item_href)

            if new_item_name in self.itemsByName:
                raise pylo.PyloEx(
                    "A VirtualService with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (
                        new_item_name, new_item_href, self.itemsByName[new_item_name].href)
                )

            self.itemsByHRef[new_item_href] = new_item
            self.itemsByName[new_item_name] = new_item

            log.debug("Found VirtualService '%s' with href '%s'", new_item_name, new_item_href)
            
    def find_by_href(self, href: str) -> Optional['pylo.VirtualService']:
        return self.itemsByHRef.get(href)

    def find_by_href_or_create_tmp(self, href: str, tmp_name: str) -> 'pylo.VirtualService':
        """
        Mostly used for deleted objects/developers speficic use cases when load imcomplete or partial PCE objects
        """
        find_object = self.find_by_href(href)
        if find_object is not None:
            return find_object

        new_tmp_item = pylo.VirtualService(tmp_name, href, self)
        new_tmp_item.deleted = True
        new_tmp_item.temporary = True

        self.itemsByHRef[href] = new_tmp_item
        self.itemsByName[tmp_name] = new_tmp_item

        return new_tmp_item
