import pylo
from pylo import log, IP4Map
from .Helpers import *
from typing import Optional, List


class WorkloadStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}  # type: dict[str,pylo.Workload]
        self.itemsByName = {}  # type: dict[str,pylo.Workload]

    def load_workloads_from_json(self, json_list):
        for json_item in json_list:
            if 'name' not in json_item or 'href' not in json_item:
                raise pylo.PyloEx("Cannot find 'value'/name or href for Workload in JSON:\n" + nice_json(json_item))

            new_item_name = json_item['name']
            new_item_href = json_item['href']

            # Workloads's name is None when it's provided by VEN through its hostname until it's manually overwritten
            # (eventually) by someone. In such a case, you need to use hostname instead
            if new_item_name is None:
                if 'hostname' not in json_item:
                    raise pylo.PyloEx("Cannot find 'value'/hostname in JSON:\n" + nice_json(json_item))
                new_item_name = json_item['hostname']

            new_item = pylo.Workload(new_item_name, new_item_href, self)
            new_item.load_from_json(json_item)

            if new_item_href in self.itemsByHRef:
                raise pylo.PyloEx("A Workload with href '%s' already exists in the table", new_item_href)

            # if new_item_name in self.itemsByName:
            #     if not pylo.ignoreWorkloadsWithSameName:
            #          raise pylo.PyloEx(
            #             "A Workload with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (
            #             new_item_name, new_item_href, self.itemsByName[new_item_name].href))
                # else:
                #    #log.warning("A Workload with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (new_item_name, new_item_href, self.itemsByName[new_item_name].href))

            self.itemsByHRef[new_item_href] = new_item
            self.itemsByName[new_item_name] = new_item

            log.debug("Found Workload '%s' with href '%s'", new_item_name, new_item_href)

    def find_by_href_or_die(self, href: str) -> 'pylo.Workload':

        find_object = self.itemsByHRef.get(href)
        if find_object is None:
            raise pylo.PyloEx("Workload with HREF '%s' was not found" % href)

        return find_object

    def find_by_href_or_create_tmp(self, href: str, tmp_wkl_name: str) -> 'pylo.Workload':
        find_object = self.itemsByHRef.get(href)
        if find_object is not None:
            return find_object

        new_tmp_item = pylo.Workload(tmp_wkl_name, href, self)
        new_tmp_item.deleted = True
        new_tmp_item.temporary = True

        self.itemsByHRef[href] = new_tmp_item
        self.itemsByName[tmp_wkl_name] = new_tmp_item

        return new_tmp_item

    def find_workloads_matching_label(self, label: 'pylo.Label') -> Dict[str, 'pylo.Workload']:
        result = {}

        for href, workload in self.itemsByHRef.items():
            if workload.is_using_label(label):
                result[href] = workload

        return result

    def find_workloads_matching_all_labels(self, labels: List[pylo.Label]) -> Dict[str, 'pylo.Workload']:
        result = {}

        for href, workload in self.itemsByHRef.items():
            matched = True
            for label in labels:
                if label is None:
                    continue
                if not workload.is_using_label(label):
                    matched = False
                    break
            if matched:
                result[href] = workload

        return result

    """
    :return Workload|None
    """

    def find_workload_matching_name(self, name: str):
        found = self.itemsByName.get(name)

        return found

    def count_workloads(self) -> int:
        """


        """
        return len(self.itemsByHRef)

    def count_managed_workloads(self) -> int:
        """


        """
        count = 0

        for item in self.itemsByHRef.values():
            if not item.unmanaged and not item.deleted:
                count += 1

        return count

    def get_managed_workloads_list(self) -> List['pylo.Workload']:
        results = []
        for item in self.itemsByHRef.values():
            if not item.unmanaged:
                results.append(item)

        return results

    def get_managed_workloads_dict_href(self) -> Dict[str, 'pylo.Workload']:
        results = {}
        for item in self.itemsByHRef.values():
            if not item.unmanaged:
                results[item.href] = item

        return results

    def count_deleted_workloads(self) -> int:
        """


        """
        count = 0
        for item in self.itemsByHRef.values():
            if item.deleted:
                count += 1
                #print(item.href)

        return count

    def count_unmanaged_workloads(self, if_not_deleted=False) -> int:
        """


        """
        count = 0

        for item in self.itemsByHRef.values():
            if item.unmanaged and (not if_not_deleted or (if_not_deleted and not item.deleted)):
                count += 1

        return count


