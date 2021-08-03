from pylo import log, IP4Map, PyloEx, Workload, nice_json, Label
from .Helpers import *
from .Organization import Organization
from typing import Optional, List


class WorkloadStore:

    def __init__(self, owner: 'Organization'):
        self.owner = owner
        self.itemsByHRef: Dict[str, Workload] = {}
        self.itemsByName: Dict[str, Workload] = {}

    def load_workloads_from_json(self, json_list):
        for json_item in json_list:
            if 'name' not in json_item or 'href' not in json_item:
                raise PyloEx("Cannot find 'value'/name or href for Workload in JSON:\n" + nice_json(json_item))

            new_item_name = json_item['name']
            new_item_href = json_item['href']

            # Workload's name is None when it's provided by VEN through its hostname until it's manually overwritten
            # (eventually) by someone. In such a case, you need to use hostname instead
            if new_item_name is None:
                if 'hostname' not in json_item:
                    raise PyloEx("Cannot find 'value'/hostname in JSON:\n" + nice_json(json_item))
                new_item_name = json_item['hostname']

            new_item = Workload(new_item_name, new_item_href, self)
            new_item.load_from_json(json_item)

            if new_item_href in self.itemsByHRef:
                raise PyloEx("A Workload with href '%s' already exists in the table", new_item_href)

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

    def find_by_href_or_die(self, href: str) -> 'Workload':
        """
        Find a Workload from its HREF, throw an Exception if not found

        :param href: the HREF you are looking for
        :return:
        :raises:
            PyloEx: if no Workload matching provided HREF
        """
        find_object = self.itemsByHRef.get(href)
        if find_object is None:
            raise PyloEx("Workload with HREF '%s' was not found" % href)

        return find_object

    def find_by_href_or_create_tmp(self, href: str, tmp_wkl_name: str) -> 'Workload':
        """
        Find a Workload from its HREF, creates a fake temporary one if not found. *Reserved for developers*

        :param href: the HREF you are looking for
        :return:
        """
        find_object = self.itemsByHRef.get(href)
        if find_object is not None:
            return find_object

        new_tmp_item = Workload(tmp_wkl_name, href, self)
        new_tmp_item.deleted = True
        new_tmp_item.temporary = True

        self.itemsByHRef[href] = new_tmp_item
        self.itemsByName[tmp_wkl_name] = new_tmp_item

        return new_tmp_item

    def find_workloads_matching_label(self, label: 'Label') -> Dict[str, 'Workload']:
        """
        Find all Workloads which are using a specific Label.

        :param label: Label you want to match on
        :return: a dictionary of all matching Workloads using their HREF as key
        """
        result = {}

        for href, workload in self.itemsByHRef.items():
            if workload.is_using_label(label):
                result[href] = workload

        return result

    def find_workloads_matching_all_labels(self, labels: List[Label]) -> Dict[str, 'Workload']:
        """
        Find all Workloads which are using all the Labels from a specified list.

        :param labels: list of Labels you want to match on
        :return: a dictionary of all matching Workloads using their HREF as key
        """
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

    def find_workload_matching_name(self, name: str) -> Optional[Workload]:
        """
        Find a Workload based on its name (case sensitive). Beware that if several are matching, only the first one will be returned

        :param name: the name you are looking for
        :return: the Workload it found, None otherwise
        """
        found = self.itemsByName.get(name)

        return found

    def find_workload_matching_hostname(self, name: str, case_sensitive: bool = True, strip_fqdn: bool = False) -> Optional[Workload]:
        """
        Find a workload based on its hostname.Beware that if several are matching, only the first one will be returned
        :param name: the name string you are looking for
        :param case_sensitive: make it a case sensitive search or not
        :param strip_fqdn: remove the fqdn part of the hostname
        :return: the Workload it found, None otherwise
        """
        if case_sensitive:
            name = name.lower()

        for workload in self.itemsByHRef.values():
            wkl_name = workload.hostname
            if strip_fqdn:
                wkl_name = Workload.static_name_stripped_fqdn(wkl_name)
            if case_sensitive:
                if wkl_name == name:
                    return workload
            else:
                if wkl_name.lower() == name:
                    return workload

        return None

    def find_all_workloads_matching_hostname(self, name: str, case_sensitive: bool = True, strip_fqdn: bool = False) -> List[Workload]:
        """
        Find all workloads based on their hostnames.
        :param name: the name string you are looking for
        :param case_sensitive: make it a case sensitive search or not
        :param strip_fqdn: remove the fqdn part of the hostname
        :return: list of matching Workloads
        """
        result = []

        if case_sensitive:
            name = name.lower()

        for workload in self.itemsByHRef.values():
            wkl_name = workload.hostname
            if strip_fqdn:
                wkl_name = Workload.static_name_stripped_fqdn(wkl_name)
            if case_sensitive:
                if wkl_name == name:
                    result.append(workload)
            else:
                if wkl_name.lower() == name:
                    result.append(workload)

        return result

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

    def get_managed_workloads_list(self) -> List['Workload']:
        """
        Get a list of all managed workloads
        :return:
        """
        results = []
        for item in self.itemsByHRef.values():
            if not item.unmanaged:
                results.append(item)

        return results

    def get_managed_workloads_dict_href(self) -> Dict[str, 'Workload']:
        """
        Get a dictionary of all managed workloads using their HREF as key
        :return:
        """
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


