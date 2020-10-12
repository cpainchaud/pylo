import pylo
from pylo import log, IP4Map
from .Helpers import *
from typing import Optional, List


class WorkloadInterface():
    def __init__(self, owner: 'pylo.Workload', name: str, ip: str, network: str, gateway: str):
        self.owner = owner
        self.name = name
        self.ip = ip
        self.network = network
        self.gateway = gateway


class Workload(pylo.ReferenceTracker):

    interfaces: List[WorkloadInterface]
    hostname: Optional[str]

    def __init__(self, name: str, href: str, owner: 'pylo.WorkloadStore'):
        pylo.ReferenceTracker.__init__(self)
        self.owner = owner
        self.name = name  # type: str
        self.href = href  # type: str
        self.forced_name = None  # type: str
        self.hostname = None

        self.description = None  # type: str
        self.interfaces = []

        self.online = False

        self.os_id = None

        self.locationLabel = None  # type: pylo.Label
        self.environmentLabel = None  # type: pylo.Label
        self.applicationLabel = None  # type: pylo.Label
        self.roleLabel = None  # type: pylo.Label

        self.ven_agent = None  # type: pylo.VENAgent

        self.unmanaged = True

        self.temporary = False
        self.deleted = False

        self.raw_json = None

    def load_from_json(self, data):
        self.raw_json = data
        # print(pylo.nice_json(data))

        self.forced_name = data['name']

        self.hostname = data['hostname']

        agent_json = data.get('agent')

        if agent_json is None:
            raise pylo.PyloEx("Workload named '%s' has no Agent record:\n%s" % (
                self.name, pylo.nice_json(data)))

        agent_href = agent_json.get('href')
        if agent_href is None:
            self.unmanaged = True
        else:
            self.unmanaged = False
            self.ven_agent = self.owner.owner.AgentStore.create_venagent_from_workload_record(self, agent_json)
            self.online = data['online']
            self.os_id = data.get('os_id')
            if self.os_id is None:
                raise pylo.PyloEx("Workload named '{}' has no os_id record:\n%s".format(self.name), data)

        if 'description' in data:
            desc = data['description']
            if desc is not None:
                self.description = desc

        interfaces_json = data.get('interfaces')
        if interfaces_json is not None:
            for interface_json in interfaces_json:
                if_object = WorkloadInterface(self, interface_json.get('name'), interface_json.get('address'),
                                              interface_json.get('cidr_block'), interface_json.get('default_gateway_address'))
                self.interfaces.append(if_object)

        self.deleted = data['deleted']

        if 'labels' in data:
            labels = data['labels']
            for label in labels:
                if 'href' not in label:
                    raise pylo.PyloEx("Workload named '%s' has labels in JSON but without any HREF:\n%s" % (
                    self.name, pylo.nice_json(labels)))
                href = label['href']
                label_object = self.owner.owner.LabelStore.find_by_href_or_die(href)

                if label_object.type_is_location():
                    if self.locationLabel is not None:
                        raise pylo.PyloEx(
                            "Workload '%s' found 2 location labels while parsing JSON, labels are '%s' and '%s':\n" % (
                            self.name, self.locationLabel.name, label_object.name))
                    self.locationLabel = label_object

                elif label_object.type_is_environment():
                    if self.environmentLabel is not None:
                        raise pylo.PyloEx(
                            "Workload '%s' found 2 environment labels while parsing JSON, labels are '%s' and '%s':\n" % (
                            self.name, self.environmentLabel.name, label_object.name))
                    self.environmentLabel = label_object

                elif label_object.type_is_application():
                    if self.applicationLabel is not None:
                        raise pylo.PyloEx(
                            "Workload '%s' found 2 application labels while parsing JSON, labels are '%s' and '%s':\n" % (
                            self.name, self.applicationLabel.name, label_object.name))
                    self.applicationLabel = label_object

                elif label_object.type_is_role():
                    if self.roleLabel is not None:
                        raise pylo.PyloEx(
                            "Workload '%s' found 2 role labels while parsing JSON, labels are '%s' and '%s':\n" % (
                            self.name, self.roleLabel.name, label_object.name))
                    self.roleLabel = label_object


    def interfaces_to_string(self, separator=','):
        tmp = []

        for interface in self.interfaces:
            tmp.append('{}:{}'.format(interface.name, interface.ip))

        return pylo.string_list_to_text(tmp, separator)


    def get_ip4map_from_interfaces(self) -> pylo.IP4Map:
        map = IP4Map()

        for interface in self.interfaces:
            map.add_from_text(interface.ip)

        return map


    def is_using_label(self, label: 'pylo.Label'):
        if self.locationLabel is label or self.environmentLabel is label \
                or self.applicationLabel is label or self.applicationLabel is label:
            return True
        return False


    def api_update_description(self, new_description: str):
        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_workload_update(self.href, data={'description': new_description})
        self.description = new_description

    def api_update_hostname(self, new_hostname: str):
        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_workload_update(self.href, data={'hostname': new_hostname})
        self.description = new_hostname

    def api_update_forced_name(self, name: str):
        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_workload_update(self.href, data={'name': name})
        self.description = name

    def api_update_labels(self):
        connector = pylo.find_connector_or_die(self.owner)
        label_data = []
        if self.locationLabel is not None:
            label_data.append({'href': self.locationLabel.href})
        if self.environmentLabel is not None:
            label_data.append({'href': self.environmentLabel.href})
        if self.applicationLabel is not None:
            label_data.append({'href': self.applicationLabel.href})
        if self.roleLabel is not None:
            label_data.append({'href': self.roleLabel.href})

        connector.objects_workload_update(self.href, data={'labels': label_data})

    def get_labels_str(self):
        labels = ''

        if self.roleLabel is None:
            labels += '*None*|'
        else:
            labels += self.roleLabel.name + '|'

        if self.applicationLabel is None:
            labels += '*None*|'
        else:
            labels += self.applicationLabel.name + '|'

        if self.environmentLabel is None:
            labels += '*None*|'
        else:
            labels += self.environmentLabel.name + '|'

        if self.locationLabel is None:
            labels += '*None*'
        else:
            labels += self.locationLabel.name

        return labels


    def get_appgroup_str(self):
        labels = ''

        if self.applicationLabel is None:
            labels += '*None*|'
        else:
            labels += self.applicationLabel.name + '|'

        if self.environmentLabel is None:
            labels += '*None*|'
        else:
            labels += self.environmentLabel.name + '|'

        if self.locationLabel is None:
            labels += '*None*'
        else:
            labels += self.locationLabel.name

        return labels


    def get_labels_list(self, missing_str=''):

        labels = []

        if self.roleLabel is None:
            labels.append(missing_str)
        else:
            labels.append(self.roleLabel.name)

        if self.applicationLabel is None:
            labels.append(missing_str)
        else:
            labels.append(self.applicationLabel.name)

        if self.environmentLabel is None:
            labels.append(missing_str)
        else:
            labels.append(self.environmentLabel.name)

        if self.locationLabel is None:
            labels.append(missing_str)
        else:
            labels.append(self.locationLabel.name)

        return labels

    def get_label_by_type_str(self, label_type: str):
        label_type = label_type.lower()
        if label_type == 'role':
            return self.roleLabel
        if label_type == 'app':
            return self.applicationLabel
        if label_type == 'env':
            return self.environmentLabel
        if label_type == 'loc':
            return self.locationLabel

        raise pylo.PyloEx("unsupported label_type '{}'".format(label_type))

    def get_name(self):
        if self.name is not None:
            return self.name
        if self.hostname is None:
            raise pylo.PyloEx("Cannot find workload name!")
        return self.hostname

    def get_name_stripped_fqdn(self):
        name_split = self.get_name().split('.')
        return name_split[0]

    @staticmethod
    def static_name_stripped_fqdn(name: str):
        name_split = name.split('.')
        return name_split[0]

    def get_status_string(self):
        if self.ven_agent is None:
            return 'not-applicable'
        return self.ven_agent.mode;


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

            if new_item_name in self.itemsByName:
                if not pylo.ignoreWorkloadsWithSameName:
                    raise pylo.PyloEx(
                        "A Workload with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (
                        new_item_name, new_item_href, self.itemsByName[new_item_name].href))
                # else:
                #    #log.warning("A Workload with name '%s' already exists in the table. This UID:%s vs other UID:%s" % (new_item_name, new_item_href, self.itemsByName[new_item_name].href))

            self.itemsByHRef[new_item_href] = new_item
            self.itemsByName[new_item_name] = new_item

            log.debug("Found Workload '%s' with href '%s'", new_item_name, new_item_href)

    def find_by_href_or_die(self, href: str):

        find_object = self.itemsByHRef.get(href)
        if find_object is None:
            raise pylo.PyloEx("Workload with HREF '%s' was not found" % href)

        return find_object

    def find_by_href_or_create_tmp(self, href: str, tmp_wkl_name: str):
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

    def count_workloads(self):
        """

        :rtype: int
        """
        return len(self.itemsByHRef)

    def count_managed_workloads(self):
        """

        :rtype: int
        """
        count = 0

        for item in self.itemsByHRef.values():
            if not item.unmanaged and not item.deleted:
                count += 1

        return count

    def get_managed_workloads_list(self):
        results = []
        for item in self.itemsByHRef.values():
            if not item.unmanaged:
                results.append(item)

        return results

    def get_managed_workloads_dict_href(self):
        results = {}
        for item in self.itemsByHRef.values():
            if not item.unmanaged:
                results[item.href] = item

        return results

    def count_deleted_workloads(self):
        """

        :rtype: int
        """
        count = 0
        for item in self.itemsByHRef.values():
            if item.deleted:
                count += 1
                print(item.href)

        return count

    def count_unamanaged_workloads(self, if_not_deleted=False):
        """

        :rtype: int
        """
        count = 0

        for item in self.itemsByHRef.values():
            if item.unmanaged and (not if_not_deleted or (if_not_deleted and not item.deleted)):
                count += 1

        return count


