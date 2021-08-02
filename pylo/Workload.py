import pylo
from pylo import log, IP4Map
from .Helpers import *
from typing import Optional, List


class WorkloadInterface:
    def __init__(self, owner: 'pylo.Workload', name: str, ip: str, network: str, gateway: str, ignored: bool):
        self.owner: 'pylo.Workload' = owner
        self.name: str = name
        self.ip: str = ip
        self.network: str = network
        self.gateway: str = gateway
        self.is_ignored: bool = ignored


class WorkloadApiUpdateStack:
    def __init__(self):
        self.json_payload = {}

    def add_payload(self, data: Dict[str, Any]):
        for prop_name, prop_value in data.items():
            self.json_payload[prop_name] = prop_value

    def get_payload_and_reset(self) -> Dict[str, Any]:
        data = self.json_payload
        self.json_payload = {}
        return data

    def count_payloads(self) -> int:
        return len(self.json_payload)


class Workload(pylo.ReferenceTracker):

    def __init__(self, name: str, href: str, owner: 'pylo.WorkloadStore'):
        pylo.ReferenceTracker.__init__(self)
        self.owner = owner
        self.name: str = name
        self.href: str = href
        self.forced_name: Optional[str] = None
        self.hostname: Optional[str] = None

        self.description: Optional[str] = None
        self.interfaces: List[WorkloadInterface] = []

        self.online = False

        self.os_id: Optional[str] = None
        self.os_detail: Optional[str] = None

        self.locationLabel: Optional['pylo.Label'] = None
        self.environmentLabel: Optional['pylo.Label'] = None
        self.applicationLabel: Optional['pylo.Label'] = None
        self.roleLabel: Optional['pylo.Label'] = None

        self.ven_agent: Optional['pylo.VENAgent'] = None

        self.unmanaged = True

        self.temporary = False
        self.deleted = False

        self.raw_json = None

        self._batch_update_stack: Optional[WorkloadApiUpdateStack] = None

    def load_from_json(self, data):
        """
        Parse and build workload properties from a PCE API JSON payload. Should be used internally by this library only.
        """
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
            self.os_detail = data.get('os_detail')
            if self.os_detail is None:
                raise pylo.PyloEx("Workload named '{}' has no os_detail record:\n%s".format(self.name), data)

        if 'description' in data:
            desc = data['description']
            if desc is not None:
                self.description = desc

        ignored_interfaces_index = {}
        ignored_interfaces_json = data.get('ignored_interface_names')

        if ignored_interfaces_json is not None:
            for interface_name in ignored_interfaces_json:
                ignored_interfaces_index[interface_name] = True

        interfaces_json = data.get('interfaces')
        if interfaces_json is not None:
            for interface_json in interfaces_json:
                if_object = WorkloadInterface(self, interface_json.get('name'), interface_json.get('address'),
                                              interface_json.get('cidr_block'), interface_json.get('default_gateway_address'),
                                              ignored=interface_json.get('name') in ignored_interfaces_index)
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

    def interfaces_to_string(self, separator=',', show_ignored=True) -> str:
        tmp = []

        for interface in self.interfaces:
            if not show_ignored and interface.is_ignored:
                continue
            tmp.append('{}:{}'.format(interface.name, interface.ip))

        return pylo.string_list_to_text(tmp, separator)

    def get_ip4map_from_interfaces(self) -> pylo.IP4Map:
        """
        Calculate and return a map of all IP4 covered by the Workload interfaces
        """
        map = IP4Map()

        for interface in self.interfaces:
            map.add_from_text(interface.ip)

        return map

    def is_using_label(self, label: 'pylo.Label') -> bool:
        """
        Check if a label is used by this Workload
        :param label: label to check for usage
        :return: true if label is used by this workload
        """
        if self.locationLabel is label or self.environmentLabel is label \
                or self.applicationLabel is label or self.applicationLabel is label:
            return True
        return False

    def api_update_description(self, new_description: str):
        data = {'description': new_description}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)
        self.description = new_description

    def api_update_hostname(self, new_hostname: str):
        data = {'hostname': new_hostname}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)
        self.description = new_hostname

    def api_update_forced_name(self, name: str):

        data = {'name': name}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)

        self.description = name

    def api_update_labels(self):
        """
        Refresh assigned workload labels in the PCE (Labels must have been changed in the workload object prior to this)
        """
        label_data = []
        if self.locationLabel is not None:
            label_data.append({'href': self.locationLabel.href})
        if self.environmentLabel is not None:
            label_data.append({'href': self.environmentLabel.href})
        if self.applicationLabel is not None:
            label_data.append({'href': self.applicationLabel.href})
        if self.roleLabel is not None:
            label_data.append({'href': self.roleLabel.href})

        data = {'labels': label_data}

        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data)
        else:
            self._batch_update_stack.add_payload(data)

    def api_stacked_updates_start(self):
        """
        Turns on 'updates stacking' mode for this Worklaod which will not push changes to API as you make them but only
        when you trigger 'api_push_stacked_updates()' function
        """
        self._batch_update_stack = WorkloadApiUpdateStack()

    def api_stacked_updates_push(self):
        """
        Push all stacked changed to API and turns off 'updates stacking' mode
        """
        if self._batch_update_stack is None:
            raise pylo.PyloEx("Workload was not in 'update stacking' mode")

        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_workload_update(self.href, self._batch_update_stack.get_payload_and_reset())
        self._batch_update_stack = None

    def api_stacked_updates_count(self) -> int:
        """
        Counts the number of stacked changed for this Workload
        :return:
        """
        if self._batch_update_stack is None:
            raise pylo.PyloEx("Workload was not in 'update stacking' mode")
        return self._batch_update_stack.count_payloads()

    def get_labels_str(self, separator: str = '|') -> str:
        """
        Conveniently returns a string with all labels names in RAEL order
        :param separator: default separator is |
        :return: example: *None*|AppA|EnvC|LocZ
        """
        labels = ''

        if self.roleLabel is None:
            labels += '*None*' + separator
        else:
            labels += self.roleLabel.name + separator

        if self.applicationLabel is None:
            labels += '*None*' + separator
        else:
            labels += self.applicationLabel.name + separator

        if self.environmentLabel is None:
            labels += '*None*' + separator
        else:
            labels += self.environmentLabel.name + separator

        if self.locationLabel is None:
            labels += '*None*'
        else:
            labels += self.locationLabel.name

        return labels

    def get_label_str_by_type(self, type: str, none_str='') -> str:
        if type == 'role':
            if self.roleLabel is None:
                return none_str
            return self.roleLabel.name

        if type == 'app':
            if self.applicationLabel is None:
                return none_str
            return self.applicationLabel.name

        if type == 'env':
            if self.environmentLabel is None:
                return none_str
            return self.environmentLabel.name

        if type == 'loc':
            if self.locationLabel is None:
                return none_str
            return self.locationLabel.name

    def get_label_href_by_type(self, type: str, none_str='') -> str:
        if type == 'role':
            if self.roleLabel is None:
                return none_str
            return self.roleLabel.href

        if type == 'app':
            if self.applicationLabel is None:
                return none_str
            return self.applicationLabel.href

        if type == 'env':
            if self.environmentLabel is None:
                return none_str
            return self.environmentLabel.href

        if type == 'loc':
            if self.locationLabel is None:
                return none_str
            return self.locationLabel.href

    def get_appgroup_str(self) -> str:
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

    def get_name(self) -> str:
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

    def get_status_string(self) -> str:
        if self.ven_agent is None:
            return 'not-applicable'
        return self.ven_agent.mode

