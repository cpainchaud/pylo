from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Union

import illumio_pylo as pylo
from illumio_pylo.API.JsonPayloadTypes import WorkloadObjectJsonStructure, WorkloadObjectCreateJsonStructure
from illumio_pylo import log
from .AgentStore import VENAgent
from .Helpers import *
from .Exception import PyloEx
from .Label import Label
from .ReferenceTracker import ReferenceTracker, Referencer
from .IPMap import IP4Map
from .LabeledObject import LabeledObject


class WorkloadInterface:
    def __init__(self, owner: 'pylo.Workload', name: str, ip: Optional[str], network: str, gateway: str, ignored: bool):
        self.owner: Workload = owner
        self.name: str = name
        self.network: str = network
        self.gateway: str = gateway
        self.is_ignored: bool = ignored
        self.ip: Optional[str] = ip
        if ip is not None and len(ip) == 0:
            self.ip = None


class WorkloadApiUpdateStack:
    def __init__(self):
        self.json_payload: WorkloadObjectCreateJsonStructure = {}

    def add_payload(self, data: WorkloadObjectCreateJsonStructure):
        for prop_name, prop_value in data.items():
            self.json_payload[prop_name] = prop_value

    def get_payload_and_reset(self) -> Dict[str, Any]:
        data = self.json_payload
        self.json_payload = {}
        return data

    def count_payloads(self) -> int:
        return len(self.json_payload)




class Workload(pylo.ReferenceTracker, pylo.Referencer, LabeledObject):

    __slots__ = ['owner', 'name', 'href', 'forced_name', 'hostname', 'description', 'interfaces', 'online', 'os_id',
                 'os_detail', 'ven_agent', 'unmanaged', 'temporary', 'deleted', 'raw_json', '_batch_update_stack']

    def __init__(self, name: str, href: str, owner: 'pylo.WorkloadStore'):
        ReferenceTracker.__init__(self)
        Referencer.__init__(self)
        LabeledObject.__init__(self)
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

        self.ven_agent: Optional[VENAgent] = None

        self.unmanaged = True

        self.temporary = False
        self.deleted = False

        self.raw_json: Optional[WorkloadObjectJsonStructure] = None

        self._batch_update_stack: Optional[WorkloadApiUpdateStack] = None

    @property
    def loc_label(self) -> Optional['pylo.Label']:
        """ @deprecated use get_label() instead """
        return self.get_label('loc')

    @property
    def env_label(self) -> Optional['pylo.Label']:
        """ @deprecated use get_label() instead """
        return self.get_label('env')

    @property
    def app_label(self) -> Optional['pylo.Label']:
        """ @deprecated use get_label() instead """
        return self.get_label('app')

    @property
    def role_label(self) -> Optional['pylo.Label']:
        """ @deprecated use get_label() instead """
        return self.get_label('role')

    def load_from_json(self, data):
        """
        Parse and build workload properties from a PCE API JSON payload. Should be used internally by this library only.
        """
        label_store = self.owner.owner.LabelStore  # forced_name quick access

        self.raw_json = data

        self.forced_name = data['name']

        self.hostname = data['hostname']

        self.deleted = data['deleted']

        agent_json = data.get('agent')

        if agent_json is None:
            raise PyloEx("Workload named '%s' has no Agent record:\n%s" % (
                self.name, nice_json(data)))

        agent_href = agent_json.get('href')
        if agent_href is None:
            self.unmanaged = True
        else:
            self.unmanaged = False
            self.ven_agent = self.owner.owner.AgentStore.create_ven_agent_from_workload_record(self, agent_json)
            self.online = data['online']
            self.os_id = data.get('os_id')
            #if self.os_id is None:
            #    raise PyloEx("Workload named '{}' has no os_id record:\n%s".format(self.name), data)
            self.os_detail = data.get('os_detail')
            #if self.os_detail is None:
            #    raise PyloEx("Workload named '{}' has no os_detail record:\n%s".format(self.name), data)

        self.description = data.get('description')

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

        if 'labels' in data:
            labels = data['labels']
            for label in labels:
                if 'href' not in label:
                    raise PyloEx("Workload named '%s' has labels in JSON but without any HREF:\n%s" % (
                        self.get_name(), nice_json(labels)))
                href = label['href']
                label_object = label_store.find_by_href(href)
                if label_object is None:
                    if not self.deleted:
                        raise pylo.PyloObjectNotFound(
                            "Workload '%s'/'%s' is referencing label href '%s' which does not exist" % (
                            self.name, self.href, href))

                self.set_label(label_object)
                # print("Workload '%s'/'%s' is referencing label '%s'/'%s'" % (self.name, self.hostname, label_object.type, label_object.name))

                label_object.add_reference(self)

    def interfaces_to_string(self, separator: str = ',', show_ignored: bool = True, show_interface_name: bool = True) -> str:
        """
        Conveniently outputs all interface of this Workload to a string.

        :param separator: string used to separate each interface in the string
        :param show_ignored: whether or not ignored interfaces should be showing
        :param show_interface_name:
        :return: string with interfaces split by specified separator
        """
        tmp = []

        for interface in self.interfaces:
            if not show_ignored and interface.is_ignored:
                continue
            if show_interface_name:
                tmp.append('{}:{}'.format(interface.name, interface.ip if interface.ip is not None else 'UnknownIP'))
            else:
                tmp.append(interface.ip if interface.ip is not None else 'UnknownIP')

        return pylo.string_list_to_text(tmp, separator)

    def get_ip4map_from_interfaces(self) -> pylo.IP4Map:
        """
        Calculate and return a map of all IP4 covered by the Workload interfaces
        """
        result = IP4Map()

        for interface in self.interfaces:
            if interface.ip is not None:
                result.add_from_text(interface.ip)

        return result

    @property
    def created_at(self) -> str:
        return self.raw_json['created_at']


    def created_at_datetime(self) -> datetime:
        return pylo.illumio_date_time_string_to_datetime(self.created_at)


    def api_update_description(self, new_description: str):
        if new_description is None or len(new_description) == 0:
            if self.description is None or self.description == '':
                return
        elif new_description == self.description:
            return

        data = {'description': new_description}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)

        self.raw_json.update(data)
        self.description = new_description

    def api_update_hostname(self, new_hostname: str):
        if new_hostname is None or len(new_hostname) == 0:
            if self.hostname is None or self.hostname == '':
                return
        elif new_hostname == self.hostname:
            return

        data = {'hostname': new_hostname}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)

        self.raw_json.update(data)
        self.hostname = new_hostname

    def api_update_forced_name(self, name: str):
        if name is None or len(name) == 0:
            if self.forced_name is None or self.forced_name == '':
                return
        elif name == self.forced_name:
            return

        data = {'name': name}
        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data=data)
        else:
            self._batch_update_stack.add_payload(data)

        self.raw_json.update(data)
        self.forced_name = name

    def api_update_labels(self, list_of_labels: Optional[List[Label]] = None, missing_label_type_means_no_change=False):
        """
        Push Workload's assigned Labels to the PCE.

        :param list_of_labels: labels to replace currently assigned ones. If not specified it will push current labels instead.
        :param missing_label_type_means_no_change: if a label type is missing and this is False then existing label of type in the Workload will be removed
        :return:
        """
        if list_of_labels is not None:
            # a list of labels were specified so are first going to change
            if not self.update_labels(list_of_labels, missing_label_type_means_no_change):
                return

        label_data = []
        for label_type in self.owner.owner.LabelStore.label_types:
            label = self.get_label(label_type)
            if label is not None:
                label_data.append({'href': label.href})

        data = {'labels': label_data}

        if self._batch_update_stack is None:
            connector = pylo.find_connector_or_die(self.owner)
            connector.objects_workload_update(self.href, data)
        else:
            self._batch_update_stack.add_payload(data)

        self.raw_json.update(data)

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
            raise PyloEx("Workload was not in 'update stacking' mode")

        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_workload_update(self.href, self._batch_update_stack.get_payload_and_reset())
        self._batch_update_stack = None

    def api_stacked_updates_get_json(self) -> Optional[WorkloadObjectCreateJsonStructure]:
        """
        Returns the JSON payload of the stacked updates
        :return:
        """
        if self._batch_update_stack is None:
            raise PyloEx("Workload was not in 'update stacking' mode")
        return self._batch_update_stack.json_payload

    def api_stacked_updates_started(self) -> bool:
        """
        Returns True if 'updates stacking' mode is currently on
        :return:
        """
        return self._batch_update_stack is not None

    def api_stacked_updates_count(self) -> int:
        """
        Counts the number of stacked changed for this Workload
        :return:
        """
        if self._batch_update_stack is None:
            raise PyloEx("Workload was not in 'update stacking' mode")
        return self._batch_update_stack.count_payloads()

    def get_labels_str(self, separator: str = '|') -> str:
        """
        Conveniently returns a string with all labels names in RAEL order
        :param separator: default separator is |
        :return: example: *None*|AppA|EnvC|LocZ
        """
        labels = ''

        first = True
        for dimensions in self.owner.owner.LabelStore.label_types:
            label = self.get_label(dimensions)
            if not first:
                labels += separator
            if label is not None:
                labels += label.name
            else:
                labels += '*None*'
            first = False

        return labels


    def get_appgroup_str(self, separator: str = '|') -> str:
        labels = ''

        if self.app_label is None:
            labels += '*None*' + separator
        else:
            labels += self.app_label.name + separator

        if self.env_label is None:
            labels += '*None*' + separator
        else:
            labels += self.env_label.name + separator

        if self.loc_label is None:
            labels += '*None*'
        else:
            labels += self.loc_label.name

        return labels

    def get_labels_str_list(self, missing_str: Optional[str] = '') -> List[str]:
        """
        Conveniently returns the list of Workload labels as a list of strings
        :param missing_str: if a label type is missing then missing_str will be used to represent it
        :return:
        """
        labels = []

        if self.role_label is None:
            labels.append(missing_str)
        else:
            labels.append(self.role_label.name)

        if self.app_label is None:
            labels.append(missing_str)
        else:
            labels.append(self.app_label.name)

        if self.env_label is None:
            labels.append(missing_str)
        else:
            labels.append(self.env_label.name)

        if self.loc_label is None:
            labels.append(missing_str)
        else:
            labels.append(self.loc_label.name)

        return labels

    def get_name(self) -> str:
        """
        Return forced name if it exists, hostname otherwise

        :return:
        """
        if self.forced_name is not None:
            return self.forced_name
        if self.hostname is None:
            pylo.get_logger().warning("workload with href '{}' has no name nor host name".format(self.href))
            return "*unknown*"
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

    def update_labels(self, list_of_labels: List[Label], missing_label_type_means_no_change=False) -> bool:
        """
        WARNING: this will not send updates to PCE API, use the 'api_' prefixed function for that

        :param list_of_labels: labels to replace currently assigned ones
        :param missing_label_type_means_no_change: if a label type is missing and this is False then existing label of type in the Workload will be removed
        :return:
        """

        if (list_of_labels is None or len(list_of_labels) == 0) and missing_label_type_means_no_change:
            return False

        original_label_set = set(self._labels.values())

        labels_by_type = pylo.LabelStore.Utils.list_to_dict_by_type(list_of_labels)

        dict_for_replacement = {}

        for label_type in labels_by_type:
            if len(labels_by_type[label_type]) > 1:
                raise PyloEx("Workload can't have more than one label of the same type")
            dict_for_replacement[label_type] = labels_by_type[label_type][0]

        new_labels_set = set(list_of_labels)

        if original_label_set == new_labels_set:
            return False

        self._labels = dict_for_replacement

        return True

    def get_pce_ui_url(self) -> str:
        """
        generates a URL link for the Workload vs PCE UI
        :return: url string
        """
        return self.owner.owner.connector.get_pce_ui_workload_url(self.href)


class WorkloadApiUpdateStackExecutionManager:

    @dataclass
    class Result:
        successful: bool
        message: Optional[str]
        workload: 'pylo.Workload'

    def __init__(self, org: 'pylo.Organization'):
        self.org: pylo.Organization = org
        self.workloads: List['pylo.Workload'] = []
        self.successful: List[Optional[bool]] = []
        self.message: List[Optional[str]] = []

    def add_workload(self, workload: 'pylo.Workload'):
        if not workload.api_stacked_updates_started():
            raise pylo.PyloEx("Workload is not in 'update stacking' mode. {}:{}".format(workload.get_name(), workload.href))
        self.workloads.append(workload)
        self.successful.append(None)
        self.message.append(None)

    def push_all(self, amount_per_batch: int = 500):
        if len(self.workloads) == 0:
            return

        #self split in list of lists of 500
        batches: List[List[pylo.Workload]] = [self.workloads[i:i + amount_per_batch] for i in range(0, len(self.workloads), amount_per_batch)]

        for batch in batches:
            batch_json_payload = []
            for workload in batch:
                batch_json_payload.append(workload.api_stacked_updates_get_json())
                batch_json_payload[-1]['href'] = workload.href

            connector = pylo.find_connector_or_die(self.org)
            results = connector.objects_workload_update_bulk(batch_json_payload)
            for result in results:
                workload_href = result['href']
                if workload_href is None:
                    raise pylo.PyloApiEx("Workload update failed. No href in results", result)
                # get index of workload
                index = -1
                for i in range(len(self.workloads)):
                    if self.workloads[i].href == workload_href:
                        index = i
                        break
                if index == -1:
                    raise pylo.PyloEx("Workload update failed. Could not find workload in list of workloads", workload_href)
                self.successful[index] = result['status'] == 'updated'
                if self.successful[index]:
                    self.message[index] = None
                else:
                    self.message[index] = result['message']


    def get_all_results(self) -> List[Result]:
        results = []
        for i in range(len(self.workloads)):
            results.append(WorkloadApiUpdateStackExecutionManager.Result(self.successful[i], self.message[i], self.workloads[i]))
        return results

    def get_result_for_workload(self, workload: 'pylo.Workload') -> Optional[Result]:
        for i in range(len(self.workloads)):
            if self.workloads[i] == workload:
                return WorkloadApiUpdateStackExecutionManager.Result(self.successful[i], self.message[i], self.workloads[i])
        return None
