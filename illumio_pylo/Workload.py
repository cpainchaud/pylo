from datetime import datetime
from typing import Optional, List, Union

import illumio_pylo as pylo
from illumio_pylo.API.JsonPayloadTypes import WorkloadObjectJsonStructure
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


class Workload(pylo.ReferenceTracker, pylo.Referencer, LabeledObject):

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


    def is_using_label(self, label: Union['pylo.Label', 'pylo.LabelGroup']) -> bool:
        """
        Check if a label is used by this Workload
        :param label: label to check for usage. If it's a label group then it will check that at least one label of the group is used
        :return: true if label is used by this workload
        """

        # check for label class
        if isinstance(label, pylo.Label):
            if self.loc_label is label or self.env_label is label \
                or self.app_label is label or self.app_label is label:
                return True
        else:
            for member_label in label.get_members().values():
                if self.is_using_label(member_label):
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

        self.raw_json.update(data)
        self.hostname = new_hostname

    def api_update_forced_name(self, name: str):

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
        if self.loc_label is not None:
            label_data.append({'href': self.loc_label.href})
        if self.env_label is not None:
            label_data.append({'href': self.env_label.href})
        if self.app_label is not None:
            label_data.append({'href': self.app_label.href})
        if self.role_label is not None:
            label_data.append({'href': self.role_label.href})

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
        changes_occurred = False
        role_label: Optional[Label] = None
        app_label: Optional[Label] = None
        env_label: Optional[Label] = None
        loc_label: Optional[Label] = None

        if len(list_of_labels) > 4:
            raise PyloEx("More than 4 labels provided")

        for label in list_of_labels:
            if label.type_is_role():
                if role_label is not None:
                    raise PyloEx("ROLE label specified more than once ('{}' vs '{}')".format(role_label.name, label.name))
                role_label = label
            elif label.type_is_application():
                if app_label is not None:
                    raise PyloEx("APP label specified more than once ('{}' vs '{}')".format(app_label.name, label.name))
                app_label = label
            elif label.type_is_environment():
                if env_label is not None:
                    raise PyloEx("ENV label specified more than once ('{}' vs '{}')".format(env_label.name, label.name))
                env_label = label
            elif label.type_is_location():
                if loc_label is not None:
                    raise PyloEx("LOC label specified more than once ('{}' vs '{}')".format(loc_label.name, label.name))
                loc_label = label

        if role_label is None:
            if not missing_label_type_means_no_change:
                if self.role_label is not None:
                    changes_occurred = True
                    self.role_label.remove_reference(self)
                    self.role_label = None
        elif role_label is not self.role_label:
            changes_occurred = True
            if self.role_label is not None:
                self.role_label.remove_reference(self)
            role_label.add_reference(self)
            self.role_label = role_label

        if app_label is None:
            if not missing_label_type_means_no_change:
                if self.app_label is not None:
                    changes_occurred = True
                    self.app_label.remove_reference(self)
                    self.app_label = None
        elif app_label is not self.app_label:
            changes_occurred = True
            if self.app_label is not None:
                self.app_label.remove_reference(self)
            app_label.add_reference(self)
            self.app_label = app_label

        if env_label is None:
            if not missing_label_type_means_no_change:
                if self.env_label is not None:
                    changes_occurred = True
                    self.env_label.remove_reference(self)
                    self.env_label = None
        elif env_label is not self.env_label:
            changes_occurred = True
            if self.env_label is not None:
                self.env_label.remove_reference(self)
            env_label.add_reference(self)
            self.env_label = env_label

        if loc_label is None:
            if not missing_label_type_means_no_change:
                if self.loc_label is not None:
                    changes_occurred = True
                    self.loc_label.remove_reference(self)
                    self.loc_label = None
        elif loc_label is not self.loc_label:
            changes_occurred = True
            if self.loc_label is not None:
                self.loc_label.remove_reference(self)
            loc_label.add_reference(self)
            self.loc_label = loc_label

        return changes_occurred

    def get_pce_ui_url(self) -> str:
        """
        generates a URL link for the Workload vs PCE UI
        :return: url string
        """
        return self.owner.owner.connector.get_pce_ui_workload_url(self.href)
