import typing
from typing import Optional, List, Union, Dict, Any, NewType

import illumio_pylo as pylo
from .API.JsonPayloadTypes import RuleServiceReferenceObjectJsonStructure, RuleDirectServiceReferenceObjectJsonStructure
from illumio_pylo import Workload, Label, LabelGroup, Ruleset, Referencer, SecurityPrincipal, PyloEx, \
    Service, nice_json, string_list_to_text, find_connector_or_die, VirtualService, IPList, PortMap

RuleActorsAcceptableTypes = NewType('RuleActorsAcceptableTypes', Union[Workload, Label, LabelGroup, IPList, VirtualService])


class RuleApiUpdateStack:
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


class Rule:

    __slots__ = ['owner', 'description', 'services', 'providers', 'consumers', 'consuming_principals', 'href', 'enabled',
                 'secure_connect', 'unscoped_consumers', 'stateless', 'machine_auth', 'raw_json', 'batch_update_stack']

    def __init__(self, owner: 'Ruleset'):
        self.owner: Ruleset = owner
        self.description: Optional[str] = None
        self.services: RuleServiceContainer = RuleServiceContainer(self)
        self.providers: RuleHostContainer = RuleHostContainer(self, 'providers')
        self.consumers: RuleHostContainer = RuleHostContainer(self, 'consumers')
        self.consuming_principals: RuleSecurityPrincipalContainer = RuleSecurityPrincipalContainer(self)
        self.href: Optional[str] = None
        self.enabled: bool = True
        self.secure_connect: bool = False
        self.unscoped_consumers: bool = False
        self.stateless: bool = False
        self.machine_auth: bool = False

        self.raw_json: Optional[Dict[str, Any]] = None
        self.batch_update_stack: Optional[RuleApiUpdateStack] = None

    def load_from_json(self, data):
        self.raw_json = data

        self.href = data['href']

        self.description = data.get('description')

        services = data.get('ingress_services')
        if services is not None:
            self.services.load_from_json(services)

        enabled = data.get('enabled')
        if enabled is not None:
            self.enabled = enabled

        stateless = data.get('stateless')
        if stateless is not None:
            self.stateless = stateless

        machine_auth = data.get('machine_auth')
        if machine_auth is not None:
            self.machine_auth = machine_auth

        secure_connect = data.get('sec_connect')
        if secure_connect is not None:
            self.secure_connect = secure_connect

        unscoped_consumers = data.get('unscoped_consumers')
        if unscoped_consumers is not None:
            self.unscoped_consumers = unscoped_consumers

        self.providers.load_from_json(data['providers'])
        self.consumers.load_from_json(data['consumers'])
        self.consuming_principals.load_from_json(data['consuming_security_principals'])

    def is_extra_scope(self):
        return self.unscoped_consumers

    def is_intra_scope(self):
        return not self.unscoped_consumers

    def api_set_description(self, new_description: str):
        data = {'description': new_description}
        if self.batch_update_stack is None:
            self.owner.owner.owner.connector.objects_rule_update(self.href, update_data=data)

        self.raw_json.update(data)
        self.description = new_description

    def api_stacked_updates_start(self):
        """
        Turns on 'updates stacking' mode for this Rule which will not push changes to API as you make them but only
        when you trigger 'api_push_stacked_updates()' function
        """
        self.batch_update_stack = RuleApiUpdateStack()

    def api_stacked_updates_push(self):
        """
        Push all stacked changed to API and turns off 'updates stacking' mode
        """
        if self.batch_update_stack is None:
            raise pylo.PyloEx("Workload was not in 'update stacking' mode")

        connector = pylo.find_connector_or_die(self.owner)
        connector.objects_rule_update(self.href, self.batch_update_stack.get_payload_and_reset())
        self.batch_update_stack = None

    def api_stacked_updates_count(self) -> int:
        """
        Counts the number of stacked changed for this Ruke
        :return:
        """
        if self.batch_update_stack is None:
            raise pylo.PyloEx("Workload was not in 'update stacking' mode")
        return self.batch_update_stack.count_payloads()


class RuleSecurityPrincipalContainer(pylo.Referencer):

    __slots__ = ['owner', '_items']

    def __init__(self, owner: 'pylo.Rule'):
        Referencer.__init__(self)
        self.owner = owner
        self._items: Dict[SecurityPrincipal, SecurityPrincipal] = {}  # type:

    def load_from_json(self, data):
        ss_store = self.owner.owner.owner.owner.SecurityPrincipalStore  # make it a local variable for fast lookups
        for item_data in data:
            wanted_href = item_data['href']
            found_object = ss_store.find_by_href(wanted_href)
            if found_object is None:
                raise pylo.PyloEx(f"Could not find SecurityPrincipal with href '{wanted_href}' inside rule href '{self.owner.href}' and Ruleset named '{self.owner.owner.name}'")
            found_object.add_reference(self)
            self._items[found_object] = found_object


class DirectServiceInRule:

    __slots__ = ['protocol', 'port', 'to_port']

    def __init__(self, proto: int, port: int = None, toport: int = None):
        self.protocol = proto
        self.port = port
        self.to_port = toport

    def is_tcp(self):
        return self.protocol == 6

    def is_udp(self):
        return self.protocol == 17

    def is_icmp(self):
        return self.protocol == 1

    def to_string_standard(self, protocol_first=True):
        if self.protocol == 17:
            if self.to_port is None:
                if protocol_first:
                    return 'udp/' + str(self.port)

                return str(self.port) + '/udp'
            if protocol_first:
                return 'udp/' + str(self.port) + '-' + str(self.to_port)

            return str(self.port) + '-' + str(self.to_port) + '/udp'
        elif self.protocol == 6:
            if self.to_port is None:
                if protocol_first:
                    return 'tcp/' + str(self.port)
                return str(self.port) + '/tcp'

            if protocol_first:
                return 'tcp/' + str(self.port) + '-' + str(self.to_port)
            return str(self.port) + '-' + str(self.to_port) + '/tcp'

        if protocol_first:
            return 'proto/' + str(self.protocol)

        return str(self.protocol) + '/proto'

    def get_api_json(self) -> Dict:
        """
        Generates json payload to be included in a rule's service update or creation
        """
        if self.protocol != 17 and self.protocol != 6:
            return {'proto': self.protocol}

        if self.to_port is None:
            return {'proto': self.protocol, 'port': self.port}
        return {'proto': self.protocol, 'port': self.port, 'to_port': self.to_port}

    @staticmethod
    def create_from_text(txt: str, seperator='/', protocol_first=True) -> 'DirectServiceInRule':
        parts = txt.split(seperator)

        if len(parts) != 2:
            lower = txt.lower()
            if lower == 'icmp':
                return DirectServiceInRule(proto=1)
            raise PyloEx("Invalid service syntax '{}'".format(txt))

        if protocol_first:
            proto = parts[0]
            port_input = parts[1]
        else:
            proto = parts[1]
            port_input = parts[0]

        if not proto.isdigit():
            proto_lower = proto.lower()
            if proto_lower == 'tcp':
                protocol_int = 6
            elif proto_lower == 'udp':
                protocol_int = 17
            elif proto_lower == 'proto':
                return DirectServiceInRule(proto=int(port_input))
            else:
                raise PyloEx("Invalid protocol provided: {}".format(proto))
        else:
            protocol_int = int(proto)

        port_parts = port_input.split('-')
        if len(port_parts) > 2:
            raise PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        if len(port_parts) == 2:
            if protocol_int != 17 and protocol_int != 6:
                raise PyloEx("Only TCP and UDP support port ranges so this service in invalid: '{}'".format(txt))
            from_port_input = port_parts[0]
            to_port_input = port_parts[1]

            if not from_port_input.isdigit():
                raise PyloEx("Invalid port provided: '{}' in string '{}'".format(from_port_input, txt))
            if not to_port_input.isdigit():
                raise PyloEx("Invalid port provided: '{}' in string '{}'".format(to_port_input, txt))

            return DirectServiceInRule(protocol_int, port=int(from_port_input), toport=int(to_port_input))

        if not port_input.isdigit():
            raise PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        return DirectServiceInRule(protocol_int, port=int(port_input))


class RuleServiceContainer(pylo.Referencer):

    __slots__ = ['owner', '_items', '_direct_services', '_cached_port_map']

    def __init__(self, owner: 'pylo.Rule'):
        Referencer.__init__(self)
        self.owner = owner
        self._items: Dict[Service, Service] = {}
        self._direct_services: List[DirectServiceInRule] = []
        self._cached_port_map: Optional[PortMap] = None

    def load_from_json(self, data_list: List[RuleServiceReferenceObjectJsonStructure|RuleDirectServiceReferenceObjectJsonStructure]):
        ss_store = self.owner.owner.owner.owner.ServiceStore  # make it a local variable for fast lookups

        for data in data_list:
            # print(data)
            href = data.get('href')
            if href is None:
                data = typing.cast(RuleDirectServiceReferenceObjectJsonStructure, data)
                port = data.get('port')
                if port is None:
                    raise PyloEx("unsupported service type in rule: {}".format(nice_json(data)))
                protocol = data.get('proto')
                if protocol is None:
                    raise PyloEx("Protocol not found in direct service use: {}".format(nice_json(data)))

                to_port = data.get('to_port')
                direct_port = DirectServiceInRule(protocol, port, to_port)
                self._direct_services.append(direct_port)

                continue

            data = typing.cast(RuleServiceReferenceObjectJsonStructure, data)
            find_service = ss_store.itemsByHRef.get(href)
            if find_service is None:
                raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

            self._items[find_service] = find_service
            find_service.add_reference(self)

    def get_direct_services(self) -> List[DirectServiceInRule]:
        """
        Return a list of services directly included in the Rule
        """
        return self._direct_services.copy()

    def get_services(self) -> List[pylo.Service]:
        return list(self._items.values())

    def remove_direct_service(self, service: DirectServiceInRule) -> bool:
        """
        Removes a direct service from the rule
        :param service:
        :return: True if the service was removed, False if it was not found
        """
        self._cached_port_map = None

        for i in range(0, len(self._direct_services)):
            if self._direct_services[i] is service:
                del(self._direct_services[i])
                return True
        return False

    def add_direct_service(self, service: DirectServiceInRule) -> bool:
        self._cached_port_map = None

        for member in self._direct_services:
            if service is member:
                return False
        self._direct_services.append(service)
        return True

    def members_to_str(self, separator: str = ',') -> str:
        text: str = ''

        for service in self._items.values():
            if len(text) > 0:
                text += separator
            text += service.name + ': ' + string_list_to_text(service.get_entries_str_list())

        for direct in self._direct_services:
            if len(text) > 0:
                text += separator
            text += direct.to_string_standard()

        return text

    def get_api_json_payload(self) -> List[Dict[str, Any]]:
        """
        Generate JSON payload for API update call
        :return:
        """
        data = []
        for service in self._direct_services:
            data.append(service.get_api_json())

        for service in self._items.values():
            data.append({'href': service.href})

        return data

    def api_sync(self):
        """
        Synchronize a Rule's services after some changes were made
        """
        connector = find_connector_or_die(self)
        data = self.get_api_json_payload()
        data = {'ingress_services': data}

        if self.owner.batch_update_stack is None:
            connector.objects_rule_update(self.owner.href, update_data=data)
        else:
            self.owner.batch_update_stack.add_payload(data)

        self.owner.raw_json.update(data)

    def get_port_map(self) -> PortMap:
        """
        Get a PortMap object with all ports and protocols from all services in this container
        :return:
        """
        if self._cached_port_map is not None:
            return self._cached_port_map

        result = PortMap()
        for service in self._items.values():
            for entry in service.entries:
                result.add(entry.protocol, entry.port, entry.to_port, skip_recalculation=True)
        for direct in self._direct_services:
            result.add(direct.protocol, direct.port, direct.to_port, skip_recalculation=True)

        result.merge_overlapping_maps()

        self._cached_port_map = result

        return result


class RuleHostContainer(pylo.Referencer):

    __slots__ = ['owner', '_items', 'name', '_hasAllWorkloads']

    def __init__(self, owner: 'pylo.Rule', name: str):
        Referencer.__init__(self)
        self.owner = owner
        self._items: Dict[RuleActorsAcceptableTypes, RuleActorsAcceptableTypes] = {}
        self.name = name
        self._hasAllWorkloads = False

    def load_from_json(self, data):
        """
        Parse from a JSON payload.
        *For developers only*

        :param data: JSON payload to parse
        """
        workload_store = self.owner.owner.owner.owner.WorkloadStore  # make it a local variable for fast lookups
        label_store = self.owner.owner.owner.owner.LabelStore  # make it a local variable for fast lookups
        virtual_service_store = self.owner.owner.owner.owner.VirtualServiceStore  # make it a local variable for fast lookups
        iplist_store = self.owner.owner.owner.owner.IPListStore  # make it a local variable for fast lookups

        for host_data in data:
            find_object = None
            if 'label' in host_data:
                href = host_data['label'].get('href')
                if href is None:
                    PyloEx('Cannot find object HREF ', host_data)
                find_object = label_store.find_by_href(href)
                if find_object is None:
                    raise Exception('Cannot find Label with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'label_group' in host_data:
                href = host_data['label_group'].get('href')
                if href is None:
                    raise PyloEx('Cannot find object HREF ', host_data)
                find_object = label_store.find_by_href(href)
                if find_object is None:
                    raise Exception('Cannot find LabelGroup with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'ip_list' in host_data:
                href = host_data['ip_list'].get('href')
                if href is None:
                    raise PyloEx('Cannot find object HREF ', host_data)
                find_object = iplist_store.items_by_href.get(href)
                if find_object is None:
                    raise Exception('Cannot find IPList with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'workload' in host_data:
                href = host_data['workload'].get('href')
                if href is None:
                    raise PyloEx('Cannot find object HREF ', host_data)
                # @TODO : better handling of temporary objects
                find_object = workload_store.itemsByHRef.get(href)
                if find_object is None:
                    # raise Exception("Cannot find Workload with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, nice_json(host_data)))
                    find_object = workload_store.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'virtual_service' in host_data:
                href = host_data['virtual_service'].get('href')
                if href is None:
                    raise PyloEx('Cannot find object HREF ', host_data)
                # @TODO : better handling of temporary objects
                find_object = virtual_service_store.items_by_href.get(href)
                if find_object is None:
                    # raise Exception("Cannot find VirtualService with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, nice_json(host_data)))
                    find_object = self.owner.owner.owner.owner.VirtualServiceStore.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'virtual_server' in host_data:
                pylo.log.warn('VirtualServer found in Rule {}. This is not supported yet by this library, beware of unexpected behaviors'.format(self.owner.href))
            elif 'actors' in host_data:
                actor_value = host_data['actors']
                if actor_value is not None and actor_value == 'ams':
                    self._hasAllWorkloads = True
                    continue
                # TODO implement actors
                raise PyloEx("An actor that is not 'ams' was detected but this library doesn't support it yet", host_data)
            else:
                raise PyloEx("Unsupported reference type", host_data)

            if find_object is not None:
                self._items[find_object] = find_object
                find_object.add_reference(self)

    def has_workloads(self) -> bool:
        """
        Check if this container references at least one Workload
        :return: True if contains at least one Workload
        """
        for item in self._items.values():
            if isinstance(item, Workload):
                return True
        return False

    def has_virtual_services(self) -> bool:
        """
        Check if this container references at least one Virtual Service
        :return: True if contains at least one Virtual Service
        """
        for item in self._items.values():
            if isinstance(item, VirtualService):
                return True
        return False

    def has_labels(self) -> bool:
        """
        Check if this container references at least one Label or LabelGroup
        :return: True if contains at least one Label or LabelGroup
        """
        for item in self._items.values():
            if isinstance(item, Label) or isinstance(item, LabelGroup):
                return True
        return False

    def get_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        """
        Get a list Labels and LabelGroups which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if isinstance(item, Label) or isinstance(item, LabelGroup):
                result.append(item)

        return result

    def get_role_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        """
        Get a list Role Labels and LabelGroups which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if (isinstance(item, Label) or isinstance(item, LabelGroup)) and item.type_is_role():
                result.append(item)

        return result

    def get_app_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        """
        Get a list App Labels and LabelGroups which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if (isinstance(item, Label) or isinstance(item, LabelGroup)) and item.type_is_application():
                result.append(item)

        return result

    def get_env_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        """
        Get a list Env Labels and LabelGroups which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if (isinstance(item, Label) or isinstance(item, LabelGroup)) and item.type_is_environment():
                result.append(item)

        return result

    def get_loc_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        """
        Get a list Loc Labels and LabelGroups which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if (isinstance(item, Label) or isinstance(item, LabelGroup)) and item.type_is_location():
                result.append(item)

        return result

    def members_to_str(self, separator=',', sort_alphabetically=True, sort_labels_by_type=True,
                       prefix_objects_with_type=False, object_types_as_section=False) -> str:
        """
        Conveniently creates a string with all members of this container, ordered by Label, IPList, Workload,
        and  Virtual Service

        :param separator: string use to separate each member in the lit
        :param sort_alphabetically: if True, the members will be sorted alphabetically
        :param sort_labels_by_type: if True, the labels will be sorted by type (role, app, env, loc ...)
        :param prefix_objects_with_type: if True, the objects will be prefixed with their type (Label, IPList, Workload, Virtual Service)
        :param object_types_as_section: if True, the objects will be grouped by type and each group will be separated with a header
        :return:
        """
        text = ''

        if self._hasAllWorkloads:
            text += "*All Workloads*"

        labels = self.get_labels()
        if len(labels) > 0:
            if len(text) > 0:
                text += separator
            text += 'LABELS:'
        if sort_alphabetically:
            labels = sorted(labels, key=lambda x: x.name.lower())
        if sort_labels_by_type:
            labels = pylo.LabelStore.Utils.list_sort_by_type(labels, self.owner.owner.owner.owner.LabelStore.label_types)

        for label in labels:
            if prefix_objects_with_type:
                if label.is_group():
                    prefix = 'lbg:'
                else:
                    prefix = 'lbl:'
            else:
                prefix = ''
            if len(text) > 0:
                text += separator
            text += prefix + label.name

        iplists = self.get_iplists()
        if len(iplists) > 0:
            if len(text) > 0:
                text += separator
            text += 'IPLISTS:'
        if sort_alphabetically:
            iplists = sorted(iplists, key=lambda x: x.name.lower())
        if prefix_objects_with_type:
            prefix = 'ipl:'
        else:
            prefix = ''
        for item in iplists:
            if len(text) > 0:
                text += separator
            text += prefix + item.name

        workloads = self.get_workloads()
        if len(workloads) > 0:
            if len(text) > 0:
                text += separator
            text += 'WORKLOADS:'
        if sort_alphabetically:
            workloads = sorted(workloads, key=lambda x: x.name.lower())
        if prefix_objects_with_type:
            prefix = 'wkl:'
        else:
            prefix = ''
        for item in workloads:
            if len(text) > 0:
                text += separator
            text += prefix + item.get_name()

        virtual_services = self.get_virtual_services()
        if len(virtual_services) > 0:
            if len(text) > 0:
                text += separator
            text += 'VIRTUAL SERVICES:'
        if sort_alphabetically:
            virtual_services = sorted(virtual_services, key=lambda x: x.name.lower())
        if prefix_objects_with_type:
            prefix = 'vs:'
        else:
            prefix = ''
        for item in virtual_services:
            if len(text) > 0:
                text += separator
            text += prefix + item.name

        return text

    def contains_iplists(self) -> bool:
        """
        Returns True if at least 1 iplist is part of this container
        """
        for item in self._items.values():
            if isinstance(item, IPList):
                return True
        return False

    def get_iplists(self) -> List[IPList]:
        """
        Get a list of IPLists which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if isinstance(item, IPList):
                result.append(item)

        return result

    def get_workloads(self) -> List[Workload]:
        """
        Get a list of Workloads which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if isinstance(item, Workload):
                result.append(item)

        return result

    def get_virtual_services(self) -> List[pylo.VirtualService]:
        """
        Get a list of VirtualServices which are part of this container
        :return:
        """
        result = []

        for item in self._items.values():
            if isinstance(item, VirtualService):
                result.append(item)

        return result

    def contains_all_workloads(self) -> bool:
        """
        Check if this container references "All Workloads"
        :return: True if "All Workloads" is referenced by this container
        """
        return self._hasAllWorkloads
