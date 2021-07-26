from typing import Optional, List, Union, Dict

import pylo
from pylo import log, Organization
import re


class Rule:

    def __init__(self, owner: 'pylo.Ruleset'):
        self.owner: 'pylo.Ruleset' = owner
        self.description: Optional[str] = None
        self.services: 'pylo.RuleServiceContainer' = pylo.RuleServiceContainer(self)
        self.providers: 'pylo.RuleHostContainer' = pylo.RuleHostContainer(self, 'providers')
        self.consumers: 'pylo.RuleHostContainer' = pylo.RuleHostContainer(self, 'consumers')
        self.consuming_principals: 'pylo.RuleSecurityPrincipalContainer' = pylo.RuleSecurityPrincipalContainer(self)
        self.href = None
        self.enabled = True
        self.secure_connect = False
        self.unscoped_consumers = False

    def load_from_json(self, data):
        self.href = data['href']

        self.description = data.get('description')

        services = data.get('ingress_services')
        if services is not None:
            self.services.load_from_json(services)

        enabled = data.get('enabled')
        if enabled is not None:
            self.enabled = enabled

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


class RuleSecurityPrincipalContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: dict[pylo.SecurityPrincipal, pylo.SecurityPrincipal]

    def load_from_json(self, data):
        ssStore = self.owner.owner.owner.owner.SecurityPrincipalStore
        for item_data in data:
            wanted_href = item_data['href']
            found_object = ssStore.find_by_href_or_die(wanted_href)
            found_object.add_reference(self)
            self._items[found_object] = found_object


class DirectServiceInRule:
    def __init__(self, proto: int, port: int = None, toport: int = None):
        self.protocol = proto
        self.port = port
        self.to_port = toport

    def is_tcp(self):
        return self.protocol == 6

    def is_udp(self):
        return self.protocol == 17

    def to_string_standard(self, protocol_first=True):
        if self.protocol == 17:
            if self.to_port is None:
                if protocol_first:
                    return 'udp/' + str(self.port)

                return str(self.port) + '/udp'
            if protocol_first:
                return 'udp/' + str(self.port) + '-' + str(self.to_port)

            return str(self.port) + '-' + str(self.to_port)+ '/udp'
        elif self.protocol == 6:
            if self.to_port is None:
                if protocol_first:
                    return 'tcp/' + str(self.port)
                return str(self.port) + '/tcp'

            if protocol_first:
                return 'tcp/' + str(self.port) + '-' + str(self.to_port)
            return str(self.port) + '-' + str(self.to_port)+ '/tcp'

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
                return pylo.DirectServiceInRule(proto=1)
            raise pylo.PyloEx("Invalid service syntax '{}'".format(txt))

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
            else:
                raise pylo.PyloEx("Invalid protocol provided: {}".format(proto))
        else:
            protocol_int = int(proto)

        port_parts = port_input.split('-')
        if len(port_parts) > 2:
            raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        if len(port_parts) == 2:
            if protocol_int != 17 and protocol_int != 6:
                raise pylo.PyloEx("Only TCP and UDP support port ranges so this service in invalid: '{}'".format(txt))
            from_port_input = port_parts[0]
            to_port_input = port_parts[1]

            if not from_port_input.isdigit():
                raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(from_port_input, txt))
            if not to_port_input.isdigit():
                raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(to_port_input, txt))

            return pylo.DirectServiceInRule(protocol_int, port=int(from_port_input), toport=int(to_port_input))

        if not port_input.isdigit():
            raise pylo.PyloEx("Invalid port provided: '{}' in string '{}'".format(port_input, txt))

        return pylo.DirectServiceInRule(protocol_int, port=int(port_input))


class RuleServiceContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule'):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items: Dict[pylo.Service, pylo.Service]= {}
        self._direct_services: List[DirectServiceInRule] = []

    def load_from_json_legacy_single(self, data):
        href = data.get('href')
        if href is None:
            raise Exception('Cannot find service HREF')

        find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
        if find_service is None:
            raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

        self._items[find_service] = find_service
        find_service.add_reference(self)

    def load_from_json(self, data_list):
        for data in data_list:
            # print(data)
            href = data.get('href')
            if href is None:
                port = data.get('port')
                if port is None:
                    raise pylo.PyloEx("unsupported service type in rule: {}".format(pylo.nice_json(data)))
                protocol = data.get('proto')
                if protocol is None:
                    raise pylo.PyloEx("Protocol not found in direct service use: {}".format(pylo.nice_json(data)))

                to_port = data.get('to_port')
                direct_port = DirectServiceInRule(protocol, port, to_port)
                self._direct_services.append(direct_port)

                continue

            find_service = self.owner.owner.owner.owner.ServiceStore.itemsByHRef.get(href)
            if find_service is None:
                raise Exception('Cannot find Service with HREF %s in Rule %s'.format(href, self.owner.href))

            self._items[find_service] = find_service
            find_service.add_reference(self)

    def get_direct_services(self) -> List[DirectServiceInRule]:
        """
        Return a list of services directly included in the Rule
        """
        return self._direct_services

    def get_services(self) -> List[pylo.Service]:
        return list(self._items.values())

    def remove_direct_service(self, service: DirectServiceInRule) -> bool:
        for i in range(0, len(self._direct_services)):
            if self._direct_services[i] is service:
                del(self._direct_services[i])
                return True
        return False

    def add_direct_service(self, service: DirectServiceInRule) -> bool:
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
            text += service.name + ': ' + pylo.string_list_to_text(service.get_entries_str_list())

        for direct in self._direct_services:
            if len(text) > 0:
                text += separator
            text += direct.to_string_standard()

        return text

    def api_sync(self):
        connector = pylo.find_connector_or_die(self)
        data = []
        for service in self._direct_services:
            data.append(service.get_api_json())

        for service in self._items.values():
            data.append({'href': service.href})

        data = {'ingress_services': data}
        connector.objects_rule_update(self.owner.href, data)


class RuleHostContainer(pylo.Referencer):
    def __init__(self, owner: 'pylo.Rule', name: str):
        pylo.Referencer.__init__(self)
        self.owner = owner
        self._items = {}  # type: Dict[Union[pylo.Label, pylo.LabelGroup, pylo.Workload, pylo.VirtualService], Union[pylo.Label, pylo.LabelGroup, pylo.Workload, pylo.VirtualService]]
        self.name = name
        self._hasAllWorkloads = False

    def load_from_json(self, data):
        for host_data in data:
            find_object = None
            if 'label' in host_data:
                href = host_data['label'].get('href')
                if href is None:
                    pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find Label with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'label_group' in host_data:
                href = host_data['label_group'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.LabelStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find LabelGroup with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'ip_list' in host_data:
                href = host_data['ip_list'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                find_object = self.owner.owner.owner.owner.IPListStore.itemsByHRef.get(href)
                if find_object is None:
                    raise Exception('Cannot find IPList with HREF {} in Rule {}'.format(href, self.owner.href))
            elif 'workload' in host_data:
                href = host_data['workload'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                # @TODO : better handling of temporary objects
                find_object = self.owner.owner.owner.owner.WorkloadStore.itemsByHRef.get(href)
                if find_object is None:
                    # raise Exception("Cannot find Workload with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, pylo.nice_json(host_data)))
                    find_object = self.owner.owner.owner.owner.WorkloadStore.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'virtual_service' in host_data:
                href = host_data['virtual_service'].get('href')
                if href is None:
                    raise pylo.PyloEx('Cannot find object HREF ', host_data)
                # @TODO : better handling of temporary objects
                find_object = self.owner.owner.owner.owner.VirtualServiceStore.itemsByHRef.get(href)
                if find_object is None:
                    # raise Exception("Cannot find VirtualService with HREF {} in Rule {}. JSON:\n {}".format(href, self.owner.href, pylo.nice_json(host_data)))
                    find_object = self.owner.owner.owner.owner.VirtualServiceStore.find_by_href_or_create_tmp(href, 'tmp-deleted-wkl-'+href)
            elif 'actors' in host_data:
                actor_value = host_data['actors']
                if actor_value is not None and actor_value == 'ams':
                    self._hasAllWorkloads = True
                    continue
                # TODO implement actors
                raise pylo.PyloEx("An actor that is not 'ams' was detected but this library doesn't support it yet", host_data)
            else:
                raise pylo.PyloEx("Unsupported reference type", host_data)

            if find_object is not None:
                self._items[find_object] = find_object
                find_object.add_reference(self)

    def has_workloads(self) -> bool:
        for item in self._items.values():
            if isinstance(item, pylo.Workload):
                return True
        return False

    def has_labels(self) -> bool:
        for item in self._items.values():
            if isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup):
                return True
        return False

    def get_labels(self) -> List[Union[pylo.Label, pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup):
                result.append(item)

        return result

    def get_role_labels(self) -> List[Union[pylo.Label,pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if (isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup)) and item.type_is_role():
                result.append(item)

        return result

    def get_app_labels(self) -> List[Union[pylo.Label,pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if (isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup)) and item.type_is_application():
                result.append(item)

        return result

    def get_env_labels(self) -> List[Union[pylo.Label,pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if (isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup)) and item.type_is_environment():
                result.append(item)

        return result

    def get_loc_labels(self) -> List[Union[pylo.Label,pylo.LabelGroup]]:
        result = []

        for item in self._items.values():
            if (isinstance(item, pylo.Label) or isinstance(item, pylo.LabelGroup)) and item.type_is_location():
                result.append(item)

        return result

    def members_to_str(self) -> str:
        text = ''

        if self._hasAllWorkloads:
            text += "All Workloads"

        for label in self.get_labels():
            if len(text) > 0:
                text += ','
            text += label.name

        for item in self.get_iplists():
            if len(text) > 0:
                text += ','
            text += item.name

        for item in self.get_workloads():
            if len(text) > 0:
                text += ','
            text += item.get_name()

        for item in self.get_virtual_services():
            if len(text) > 0:
                text += ','
            text += item.name

        return text

    def contains_iplists(self) -> bool:
        """
        Returns True if at least 1 iplist is part of this container
        """
        for item in self._items.values():
            if isinstance(item, pylo.IPList):
                return True
        return False

    def get_iplists(self) -> List[pylo.IPList]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.IPList):
                result.append(item)

        return result

    def get_workloads(self) -> List[pylo.Workload]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.Workload):
                result.append(item)

        return result

    def get_virtual_services(self) -> List[pylo.VirtualService]:
        result = []

        for item in self._items.values():
            if isinstance(item, pylo.VirtualService):
                result.append(item)

        return result

    def contains_all_workloads(self) -> bool:
        return self._hasAllWorkloads
