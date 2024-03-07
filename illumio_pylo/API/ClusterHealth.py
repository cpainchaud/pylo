from .APIConnector import APIConnector, get_field_or_die
from .. import PyloEx, string_list_to_text


class ClusterHealth:

    allowed_status_list = {'normal': True, 'warning': True, 'error': True}

    class ClusterHealthNode:

        class ServiceStatus:

            allowed_status = {'running': True, 'stopped': True, 'partial': True, 'not_running': True}

            def __init__(self, name: str, status: str):
                self.name = name
                if status not in ClusterHealth.ClusterHealthNode.ServiceStatus.allowed_status:
                    raise PyloEx("A services status is created with unsupported status '{}'".format(status))

                self.status = status  # type: str

            def is_running(self):
                return self.status == 'running'

            def is_not_running(self):
                return self.status == 'stopped'

            def is_partially_running(self):
                return self.status == 'partial'

        def __init__(self, json_data):
            self.name = get_field_or_die('hostname', json_data)  # type: str
            self.type = get_field_or_die('type', json_data)  # type: str
            self.ip_address = get_field_or_die('ip_address', json_data)  # type: str
            self.runlevel = get_field_or_die('runlevel', json_data)  # type: int

            self.services = {}  # type: dict[str, ClusterHealth.ClusterHealthNode.ServiceStatus]
            services_statuses = get_field_or_die('services', json_data)

            self.global_service_status = get_field_or_die('status', services_statuses)

            def process_services(json_data: dict, status):
                services = json_data.get(status)
                if services is None:
                    return

                for service in services:
                    new_service = ClusterHealth.ClusterHealthNode.ServiceStatus(service, status)
                    if new_service.name in self.services:
                        raise PyloEx("duplicated service name '{}'".format(new_service.name))
                    self.services[new_service.name] = new_service

            process_services(services_statuses, 'running')
            process_services(services_statuses, 'not_running')
            process_services(services_statuses, 'partial')
            process_services(services_statuses, 'optional')

        def is_offline_or_unreachable(self):
            if self.runlevel is None:
                return True
            return False

        def get_troubled_services(self):
            ret = []
            for service in self.services.values():
                if not service.is_running():
                    ret.append(service)
            return ret

        def get_running_services(self):
            ret = []
            for service in self.services.values():
                if service.is_running():
                    ret.append(service)
            return ret

        def to_string(self, indent='', marker='*'):
            def val_str(display_name: str, value):
                return "{}{}{}: {}\n".format(indent, marker, display_name, value)
            ret = ''
            ret += val_str('fqdn', self.name)
            indent += ' '
            marker = '-'

            ret += val_str('ip_address', self.ip_address)
            ret += val_str('type', self.type)
            if self.runlevel is None:
                ret += val_str('runlevel', 'not running or not available')
                return ret
            else:
                ret += val_str('runlevel', self.runlevel)

            troubled_services = string_list_to_text(self.get_troubled_services())
            ret += val_str('non-functional services', troubled_services)

            running_services = string_list_to_text(self.get_running_services())
            ret += val_str('running services', running_services)

            return ret

    def __init__(self, json_data):
        self.raw_json = json_data

        self.fqdn = get_field_or_die('fqdn', json_data)  # type: str
        self._status = get_field_or_die('status', json_data)  # type: str
        self.type = get_field_or_die('type', json_data)  # type: str

        if self._status not in ClusterHealth.allowed_status_list:
            raise PyloEx("ClusterHealth has unsupported status '{}'".format(self._status))

        nodes_list = get_field_or_die('nodes', json_data)
        self.nodes_dict = {}

        for node in nodes_list:
            new_node = ClusterHealth.ClusterHealthNode(node)
            self.nodes_dict[new_node.name] = new_node

    def to_string(self):
        ret = ''
        ret += "cluster fqdn: '{}'\n".format(self.fqdn)
        ret += "type: '{}'\n".format(self.type)
        ret += "status: '{}'\n".format(self._status)
        ret += "nodes details:\n"
        for node in self.nodes_dict.values():
            ret += node.to_string(indent='  ')

        return ret

    def status_is_ok(self):
        return self._status == 'normal'

    def status_is_warning(self):
        return self._status == 'warning'

    def status_is_error(self):
        return self._status == 'error'
