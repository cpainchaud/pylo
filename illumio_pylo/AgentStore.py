from datetime import datetime

import illumio_pylo as pylo
from illumio_pylo import log, SoftwareVersion
from .Helpers import *
import re
import datetime
from typing import *

# version_regex = re.compile(r"^(?P<major>[0-9]+)\.(?P<middle>[0-9]+)\.(?P<minor>[0-9]+)-(?P<build>[0-9]+)(u[0-9]+)?$")


class VENAgent(pylo.ReferenceTracker):

    __slots__ = ['href', 'owner', 'workload', 'software_version', '_last_heartbeat', '_status_security_policy_sync_state',
                 '_status_security_policy_applied_at', '_status_rule_count', 'mode', 'raw_json']

    def __init__(self, href: str, owner: 'pylo.AgentStore', workload: 'pylo.Workload' = None):
        pylo.ReferenceTracker.__init__(self)
        self.href: str = href
        self.owner: 'pylo.AgentStore' = owner
        self.workload: Optional['pylo.Workload'] = workload

        self.software_version: Optional['pylo.SoftwareVersion'] = None
        self._last_heartbeat: Optional[datetime.datetime] = None

        self._status_security_policy_sync_state: Optional[str] = None
        self._status_security_policy_applied_at: Optional[str] = None
        self._status_rule_count: Optional[int] = None

        self.mode = None

        self.raw_json = None

    def _get_date_from_json(self, prop_name_in_json: str) -> Optional[datetime.datetime]:
        status_json = self.raw_json.get('status')
        if status_json is None:
            return None

        prop_value = status_json.get(prop_name_in_json)
        if prop_value is None:
            return None

        if '.' in prop_value:
            time_found = datetime.datetime.strptime(prop_value, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            time_found = datetime.datetime.strptime(prop_value, "%Y-%m-%dT%H:%M:%SZ")

        return time_found

    def load_from_json(self, data):
        self.raw_json = data

        status_json = data.get('status')
        if status_json is None:
            raise pylo.PyloEx("Cannot find VENAgent status in JSON from '{}'".format(self.href))

        version_string = status_json.get('agent_version')
        if version_string is None:
            raise pylo.PyloEx("Cannot find VENAgent version from '{}'".format(self.href))
        self.software_version = pylo.SoftwareVersion(version_string)
        if self.software_version.is_unknown:
            pylo.log.warn("Agent {} from Workload {}/{} has unknown software version: {}".format(
                self.href,
                self.workload.get_name(),
                self.workload.href,
                self.software_version.version_string))

        self._status_security_policy_sync_state = status_json.get('security_policy_sync_state')

        self._status_rule_count = status_json.get('firewall_rule_count')
        # if self._status_rule_count is None:
        #    raise pylo.PyloEx("Cannot find firewall_rule_count VENAgent '{}' rule count ".format(self.href), status_json)

        config_json = data.get('config')
        if config_json is None:
            raise pylo.PyloEx("Cannot find Agent's config in JSON", data)

        self.mode = config_json.get('mode')
        if self.mode is None:
            raise pylo.PyloEx("Cannot find Agent's mode in config JSON", config_json)

        if self.mode == 'illuminated':
            log_traffic = config_json.get('log_traffic')
            if log_traffic:
                self.mode = "test"
            else:
                self.mode = "build"

    @property
    def status(self) -> Literal['stopped','active','suspended','uninstalled']:
        return self.raw_json['status']['status']

    def get_last_heartbeat_date(self) -> Optional[datetime.datetime]:
        if self._last_heartbeat is None:
            self._last_heartbeat = self._get_date_from_json('last_heartbeat_on')
        return self._last_heartbeat

    def get_status_security_policy_applied_at(self):
        if self._status_security_policy_applied_at is None:
            self._status_security_policy_applied_at = self._get_date_from_json('security_policy_applied_at')
        return self._status_security_policy_applied_at

    def get_status_security_policy_sync_state(self):
        return self._status_security_policy_sync_state


class AgentStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.items_by_href: Dict[str, pylo.VENAgent] = {}

    @property
    def agents(self) -> List['pylo.VENAgent']:
        """
        Returns a copy of the list of all agents in the store
        :return:
        """
        return list(self.items_by_href.values())

    def find_by_href(self, href: str) -> VENAgent:
        return self.items_by_href.get(href)

    def create_ven_agent_from_workload_record(self, workload: 'pylo.Workload', json_data) -> 'pylo.VENAgent':
        # For developer use only. This is called by the WorkloadStore when it creates a new workload as the Agent records are read from there
        href = json_data.get('href')
        if href is None:
            raise pylo.PyloEx("Cannot extract Agent href from workload '{}'".format(workload.href))

        agent = pylo.VENAgent(href, self, workload)
        agent.load_from_json(json_data)

        self.items_by_href[href] = agent

        return agent

    def count_agents(self) -> int:
        return len(self.items_by_href)



