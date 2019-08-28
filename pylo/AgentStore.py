import pylo
from pylo import log
from .Helpers import *
import re
import datetime


version_regex = re.compile(r"^(?P<major>[0-9]+)\.(?P<middle>[0-9]+)\.(?P<minor>[0-9]+)-(?P<build>[0-9]+)(u[0-9]+)?$")


class VENAgent(pylo.ReferenceTracker):

    """
    :type software_version: pylo.SoftwareVersion
    :type last_heartbeat: datetime.datetime
    """

    def __init__(self, href: str, owner: 'pylo.AgentStore', workload: 'pylo.Workload' = None):
        pylo.ReferenceTracker.__init__(self)
        self.href = href
        self.owner = owner
        self.workload = workload

        self.software_version = None
        self.last_heartbeat = None

        self.raw_json = None

    def load_from_json(self, data):
        self.raw_json = data

        status_json = data.get('status')
        if status_json is None:
            raise pylo.PyloEx("Cannot find VENAgent status in JSON from '{}'".format(self.href))

        version_string = status_json.get('agent_version')
        if version_string is None:
            raise pylo.PyloEx("Cannot find VENAgent version from '{}'".format(self.href))

        last_heartbeat = status_json.get("last_heartbeat_on")
        if last_heartbeat is not None:
            # self.last_heartbeat = dparser.parse(last_heartbeat)
            # "2019-07-30T11:13:25.006Z"
            self.last_heartbeat = datetime.datetime.strptime(last_heartbeat, "%Y-%m-%dT%H:%M:%S.%fZ")



        self.software_version = pylo.SoftwareVersion(version_string)



class AgentStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}  # type: dict[str,pylo.VENAgent]

    def find_by_href_or_die(self, href: str):

        find_object = self.itemsByHRef.get(href)
        if find_object is None:
            raise pylo.PyloEx("Agent with ID {} was not found".format(href))

        return find_object

    def create_venagent_from_workload_record(self, workload: 'pylo.Workload', json_data):
        href = json_data.get('href')
        if href is None:
            raise pylo.PyloEx("Cannot extract Agent href from workload '{}'".format(workload.href))

        agent = pylo.VENAgent(href, self, workload)
        agent.load_from_json(json_data)

        self.itemsByHRef[href] = agent

        return agent


    def count_agents(self):
        """

        :rtype: int
        """
        return len(self.itemsByHRef)



