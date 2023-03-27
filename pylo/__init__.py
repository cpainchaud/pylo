import os
import sys


parent_dir = os.path.abspath(os.path.dirname(__file__))

from .tmp import *
from .Helpers import *

from .Exception import PyloEx, PyloApiEx, PyloApiTooManyRequestsEx, PyloApiUnexpectedSyntax, PyloObjectNotFound
from .SoftwareVersion import SoftwareVersion
from .IPMap import IP4Map
from .ReferenceTracker import ReferenceTracker, Referencer, Pathable
from .API.APIConnector import APIConnector, ObjectTypes
from .API.RuleSearchQuery import RuleSearchQuery, RuleSearchQueryResolvedResultSet
from .API.ClusterHealth import ClusterHealth
from .API.Explorer import ExplorerResultSetV1, RuleCoverageQueryManager
from API.CredentialsFileType import CredentialsFileType
from .LabelCommon import LabelCommon
from .Label import Label
from .LabelGroup import LabelGroup
from .LabelStore import LabelStore, label_type_app, label_type_env, label_type_loc, label_type_role
from .IPList import IPList, IPListStore
from .AgentStore import AgentStore, VENAgent
from .Workload import Workload, WorkloadInterface
from .WorkloadStore import WorkloadStore
from .VirtualService import VirtualService
from .VirtualServiceStore import VirtualServiceStore
from .Service import Service, ServiceStore, PortMap, ServiceEntry
from .Rule import Rule, RuleServiceContainer, RuleSecurityPrincipalContainer, DirectServiceInRule, RuleHostContainer, RuleActorsAcceptableTypes
from .Ruleset import Ruleset, RulesetScope, RulesetScopeEntry
from .RulesetStore import RulesetStore
from .SecurityPrincipal import SecurityPrincipal, SecurityPrincipalStore
from .Organization import Organization
from .Query import Query


def load_organization(hostname: str, port: int,  api_user: str, api_key: str,
                      organization_id: int, verify_ssl: bool = True,
                      list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None,
                      include_deleted_workloads: bool = False) -> Organization:
    """
    Load an organization from the API with parameters provided as arguments.
    """
    api = APIConnector(hostname=hostname, port=port, apiuser=api_user, apikey=api_key, org_id=organization_id,
                       skip_ssl_cert_check=not verify_ssl)
    org = Organization(1)
    org.load_from_api(api, include_deleted_workloads=include_deleted_workloads,
                      list_of_objects_to_load=list_of_objects_to_load)

    return org


def load_organization_using_credential_file(hostname_or_profile_name: str = None,
                                            credential_file: str = None,
                                            list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None,
                                            include_deleted_workloads: bool = False) -> Organization:
    """
    Credentials files will be looked for in the following order:
    1. The path provided in the credential_file argument
    2. The path provided in the PYLO_CREDENTIAL_FILE environment variable
    3. The path ~/.pylo/credentials.json
    4. Current working directory credentials.json
    :param hostname_or_profile_name:
    :param credential_file:
    :param list_of_objects_to_load:
    :param include_deleted_workloads:
    :return:
    """

    def check_profile_json_structure(profile: Dict) -> None:
        if "name" not in profile:
            raise PyloEx("The profile {} does not contain a name".format(profile))
        if "hostname" not in profile:
            raise PyloEx("The profile {} does not contain a hostname".format(profile))
        if "port" not in profile:
            raise PyloEx("The profile {} does not contain a port".format(profile))
        if "api_user" not in profile:
            raise PyloEx("The profile {} does not contain an api_user".format(profile))
        if "api_key" not in profile:
            raise PyloEx("The profile {} does not contain an api_key".format(profile))
        if "organization_id" not in profile:
            raise PyloEx("The profile {} does not contain an organization_id".format(profile))
        if "verify_ssl" not in profile:
            raise PyloEx("The profile {} does not contain a verify_ssl".format(profile))

    if hostname_or_profile_name is None:
        log.debug("No hostname_or_profile_name provided, profile_name=default will be used")
        hostname_or_profile_name = "default"

    if credential_file is None:
        log.debug("No credential_file provided, looking for one in the environment variable PYLO_CREDENTIAL_FILE")
        credential_file = os.environ.get('PYLO_CREDENTIAL_FILE', None)
        if credential_file is None:
            log.debug("No credential_file provided, looking for one in the default path ~/.pylo/credentials.json")
            credential_file = os.path.expanduser("~/.pylo/credentials.json")
            if not os.path.exists(credential_file):
                log.debug("No credential_file provided, looking for one in the current working directory credentials.json")
                credential_file = os.path.join(os.getcwd(), "credentials.json")
                if not os.path.exists(credential_file):
                    raise PyloEx("No credential file found. Please provide a path to a credential file or create one in the default location ~/.pylo/credentials.json")

        log.debug("Loading credentials from file: {}".format(credential_file))
        with open(credential_file, 'r') as f:
            credentials: CredentialsFileType = json.load(f)
            found_profile = None
            # if it is a list, we need to find the right one
            if isinstance(credentials, list):
                for profile in credentials:
                    check_profile_json_structure(profile)
                    if profile['name'].lower() == hostname_or_profile_name.lower():
                        found_profile = profile
                        break
                    if profile['hostname'].lower() == hostname_or_profile_name.lower():
                        found_profile = profile
                        break
                if found_profile is None:
                    raise PyloEx("No profile found in credential file '{}' with hostname: {}".
                                 format(credential_file, hostname_or_profile_name))

            else:
                log.debug("Credentials file is not a list, assuming it is a single profile")
                check_profile_json_structure(credentials)
                found_profile = credentials

            api = APIConnector(hostname=found_profile['hostname'], port=found_profile['port'],
                               apiuser=found_profile['api_user'], apikey=found_profile['api_key'],
                               org_id=found_profile['org_id'],
                               skip_ssl_cert_check=not found_profile['verify_ssl'])
            org = Organization(1)
            org.load_from_api(api, include_deleted_workloads=include_deleted_workloads,
                              list_of_objects_to_load=list_of_objects_to_load)
            return org


ignoreWorkloadsWithSameName = True

objectNotFound = object()









