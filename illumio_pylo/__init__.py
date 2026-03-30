from typing import Callable

__version__ = "0.4.0"

from .tmp import *
from .Helpers import *

_SYSTEM_CA_DISABLE_ENV = 'PYLO_DISABLE_SYSTEM_CA'
_trusty_env_values = {'1', 'true', 'yes', 'on'}


def _env_flag_is_true(env_flag: str) -> bool:
    value = os.getenv(env_flag)
    return value is not None and value.strip().lower() in _trusty_env_values


def _load_system_ca_store() -> None:
    if _env_flag_is_true(_SYSTEM_CA_DISABLE_ENV):
        log.debug('%s is set to a truthy value; skipping pip-system-certs injection', _SYSTEM_CA_DISABLE_ENV)
        return
    try:
        import pip_system_certs
    except ImportError:
        log.debug('pip-system-certs not installed; continuing without system CA injection')
        return

    inject = getattr(pip_system_certs, 'inject_into_requests', None)
    if callable(inject):
        inject()
        log.debug('Injected OS CA certificates into requests via pip-system-certs')
        return

    log.debug('pip-system-certs is installed but does not expose inject_into_requests; no injection performed')


_load_system_ca_store()

from .Exception import PyloEx, PyloApiEx, PyloApiTooManyRequestsEx, PyloApiUnexpectedSyntax, PyloObjectNotFound, PyloApiRequestForbiddenEx
from .SoftwareVersion import SoftwareVersion
from .IPMap import IP4Map
from .ReferenceTracker import ReferenceTracker, Referencer, Pathable
from .API.APIConnector import APIConnector, ObjectTypes
from .API.RuleSearchQuery import RuleSearchQuery, RuleSearchQueryResolvedResultSet
from .API.ClusterHealth import ClusterHealth
from .API.Explorer import (ExplorerResultSetV1, ExplorerResultSetV2, RuleCoverageQueryManager, ExplorerFilterSetV1,
                           ExplorerFilterSetV2, ExplorerQuery, ExplorerQueryV2, ExplorerResultV2)
from .API.AuditLog import AuditLogQuery, AuditLogQueryResultSet, AuditLogFilterSet
from .API.CredentialsManager import get_credentials_from_file
from .LabelDimension import LabelDimension, DIMENSION_KEY_ROLE, DIMENSION_KEY_APP, DIMENSION_KEY_ENV, DIMENSION_KEY_LOC
from .LabelCommon import LabelCommon
from .Label import Label
from .LabelGroup import LabelGroup
from .LabelStore import LabelStore, label_type_app, label_type_env, label_type_loc, label_type_role
from .IPList import IPList, IPListStore
from .AgentStore import AgentStore, VENAgent
from .Workload import Workload, WorkloadInterface, WorkloadApiUpdateStackExecutionManager
from .WorkloadStore import WorkloadStore
from .VirtualService import VirtualService
from .VirtualServiceStore import VirtualServiceStore
from .Service import Service, ServiceStore, PortMap, ServiceEntry
from .Rule import Rule, RuleServiceContainer, RuleSecurityPrincipalContainer, DirectServiceInRule, RuleHostContainer, RuleActorsAcceptableTypes
from .Ruleset import Ruleset, RulesetScope, RulesetScopeEntry
from .RulesetStore import RulesetStore
from .SecurityPrincipal import SecurityPrincipal, SecurityPrincipalStore
from .Organization import Organization
from .FilterQuery import (
    FilterQuery, FilterRegistry, FilterField, ValueType,
    WorkloadFilterRegistry, get_workload_filter_registry,
    QueryLexer, QueryParser, QueryNode, AndNode, OrNode, NotNode, ConditionNode
)


def get_organization(fqdn: str, port: int, api_user: str, api_key: str,
                     organization_id: int, verify_ssl: bool = True,
                     list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None,
                     include_deleted_workloads: bool = False) -> Organization:
    """
    Load an organization from the API with parameters provided as arguments.
    """
    api = APIConnector(fqdn=fqdn, port=port, api_user=api_user, api_key=api_key, org_id=organization_id,
                       skip_ssl_cert_check=not verify_ssl)
    org = Organization(1)
    org.load_from_api(api, include_deleted_workloads=include_deleted_workloads,
                      list_of_objects_to_load=list_of_objects_to_load)

    return org


def get_organization_using_credential_file(fqdn_or_profile_name: str = None,
                                           credential_file: str = None,
                                           list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None,
                                           include_deleted_workloads: bool = False,
                                           callback_api_objects_downloaded: Callable = None) -> Organization:
    """
    Credentials files will be looked for in the following order:
    1. The path provided in the credential_file argument
    2. The path provided in the PYLO_CREDENTIAL_FILE environment variable
    3. The path ~/.pylo/credentials.json
    4. Current working directory credentials.json
    :param fqdn_or_profile_name:
    :param credential_file:
    :param list_of_objects_to_load:
    :param include_deleted_workloads:
    :param callback_api_objects_downloaded: callback function that will be called after each API has finished downloading all objects
    :return:
    """
    return Organization.get_from_api_using_credential_file(fqdn_or_profile_name=fqdn_or_profile_name,
                                                           credential_file=credential_file,
                                                           list_of_objects_to_load=list_of_objects_to_load,
                                                           include_deleted_workloads=include_deleted_workloads,
                                                           callback_api_objects_downloaded=callback_api_objects_downloaded)


ignoreWorkloadsWithSameName = True

objectNotFound = object()
