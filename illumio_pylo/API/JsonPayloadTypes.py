"""This module contains the JSON payload types for the PCE API."""

from typing import List, Optional, TypedDict, NotRequired, Union, Literal


class HrefReference(TypedDict):
    href: str

class HrefReferenceWithName(TypedDict):
    href: str
    name: str

class LabelHrefRef(TypedDict):
    label: HrefReference

class WorkloadHrefRef(TypedDict):
    workload: HrefReference

class IPListHrefRef(TypedDict):
    ip_list: HrefReference

class ServiceHrefRef(TypedDict):
    service: HrefReference

class VirtualServiceHrefRef(TypedDict):
    virtual_service: HrefReference

class LabelObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    deleted: bool
    href: str
    key: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]
    value: str


class LabelObjectCreationJsonStructure(TypedDict):
    value: str
    key: str


class LabelObjectUpdateJsonStructure(TypedDict):
    value: str


class LabelGroupObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    deleted: bool
    href: str
    key: str
    labels: List[HrefReference]
    name: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]


class LabelGroupObjectUpdateJsonStructure(TypedDict):
    labels: NotRequired[List[HrefReference]]
    name: NotRequired[str]

class IPListObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    description: str
    href: str
    ip_ranges: List[TypedDict('record', {'from_ip': str, 'to_ip': str, 'exclusion': bool})]
    name: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]

class IPListObjectCreationJsonStructure(TypedDict):
    description: str
    ip_ranges: List[TypedDict('record', {'from_ip': str, 'to_ip': str, 'exclusion': bool})]
    name: str


class WorkloadInterfaceObjectJsonStructure(TypedDict):
    name: str
    address: str

class WorkloadObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    description: Optional[str]
    hostname: Optional[str]
    href: str
    interfaces: List[WorkloadInterfaceObjectJsonStructure]
    labels: List[HrefReference]
    name: Optional[str]
    public_ip: Optional[str]
    managed: bool
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]

class WorkloadObjectCreateJsonStructure(TypedDict):
    """
    This is the structure of the JSON payload for creating a workload.
    """
    description:NotRequired[str]
    hostname: NotRequired[str]
    interfaces: NotRequired[List[WorkloadInterfaceObjectJsonStructure]]
    labels: NotRequired[List[HrefReference]]
    name: NotRequired[str]
    public_ip: NotRequired[Optional[str]]

class WorkloadObjectMultiCreateJsonStructure(WorkloadObjectCreateJsonStructure):
    href: str

WorkloadObjectMultiCreateJsonRequestPayload = List[WorkloadObjectMultiCreateJsonStructure]

class WorkloadBulkUpdateEntryJsonStructure(WorkloadObjectCreateJsonStructure):
    href: str

class WorkloadBulkUpdateResponseEntry(TypedDict):
    href: str
    status: Literal['updated', 'error', 'validation_failure']
    token: NotRequired[str]
    message: NotRequired[str]


class VenObjectWorkloadSummaryJsonStructure(TypedDict):
    href: str
    mode: str
    online: bool


class VenObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    description: Optional[str]
    hostname: Optional[str]
    href: str
    labels: List[HrefReference]
    name: Optional[str]
    interfaces: List[WorkloadInterfaceObjectJsonStructure]
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]
    last_heartbeat_at: Optional[str]
    last_goodbye_at: Optional[str]
    ven_type: Literal['server', 'endpoint', 'containerized-ven']
    active_pce_fqdn: Optional[str]
    target_pce_fqdn: Optional[str]
    workloads: List[VenObjectWorkloadSummaryJsonStructure]
    version: Optional[str]
    os_id: Optional[str]
    os_version: Optional[str]
    os_platform: Optional[str]
    uid: Optional[str]


class RuleServiceReferenceObjectJsonStructure(TypedDict):
    href: str
    name: str


class RuleDirectServiceReferenceObjectJsonStructure(TypedDict):
    port: int
    proto: int
    to_port: NotRequired[int]


class RuleObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    href: str
    ingress_services: List[RuleDirectServiceReferenceObjectJsonStructure|RuleServiceReferenceObjectJsonStructure]
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]


class RulesetScopeEntryLineJsonStructure(TypedDict):
    label: NotRequired[HrefReference]
    label_group: NotRequired[HrefReference]


class RulesetObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    description: str
    href: str
    name: str
    rules: List[RuleObjectJsonStructure]
    scopes: List[List[RulesetScopeEntryLineJsonStructure]]
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]


class RulesetObjectUpdateStructure(TypedDict):
    name: NotRequired[str]
    description: NotRequired[str]
    scopes: NotRequired[List[List[RulesetScopeEntryLineJsonStructure]]]


class ServiceObjectJsonStructure(TypedDict):
    created_at: str
    href: str
    name: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]


class VirtualServiceObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    href: str
    name: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]

class NetworkDeviceConfigObjectJsonStructure(TypedDict):
    device_type: Literal['switch']
    name: str

class NetworkDeviceObjectJsonStructure(TypedDict):
    href: str
    config: NetworkDeviceConfigObjectJsonStructure
    supported_endpoint_type: Literal['switch_port']

class NetworkDeviceEndpointConfigObjectJsonStructure(TypedDict):
    type: Literal['switch_port']
    name: str
    workload_discovery: bool

class NetworkDeviceEndpointObjectJsonStructure(TypedDict):
    href: str
    config: NetworkDeviceEndpointConfigObjectJsonStructure
    status: Literal['unmonitored', 'monitored']
    workloads: List[HrefReference]

class SecurityPrincipalObjectJsonStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    href: str
    name: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]

class LabelDimensionObjectStructure(TypedDict):
    created_at: str
    created_by: Optional[HrefReferenceWithName]
    display_name: str
    href: str
    key: str
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]


class PCEObjectsJsonStructure(TypedDict):
    iplists: List[IPListObjectJsonStructure]
    labelgroups: List[LabelGroupObjectJsonStructure]
    labels: List[LabelObjectJsonStructure]
    rulesets: List[RulesetObjectJsonStructure]
    security_principals: List[SecurityPrincipalObjectJsonStructure]
    services: List[ServiceObjectJsonStructure]
    virtual_services: List[VirtualServiceObjectJsonStructure]
    workloads: List[WorkloadObjectJsonStructure]
    label_dimensions: List[LabelDimensionObjectStructure]


class PCECacheFileJsonStructure(TypedDict):
    data: PCEObjectsJsonStructure
    pce_version: str
    generation_date: str


class RuleCoverageQueryEntryJsonStructure(TypedDict):
    source: Union[IPListHrefRef, WorkloadHrefRef]
    destination: Union[IPListHrefRef, WorkloadHrefRef]
    services: List


WorkloadsGetQueryLabelFilterJsonStructure = List[List[str]]

AuditLogApiEventType = Literal['agent.clone_detected', 'workloads.update', 'workload.update', 'workload_interfaces.update']

class AuditLogEntryJsonStructure(TypedDict):
    event_type: AuditLogApiEventType
    timestamp: str

class AuditLogApiRequestPayloadStructure(TypedDict):
    pass

class AuditLogApiReplyEventJsonStructure(TypedDict):
    pass

