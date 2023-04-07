"""This module contains the JSON payload types for the PCE API."""

from typing import List, Optional, TypedDict, NotRequired, Union


class HrefReference(TypedDict):
    href: str

class HrefReferenceWithName(TypedDict):
    href: str
    name: str

class WorkloadHrefRef(TypedDict):
    workload: HrefReference

class IPListHrefRef(TypedDict):
    ip_list: HrefReference

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
    updated_at: str
    updated_by: Optional[HrefReferenceWithName]

class WorkloadObjectCreateJsonStructure(TypedDict):
    description: Optional[str]
    hostname: Optional[str]
    interfaces: NotRequired[List[WorkloadInterfaceObjectJsonStructure]]
    labels: NotRequired[List[HrefReference]]
    name: Optional[str]
    public_ip: NotRequired[Optional[str]]

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
    href: str
    name: str
    created_at: str
    updated_at: str


class VirtualServiceObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str


class SecurityPrincipalObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str


class PCEObjectsJsonStructure(TypedDict):
    iplists: List[IPListObjectJsonStructure]
    labelgroups: List[LabelGroupObjectJsonStructure]
    labels: List[LabelObjectJsonStructure]
    rulesets: List[RulesetObjectJsonStructure]
    security_principals: List[SecurityPrincipalObjectJsonStructure]
    services: List[ServiceObjectJsonStructure]
    virtual_services: List[VirtualServiceObjectJsonStructure]
    workloads: List[WorkloadObjectJsonStructure]


class PCECacheFileJsonStructure(TypedDict):
    data: PCEObjectsJsonStructure
    pce_version: str
    generation_date: str


class RuleCoverageQueryEntryJsonStructure(TypedDict):
    source: Union[IPListHrefRef, WorkloadHrefRef]
    destination: Union[IPListHrefRef, WorkloadHrefRef]
    services: List

