"""This module contains the JSON payload types for the PCE API."""

from typing import List, Optional, TypedDict, NotRequired, Union


class HrefReference(TypedDict):
    href: str

class WorkloadHrefRef(TypedDict):
    workload: HrefReference

class IPListHrefRef(TypedDict):
    ip_list: HrefReference

class LabelObjectJsonStructure(TypedDict):
    value: str
    href: str
    key: str
    deleted: bool
    created_at: str
    updated_at: str


class LabelObjectCreationJsonStructure(TypedDict):
    value: str
    key: str


class LabelObjectUpdateJsonStructure(TypedDict):
    value: str


class LabelGroupObjectJsonStructure(TypedDict):
    name: str
    href: str
    key: str
    deleted: bool
    created_at: str
    updated_at: str
    labels: List[HrefReference]


class LabelGroupObjectUpdateJsonStructure(TypedDict):
    name: NotRequired[str]
    labels: NotRequired[List[HrefReference]]


class IPListObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str
    description: str
    ip_ranges: List[TypedDict('record', {'from_ip': str, 'to_ip': str, 'exclusion': bool})]


class IPListObjectCreationJsonStructure(TypedDict):
    name: str
    description: str
    ip_ranges: List[TypedDict('record', {'from_ip': str, 'to_ip': str, 'exclusion': bool})]


class WorkloadObjectJsonStructure(TypedDict):
    href: str
    name: Optional[str]
    hostname: Optional[str]
    created_at: str
    updated_at: str


class RuleServiceReferenceObjectJsonStructure(TypedDict):
    href: str
    name: str


class RuleDirectServiceReferenceObjectJsonStructure(TypedDict):
    port: int
    proto: int
    to_port: NotRequired[int]


class RuleObjectJsonStructure(TypedDict):
    href: str
    created_at: str
    updated_at: str
    ingress_services: List[RuleServiceReferenceObjectJsonStructure|RuleDirectServiceReferenceObjectJsonStructure]


class RulesetScopeEntryLineJsonStructure(TypedDict):
    label: NotRequired[HrefReference]
    label_group: NotRequired[HrefReference]


class RulesetObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str
    description: str
    rules: List[RuleObjectJsonStructure]
    scopes: List[List[RulesetScopeEntryLineJsonStructure]]


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
    labels: List[LabelObjectJsonStructure]
    labelgroups: List[LabelGroupObjectJsonStructure]
    iplists: List[IPListObjectJsonStructure]
    workloads: List[WorkloadObjectJsonStructure]
    rulesets: List[RulesetObjectJsonStructure]
    services: List[ServiceObjectJsonStructure]
    virtual_services: List[VirtualServiceObjectJsonStructure]
    security_principals: List[SecurityPrincipalObjectJsonStructure]


class PCECacheFileJsonStructure(TypedDict):
    data: PCEObjectsJsonStructure
    pce_version: str
    generation_date: str


class RuleCoverageQueryEntryJsonStructure(TypedDict):
    source: Union[IPListHrefRef, WorkloadHrefRef]
    destination: Union[IPListHrefRef, WorkloadHrefRef]
    services: List

