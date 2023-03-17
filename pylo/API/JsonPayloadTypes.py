from typing import List, Optional, TypedDict, NotRequired


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
    labels: List[TypedDict('record', {'href': str})]


class LabelGroupObjectUpdateJsonStructure(TypedDict):
    name: NotRequired[str]
    labels: NotRequired[List[TypedDict('record', {'href': str})]]


class IPListObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str


class WorkloadObjectJsonStructure(TypedDict):
    href: str
    name: Optional[str]
    hostname: Optional[str]
    created_at: str
    updated_at: str


class RulesetObjectJsonStructure(TypedDict):
    href: str
    name: str
    created_at: str
    updated_at: str


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

