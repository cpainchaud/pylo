import os
import sys

parent_dir = os.path.abspath(os.path.dirname(__file__))
vendor_dir = os.path.join(parent_dir, 'vendors')
sys.path.append(vendor_dir)

from .tmp import *
from .Helpers import *

from .Exception import PyloEx, PyloApiEx, PyloApiTooManyRequestsEx, PyloApiUnexpectedSyntax
from .SoftwareVersion import SoftwareVersion
from .IPMap import IP4Map
from .ReferenceTracker import ReferenceTracker, Referencer, Pathable
from .APIConnector import APIConnector
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
from .Rule import Rule, RuleServiceContainer, RuleSecurityPrincipalContainer, DirectServiceInRule, RuleHostContainer
from .Ruleset import Ruleset, RulesetScope, RulesetScopeEntry
from .RulesetStore import RulesetStore
from .SecurityPrincipal import SecurityPrincipal, SecurityPrincipalStore
from .Organization import Organization
from .Query import Query


ignoreWorkloadsWithSameName = True

objectNotFound = object()









