import os
import sys

parent_dir = os.path.abspath(os.path.dirname(__file__))
vendor_dir = os.path.join(parent_dir, 'vendors')
sys.path.append(vendor_dir)

from .tmp import *

from .Exception import PyloEx
from .ReferenceTracker import ReferenceTracker, Referencer, Pathable
from .APIConnector import APIConnector
from .LabelCommon import LabelCommon
from .Label import Label
from .LabelGroup import LabelGroup
from .LabelStore import LabelStore
from .IPList import IPList, IPListStore
from .WorkloadStore import WorkloadStore, Workload
from .Service import Service, ServiceStore
from .Ruleset import Ruleset, RulesetStore, Rule, RuleServiceContainer, RuleHostContainer, \
    RuleSecurityPrincipalContainer, RulesetScope, RulesetScopeEntry
from .SecurityPrincipal import SecurityPrincipal, SecurityPrincipalStore
from .Organization import Organization
from .Query import Query


from .Helpers import *

ignoreWorkloadsWithSameName = True

objectNotFound = object()









