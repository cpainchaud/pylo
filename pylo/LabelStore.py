

import pylo
from pylo import log
from .Helpers import *

label_type_loc = 1
label_type_env = 2
label_type_app = 4
label_type_role = 3

class LabelStore:

    """
    :type owner: pylo.Organization
    :type itemsByHRef: dict[str,pylo.Label|pylo.LabelGroup]
    :type locationLabels: dict[str,pylo.Label|pylo.LabelGroup]
    :type roleLabels: dict[str,pylo.Label|pylo.LabelGroup]
    :type environmentLabels: dict[str,pylo.Label|pylo.LabelGroup]
    :type applicationLabels: dict[str,pylo.Label|pylo.LabelGroup]
    :type label_resolution_cache: dict[str,[pylo.Label|pylo.LabelGroup]]
    """

    def __init__(self, owner: 'pylo.Organization'):
        self.owner = owner
        self.itemsByHRef = {}

        self.locationLabels = {}
        self.environmentLabels = {}
        self.roleLabels = {}
        self.applicationLabels = {}

        self.label_resolution_cache = None

    def loadLabelsFromJson(self, json_list):
        for json_label in json_list:
            if 'value' not in json_label or 'href' not in json_label or 'key' not in json_label:
                raise Exception("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
            new_label_name = json_label['value']
            new_label_href = json_label['href']
            new_label_type = json_label['key']

            new_label = pylo.Label(new_label_name, new_label_href, new_label_type, self)

            if new_label_href in self.itemsByHRef:
                raise Exception("A Label with href '%s' already exists in the table", new_label_href)

            self.itemsByHRef[new_label_href] = new_label

            if new_label.type_is_location():
                self.locationLabels[new_label_name] = new_label
            elif new_label.type_is_environment():
                self.environmentLabels[new_label_name] = new_label
            elif new_label.type_is_application():
                self.applicationLabels[new_label_name] = new_label
            elif new_label.type_is_role():
                self.roleLabels[new_label_name] = new_label
            
            log.debug("Found Label '%s' with href '%s' and type '%s'", new_label_name, new_label_href, new_label_type)

    def loadLabelGroupsFromJson(self, json_list):

        created_groups = []

        for json_label in json_list:
            if 'name' not in json_label or 'href' not in json_label or 'key' not in json_label:
                raise Exception("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
            new_label_name = json_label['name']
            newLabelHref = json_label['href']
            newLabelType = json_label['key']

            new_label = pylo.LabelGroup(new_label_name, newLabelHref, newLabelType, self)
            created_groups.append(new_label)

            if newLabelHref in self.itemsByHRef:
                raise Exception("A Label with href '%s' already exists in the table", newLabelHref)

            self.itemsByHRef[newLabelHref] = new_label

            if newLabelType == 1:
                self.locationLabels[new_label_name] = new_label
            elif newLabelType == 2:
                self.environmentLabels[new_label_name] = new_label
            elif newLabelType == 4:
                self.applicationLabels[new_label_name] = new_label
            elif newLabelType == 3:
                self.roleLabels[new_label_name] = new_label

            new_label.raw_json = json_label

            log.info("Found LabelGroup '%s' with href '%s' and type '%s'", new_label_name, newLabelHref, newLabelType)

        for group in created_groups:
            group.load_from_json()

    def count_labels(self):
        return len(self.itemsByHRef)

    def count_location_labels(self):
        return len(self.locationLabels)

    def count_environment_labels(self):
        return len(self.environmentLabels)

    def count_application_labels(self):
        return len(self.applicationLabels)

    def count_role_labels(self):
        return len(self.roleLabels)

    def get_location_labels_as_list(self):
        return self.locationLabels.values()

    def find_label_by_name_and_type(self, name: str, type: int):
        if type == 1:
            return self.locationLabels.get(name)
        if type == 2:
            return self.environmentLabels.get(name)
        if type == 4:
            return self.applicationLabels.get(name)
        if type == 3:
            return self.roleLabels.get(name)
        raise Exception("Unsupported")

    cache_label_all_string = '-All-'
    cache_label_all_separator = '|'

    def generate_label_resolution_cache(self):
        self.label_resolution_cache = {}

        roles = list(self.roleLabels.keys())
        roles.append(self.cache_label_all_string)
        for role in roles:
            apps = list(self.applicationLabels.keys())
            apps.append(self.cache_label_all_string)
            for app in apps:
                envs = list(self.environmentLabels.keys())
                envs.append(self.cache_label_all_string)
                for env in envs:
                    locs = list(self.locationLabels.keys())
                    locs.append(self.cache_label_all_string)
                    for loc in locs:
                        group_name = role + LabelStore.cache_label_all_separator + app + LabelStore.cache_label_all_separator + env + LabelStore.cache_label_all_separator + loc
                        self.label_resolution_cache[group_name] = []

        all_string_and_sep = LabelStore.cache_label_all_string + LabelStore.cache_label_all_separator

        masks = [[False, False, False, False],
                 [True, False, False, False],
                 [False, True, False, False],
                 [True, True, False, False],
                 [False, False, True, False],
                 [True, False, True, False],
                 [True, True, True, False],
                 [False, False, False, True],
                 [True, False, False, True],
                 [False, True, False, True],
                 [True, True, False, True],
                 [False, False, True, True],
                 [True, False, True, True],
                 [True, True, True, True]]

        """masks = [
                 [True, True, True, True]]"""

        for workload in self.owner.WorkloadStore.itemsByHRef.values():
            if workload.deleted:
                continue

            already_processed = {}

            for mask in masks:
                if workload.roleLabel is not None and mask[0]:
                    group_name = workload.roleLabel.name + LabelStore.cache_label_all_separator
                else:
                    group_name = all_string_and_sep
                if workload.applicationLabel is not None and mask[1]:
                    group_name += workload.applicationLabel.name + LabelStore.cache_label_all_separator
                else:
                    group_name += all_string_and_sep
                if workload.environmentLabel is not None and mask[2]:
                    group_name += workload.environmentLabel.name + LabelStore.cache_label_all_separator
                else:
                    group_name += all_string_and_sep
                if workload.locationLabel is not None and mask[3]:
                    group_name += workload.locationLabel.name
                else:
                    group_name += LabelStore.cache_label_all_string

                if group_name not in already_processed:
                    self.label_resolution_cache[group_name].append(workload)
                already_processed[group_name] = True

    def get_workloads_by_label_scope(self, role: 'pylo.Label', app: 'pylo.Label', env: 'pylo.Label', loc: 'pylo.Label'):
        if self.label_resolution_cache is None:
            self.generate_label_resolution_cache()

        if role is None:
            role = LabelStore.cache_label_all_string
        else:
            role = role.name

        if app is None:
            app = LabelStore.cache_label_all_string
        else:
            app = app.name

        if env is None:
            env = LabelStore.cache_label_all_string
        else:
            env = env.name

        if loc is None:
            loc = LabelStore.cache_label_all_string
        else:
            loc = loc.name

        group_name = role + LabelStore.cache_label_all_separator + app + LabelStore.cache_label_all_separator + env + LabelStore.cache_label_all_separator + loc

        return self.label_resolution_cache[group_name]





    def find_label_by_name_lowercase_and_type(self, name: str, type: int):
        """

        :rtype: None|pylo.LabelCommon
        """
        ref = None
        name = name.lower()

        if type == label_type_loc:
            ref = self.locationLabels
        elif type == label_type_env:
            ref = self.environmentLabels
        elif type == label_type_app:
            ref = self.applicationLabels
        elif type == label_type_role:
            ref = self.roleLabels

        for labelName in ref.keys():
            if name == labelName.lower():
                return ref[labelName]

        return None

    def find_label_multi_by_name_lowercase_and_type(self, name: str, type: int):
        """

        :rtype: list[pylo.LabelCommon]
        """
        ref = None
        name = name.lower()
        result = []

        if type == label_type_loc:
            ref = self.locationLabels
        elif type == label_type_env:
            ref = self.environmentLabels
        elif type == label_type_app:
            ref = self.applicationLabels
        elif type == label_type_role:
            ref = self.roleLabels

        for labelName in ref.keys():
            if name == labelName.lower():
                result.append(ref[labelName])

        return result

    def find_by_href_or_die(self, href: str):

        obj = self.itemsByHRef.get(href)

        if obj is None:
            raise Exception("Workload with HREF '%s' was not found" % href)

        return obj
