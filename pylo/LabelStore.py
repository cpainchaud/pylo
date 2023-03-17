from typing import Optional, Union, Dict
import pylo
from pylo import log
from .API.JsonPayloadTypes import LabelObjectJsonStructure
from .Helpers import *
import random
from hashlib import md5

label_type_loc = 'loc'
label_type_env = 'env'
label_type_app = 'app'
label_type_role = 'role'


class LabelStore:

    def __init__(self, owner: 'pylo.Organization'):
        self.owner: "pylo.Organization" = owner
        self.itemsByHRef: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        #self.locationLabels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        #self.environmentLabels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        #self.roleLabels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        #self.applicationLabels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}

        self.label_types = [label_type_role, label_type_loc, label_type_env, label_type_app]

        self.label_resolution_cache: Optional[Dict[str, Union[pylo.Label, pylo.LabelGroup]]] = None

    @property
    def roleLabels(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """Returns a dict of all role labels, including groups. Href is the key.
        This function is there only for retro-compatibility @deprecated"""
        results = {}
        for label in self.itemsByHRef.values():
            if label.type_is_role():
                results[label.href] = label

        return results

    @property
    def applicationLabels(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """Returns a dict of all application labels, including groups. Href is the key.
        This function is there only for retro-compatibility @deprecated"""
        results = {}
        for label in self.itemsByHRef.values():
            if label.type_is_application():
                results[label.href] = label

        return results

    @property
    def environmentLabels(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """Returns a dict of all environment labels, including groups. Href is the key.
        This function is there only for retro-compatibility @deprecated"""
        results = {}
        for label in self.itemsByHRef.values():
            if label.type_is_environment():
                results[label.href] = label

        return results

    @property
    def locationLabels(self) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
        """Returns a dict of all location labels, including groups. Href is the key.
        This function is there only for retro-compatibility @deprecated"""
        results = {}
        for label in self.itemsByHRef.values():
            if label.type_is_location():
                results[label.href] = label

        return results

    def load_labels_from_json(self, json_list: List[LabelObjectJsonStructure]):
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
            
            log.debug("Found Label '%s' with href '%s' and type '%s'", new_label_name, new_label_href, new_label_type)

    def load_label_groups_from_json(self, json_list):
        created_groups = []

        for json_label in json_list:
            if 'name' not in json_label or 'href' not in json_label or 'key' not in json_label:
                raise Exception("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
            new_label_name = json_label['name']
            new_label_href = json_label['href']
            new_label_type = json_label['key']

            new_label = pylo.LabelGroup(new_label_name, new_label_href, new_label_type, self)
            created_groups.append(new_label)

            if new_label_href in self.itemsByHRef:
                raise Exception("A Label with href '%s' already exists in the table", new_label_href)

            self.itemsByHRef[new_label_href] = new_label

            new_label.raw_json = json_label

            log.info("Found LabelGroup '%s' with href '%s' and type '%s'", new_label_name, new_label_href, new_label_type)

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

    def get_labels_no_groups(self) -> Dict[str, 'pylo.Label']:
        data = {}
        for label in self.itemsByHRef.values():
            if label.is_label():
                data[label.href] = label
        return data

    def get_label_groups(self) -> Dict[str, 'pylo.LabelGroup']:
        data = {}

        for label in self.itemsByHRef.values():
            if label.is_group():
                data[label.href] = label
        return data

    def find_label_by_name_whatever_type(self, name: str) -> Optional[Union['pylo.Label', 'pylo.LabelGroup']]:

        for label in self.itemsByHRef.values():
            if label.name == name:
                return label

        return None

    def find_label_by_name_and_type(self, name: str, type: str):
        if type not in self.label_types:
            raise Exception("Unsupported label type '%s'", type)

        label = self.find_label_by_name_whatever_type(name)
        if label is None:
            return None

        if label.type == type:
            return label

        return None

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
                if workload.role_label is not None and mask[0]:
                    group_name = workload.role_label.name + LabelStore.cache_label_all_separator
                else:
                    group_name = all_string_and_sep
                if workload.app_label is not None and mask[1]:
                    group_name += workload.app_label.name + LabelStore.cache_label_all_separator
                else:
                    group_name += all_string_and_sep
                if workload.env_label is not None and mask[2]:
                    group_name += workload.env_label.name + LabelStore.cache_label_all_separator
                else:
                    group_name += all_string_and_sep
                if workload.loc_label is not None and mask[3]:
                    group_name += workload.loc_label.name
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

    def create_label(self, name: str, label_type: str):

        new_label_name = name
        new_label_type = label_type
        new_label_href = '**fake-label-href**/{}'.format( md5(str(random.random()).encode('utf8')).digest() )

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

        return new_label

    def api_create_label(self, name: str, type: str):

        connector = pylo.find_connector_or_die(self.owner)
        json_label = connector.objects_label_create(name, type)

        if 'value' not in json_label or 'href' not in json_label or 'key' not in json_label:
            raise pylo.PyloEx("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
        new_label_name = json_label['value']
        new_label_href = json_label['href']
        new_label_type = json_label['key']

        new_label = pylo.Label(new_label_name, new_label_href, new_label_type, self)

        if new_label_href in self.itemsByHRef:
            raise Exception("A Label with href '%s' already exists in the table", new_label_href)

        self.itemsByHRef[new_label_href] = new_label

        return new_label

    def find_label_by_name_lowercase_and_type(self, name: str, type: str) -> Union['pylo.Label', 'pylo.LabelGroup', None]:
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
        else:
            raise pylo.PyloEx("Unsupported type '{}'".format(type))

        for labelName in ref.keys():
            if name == labelName.lower():
                return ref[labelName]

        return None

    def find_label_multi_by_name_lowercase_and_type(self, name: str, type: str) -> Union['pylo.Label', 'pylo.LabelGroup', None]:
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

    def find_by_href(self, href: str):
        return self.itemsByHRef.get(href)

    def find_by_href_or_die(self, href: str):

        obj = self.itemsByHRef.get(href)

        if obj is None:
            raise pylo.PyloObjectNotFound("Label with HREF '%s' was not found" % href)

        return obj
