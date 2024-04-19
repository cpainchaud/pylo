from hashlib import md5
import random
from typing import Union, Set, Iterable
# Pylo imports
from illumio_pylo import log
from .API.JsonPayloadTypes import LabelObjectJsonStructure, LabelGroupObjectJsonStructure, LabelDimensionObjectStructure
from .Helpers import *

label_type_loc = 'loc'
label_type_env = 'env'
label_type_app = 'app'
label_type_role = 'role'


class LabelStore:

    class Utils:
        """
        Container meant to provide reusable utility functions for Label objects and their Store
        """

        @staticmethod
        def list_to_dict_by_href(label_list: List[Union['pylo.Label', 'pylo.LabelGroup']]) -> Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]:
            """Converts a list of labels into a dict, where the href is the key"""
            result: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']] = {}
            for label in label_list:
                result[label.href] = label
            return result

        @staticmethod
        def list_to_dict_by_type(label_list: Iterable[Union['pylo.Label', 'pylo.LabelGroup']]) -> Dict[str, List[Union['pylo.Label', 'pylo.LabelGroup']]]:
            """Converts a list of labels into a dict, where the type is the key"""
            result: Dict[str, List[Union['pylo.Label', 'pylo.LabelGroup']]] = {}
            for label in label_list:
                if label.type not in result:
                    result[label.type] = []
                result[label.type].append(label)
            return result

        @staticmethod
        def list_sort_by_type(label_list: Iterable[Union['pylo.Label', 'pylo.LabelGroup']], type_order: List[str]) -> List[Union['pylo.Label', 'pylo.LabelGroup']]:
            """Sorts a list of labels by type, using the provided type order"""
            result: List[Union['pylo.Label', 'pylo.LabelGroup']] = []
            for label_type in type_order:
                for label in label_list:
                    if label.type == label_type:
                        result.append(label)
            return result

    __slots__ = ['owner', '_items_by_href', 'label_types', 'label_types_as_set', 'label_resolution_cache']

    def __init__(self, owner: 'pylo.Organization'):
        self.owner: "pylo.Organization" = owner
        self._items_by_href: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        self.label_types: List[str] = []
        self.label_types_as_set: Set[str] = set()

        self.label_resolution_cache: Optional[Dict[str, Union[pylo.Label, pylo.LabelGroup]]] = None
        
    def _add_dimension(self, dimension: str):
        if dimension not in self.label_types_as_set:
            self.label_types_as_set.add(dimension)
            self.label_types.append(dimension)

    def load_label_dimensions(self, json_list: Optional[List[LabelDimensionObjectStructure]]):
        if json_list is None or len(json_list) == 0:
            # add the default built-in label types
            self._add_dimension(label_type_role)
            self._add_dimension(label_type_app)
            self._add_dimension(label_type_env)
            self._add_dimension(label_type_loc)
            return

        for dimension in json_list:
            self._add_dimension(dimension['key'])

    def load_labels_from_json(self, json_list: List[LabelObjectJsonStructure]):
        for json_label in json_list:
            if 'value' not in json_label or 'href' not in json_label or 'key' not in json_label:
                raise Exception("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
            new_label_name = json_label['value']
            new_label_href = json_label['href']
            new_label_type = json_label['key']

            if new_label_type not in self.label_types_as_set:
                raise pylo.PyloApiEx("Label type '%s' is not a valid type for Label '%s' (href: %s)" % (new_label_type, new_label_name, new_label_href))

            new_label = pylo.Label(new_label_name, new_label_href, new_label_type, self)

            if new_label_href in self._items_by_href:
                raise Exception("A Label with href '%s' already exists in the table", new_label_href)

            self._items_by_href[new_label_href] = new_label
            
            log.debug("Found Label '%s' with href '%s' and type '%s'", new_label_name, new_label_href, new_label_type)

    def load_label_groups_from_json(self, json_list: List[LabelGroupObjectJsonStructure]):
        # groups cannot be loaded straight away : we need to extract of their principal properties (name, href and type)
        #then we can extract their members in case there are nested groups
        created_groups = []
        for json_label in json_list:
            if 'name' not in json_label or 'href' not in json_label or 'key' not in json_label:
                raise Exception("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
            new_label_name = json_label['name']
            new_label_href = json_label['href']
            new_label_type = json_label['key']

            new_label = pylo.LabelGroup(new_label_name, new_label_href, new_label_type, self)
            created_groups.append(new_label)

            if new_label_href in self._items_by_href:
                raise Exception("A Label with href '%s' already exists in the table", new_label_href)

            self._items_by_href[new_label_href] = new_label

            new_label.raw_json = json_label

            log.info("Found LabelGroup '%s' with href '%s' and type '%s'", new_label_name, new_label_href, new_label_type)

        for group in created_groups:
            group.load_from_json()

    def count_labels(self, label_type: Optional[str] = None) -> int:
        return len(self.get_labels(label_type=label_type))

    def count_location_labels(self) -> int:
        return len(self.get_labels(label_type=label_type_loc))

    def count_environment_labels(self) -> int:
        return len(self.get_labels(label_type=label_type_env))

    def count_application_labels(self) -> int:
        return len(self.get_labels(label_type=label_type_app))

    def count_role_labels(self) -> int:
        return len(self.get_labels(label_type=label_type_role))
    
    def count_label_groups(self, label_type: Optional[str] = None) -> int:
        return len(self.get_label_groups(label_type))
    
    def count_location_label_groups(self) -> int:
        return len(self.get_label_groups(label_type=label_type_loc))
    
    def count_environment_label_groups(self) -> int:
        return len(self.get_label_groups(label_type=label_type_env))
    
    def count_application_label_groups(self) -> int:
        return len(self.get_label_groups(label_type=label_type_app))
    
    def count_role_label_groups(self) -> int:
        return len(self.get_label_groups(label_type=label_type_role))
    
    def get_labels(self, label_type: Optional[str] = None) -> List['pylo.Label']:
        if label_type is not None:  # label_type must be a valid type
            if label_type not in self.label_types_as_set:
                raise pylo.PyloEx("Invalid label type '%s'. Valid types are: %s" % (label_type, self.label_types_as_set))
        data = []
        for label in self._items_by_href.values():
            if label.is_label() and (label_type is None or label.type == label_type):
                data.append(label)
        return data
    
    def get_labels_as_dict_by_href(self, label_type: Optional[str] = None) -> Dict[str, 'pylo.Label']:
        label_list = self.get_labels(label_type)
        return self.Utils.list_to_dict_by_href(label_list)

    def get_label_groups(self, label_type: Optional[str] = None) -> List['pylo.LabelGroup']:
        if label_type is not None:  # label_type must be a valid type
            if label_type not in self.label_types_as_set:
                raise pylo.PyloEx("Invalid label type '%s'. Valid types are: %s" % (label_type, self.label_types_as_set))
        data = []
        for label in self._items_by_href.values():
            if label.is_group() and (label_type is None or label.type == label_type):
                data.append(label)
        return data


    def get_label_groups_as_dict_by_href(self, label_type: Optional[str] = None) -> Dict[str, 'pylo.LabelGroup']:
        label_list = self.get_label_groups(label_type)
        return self.Utils.list_to_dict_by_href(label_list)


    def get_both_labels_and_groups(self, label_type: Optional[str] = None) -> List[Union['pylo.Label','pylo.LabelGroup']]:
        data = []
        if label_type is not None:
            if label_type not in self.label_types_as_set:
                raise pylo.PyloEx("Invalid label type '%s'. Valid types are: %s" % (label_type, self.label_types_as_set))

        for label in self._items_by_href.values():
            if label_type is None or label.type == label_type:
                data.append(label)
        return data

    def get_both_labels_and_groups_as_dict_by_href(self, label_type: Optional[str] = None) -> Dict[str, Union['pylo.Label','pylo.LabelGroup']]:
        label_list = self.get_both_labels_and_groups(label_type)
        return self.Utils.list_to_dict_by_href(label_list)


    def find_object_by_name(self, name: str|List[str], label_type: Optional[str] = None, case_sensitive: bool = True,
                           missing_labels_names: Optional[List[str]] = None,
                           allow_label_group: bool = True,
                           allow_label: bool = True,
                           raise_exception_if_not_found: bool = False) -> Optional[Union['pylo.Label','pylo.LabelGroup',List[Union['pylo.Label','pylo.LabelGroup']]]]:
        """Find a label by its name. If case_sensitive is False, the search is case-insensitive.
        If case_sensitive is False it will return a list of labels with the same name rather than a single object.
        If missing_labels_names is not None, it will be filled with the names of the labels not found.
        If raise_exception_if_not_found is True, an exception will be raised if a label is not found.
        If name is a list, a list of labels will be returned, in the same order as the list of names.
        If a label is not found, None will be returned in the list.
        """
        if not isinstance(name, list):
            if case_sensitive is False:
                return self.find_object_by_name([name], label_type=label_type, case_sensitive=case_sensitive,
                                                missing_labels_names=missing_labels_names,
                                                allow_label_group=allow_label_group,
                                                allow_label=allow_label,
                                                raise_exception_if_not_found=raise_exception_if_not_found)
            for label in self._items_by_href.values():
                if label_type is not None and label.type != label_type:
                    continue
                if label.is_label() and allow_label: # ignore groups
                    if case_sensitive:
                        if label.name == name:
                            return label
                elif allow_label_group:
                        if label.name.lower() == name.lower():
                            return label
            if raise_exception_if_not_found:
                raise pylo.PyloEx("Label/group '%s' not found", name)
            if missing_labels_names is not None:
                missing_labels_names.append(name)
            return None
        else:
            results = []
            local_notfound_labels = []
            for name_to_find in name:
                result = self.find_object_by_name(name_to_find, label_type=label_type ,case_sensitive=case_sensitive,
                                                 allow_label_group=allow_label_group, allow_label=allow_label)
                if result is None:
                        local_notfound_labels.append(name_to_find)
                else:
                    results.append(result)
            if raise_exception_if_not_found and len(local_notfound_labels) > 0:
                raise pylo.PyloEx("Some labels not found: {}".format(local_notfound_labels))
            if missing_labels_names is not None:
                missing_labels_names.extend(local_notfound_labels)
            return results

    def find_label_by_name(self, name: str|List[str], label_type: Optional[str] = None, case_sensitive: bool = True,
                            missing_labels_names: Optional[List[str]] = None,
                            raise_exception_if_not_found: bool = False) -> Optional['pylo.Label'|List['pylo.Label']]:
        """Find a label by its name.
        If case_sensitive is False it will return a list of labels with the same name rather than a single object.
        If missing_labels_names is not None, it will be filled with the names of the labels not found.
        If raise_exception_if_not_found is True, an exception will be raised if a label is not found.
        If name is a list, a list of labels will be returned, in the same order as the list of names.
        If a label is not found, None will be returned in the list.
        """
        return self.find_object_by_name(name, label_type=label_type, case_sensitive=case_sensitive,
                                        missing_labels_names=missing_labels_names,
                                        allow_label_group=False, allow_label=True,
                                        raise_exception_if_not_found=raise_exception_if_not_found)


    def find_label_by_name_whatever_type(self, name: str, case_sensitive: bool = True) -> Optional[Union['pylo.Label', 'pylo.LabelGroup']]:
        pylo.log.warn("find_label_by_name_whatever_type is deprecated, use find_label_by_name instead")
        return self.find_label_by_name(name, case_sensitive=case_sensitive)


    def find_label_by_name_and_type(self, name: str, label_type: str, case_sensitive: bool = True) \
            -> Optional[Union['pylo.Label', 'pylo.LabelGroup']]:
        pylo.log.warn("find_label_by_name_and_type is deprecated, use find_label_by_name instead")
        return self.find_label_by_name(name, label_type=label_type, case_sensitive=case_sensitive)

    cache_label_all_string = '-All-'
    cache_label_all_separator = '|'

    def generate_label_resolution_cache(self):
        """
        Mostly for internal use. This method will generate a cache of all possible combinations of labels.
        """
        self.label_resolution_cache = {}

        roles = list(self.get_labels_as_dict_by_href('role').keys())
        roles.append(self.cache_label_all_string)
        for role in roles:
            apps = list(self.get_labels_as_dict_by_href('app').keys())
            apps.append(self.cache_label_all_string)
            for app in apps:
                envs = list(self.get_labels_as_dict_by_href('env').keys())
                envs.append(self.cache_label_all_string)
                for env in envs:
                    locs = list(self.get_labels_as_dict_by_href('loc').keys())
                    locs.append(self.cache_label_all_string)
                    for loc in locs:
                        group_name = role + LabelStore.cache_label_all_separator + app + LabelStore.cache_label_all_separator + env + LabelStore.cache_label_all_separator + loc
                        self.label_resolution_cache[group_name] = []

        all_string_and_sep = LabelStore.cache_label_all_string + LabelStore.cache_label_all_separator

        masks = []
        for i in range(2**4):
            binary = bin(i)[2:].zfill(4)
            mask = [bool(bit) for bit in binary]
            masks.append(mask)

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

    def create_label(self, name: str, label_type: str) -> 'pylo.Label':
        """Create a label *locally* (not on the server). Mostly for internal use.
        """

        new_label_name = name
        new_label_type = label_type
        new_label_href = '**fake-label-href**/{}'.format( md5(str(random.random()).encode('utf8')).digest() )

        new_label = pylo.Label(new_label_name, new_label_href, new_label_type, self)

        if new_label_href in self._items_by_href:
            raise Exception("A Label with href '%s' already exists in the table", new_label_href)

        self._items_by_href[new_label_href] = new_label

        return new_label

    def api_create_label(self, name: str, label_type: str) -> 'pylo.Label':

        connector = pylo.find_connector_or_die(self.owner)
        json_label = connector.objects_label_create(name, label_type)

        if 'value' not in json_label or 'href' not in json_label or 'key' not in json_label:
            raise pylo.PyloEx("Cannot find 'value'/name or href for Label in JSON:\n" + nice_json(json_label))
        new_label_name = json_label['value']
        new_label_href = json_label['href']
        new_label_type = json_label['key']

        new_label = pylo.Label(new_label_name, new_label_href, new_label_type, self)

        if new_label_href in self._items_by_href:
            raise Exception("A Label with href '%s' already exists in the table", new_label_href)

        self._items_by_href[new_label_href] = new_label

        return new_label

    def find_label_by_name_lowercase_and_type(self, name: str, label_type: str) -> Optional[Union['pylo.Label', 'pylo.LabelGroup']]:
        pylo.log.warn("find_label_by_name_lowercase_and_type is deprecated, use find_object_by_name instead")
        return self.find_object_by_name(name, label_type)

    def find_by_href(self, href: str) -> Optional[Union['pylo.Label', 'pylo.LabelGroup']]:
        return self._items_by_href.get(href)
