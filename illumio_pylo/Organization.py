from typing import Optional, List, Callable
import json
import os
import getpass
import illumio_pylo as pylo
from .API.JsonPayloadTypes import PCEObjectsJsonStructure, PCECacheFileJsonStructure
from .API.CredentialsManager import get_credentials_from_file


class Organization:

    __slots__ = ['id', 'connector', 'LabelStore', 'IPListStore', 'WorkloadStore', 'VirtualServiceStore', 'AgentStore',
                 'ServiceStore', 'RulesetStore', 'SecurityPrincipalStore', 'pce_version']

    def __init__(self, org_id):
        self.id: int = org_id
        self.connector: Optional['pylo.APIConnector'] = None
        self.LabelStore: 'pylo.LabelStore' = pylo.LabelStore(self)
        self.IPListStore: 'pylo.IPListStore' = pylo.IPListStore(self)
        self.WorkloadStore: 'pylo.WorkloadStore' = pylo.WorkloadStore(self)
        self.VirtualServiceStore: 'pylo.VirtualServiceStore' = pylo.VirtualServiceStore(self)
        self.AgentStore: 'pylo.AgentStore' = pylo.AgentStore(self)
        self.ServiceStore: 'pylo.ServiceStore' = pylo.ServiceStore(self)
        self.RulesetStore: 'pylo.RulesetStore' = pylo.RulesetStore(self)
        self.SecurityPrincipalStore: 'pylo.SecurityPrincipalStore' = pylo.SecurityPrincipalStore(self)
        self.pce_version: Optional['pylo.SoftwareVersion'] = None

    def load_from_cached_file(self, fqdn: str, no_exception_if_file_does_not_exist=False) -> bool:
        # filename should be like 'cache_xxx.yyy.zzz.json'
        filename = 'cache_' + fqdn + '.json'

        if os.path.isfile(filename):
            # now we try to open that JSON file
            with open(filename) as json_file:
                data: PCECacheFileJsonStructure = json.load(json_file)
                if 'pce_version' not in data:
                    raise pylo.PyloEx("Cannot find PCE version in cache file")
                self.pce_version = pylo.SoftwareVersion(data['pce_version'])
                if 'data' not in data:
                    raise pylo.PyloEx("Cache file '%s' was found and successfully loaded but no 'data' object could be found" % filename)
                self.load_from_json(data['data'])
                return True

        if no_exception_if_file_does_not_exist:
            return False

        raise pylo.PyloEx("Cache file '%s' was not found!" % filename)

    @staticmethod
    def get_from_cache_file(fqdn: str) -> 'pylo.Organization':
        org = pylo.Organization(1)
        org.load_from_cached_file(fqdn)
        return org

    @staticmethod
    def get_from_api_using_credential_file(fqdn_or_profile_name: str = None,
                                           credential_file: str = None,
                                           list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None,
                                           include_deleted_workloads: bool = False,
                                           callback_api_objects_downloaded: Callable = None) -> 'Organization':
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
        credentials = get_credentials_from_file(fqdn_or_profile_name, credential_file)

        connector = pylo.APIConnector(fqdn=credentials.fqdn, port=credentials.port,
                                      api_user=credentials.api_user, api_key=credentials.api_key,
                                      org_id=credentials.org_id,
                                      skip_ssl_cert_check=not credentials.verify_ssl, name=fqdn_or_profile_name)

        objects = connector.get_pce_objects(list_of_objects_to_load=list_of_objects_to_load,
                                            include_deleted_workloads=include_deleted_workloads)

        if callback_api_objects_downloaded is not None:
            callback_api_objects_downloaded()

        org = Organization(1)
        org.load_from_json(objects,list_of_objects_to_load=list_of_objects_to_load)

        return org

    def load_from_cache_or_saved_credentials(self, fqdn: str, include_deleted_workloads=False, prompt_for_api_key_if_missing=True):
        """
        Load the organization from a cache file on disk or default to the API
        :param fqdn: the hostname of the PCE
        :param include_deleted_workloads: if True, deleted workloads will be loaded from the API
        :param prompt_for_api_key_if_missing: if True, the user will be prompted for an API key if it's unknown
        :return:
        """
        if not self.load_from_cached_file(fqdn, no_exception_if_file_does_not_exist=True):
            self.load_from_saved_credentials(fqdn, include_deleted_workloads=include_deleted_workloads, prompt_for_api_key=prompt_for_api_key_if_missing)

    def load_from_saved_credentials(self, fqdn: str, include_deleted_workloads=False, prompt_for_api_key=False,
                                    list_of_objects_to_load: Optional[List[str]] = None):
        separator_pos = fqdn.find(':')
        port = 8443

        if separator_pos > 0:
            port = fqdn[separator_pos + 1:]
            fqdn = fqdn[0:separator_pos]

        connector = pylo.APIConnector.create_from_credentials_in_file(fqdn)
        if connector is None:
            if not prompt_for_api_key:
                raise pylo.PyloEx('Cannot find credentials for host {}'.format(fqdn))
            print('Cannot find credentials for host "{}".\nPlease input an API user:'.format(fqdn), end='')
            user = input()
            password = getpass.getpass()
            connector = pylo.APIConnector(fqdn, port, user, password, skip_ssl_cert_check=True, org_id=self.id)

        self.load_from_api(connector, include_deleted_workloads=include_deleted_workloads,
                           list_of_objects_to_load=list_of_objects_to_load)

    def load_from_json(self, data: PCEObjectsJsonStructure,
                       list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None)\
            -> None:
        """
        Load the organization from a JSON structure, mostly for developers use only
        """
        object_to_load = {}
        if list_of_objects_to_load is not None:
            all_types = pylo.APIConnector.get_all_object_types()
            for object_type in list_of_objects_to_load:
                if object_type not in all_types:
                    raise pylo.PyloEx("Unknown object type '{}'".format(object_type))
                object_to_load[object_type] = True
        else:
            object_to_load = pylo.APIConnector.get_all_object_types()

        if self.pce_version is None:
            if 'pce_version' in data:
                self.pce_version = pylo.SoftwareVersion(data['pce_version'])
            else:
                raise pylo.PyloEx('Organization has no "version" specified')

        self.LabelStore.load_label_dimensions(data.get('label_dimensions'))

        if 'labels' in object_to_load:
            if 'labels' not in data:
                raise Exception("'labels' was not found in json data")
            self.LabelStore.load_labels_from_json(data['labels'])

        if 'labelgroups' in object_to_load:
            if 'labelgroups' not in data:
                raise Exception("'labelgroups' was not found in json data")
            self.LabelStore.load_label_groups_from_json(data['labelgroups'])

        if 'iplists' in object_to_load:
            if 'iplists' not in data:
                raise Exception("'iplists' was not found in json data")
            self.IPListStore.load_iplists_from_json(data['iplists'])

        if 'services' in object_to_load:
            if 'services' not in data:
                raise Exception("'services' was not found in json data")
            self.ServiceStore.load_services_from_json(data['services'])

        if 'workloads' in object_to_load:
            if 'workloads' not in data:
                raise Exception("'workloads' was not found in json data")
            self.WorkloadStore.load_workloads_from_json(data['workloads'])

        if 'virtual_services' in object_to_load:
            if 'virtual_services' not in data:
                raise Exception("'virtual_services' was not found in json data")
            self.VirtualServiceStore.load_virtualservices_from_json(data['virtual_services'])

        if 'security_principals' in object_to_load:
            if 'security_principals' not in data:
                raise Exception("'security_principals' was not found in json data")
            self.SecurityPrincipalStore.load_principals_from_json(data['security_principals'])

        if 'rulesets' in object_to_load:
            if 'rulesets' not in data:
                raise Exception("'rulesets' was not found in json data")
            self.RulesetStore.load_rulesets_from_json(data['rulesets'])

    def load_from_api(self, con: pylo.APIConnector, include_deleted_workloads=False,
                      list_of_objects_to_load: Optional[List['pylo.ObjectTypes']] = None):
        """
        Load the organization from the API with the API Connector provided. Mostly intended for developers use only
        :param con:
        :param include_deleted_workloads:
        :param list_of_objects_to_load:
        :return:
        """
        self.pce_version = con.get_software_version()
        self.connector = con
        return self.load_from_json(self.get_config_from_api(con, include_deleted_workloads=include_deleted_workloads,
                                                            list_of_objects_to_load=list_of_objects_to_load))

    @staticmethod
    def create_fake_empty_config() -> PCEObjectsJsonStructure:
        """
        Create a fake empty config, mostly for developers use only
        :return:
        """
        data = {}
        for object_type in pylo.APIConnector.get_all_object_types().values():
            data[object_type] = []
        return data

    def get_config_from_api(self, con: pylo.APIConnector, include_deleted_workloads=False,
                            list_of_objects_to_load: Optional[List[str]] = None) -> PCEObjectsJsonStructure:
        """
        Get the config/objects from the API using the API connector provided
        :param con:
        :param include_deleted_workloads:
        :param list_of_objects_to_load:
        :return:
        """
        self.connector = con
        return con.get_pce_objects(include_deleted_workloads=include_deleted_workloads,
                                   list_of_objects_to_load=list_of_objects_to_load)

    def stats_to_str(self, padding='') -> str:
        """ Dumps basic stats about the organization
        :param padding: String to be added at the beginning of each line
        Example:
        - Version 21.5.33-3
        - 539 Labels in total. Loc: 35 / Env: 13 / App: 368 / Role: 123
        - Workloads: Managed: 5822 / Unmanaged: 1483 / Deleted: 0
        - 0 IPlists in total.
        - 0 RuleSets and 0 Rules.
        """
        stats = ""
        stats += "{}- Version {}".format(padding, self.pce_version.generate_str_from_numbers()) + os.linesep

        labels_str = ''
        for dimension in self.LabelStore.label_types:
            labels_str += " {}: {} /".format(dimension, self.LabelStore.count_labels(dimension))
        # remove last ' /'
        labels_str = labels_str[:-2]
        stats += "{}- {} Labels in total. {}".\
            format(padding,
                   self.LabelStore.count_labels(),
                   labels_str)

        stats += os.linesep + "{}- Workloads: Managed: {} / Unmanaged: {} / Deleted: {}". \
            format(padding,
                   self.WorkloadStore.count_managed_workloads(),
                   self.WorkloadStore.count_unmanaged_workloads(True),
                   self.WorkloadStore.count_deleted_workloads())

        stats += os.linesep + "{}- {} IPlists in total.". \
            format(padding,
                   self.IPListStore.count())

        stats += os.linesep + "{}- {} RuleSets and {} Rules.". \
            format(padding, self.RulesetStore.count_rulesets(), self.RulesetStore.count_rules())

        return stats


