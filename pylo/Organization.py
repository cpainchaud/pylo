import json
import pylo
import os
import time
import getpass
from pylo import log, LabelStore


class Organization:

    """
    :type id: int
    :type connector: pylo.APIConnector
    """

    def __init__(self, orgID):
        """@type orgID: int"""
        self.id = orgID
        self.connector = None
        self.LabelStore = pylo.LabelStore(self)
        self.IPListStore = pylo.IPListStore(self)
        self.WorkloadStore = pylo.WorkloadStore(self)
        self.ServiceStore = pylo.ServiceStore(self)
        self.RulesetStore = pylo.RulesetStore(self)
        self.SecurityPrincipalStore = pylo.SecurityPrincipalStore(self)

    def load_from_cache_or_saved_credentials(self, hostname: str, include_deleted_workloads=False, prompt_for_api_key_if_missing=True):
        # filename should be like 'cache_xxx.yyy.zzz.json'
        filename = 'cache_' + hostname + '.json'

        if os.path.isfile(filename):
            # now we try to open that JSON file
            with open(filename) as json_file:
                data = json.load(json_file)
                if 'data' not in data:
                    raise Exception("Cache file '%s' was found and successfully loaded but no 'data' object could be found" % filename)
                self.load_from_json(data['data'])
            self.connector = pylo.APIConnector.create_from_credentials_in_file(hostname)
        else:
            return self.load_from_saved_credentials(hostname, include_deleted_workloads=include_deleted_workloads, prompt_for_api_key=prompt_for_api_key_if_missing)

    def make_cache_file_from_api(self, con: pylo.APIConnector, include_deleted_workloads=False):
        # filename should be like 'cache_xxx.yyy.zzz.json'
        filename = 'cache_' + con.hostname + '.json'

        data = self.get_config_from_api(con, include_deleted_workloads=include_deleted_workloads)

        timestamp = time.time()

        json_content = {'generation_date': timestamp,
                       'data': data
                        }

        with open(filename, 'w') as outfile:
            json.dump(json_content, outfile)

        size = os.path.getsize(filename)

        return filename, size

    def load_from_saved_credentials(self, hostname: str, include_deleted_workloads=False, prompt_for_api_key=False):
        separator_pos = hostname.find(':')
        port = 8443

        if separator_pos > 0:
            port = hostname[separator_pos+1:]
            hostname = hostname[0:separator_pos]

        connector = pylo.APIConnector.create_from_credentials_in_file(hostname)
        if connector is None:
            if not prompt_for_api_key:
                raise pylo.PyloEx('Cannot find credentials for host {}'.format(hostname))
            print('Cannot find credentials for host "{}".\nPlease input an API user:'.format(hostname), end='')
            user = input()
            password = getpass.getpass()

            connector = pylo.APIConnector(hostname, port, user, password, skip_ssl_cert_check=True)

        self.load_from_api(connector, include_deleted_workloads=include_deleted_workloads)

    def load_from_json(self, data):
        if 'labels' not in data:
            raise Exception("'labels' was not found in json data")
        self.LabelStore.loadLabelsFromJson(data['labels'])

        if 'labelgroups' not in data:
            raise Exception("'labelgroups' was not found in json data")
        self.LabelStore.loadLabelGroupsFromJson(data['labelgroups'])

        if 'iplists' not in data:
            raise Exception("'iplists' was not found in json data")
        self.IPListStore.load_iplists_from_json(data['iplists'])

        if 'services' not in data:
            raise Exception("'services' was not found in json data")
        self.ServiceStore.load_services_from_json(data['services'])

        if 'workloads' not in data:
            raise Exception("'workloads' was not found in json data")
        self.WorkloadStore.load_workloads_from_json(data['workloads'])

        if 'security_principals' not in data:
            raise Exception("'security_principals' was not found in json data")
        self.SecurityPrincipalStore.load_principals_from_json(data['security_principals'])

        if 'rulesets' not in data:
            raise Exception("'rulesets' was not found in json data")
        self.RulesetStore.load_rulesets_from_json(data['rulesets'])

    def load_from_api(self, con: pylo.APIConnector, include_deleted_workloads=False):
        return self.load_from_json(self.get_config_from_api(con, include_deleted_workloads=include_deleted_workloads))

    def get_config_from_api(self, con: pylo.APIConnector, include_deleted_workloads=False):
        self.connector = con

        return con.get_pce_objects(include_deleted_workloads=include_deleted_workloads)

        data = {}

        # we start by loading labels
        data['labels'] = con.objects_label_get()

        # now LabelGroups
        data['labelgroups'] = con.objects_labelgroup_get()

        # IPLists
        data['iplists'] = con.objects_iplist_get()

        # Services
        data['services'] = con.objects_service_get()

        # Workloads
        data['workloads'] = con.objects_workload_get(include_deleted=include_deleted_workloads)

        # Security Principals
        data['security_principals'] = con.objects_securityprincipal_get()

        # Workloads
        data['rulesets'] = con.objects_ruleset_get()

        return data

    def stats_to_str(self):
        stats = ""
        stats += "- {} Labels in total. Loc: {} / Env: {} / App: {} / Role: {}".\
            format(self.LabelStore.count_labels(),
                   self.LabelStore.count_location_labels(),
                   self.LabelStore.count_environment_labels(),
                   self.LabelStore.count_application_labels(),
                   self.LabelStore.count_role_labels())

        stats += "\n- Workloads: Managed: {} / Unmanaged: {} / Deleted: {}". \
            format(self.WorkloadStore.count_managed_workloads(),
                   self.WorkloadStore.count_unamanaged_workloads(),
                   self.WorkloadStore.count_deleted_workloads())

        stats += "\n- {} IPlists in total.". \
            format(self.IPListStore.count() )


        stats += "\n- {} RuleSets and {} Rules.".format(self.RulesetStore.count_rulesets(), self.RulesetStore.count_rules())

        return stats


