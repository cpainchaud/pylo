import json
import time
import os
import getpass
import pylo.vendors.requests as requests
from threading import Thread
from queue import Queue
import pylo
from pylo import log
from typing import Union, Dict, Any

#urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()


def get_field_or_die(field_name: str, data):
    if type(data) is not dict:
        raise pylo.PyloEx("Data argument should of type DICT, '{}' was given".format(type(data)))

    field = data.get(field_name, pylo.objectNotFound)

    if field is pylo.objectNotFound:
        raise pylo.PyloEx("Could not find field named '{}' in data".format(field_name), data)
    return field


class APIConnector:
    """docstring for APIConnector."""


    def __init__(self, hostname: str, port, apiuser: str, apikey: str, skip_ssl_cert_check=False, orgID=1):
        self.hostname = hostname
        if type(port) is int:
            port = str(port)
        self.port = port
        self.api_key = apikey
        self.api_user = apiuser
        self.orgID = orgID
        self.skipSSLCertCheck = skip_ssl_cert_check
        self.version = None  # type: pylo.SoftwareVersion
        self.version_string = "Not Defined"
        self._cached_session = requests.session()

    @staticmethod
    def create_from_credentials_in_file(hostname: str, request_if_missing = False):

        separator_pos = hostname.find(':')
        port = 8443

        if separator_pos > 0:
            port = hostname[separator_pos+1:]
            hostname = hostname[0:separator_pos]

        if os.path.isfile('ilo.json'):
            with open('ilo.json') as json_file:
                data = json.load(json_file)
                if hostname in data:
                    cur = data[hostname]
                    ignore_ssl = False
                    org_id = 1
                    if 'ignore-ssl' in cur:
                        ssl_value = cur['ignore-ssl']
                        if type(ssl_value) is str:
                            if ssl_value.lower() == 'yes':
                                ignore_ssl = True
                    if 'org_id' in cur:
                        org_id_value = cur['org_id']
                        if type(org_id_value) is int:
                            org_id = org_id_value
                        else:
                            raise pylo.PyloEx("org_id must be an integer", cur)
                    return APIConnector(hostname, cur['port'], cur['user'], cur['key'], orgID=org_id, skip_ssl_cert_check=ignore_ssl)

        if not request_if_missing:
            return None

        print('Cannot find credentials for host "{}".\nPlease input an API user:'.format(hostname), end='')
        user = input()
        password = getpass.getpass()

        connector = pylo.APIConnector(hostname, port, user, password, skip_ssl_cert_check=True)
        return connector


    def _make_url(self, path: str, includeOrgID):
        url = "https://" + self.hostname + ":" + self.port + "/api/v2"
        if includeOrgID:
            url += '/orgs/' + str(self.orgID)
        url += path

        return url

    def do_get_call(self, path, json_arguments=None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False, params=None, skip_product_version_check=False):
        return self._doCall('GET', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall, skip_product_version_check=skip_product_version_check, params=params)

    def do_post_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('POST', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)

    def do_put_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('PUT', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)


    def do_delete_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('DELETE', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)


    def _doCall(self, method, path, json_arguments=None, include_org_id=True, jsonOutputExpected=True, asyncCall=False, skip_product_version_check=False, params=None):

        if self.version is None and not skip_product_version_check:
            self.collect_pce_infos()

        url = self._make_url(path, include_org_id)

        headers = {'Accept' : 'application/json'}

        if json_arguments is not None:
            headers['Content-Type'] = 'application/json'

        if asyncCall:
            headers['Prefer'] = 'respond-async'

        log.info("Request URL: " + url)
        req = self._cached_session.request(method, url, headers=headers, auth=(self.api_user, self.api_key),
                               verify=(not self.skipSSLCertCheck), json=json_arguments, params=params)

        answerSize = len(req.content) / 1024
        log.info("URL downloaded (size "+str( int(answerSize) )+"KB) Reply headers:\n" +
                 "HTTP " + method + " " + url + " STATUS " + str(req.status_code) + " " + req.reason)
        log.info(req.headers)
        log.info("Request Body:" + pylo.nice_json(json_arguments))
        # log.info("Request returned code "+ str(req.status_code) + ". Raw output:\n" + req.text[0:2000])

        if asyncCall:
            if method == 'GET' and req.status_code != 202:
                orig_request = req.request  # type: requests.PreparedRequest
                raise Exception("Status code for Async call should be 202 but " + str(req.status_code)
                                + " " + req.reason + " was returned with the following body: " + req.text +
                                "\n\n Request was: " + orig_request.url + "\nHEADERS: " + str(orig_request.headers) +
                                "\nBODY:\n" + str(orig_request.body))

            if 'Location' not in req.headers:
                raise Exception('Header "Location" was not found in API answer!')
            if 'Retry-After' not in req.headers:
                raise Exception('Header "Retry-After" was not found in API answer!')

            jobLocation = req.headers['Location']
            retryInterval = int(req.headers['Retry-After'])

            retryLoopTimes = 0

            while True:
                log.info("Sleeping " + str(retryInterval) + " seconds before polling for job status, elapsed " + str(retryInterval*retryLoopTimes) + " seconds so far" )
                retryLoopTimes += 1
                time.sleep(retryInterval)
                jobPoll = self.do_get_call(jobLocation, includeOrgID=False)
                if 'status' not in jobPoll:
                    raise Exception('Job polling request did not return a "status" field')
                jobPollStatus = jobPoll['status']

                if jobPollStatus == 'failed':
                    if 'result' in jobPoll and 'message' in jobPoll['result']:
                        raise Exception('Job polling return with status "Failed": ' + jobPoll['result']['message'])
                    else:
                        raise Exception('Job polling return with status "Failed": ' + jobPoll)

                if jobPollStatus == 'done':
                    if 'result' not in jobPoll:
                        raise Exception('Job is marked as done but has no "result"')
                    if 'href' not in jobPoll['result']:
                        raise Exception("Job is marked as done but did not return a href to download resulting Dataset")

                    resultHref = jobPoll['result']['href']
                    break

                log.info("Job status is " + jobPollStatus)

            log.info("Job is done, we will now download the resulting dataset")
            dataset = self.do_get_call(resultHref, includeOrgID=False)

            return dataset

        if method == 'GET' and req.status_code != 200 \
                or\
                method == 'POST' and req.status_code != 201 and req.status_code != 204 \
                or\
                method == 'DELETE' and req.status_code != 204 \
                or \
                method == 'PUT' and req.status_code != 204 and req.status_code != 200:

            if req.status_code == 429:  # too many requests sent in short amount of time? [{"token":"too_many_request_error", ....}]
                jout = req.json()
                if len(jout) > 0:
                    if "token" in jout[0]:
                        if jout[0]['token'] == 'too_many_request_error':
                            raise pylo.PyloApiTooManyRequestsEx('API has hit DOS protection limit (X calls per minute)', jout)


            raise pylo.PyloApiEx('API returned error status "' + str(req.status_code) + ' ' + req.reason
                            + '" and error message: ' + req.text)

        if jsonOutputExpected:
            log.info("Parsing API answer to JSON (with a size of " + str( int(answerSize) ) + "KB)")
            jout = req.json()
            log.info("Done!")
            if answerSize < 5:
                log.info("Resulting JSON object:")
                log.info(json.dumps(jout, indent=2, sort_keys=True))
            return jout

        return req.text

    def getSoftwareVersion(self):
        self.collect_pce_infos()
        return self.version

    def getSoftwareVersionString(self):
        self.collect_pce_infos()
        return self.version_string

    def get_pce_objects(self, include_deleted_workloads=False):

        threads_count = 4
        data = {}
        errors = []
        thread_queue = Queue()

        def get_objects(q: Queue, thread_num: int):
            while True:
                object_type, errors = q.get()
                try:
                    if len(errors) > 0:
                        q.task_done()
                        continue
                    if object_type == 'workloads':
                        data['workloads'] = self.objects_workload_get(include_deleted=include_deleted_workloads)
                    elif object_type == 'labels':
                        data['labels'] = self.objects_label_get()
                    elif object_type == 'labelgroups':
                        data['labelgroups'] = self.objects_labelgroup_get()
                    elif object_type == 'iplists':
                        data['iplists'] = self.objects_iplist_get()
                    elif object_type == 'services':
                        data['services'] = self.objects_service_get()
                    elif object_type == 'rulesets':
                        data['rulesets'] = self.objects_ruleset_get()
                    elif object_type == 'security_principals':
                        data['security_principals'] = self.objects_securityprincipal_get()
                    else:
                        raise pylo.PyloEx("Unsupported object type '{}'".format(object_type))
                except Exception as e:
                    errors.append(e)

                q.task_done()


        for i in range(threads_count):
            worker = Thread(target=get_objects, args=(thread_queue, i))
            worker.setDaemon(True)
            worker.daemon = True
            worker.start()

        thread_queue.put(('workloads', errors,))
        thread_queue.put(('rulesets', errors,))
        thread_queue.put(('services', errors,))
        thread_queue.put(('labels', errors,))
        thread_queue.put(('labelgroups', errors,))
        thread_queue.put(('services', errors,))
        thread_queue.put(('iplists', errors,))
        thread_queue.put(('security_principals',errors, ))

        thread_queue.join()

        if len(errors) > 0:
            raise errors[0]


        return data

    def collect_pce_infos(self):
        if self.version is not None:  # Make sure we collect data only once
            return
        path = "/product_version"
        jout = self.do_get_call(path, includeOrgID=False, skip_product_version_check=True)

        self.version_string = jout['version']
        self.version = pylo.SoftwareVersion(jout['long_display'])

    def policy_check(self, protocol, port=None, src_ip=None, src_href=None, dst_ip=None, dst_href=None):

        if type(port) is str:
            lower = protocol.lower()
            if lower == 'udp':
                protocol = 17
            elif lower == 'tcp':
                protocol = 6
            else:
                raise pylo.PyloEx("Unsupported protocol '{}'".format(protocol))

        if src_ip is None and src_href is None:
            raise pylo.PyloEx('src_ip and src_href cannot be both null')
        if dst_ip is None and dst_href is None:
            raise pylo.PyloEx('dst_ip and dst_href cannot be both null')

        path = "/sec_policy/draft/allow?protocol={}".format(protocol)

        if port is not None:
            path += "&port={}".format(port)

        if src_ip is not None:
            path += "&src_external_ip={}".format(src_ip)
        if src_href is not None:
            path += "&src_workload={}".format(src_href)

        if src_ip is not None:
            path += "&src_external_ip={}".format(src_ip)
        if src_href is not None:
            path += "&src_workload={}".format(src_href)

        if dst_ip is not None:
            path += "&dst_external_ip={}".format(dst_ip)
        if dst_href is not None:
            path += "&dst_workload={}".format(dst_href)

        return self.do_get_call(path=path, asyncCall=False)

    def objects_label_get(self):
        path = '/labels'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_label_delete(self, href):
        """

        :type href: str|pylo.Label
        """
        path = href
        if type(href) is pylo.Label:
            path = href.href

        return self.do_delete_call(path=path, jsonOutputExpected=False, includeOrgID=False)

    def objects_label_create(self, label_name: str, label_type: str):
        path = '/labels'
        if label_type != 'app' and label_type != 'env' and label_type != 'role' and label_type != 'loc':
            raise Exception("Requested to create a Label '%s' with wrong type '%s'" % (label_name, label_type))
        data = {'key': label_type, 'value': label_name}
        return self.do_post_call(path=path, json_arguments=data)

    def objects_labelgroup_get(self):
        path = '/sec_policy/draft/label_groups'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_iplist_get(self):
        path = '/sec_policy/draft/ip_lists'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_iplist_create(self, json_blob):
        path = '/sec_policy/draft/ip_lists'
        return self.do_post_call(path=path, json_arguments=json_blob)

    def objects_workload_get(self, include_deleted=False, filter_by_ip: str = None, max_results: int = None, fast_mode=False):
        path = '/workloads'
        data = {}

        if include_deleted:
            data['include_deleted'] = 'yes'

        if filter_by_ip is not None:
            data['ip_address'] = filter_by_ip

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, asyncCall=not fast_mode, params=data)

    def objects_workload_agent_upgrade(self, workload_href: str, target_version: str):
        path = '{}/upgrade'.format(workload_href)
        data = {"release": target_version}

        return self.do_post_call(path=path, json_arguments=data, jsonOutputExpected=False, includeOrgID=False)

    def objects_workload_update(self, href: str, data):
        path = href

        return self.do_put_call(path=path, json_arguments=data, jsonOutputExpected=False, includeOrgID=False)

    def objects_workload_update_bulk(self, json_object):
        path = '/workloads/bulk_update'
        return self.do_put_call(path=path, json_arguments=json_object)

    def objects_workload_delete(self, href):
        """

        :type href: str|pylo.Workload
        """
        path = href
        if type(href) is pylo.Workload:
            path = href.href

        return self.do_delete_call(path=path, jsonOutputExpected=False, includeOrgID=False)

    class WorkloadMultiDeleteTracker:
        _errors: Dict[str, str]
        _hrefs: Dict[str, bool]
        _wkls: Dict[str, 'pylo.Workload']
        connector: 'pylo.APIConnector'

        def __init__(self, connector: 'pylo.APIConnector'):
            self.connector = connector
            self._hrefs = {}
            self._errors = {}
            self._wkls = {}

        def add_workload(self, wkl: 'pylo.Workload'):
            self._hrefs[wkl.href] = True
            self._wkls[wkl.href] = wkl

        def add_href(self, href: str):
            self._hrefs[href] = True

        def add_error(self, href: str, message: str):
            self._errors[href] = message

        def get_error_by_wlk(self, wkl: 'pylo.Workload') -> Union[str, None]:
            found = self._errors.get(wkl.href, pylo.objectNotFound)
            if found is pylo.objectNotFound:
                return None
            return found

        def get_error_by_href(self, href: str) -> Union[str, None]:
            return self._errors.get(href)


        def execute(self):
            result = self.connector.objects_workload_delete_multi(list(self._hrefs.keys()))
            print(pylo.nice_json(result))
            if not type(result) is list:
                raise pylo.PyloEx("API didnt return expected JSON format", result)

            for entry in result:
                if not type(entry) is dict:
                    raise pylo.PyloEx("API didnt return expected JSON format", entry)
                href = entry.get("href")
                if href is None or type(href) is not str:
                    raise pylo.PyloEx("API didnt return expected JSON format", entry)

                error = entry.get("errors")
                if href is not None:
                    self._errors[href] = json.dumps(error)


        def count_entries(self):
            return len(self._hrefs)

        def count_errors(self):
            return len(self._errors)


    def new_tracker_workload_multi_delete(self):
        return APIConnector.WorkloadMultiDeleteTracker(self)

    def objects_workload_delete_multi(self, href_or_workload_array):
        """

        :type href_or_workload_array: list[str]|list[pylo.Workload]
        """

        if len(href_or_workload_array) < 1:
            return

        json_data = []

        if type(href_or_workload_array[0]) is str:
            for href in href_or_workload_array:
                json_data.append({"href": href})
        else:
            href: 'pylo.Workload'
            for href in href_or_workload_array:
                json_data.append({"href": href.href})

        print(json_data)

        path = "/workloads/bulk_delete"

        return self.do_put_call(path=path, json_arguments=json_data, jsonOutputExpected=True)

    def objects_workload_create_single_unmanaged(self, json_object):
        path = '/workloads'
        return self.do_post_call(path=path, json_arguments=json_object)

    def objects_workload_create_bulk_unmanaged(self, json_object):
        path = '/workloads/bulk_create'
        return self.do_put_call(path=path, json_arguments=json_object)

    def objects_service_get(self):
        path = '/sec_policy/draft/services'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_service_delete(self, href):
        """

        :type href: str|pylo.Workload
        """
        path = href
        if type(href) is pylo.Service:
            path = href.href

        return self.do_delete_call(path=path, jsonOutputExpected=False, includeOrgID=False)

    def objects_ruleset_get(self):
        path = '/sec_policy/draft/rule_sets'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_securityprincipal_get(self):
        path = '/security_principals'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_securityprincipal_create(self, name: str = None, sid: str = None, json_object=None):
        path = '/security_principals'

        if json_object is not None and name is not None:
            raise pylo.PyloEx("You must either use json_object or name but you cannot use both they are mutually exclusive")

        if json_object is not None:
            return get_field_or_die('href', self.do_post_call(path=path, json_arguments=json_object))

        if name is None:
            raise pylo.PyloEx("You need to provide a group name")
        if sid is None:
            raise pylo.PyloEx("You need to provide a SID")

        return get_field_or_die('href', self.do_post_call(path=path, json_arguments={'name': name, 'sid': sid}))


    class ApiAgentCompatibilityReport:

        class ApiAgentCompatibilityReportItem:
            def __init__(self, name, value, status, extra_debug_message=None):
                self.name = name
                self.value = value
                self.status = status
                self.extra_debug_message = extra_debug_message

        def __init__(self, raw_json):
            self._items = {}
            self.empty = False

            if len(raw_json) == 0:
                self.empty = True
                return

            self.global_status = raw_json.get('qualify_status')
            if self.global_status is None:
                raise pylo.PyloEx('Cannot find Compatibility Report status in JSON', json_object=raw_json)

            results = raw_json.get('results')
            if results is None:
                raise pylo.PyloEx('Cannot find Compatibility Report results in JSON', json_object=raw_json)

            results = results.get('qualify_tests')
            if results is None:
                raise pylo.PyloEx('Cannot find Compatibility Report results in JSON', json_object=raw_json)

            for result in results:
                status = result.get('status')
                if status is None:
                    continue

                for result_name in result.keys():
                    if result_name == 'status':
                        continue
                    self._items[result_name] = APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem(result_name, result[result_name], status)
                    if result_name == "required_packages_installed" and status != "green":
                        for tmp in results:
                            if "required_packages_missing" in tmp:
                                extra_infos = 'missing packages:{}'.format(pylo.string_list_to_text(tmp["required_packages_missing"]))
                                self._items[result_name].extra_debug_message = extra_infos
                                break


        def get_failed_items(self):
            results = {}  # type: {str,APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem}
            for infos in self._items.values():
                if infos.status != 'green':
                    results[infos.name] = infos

            return results

    def agent_get_compatibility_report(self, agent_href: str = None, agent_id: str = None, return_raw_json=True):
        if agent_href is None and agent_id is None:
            raise pylo.PyloEx('you need to provide a HREF or an ID')
        if agent_href is not None and agent_id is not None:
            raise pylo.PyloEx('you need to provide a HREF or an ID but not BOTH')

        if agent_href is None:
            path = '/agents/{}/compatibility_report'.format(agent_id)
        else:
            path = '{}/compatibility_report'.format(agent_href)

        if return_raw_json:
            return self.do_get_call(path=path)

        retryCount = 5

        while retryCount >= 0:
            retryCount -= retryCount
            try:
                api_result = self.do_get_call(path=path, includeOrgID=False)
                break

            except pylo.PyloApiTooManyRequestsEx as ex:
                time.sleep(4)

        return APIConnector.ApiAgentCompatibilityReport()

    def objects_agent_change_mode(self, agent_href: str, mode: str):
        path = agent_href

        if mode != 'build' and mode != 'idle' and mode != 'test':
            raise pylo.PyloEx("unsupported mode {}".format(mode))

        log_traffic = False

        if mode == 'build':
            mode = 'illuminated'
        elif mode == 'test':
            mode = 'illuminated'
            log_traffic = True

        data = {"agent": {"config": {"mode": mode, 'log_traffic': log_traffic}}}

        return self.do_put_call(path, json_arguments=data, includeOrgID=False, jsonOutputExpected=False)

    def objects_agent_reassign_pce(self, agent_href: str, target_pce: str):
        path = agent_href + '/update'
        data = {"target_pce_fqdn": target_pce}
        return self.do_put_call(path, json_arguments=data, includeOrgID=False, jsonOutputExpected=False)

    class ExplorerFilterSetV1:
        def __init__(self, max_results=10000):
            self._consumer_labels = {}
            self._consumer_exclude_labels = {}
            self._provider_labels = {}
            self._provider_exclude_labels = {}
            self.max_results = max_results


        @staticmethod
        def __filter_prop_add_label(prop_dict, label_or_href):
            """

            @type prop_dict: dict
            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            if isinstance(label_or_href, str):
                prop_dict[label_or_href] = label_or_href
                return
            elif isinstance(label_or_href, pylo.Label):
                prop_dict[label_or_href.href] = label_or_href
                return
            elif isinstance(label_or_href, pylo.LabelGroup):
                for nested_label in label_or_href.expand_nested_to_array():
                    prop_dict[nested_label.href] = nested_label
            else:
                raise pylo.PyloEx("Unsupported object type {}".format(type(label_or_href)))

        def consumer_include_label(self, label_or_href):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._consumer_labels, label_or_href)

        def consumer_exclude_label(self, label_or_href):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._consumer_exclude_labels, label_or_href)

        def provider_include_label(self, label_or_href):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._provider_labels, label_or_href)

        def provider_exclude_label(self, label_or_href):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._provider_exclude_labels, label_or_href)

        def set_max_results(self, max: int):
            self.max_results = max


        def generate_json_query(self):
            # examples:
            # {"sources":{"include":[[]],"exclude":[]}
            #  "destinations":{"include":[[]],"exclude":[]},
            #  "services":{"include":[],"exclude":[]},
            #  "sources_destinations_query_op":"and",
            #  "start_date":"2015-02-21T09:18:46.751Z","end_date":"2020-02-21T09:18:46.751Z",
            #  "policy_decisions":[],
            #  "max_results":10000}
            #
            filters = {
                "sources": {"include": [], "exclude": []},
                "destinations": {"include": [], "exclude": []},
                "services": {"include": [], "exclude": []},
                "sources_destinations_query_op": "and",
                "start_date": "2015-02-21T09:18:46.751Z", "end_date": "2020-02-21T09:18:46.751Z",
                "policy_decisions": [],
                "max_results": self.max_results
                }
            return filters


    def explorer_search(self, filters: 'pylo.APIConnector.ExplorerFilterSetV1'):
        path = "/traffic_flows/traffic_analysis_queries"
        data = filters.generate_json_query()
        return self.do_post_call(path, json_arguments=data, includeOrgID=True, jsonOutputExpected=True)


    class ClusterHealth:

        allowed_status_list = {'normal': True, 'warning': True, 'error': True}

        class ClusterHealthNode:

            class ServiceStatus:

                allowed_status = {'running': True, 'stopped': True, 'not_running': True }

                def __init__(self, name: str, status: str):
                    self.name = name
                    if status not in APIConnector.ClusterHealth.ClusterHealthNode.ServiceStatus.allowed_status:
                        raise pylo.PyloEx("A services status is created with unsupported status '{}'".format(status))

                    self.status = status  # type: str

                def is_running(self):
                    return self.status == 'running'

                def is_not_running(self):
                    return self.status == 'stopped'

                def is_partially_running(self):
                    return self.status == 'partial'

            def __init__(self, json_data):
                self.name = get_field_or_die('hostname', json_data)  # type: str
                self.type = get_field_or_die('type', json_data)  # type: str
                self.ip_address = get_field_or_die('ip_address', json_data)  # type: str
                self.runlevel = get_field_or_die('runlevel', json_data)  # type: int

                self.services = {}  # type: dict[str,APIConnector.ClusterHealth.ClusterHealthNode.ServiceStatus]
                services_statuses = get_field_or_die('services', json_data)

                self.global_service_status = get_field_or_die('status', services_statuses)

                def process_services(json_data: dict, status):
                    services = json_data.get(status)
                    if services is None:
                        return

                    for service in services:
                        new_service = APIConnector.ClusterHealth.ClusterHealthNode.ServiceStatus(service, status)
                        if new_service.name in self.services:
                            raise pylo.PyloEx("duplicated service name '{}'".format(new_service.name))
                        self.services[new_service.name] = new_service


                process_services(services_statuses, 'running')
                process_services(services_statuses, 'not_running')
                process_services(services_statuses, 'partial')
                process_services(services_statuses, 'optional')

            def is_offline_or_unreachable(self):
                if self.runlevel is None:
                    return True
                return False

            def get_troubled_services(self):
                ret = []
                for service in self.services.values():
                    if not service.is_running():
                        ret.append(service)
                return ret

            def get_running_services(self):
                ret = []
                for service in self.services.values():
                    if service.is_running():
                        ret.append(service)
                return ret


            def to_string(self, indent='', marker='*'):
                def val_str(display_name: str, value):
                    return "{}{}{}: {}\n".format(indent, marker, display_name, value)
                ret = ''
                ret += val_str('fqdn', self.name)
                indent += ' '
                marker = '-'

                ret += val_str('ip_address', self.ip_address)
                ret += val_str('type', self.type)
                if self.runlevel is None:
                    ret += val_str('runlevel', 'not running or not available')
                    return ret
                else:
                    ret += val_str('runlevel', self.runlevel)

                troubled_services = pylo.string_list_to_text(self.get_troubled_services())
                ret += val_str('non-functional services', troubled_services)

                running_services = pylo.string_list_to_text(self.get_running_services())
                ret += val_str('running services', running_services)

                return ret


        def __init__(self, json_data):
            self.raw_json = json_data

            self.fqdn = get_field_or_die('fqdn', json_data)  # type: str
            self._status = get_field_or_die('status', json_data)  # type: str
            self.type = get_field_or_die('type', json_data)  # type: str

            if self._status not in APIConnector.ClusterHealth.allowed_status_list:
                raise pylo.PyloEx("ClusterHealth has unsupported status '{}'".format(self.status))

            nodes_list = get_field_or_die('nodes', json_data)
            self.nodes_dict = {}

            for node in nodes_list:
                new_node = APIConnector.ClusterHealth.ClusterHealthNode(node)
                self.nodes_dict[new_node.name] = new_node



        def to_string(self):
            ret = ''
            ret += "cluster fqdn: '{}'\n".format(self.fqdn)
            ret += "type: '{}'\n".format(self.type)
            ret += "status: '{}'\n".format(self._status)
            ret += "nodes details:\n"
            for node in self.nodes_dict.values():
                ret += node.to_string(indent='  ')

            return ret

        def status_is_ok(self):
            return self._status == 'normal'

        def status_is_warning(self):
            return self._status == 'warning'

        def status_is_error(self):
            return self._status == 'error'


    def cluster_health_get(self, return_object=False):
        path = '/health'

        if not return_object:
            return self.do_get_call(path)

        # cluster_health list
        json_output = self.do_get_call(path, includeOrgID=False)
        if type(json_output) is not list:
            raise pylo.PyloEx("A list object was expected but we received a '{}' instead".format(type(json_output)))

        dict_of_health_reports = {}

        for single_output in json_output:
            new_report = APIConnector.ClusterHealth(single_output)
            dict_of_health_reports[new_report.fqdn] = new_report

        return dict_of_health_reports

