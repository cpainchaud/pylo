import json
import time
import os
import getpass
from pathlib import Path

try:
    import requests as requests
except ImportError:
    import pylo.vendors.requests as requests

from threading import Thread
from queue import Queue
from datetime import datetime, timedelta
import pylo
from pylo import log
from typing import Union, Dict, Any, List, Optional, Tuple


requests.packages.urllib3.disable_warnings()

default_retry_count_if_api_call_limit_reached = 3
default_retry_wait_time_if_api_call_limit_reached = 10
default_max_objects_for_sync_calls = 99999


def get_field_or_die(field_name: str, data):
    if type(data) is not dict:
        raise pylo.PyloEx("Data argument should of type DICT, '{}' was given".format(type(data)))

    field = data.get(field_name, pylo.objectNotFound)

    if field is pylo.objectNotFound:
        raise pylo.PyloEx("Could not find field named '{}' in data".format(field_name), data)
    return field


_all_object_types: Dict[str, str] = {
        'iplists': 'iplists',
        'workloads': 'workloads',
        'virtual_services': 'virtual_services',
        'labels': 'labels',
        'labelgroups': 'labelgroups',
        'services': 'services',
        'rulesets': 'rulesets',
        'security_principals': 'security_principals',
    }


class APIConnector:
    """docstring for APIConnector."""

    def __init__(self, hostname: str, port, apiuser: str, apikey: str, skip_ssl_cert_check=False, orgID=1):
        self.hostname: str = hostname
        if type(port) is int:
            port = str(port)
        self.port: int = port
        self.api_key: str = apikey
        self.api_user: str = apiuser
        self.orgID: int = orgID
        self.skipSSLCertCheck: bool = skip_ssl_cert_check
        self.version: Optional['pylo.SoftwareVersio'] = None
        self.version_string: str = "Not Defined"
        self._cached_session = requests.session()

    @staticmethod
    def get_all_object_types_names_except(exception_list: List[str]):

        if len(exception_list) == 0:
            return _all_object_types.values()

        # first let's check that all names in exception_list are valid
        for name in exception_list:
            if name not in _all_object_types:
                raise pylo.PyloEx("object type named '{}' doesn't exist. The list of supported objects names is: {}".
                                  format(name, pylo.string_list_to_text(_all_object_types.values())))

        object_names_list: List[str] = []
        for name in _all_object_types.values():
            for lookup_name in exception_list:
                if name == lookup_name:
                    break
            if lookup_name == name:
                continue

            object_names_list.append(name)

    @staticmethod
    def get_all_object_types():
        return _all_object_types.copy()

    @staticmethod
    def create_from_credentials_in_file(hostname: str, request_if_missing = False):

        separator_pos = hostname.find(':')
        port = 8443

        if separator_pos > 0:
            port = hostname[separator_pos+1:]
            hostname = hostname[0:separator_pos]

        filename = str(Path.home()) + '/pylo/credentials.json'
        filename = Path(filename)

        if not os.path.isfile(filename):
            filename ='ilo.json'

        if os.path.isfile(filename):
            with open(filename) as json_file:
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

    def _make_url(self, path: str, include_org_id):
        url = "https://" + self.hostname + ":" + self.port + "/api/v2"
        if include_org_id:
            url += '/orgs/' + str(self.orgID)
        url += path

        return url

    def do_get_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False, params=None, skip_product_version_check=False,
                    retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                    retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached,
                    return_headers: bool = False):

        return self._doCall('GET', path, json_arguments=json_arguments, include_org_id=include_org_id,
                            json_output_expected=json_output_expected, async_call=async_call, skip_product_version_check=skip_product_version_check, params=params,
                            retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                            retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached,
                            return_headers=return_headers)

    def do_post_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False,
                     retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                     retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._doCall('POST', path, json_arguments=json_arguments, include_org_id=include_org_id,
                            json_output_expected=json_output_expected, async_call=async_call,
                            retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                            retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def do_put_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False,
                    retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                    retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._doCall('PUT', path, json_arguments=json_arguments, include_org_id=include_org_id,
                            json_output_expected=json_output_expected, async_call=async_call,
                            retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                            retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def do_delete_call(self, path, json_arguments = None, include_org_id=True, json_output_expected=True, async_call=False,
                       retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                       retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._doCall('DELETE', path, json_arguments=json_arguments, include_org_id=include_org_id,
                            json_output_expected=json_output_expected, async_call=async_call,
                            retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                            retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def _doCall(self, method, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False,
                skip_product_version_check=False, params=None,
                retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached,
                return_headers: bool = False):

        if self.version is None and not skip_product_version_check:
            self.collect_pce_infos()

        url = self._make_url(path, include_org_id)

        headers = {'Accept': 'application/json'}

        if json_arguments is not None:
            headers['Content-Type'] = 'application/json'

        if async_call:
            headers['Prefer'] = 'respond-async'

        while True:

            log.info("Request URL: " + url)

            try:
                req = self._cached_session.request(method, url, headers=headers, auth=(self.api_user, self.api_key),
                                       verify=(not self.skipSSLCertCheck), json=json_arguments, params=params)
            except Exception as e:
                raise pylo.PyloApiEx("PCE connectivity or low level issue: {}".format(e))


            answerSize = len(req.content) / 1024
            log.info("URL downloaded (size "+str( int(answerSize) )+"KB) Reply headers:\n" +
                     "HTTP " + method + " " + url + " STATUS " + str(req.status_code) + " " + req.reason)
            log.info(req.headers)
            # log.info("Request Body:" + pylo.nice_json(json_arguments))
            # log.info("Request returned code "+ str(req.status_code) + ". Raw output:\n" + req.text[0:2000])

            if async_call:
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
                    jobPoll = self.do_get_call(jobLocation, include_org_id=False)
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
                dataset = self.do_get_call(resultHref, include_org_id=False)

                return dataset

            if method == 'GET' and req.status_code != 200 \
                    or\
                    method == 'POST' and req.status_code != 201 and req.status_code != 204 and req.status_code != 200 \
                    or\
                    method == 'DELETE' and req.status_code != 204 \
                    or \
                    method == 'PUT' and req.status_code != 204 and req.status_code != 200:

                if req.status_code == 429:  # too many requests sent in short amount of time? [{"token":"too_many_requests_error", ....}]
                    jout = req.json()
                    if len(jout) > 0:
                        if "token" in jout[0]:
                            if jout[0]['token'] == 'too_many_requests_error':
                                if retry_count_if_api_call_limit_reached < 1:
                                    raise pylo.PyloApiTooManyRequestsEx('API has hit DOS protection limit (X calls per minute)', jout)

                                retry_count_if_api_call_limit_reached = retry_count_if_api_call_limit_reached - 1
                                log.info("API has returned 'too_many_requests_error', we will sleep for {} seconds and retry {} more times".format(retry_wait_time_if_api_call_limit_reached,
                                                                                                                                                   retry_count_if_api_call_limit_reached))
                                time.sleep(retry_wait_time_if_api_call_limit_reached)
                                continue


                raise pylo.PyloApiEx('API returned error status "' + str(req.status_code) + ' ' + req.reason
                                + '" and error message: ' + req.text)

            if return_headers:
                return req.headers

            if json_output_expected:
                log.info("Parsing API answer to JSON (with a size of " + str( int(answerSize) ) + "KB)")
                jout = req.json()
                log.info("Done!")
                if answerSize < 5:
                    log.info("Resulting JSON object:")
                    log.info(json.dumps(jout, indent=2, sort_keys=True))
                else:
                    log.info("Answer is too large to be printed")
                return jout

            return req.text

        raise pylo.PyloApiEx("Unexpected API output or race condition")

    def getSoftwareVersion(self) -> Optional['pylo.SoftwareVersion']:
        self.collect_pce_infos()
        return self.version

    def getSoftwareVersionString(self) -> str:
        self.collect_pce_infos()
        return self.version_string

    def get_objects_count_by_type(self, object_type: str) -> int:

        def extract_count(headers: requests.Response):
            count = headers.get('x-total-count')
            if count is None:
                raise pylo.PyloApiEx('API didnt provide field "x-total-count"')

            return int(count)

        if object_type == 'workloads':
            return extract_count(self.do_get_call('/workloads', async_call=False, return_headers=True))
        elif object_type == 'virtual_services':
            return extract_count(self.do_get_call('/sec_policy/draft/virtual_services', async_call=False, return_headers=True))
        elif object_type == 'labels':
            return extract_count(self.do_get_call('/labels', async_call=False, return_headers=True))
        elif object_type == 'labelgroups':
            return extract_count(self.do_get_call('/sec_policy/draft/label_groups', async_call=False, return_headers=True))
        elif object_type == 'iplists':
            return extract_count(self.do_get_call('/sec_policy/draft/ip_lists', async_call=False, return_headers=True))
        elif object_type == 'services':
            return extract_count(self.do_get_call('/sec_policy/draft/services', async_call=False, return_headers=True))
        elif object_type == 'rulesets':
            return extract_count(self.do_get_call('/sec_policy/draft/rule_sets', async_call=False, return_headers=True))
        elif object_type == 'security_principals':
            return extract_count(self.do_get_call('/security_principals', async_call=False, return_headers=True))
        else:
            raise pylo.PyloEx("Unsupported object type '{}'".format(object_type))

    def get_pce_objects(self, include_deleted_workloads=False, list_of_objects_to_load: Optional[List[str]] = None):

        object_to_load = {}
        if list_of_objects_to_load is not None:
            all_types = pylo.APIConnector.get_all_object_types()
            for object_type in list_of_objects_to_load:
                if object_type not in all_types:
                    raise pylo.PyloEx("Unknown object type '{}'".format(object_type))
                object_to_load[object_type] = True
        else:
            object_to_load = pylo.APIConnector.get_all_object_types()

        threads_count = 4
        data = pylo.Organization.create_fake_empty_config()
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
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['workloads'] = self.objects_workload_get(include_deleted=include_deleted_workloads)
                        else:
                            data['workloads'] = self.objects_workload_get(include_deleted=include_deleted_workloads, async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'virtual_services':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['virtual_services'] = self.objects_virtual_service_get()
                        else:
                            data['virtual_services'] = self.objects_virtual_service_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'labels':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['labels'] = self.objects_label_get()
                        else:
                            data['labels'] = self.objects_label_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'labelgroups':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['labelgroups'] = self.objects_labelgroup_get()
                        else:
                            data['labelgroups'] = self.objects_labelgroup_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'iplists':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['iplists'] = self.objects_iplist_get()
                        else:
                            data['iplists'] = self.objects_iplist_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'services':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['services'] = self.objects_service_get()
                        else:
                            data['services'] = self.objects_service_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'rulesets':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['rulesets'] = self.objects_ruleset_get()
                        else:
                            data['rulesets'] = self.objects_ruleset_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

                    elif object_type == 'security_principals':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['security_principals'] = self.objects_securityprincipal_get()
                        else:
                            data['security_principals'] = self.objects_securityprincipal_get(async_mode=False, max_results=default_max_objects_for_sync_calls)

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

        for type in object_to_load.keys():
            thread_queue.put((type, errors,))

        thread_queue.join()

        if len(errors) > 0:
            raise errors[0]

        return data

    def collect_pce_infos(self):
        if self.version is not None:  # Make sure we collect data only once
            return
        path = "/product_version"
        jout = self.do_get_call(path, include_org_id=False, skip_product_version_check=True)

        self.version_string = jout['version']
        self.version = pylo.SoftwareVersion(jout['long_display'])

    def policy_check(self, protocol, port=None, src_ip=None, src_href=None, dst_ip=None, dst_href=None,
                     retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                     retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

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

        return self.do_get_call(path=path, async_call=False,
                                retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                                retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def rule_coverage_query(self, data):
        return self.do_post_call(path='/sec_policy/draft/rule_coverage', json_arguments=data, include_org_id=True, json_output_expected=True, async_call=False)

    def objects_label_get(self, max_results: int = None, async_mode=True):
        path = '/labels'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_label_update(self, href: str, data: Dict[str, Any]):
        path = href
        return self.do_put_call(path=path, json_arguments=data, json_output_expected=False, include_org_id=False)

    def objects_label_delete(self, href: str):
        """

        :type href: str|pylo.Label
        """
        path = href
        if type(href) is pylo.Label:
            path = href.href

        return self.do_delete_call(path=path, json_output_expected=False, include_org_id=False)

    def objects_label_create(self, label_name: str, label_type: str):
        path = '/labels'
        if label_type != 'app' and label_type != 'env' and label_type != 'role' and label_type != 'loc':
            raise Exception("Requested to create a Label '%s' with wrong type '%s'" % (label_name, label_type))
        data = {'key': label_type, 'value': label_name}
        return self.do_post_call(path=path, json_arguments=data)

    def objects_labelgroup_get(self, max_results: int = None, async_mode=True):
        path = '/sec_policy/draft/label_groups'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_virtual_service_get(self, max_results: int = None, async_mode=True):
        path = '/sec_policy/draft/virtual_services'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_iplist_get(self, max_results: int = None, async_mode=True, search_name: str = None):
        path = '/sec_policy/draft/ip_lists'
        data = {}

        if search_name is not None:
            data['name'] = search_name

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_iplist_create(self, json_blob):
        path = '/sec_policy/draft/ip_lists'
        return self.do_post_call(path=path, json_arguments=json_blob)

    def objects_iplists_get_default_any(self) -> Optional[str]:
        response = self.objects_iplist_get(max_results=10, async_mode=False, search_name='0.0.0.0')

        for item in response:
            if item['created_by']['href'] == '/users/0':
                return item['href']

        return None

    def objects_workload_get(self, include_deleted=False, filter_by_ip: str = None, max_results: int = None, async_mode=True):
        path = '/workloads'
        data = {}

        if include_deleted:
            data['include_deleted'] = 'yes'

        if filter_by_ip is not None:
            data['ip_address'] = filter_by_ip

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_workload_agent_upgrade(self, workload_href: str, target_version: str):
        path = '{}/upgrade'.format(workload_href)
        data = {"release": target_version}

        return self.do_post_call(path=path, json_arguments=data, json_output_expected=False, include_org_id=False)

    def objects_workload_update(self, href: str, data):
        path = href

        return self.do_put_call(path=path, json_arguments=data, json_output_expected=False, include_org_id=False)

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

        return self.do_delete_call(path=path, json_output_expected=False, include_org_id=False)

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

        def execute(self, unpair_agents=False):

            if len(self._hrefs) < 1:
                raise pylo.PyloEx("WorkloadMultiDeleteTracker is empty")

            result = self.connector.objects_workload_delete_multi(list(self._hrefs.keys()))
            # print(pylo.nice_json(result))
            if not type(result) is list:
                raise pylo.PyloApiEx("API didnt return expected JSON format", result)

            agents_to_unpair = []

            for entry in result:
                if not type(entry) is dict:
                    raise pylo.PyloApiEx("API didnt return expected JSON format", entry)
                href = entry.get("href")
                if href is None or type(href) is not str:
                    raise pylo.PyloApiEx("API didnt return expected JSON format", entry)

                error = entry.get("errors")
                error_string = json.dumps(error)
                if unpair_agents and error_string.find("method_not_allowed_error") > -1:
                    agents_to_unpair.append(href)
                else:
                    if href is not None:
                        self._errors[href] = error_string

            if len(agents_to_unpair) > 0:
                self._unpair_agents(agents_to_unpair)

        def _unpair_agents(self, workloads_hrefs: [str]):
            for href in workloads_hrefs:
                retryCount = 5
                api_result = None

                while retryCount >= 0:
                    retryCount -= 1
                    try:
                        api_result = self.connector.objects_workload_unpair_multi([href])
                        break

                    except pylo.PyloApiTooManyRequestsEx as ex:
                        if retryCount <= 0:
                            self._errors[href] = str(ex)
                            break
                        time.sleep(6)

                    except pylo.PyloApiEx as ex:
                        self._errors[href] = str(ex)
                        break

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

        # print(json_data)

        path = "/workloads/bulk_delete"

        return self.do_put_call(path=path, json_arguments=json_data, json_output_expected=True)

    def objects_workload_unpair_multi(self, href_or_workload_array):
        """

        :type href_or_workload_array: list[str]|list[pylo.Workload]
        """

        if len(href_or_workload_array) < 1:
            return

        json_data = {
            "ip_table_restore": "disable",
            "workloads": []
        }

        if type(href_or_workload_array[0]) is str:
            for href in href_or_workload_array:
                json_data['workloads'].append({"href": href})
        else:
            href: 'pylo.Workload'
            for href in href_or_workload_array:
                json_data['workloads'].append({"href": href.href})

        # print(json_data)

        path = "/workloads/unpair"

        return self.do_put_call(path=path, json_arguments=json_data, json_output_expected=False)

    def objects_workload_create_single_unmanaged(self, json_object):
        path = '/workloads'
        return self.do_post_call(path=path, json_arguments=json_object)

    def objects_workload_create_bulk_unmanaged(self, json_object):
        path = '/workloads/bulk_create'
        return self.do_put_call(path=path, json_arguments=json_object)

    def objects_service_get(self, max_results: int = None, async_mode=True):
        path = '/sec_policy/draft/services'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_service_delete(self, href):
        """

        :type href: str|pylo.Workload
        """
        path = href
        if type(href) is pylo.Service:
            path = href.href

        return self.do_delete_call(path=path, json_output_expected=False, include_org_id=False)

    def objects_ruleset_get(self, max_results: int = None, async_mode=True):
        path = '/sec_policy/draft/rule_sets'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_ruleset_create(self, name: str,
                               scope_app: 'pylo.Label' = None,
                               scope_env: 'pylo.Label' = None,
                               scope_loc: 'pylo.Label' = None,
                               description: str = '', enabled: bool = True) -> Dict:
        path = '/sec_policy/draft/rule_sets'

        scope = []
        if scope_app is not None:
            scope.append(scope_app.get_api_reference_json())
        if scope_env is not None:
            scope.append(scope_env.get_api_reference_json())
        if scope_app is not None:
            scope.append(scope_loc.get_api_reference_json())

        data = {
            'name': name,
            'enabled': enabled,
            'description': description,
            'scopes': [scope]
        }

        return self.do_post_call(path=path, json_arguments=data, json_output_expected=True)

    def objects_ruleset_update(self, ruleset_href: str, update_data):
        return self.do_put_call(path=ruleset_href,
                                json_arguments=update_data,
                                include_org_id=False,
                                json_output_expected=False
                                )

    def objects_ruleset_delete(self, ruleset_href: str):
        return self.do_delete_call(path=ruleset_href,
                                   include_org_id=False,
                                   json_output_expected=False
                                   )

    def objects_rule_update(self, rule_href: str, update_data):
        return self.do_put_call(path=rule_href,
                                json_arguments=update_data,
                                include_org_id=False,
                                json_output_expected=False
                                )

    def objects_rule_delete(self, rule_href: str):
        return self.do_delete_call(path=rule_href,
                                   include_org_id=False,
                                   json_output_expected=False
                                   )

    def objects_rule_create(self, ruleset_href: str,
                            intra_scope: bool,
                            consumers: List[Union['pylo.IPList', 'pylo.Label', 'pylo.LabelGroup', Dict]],
                            providers: List[Union['pylo.IPList', 'pylo.Label', 'pylo.LabelGroup', Dict]],
                            services: List[Union['pylo.Service', 'pylo.DirectServiceInRule', Dict]],
                            description='', machine_auth=False, secure_connect=False, enabled=True,
                            stateless=False, consuming_security_principals=[],
                            resolve_consumers_as_virtual_services=True, resolve_consumers_as_workloads=True,
                            resolve_providers_as_virtual_services=True, resolve_providers_as_workloads=True) -> Dict[str, Any]:

        resolve_consumers = []
        if resolve_consumers_as_virtual_services:
            resolve_consumers.append('virtual_services')
        if resolve_consumers_as_workloads:
            resolve_consumers.append('workloads')

        resolve_providers = []
        if resolve_providers_as_virtual_services:
            resolve_providers.append('virtual_services')
        if resolve_providers_as_workloads:
            resolve_providers.append('workloads')

        consumers_json = []
        for item in consumers:
            if type(item) is dict:
                consumers_json.append(item)
            else:
                consumers_json.append(item.get_api_reference_json())

        providers_json = []
        for item in providers:
            if type(item) is dict:
                providers_json.append(item)
            else:
                providers_json.append(item.get_api_reference_json())

        services_json = []
        for item in services:
            if type(item) is dict:
                services_json.append(item)
            elif type(item) is pylo.DirectServiceInRule:
                services_json.append(item.get_api_json())
            else:
                providers_json.append(item.get_api_reference_json())

        data = {
            'unscoped_consumers': not intra_scope,
            'description': description,
            'machine_auth':  machine_auth,
            'sec_connect': secure_connect,
            'enabled': enabled,
            'stateless': stateless,
            'consuming_security_principals': consuming_security_principals,
            'resolve_labels_as': {'providers': resolve_providers, 'consumers': resolve_consumers,},
            'consumers': consumers_json,
            'providers': providers_json,
            'ingress_services': services_json
        }

        path = ruleset_href+'/sec_rules'

        return self.do_post_call(path, json_arguments=data, json_output_expected=True, include_org_id=False)

    def objects_securityprincipal_get(self, max_results: int = None, async_mode=True) -> Dict[str, Any]:
        path = '/security_principals'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_securityprincipal_create(self, name: str = None, sid: str = None, json_data=None) -> str:
        """

        :param name: friendly name for this object
        :param sid: Windows SID for that Group
        :param json_data:
        :return: HREF of the created Security Principal
        """
        path = '/security_principals'

        if json_data is not None and name is not None:
            raise pylo.PyloApiEx("You must either use json_data or name but you cannot use both they are mutually exclusive")

        if json_data is not None:
            return get_field_or_die('href', self.do_post_call(path=path, json_arguments=json_data))

        if name is None:
            raise pylo.PyloApiEx("You need to provide a group name")
        if sid is None:
            raise pylo.PyloApiEx("You need to provide a SID")

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

        def get_failed_items(self) -> Dict[str, 'pylo.APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem']:
            results: Dict[str, 'pylo.APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem'] = {}
            for infos in self._items.values():
                if infos.status != 'green':
                    results[infos.name] = infos

            return results

    def agent_get_compatibility_report(self, agent_href: str = None, agent_id: str = None, return_raw_json=True) -> 'pylo.APIConnector.ApiAgentCompatibilityReport':
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
        api_result = None

        while retryCount >= 0:
            retryCount -= 1
            try:
                api_result = self.do_get_call(path=path, include_org_id=False)
                break

            except pylo.PyloApiTooManyRequestsEx as ex:
                if retryCount <= 0:
                    raise ex
                time.sleep(6)

        return APIConnector.ApiAgentCompatibilityReport(api_result)

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

        return self.do_put_call(path, json_arguments=data, include_org_id=False, json_output_expected=False)

    def objects_agent_reassign_pce(self, agent_href: str, target_pce: str):
        path = agent_href + '/update'
        data = {"target_pce_fqdn": target_pce}
        return self.do_put_call(path, json_arguments=data, include_org_id=False, json_output_expected=False)

    class ExplorerFilterSetV1:
        exclude_processes_emulate: Dict[str,str]
        _exclude_processes: List[str]
        _exclude_direct_services: List['pylo.DirectServiceInRule']
        _time_from: Optional[datetime]
        _time_to: Optional[datetime]
        _policy_decision_filter: List[str]
        _consumer_labels: Dict[str, Union['pylo.Label', 'pylo.LabelGroup']]
        __filter_provider_ip_exclude: List[str]
        __filter_consumer_ip_exclude: List[str]
        __filter_provider_ip_include: List[str]
        __filter_consumer_ip_include: List[str]

        def __init__(self, max_results=10000):
            self.__filter_consumer_ip_exclude = []
            self.__filter_provider_ip_exclude = []
            self.__filter_consumer_ip_include = []
            self.__filter_provider_ip_include = []
            self._consumer_labels = {}
            self._consumer_exclude_labels = {}
            self._provider_labels = {}
            self._provider_exclude_labels = {}

            self._consumer_workloads = {}
            self._provider_workloads = {}

            self.max_results = max_results
            self._policy_decision_filter = []
            self._time_from = None
            self._time_to = None
            self._exclude_broadcast = False
            self._exclude_multicast = False
            self._exclude_direct_services = []
            self.exclude_processes_emulate = {}
            self._exclude_processes = []

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

        def consumer_include_workload(self, workload_or_href:Union[str, 'pylo.Workload']):
            if isinstance(workload_or_href, str):
                self._consumer_workloads[workload_or_href] = workload_or_href
                return

            if isinstance(workload_or_href, pylo.Workload):
                self._consumer_workloads[workload_or_href.href] = workload_or_href.href
                return

            raise pylo.PyloEx("Unsupported object type {}".format(type(workload_or_href)))

        def provider_include_workload(self, workload_or_href:Union[str, 'pylo.Workload']):
            if isinstance(workload_or_href, str):
                self._provider_workloads[workload_or_href] = workload_or_href
                return

            if isinstance(workload_or_href, pylo.Workload):
                self._provider_workloads[workload_or_href.href] = workload_or_href.href
                return

            raise pylo.PyloEx("Unsupported object type {}".format(type(workload_or_href)))

        def consumer_exclude_cidr(self, ipaddress: str):
            self.__filter_consumer_ip_exclude.append(ipaddress)

        def consumer_exclude_ip4map(self, map: 'pylo.IP4Map'):
            for item in map.to_list_of_cidr_string():
                self.consumer_exclude_cidr(item)

        def consumer_include_cidr(self, ipaddress: str):
            self.__filter_consumer_ip_include.append(ipaddress)

        def consumer_include_ip4map(self, map: 'pylo.IP4Map'):
            for item in map.to_list_of_cidr_string(skip_netmask_for_32=True):
                self.consumer_include_cidr(item)

        def provider_include_label(self, label_or_href):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._provider_labels, label_or_href)

        def provider_exclude_label(self, label_or_href: Union[str, 'pylo.Label', 'pylo.LabelGroup']):
            """

            @type label_or_href: str|pylo.Label|pylo.LabelGroup
            """
            self.__filter_prop_add_label(self._provider_exclude_labels, label_or_href)

        def provider_exclude_cidr(self, ipaddress: str):
            self.__filter_provider_ip_exclude.append(ipaddress)

        def provider_exclude_ip4map(self, map: 'pylo.IP4Map'):
            for item in map.to_list_of_cidr_string(skip_netmask_for_32=True):
                self.provider_exclude_cidr(item)

        def provider_include_cidr(self, ipaddress: str):
            self.__filter_provider_ip_include.append(ipaddress)

        def provider_include_ip4map(self, map: 'pylo.IP4Map'):
            for item in map.to_list_of_cidr_string():
                self.provider_include_cidr(item)

        def service_exclude_add(self, service: 'pylo.DirectServiceInRule'):
            self._exclude_direct_services.append(service)

        def process_exclude_add(self, process_name: str, emulate_on_client=False):
            if emulate_on_client:
                self.exclude_processes_emulate[process_name] = process_name
            else:
                self._exclude_processes.append(process_name)

        def set_exclude_broadcast(self, exclude=True):
            self._exclude_broadcast = exclude

        def set_exclude_multicast(self, exclude=True):
            self._exclude_multicast = exclude

        def set_time_from(self, time: datetime):
            self._time_from = time

        def set_time_from_x_seconds_ago(self, seconds: int):
            self._time_from = datetime.utcnow() - timedelta(seconds=seconds)

        def set_time_from_x_days_ago(self, days: int):
            return self.set_time_from_x_seconds_ago(days*60*60*24)

        def set_max_results(self, max: int):
            self.max_results = max

        def set_time_to(self, time: datetime):
            self._time_to = time

        def filter_on_policy_decision_blocked(self):
            self._policy_decision_filter.append('blocked')

        def filter_on_policy_decision_potentially_blocked(self):
            self._policy_decision_filter.append('potentially_blocked')

        def filter_on_policy_decision_all_blocked(self):
            self.filter_on_policy_decision_blocked()
            self.filter_on_policy_decision_potentially_blocked()

        def filter_on_policy_decision_allowed(self):
            self._policy_decision_filter.append('allowed')

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
                "policy_decisions": self._policy_decision_filter,
                "max_results": self.max_results
                }

            if self._exclude_broadcast:
                filters['destinations']['exclude'].append({'transmission': 'broadcast'})

            if self._exclude_multicast:
                filters['destinations']['exclude'].append({'transmission': 'multicast'})

            if self._time_from is not None:
                filters["start_date"] = self._time_from.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                filters["start_date"] = "2010-10-13T11:27:28.824Z",

            if self._time_to is not None:
                filters["end_date"] = self._time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                filters["end_date"] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            if len(self._consumer_labels) > 0:
                tmp = []
                for label_href in self._consumer_labels.keys():
                    tmp.append({'label': {'href': label_href}})
                filters['sources']['include'].append(tmp)

            if len(self._consumer_workloads) > 0:
                tmp = []
                for workload_href in self._consumer_workloads.keys():
                    tmp.append({'workload': {'href': workload_href}})
                filters['sources']['include'].append(tmp)

            if len(self.__filter_consumer_ip_include) > 0:
                tmp = []
                for ip_txt in self.__filter_consumer_ip_include:
                    tmp.append({'ip_address': ip_txt})
                filters['sources']['include'].append(tmp)

            if len(self._provider_labels) > 0:
                tmp = []
                for label_href in self._provider_labels.keys():
                    tmp.append({'label': {'href': label_href}})
                filters['destinations']['include'].append(tmp)

            if len(self._provider_workloads) > 0:
                tmp = []
                for workload_href in self._provider_workloads.keys():
                    tmp.append({'workload': {'href': workload_href}})
                filters['destinations']['include'].append(tmp)

            if len(self.__filter_provider_ip_include) > 0:
                tmp = []
                for ip_txt in self.__filter_provider_ip_include:
                    tmp.append({'ip_address': ip_txt})
                filters['destinations']['include'].append(tmp)

            consumer_exclude_json = []
            if len(self._consumer_exclude_labels) > 0:
                for label_href in self._consumer_exclude_labels.keys():
                    filters['sources']['exclude'].append({'label': {'href': label_href}})

            if len(self.__filter_consumer_ip_exclude) > 0:
                for ipaddress in self.__filter_consumer_ip_exclude:
                    filters['sources']['exclude'].append({'ip_address': ipaddress})

            provider_exclude_json = []
            if len(self._provider_exclude_labels) > 0:
                for label_href in self._provider_exclude_labels.keys():
                    filters['destinations']['exclude'].append({'label': {'href': label_href}})

            if len(self.__filter_provider_ip_exclude) > 0:
                for ipaddress in self.__filter_provider_ip_exclude:
                    filters['destinations']['exclude'].append({'ip_address': ipaddress})

            if len(self._exclude_direct_services) > 0:
                for service in self._exclude_direct_services:
                    filters['services']['exclude'].append(service.get_api_json())

            if len(self._exclude_processes) > 0:
                for process in self._exclude_processes:
                    filters['services']['exclude'].append({'process_name': process})

            return filters

    def explorer_search(self, filters: Union[Dict, 'pylo.APIConnector.ExplorerFilterSetV1']):
        """

        :param filters: should be an instance of ExplorerFilterSetV1 or a json payload built with your own logic
        :return:
        """
        path = "/traffic_flows/traffic_analysis_queries"
        if isinstance(filters, pylo.APIConnector.ExplorerFilterSetV1):
            data = filters.generate_json_query()
        else:
            data = filters
        result = pylo.ExplorerResultSetV1(self.do_post_call(path, json_arguments=data, include_org_id=True, json_output_expected=True),
                                                  owner=self, emulated_process_exclusion=filters.exclude_processes_emulate)

        return result

    class ClusterHealth:

        allowed_status_list = {'normal': True, 'warning': True, 'error': True}

        class ClusterHealthNode:

            class ServiceStatus:

                allowed_status = {'running': True, 'stopped': True, 'partial': True, 'not_running': True}

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
                raise pylo.PyloEx("ClusterHealth has unsupported status '{}'".format(self._status))

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
        json_output = self.do_get_call(path, include_org_id=False)
        if type(json_output) is not list:
            raise pylo.PyloEx("A list object was expected but we received a '{}' instead".format(type(json_output)))

        dict_of_health_reports = {}

        for single_output in json_output:
            new_report = APIConnector.ClusterHealth(single_output)
            dict_of_health_reports[new_report.fqdn] = new_report

        return dict_of_health_reports

    class RuleSearchQuery:
        _advanced_mode_consumer_labels: Dict[str, 'pylo.Label']
        _advanced_mode_provider_labels: Dict[str, 'pylo.Label']
        _basic_mode_labels: Dict[str, 'pylo.Label']
        connector: 'pylo.APIConnector'

        def __init__(self, connector: 'pylo.APIConnector'):
            self.connector = connector
            self.mode_is_basic = True
            self._basic_mode_labels = {}
            self._advanced_mode_provider_labels = {}
            self._advanced_mode_consumer_labels = {}
            self._exact_matches = True
            self._mode_is_draft = True

        def set_basic_mode(self):
            self.mode_is_basic = True
            self._advanced_mode_provider_labels = {}
            self._advanced_mode_consumer_labels = {}

        def set_advanced_mode(self):
            self.mode_is_basic = False
            self._basic_mode_labels = {}

        def set_draft_mode(self):
            self._mode_is_draft = True

        def set_active_mode(self):
            self._mode_is_draft = False

        def add_label(self, label: 'pylo.Label'):
            if not self.mode_is_basic:
                raise pylo.PyloEx('You can add labels to RuleSearchQuery only in Basic mode. Use consumer/provider counterparts with Advanced mode')
            self._basic_mode_labels[label.href] = label

        def add_consumer_label(self, label: 'pylo.Label'):
            if self.mode_is_basic:
                raise pylo.PyloEx('You can add labels to RuleSearchQuery consumers only in Advanced mode')
            self._advanced_mode_consumer_labels[label.href] = label

        def add_provider_label(self, label: 'pylo.Label'):
            if self.mode_is_basic:
                raise pylo.PyloEx('You can add labels to RuleSearchQuery providers only in Advanced mode')
            self._advanced_mode_provider_labels[label.href] = label

        def use_exact_matches(self):
            self._exact_matches = True

        def use_resolved_matches(self):
            self._exact_matches = False

        def execute(self):
            data = {}
            if not self._exact_matches:
                data['resolve_actors'] = True

            uri = '/sec_policy/draft/rule_search'
            if not self._mode_is_draft:
                uri = '/sec_policy/active/rule_search'

            if self.mode_is_basic:
                if len(self._basic_mode_labels) > 0:
                    data['providers_or_consumers'] = []
                    for label_href in self._basic_mode_labels.keys():
                        data['providers_or_consumers'].append({'label': {'href': label_href}})
            else:
                if len(self._advanced_mode_provider_labels) > 0:
                    data['providers'] = []
                    for label_href in self._advanced_mode_provider_labels.keys():
                        data['providers'].append({'label': {'href': label_href}})
                if len(self._advanced_mode_consumer_labels) > 0:
                    data['consumers'] = []
                    for label_href in self._advanced_mode_consumer_labels.keys():
                        data['consumers'].append({'label': {'href': label_href}})

            # print(data)
            return self.connector.do_post_call(uri, data)

        def execute_and_resolve(self, organization: 'pylo.Organization'):
            return APIConnector.RuleSearchQuery.RuleSearchQueryResolvedResultSet(self.execute(), organization)

        class RuleSearchQueryResolvedResultSet:
            rules: Dict[str, 'pylo.Rule']
            rules_per_ruleset: Dict['pylo.Ruleset', Dict[str, 'pylo.Rule']]

            def count_results(self):
                return len(self.rules)

            def __init__(self, raw_json_data, organization: 'pylo.Organization'):
                self._raw_json = raw_json_data
                self.rules = {}
                self.rules_per_ruleset = {}

                for rule_data in raw_json_data:
                    rule_href = rule_data.get('href')
                    if rule_href is None:
                        raise pylo.PyloEx('Cannot find rule HREF in RuleSearchQuery response', rule_data)
                    rule_found = organization.RulesetStore.find_rule_by_href(rule_href)
                    if rule_found is None:
                        raise pylo.PyloEx("Cannot find rule with HREF '{}' in Organization".format(rule_href), rule_data)

                    self.rules[rule_found.href] = rule_found
                    ruleset_found = self.rules_per_ruleset.get(rule_found.owner)
                    if ruleset_found is None:
                        # print("new ruleset")
                        self.rules_per_ruleset[rule_found.owner] = {rule_found.href: rule_found}
                    else:
                        # print("existing rs")
                        self.rules_per_ruleset[rule_found.owner][rule_found.href] = rule_found

    def new_RuleSearchQuery(self):
        return self.RuleSearchQuery(self)

