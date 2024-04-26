import json
import time
import getpass

from .CredentialsManager import is_api_key_encrypted, decrypt_api_key, CredentialProfile
from .JsonPayloadTypes import LabelGroupObjectJsonStructure, LabelObjectCreationJsonStructure, \
    LabelObjectJsonStructure, LabelObjectUpdateJsonStructure, PCEObjectsJsonStructure, \
    LabelGroupObjectUpdateJsonStructure, IPListObjectCreationJsonStructure, IPListObjectJsonStructure, \
    VirtualServiceObjectJsonStructure, RuleCoverageQueryEntryJsonStructure, RulesetObjectUpdateStructure, \
    WorkloadHrefRef, IPListHrefRef, VirtualServiceHrefRef, RuleDirectServiceReferenceObjectJsonStructure, \
    RulesetObjectJsonStructure, WorkloadObjectJsonStructure, SecurityPrincipalObjectJsonStructure, \
    LabelDimensionObjectStructure, AuditLogApiReplyEventJsonStructure, WorkloadsGetQueryLabelFilterJsonStructure, \
    NetworkDeviceObjectJsonStructure, NetworkDeviceEndpointObjectJsonStructure, HrefReference, \
    WorkloadObjectCreateJsonStructure, WorkloadObjectMultiCreateJsonRequestPayload, \
    WorkloadBulkUpdateEntryJsonStructure, WorkloadBulkUpdateResponseEntry, VenObjectJsonStructure

try:
    import requests as requests
except ImportError:
    import requests

from threading import Thread
from queue import Queue
import illumio_pylo as pylo
from illumio_pylo import log
from typing import Union, Dict, Any, List, Optional, Literal

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


ObjectTypes = Literal['iplists', 'workloads', 'virtual_services', 'labels', 'labelgroups', 'services', 'rulesets',
                     'security_principals', 'label_dimensions']

all_object_types: Dict[ObjectTypes, ObjectTypes] = {
        'iplists': 'iplists',
        'workloads': 'workloads',
        'virtual_services': 'virtual_services',
        'labels': 'labels',
        'labelgroups': 'labelgroups',
        'services': 'services',
        'rulesets': 'rulesets',
        'security_principals': 'security_principals',
        'label_dimensions': 'label_dimensions'
    }


class APIConnector:
    """docstring for APIConnector."""

    def __init__(self, fqdn: str, port, api_user: str, api_key: str, skip_ssl_cert_check=False, org_id=1, name='unnamed'):
        self.name = name
        self.fqdn: str = fqdn
        if type(port) is int:
            port = str(port)
        self.port: int = port
        self._api_key: str = api_key
        self._decrypted_api_key: str = None
        self.api_user: str = api_user
        self.org_id: int = org_id
        self.skipSSLCertCheck: bool = skip_ssl_cert_check
        self.version: Optional['pylo.SoftwareVersion'] = None
        self.version_string: str = "Not Defined"
        self._cached_session = requests.session()

    @property
    def api_key(self):
        if self._decrypted_api_key is not None:
            return self._decrypted_api_key
        if is_api_key_encrypted(self._api_key):
            self._decrypted_api_key = decrypt_api_key(self._api_key)
            return self._decrypted_api_key
        return self._api_key


    @staticmethod
    def get_all_object_types_names_except(exception_list: List[ObjectTypes]):

        if len(exception_list) == 0:
            return all_object_types.values()

        # first let's check that all names in exception_list are valid (case mismatches and typos...)
        for name in exception_list:
            if name not in all_object_types:
                raise pylo.PyloEx("object type named '{}' doesn't exist. The list of supported objects names is: {}".
                                  format(name, pylo.string_list_to_text(all_object_types.values())))

        object_names_list: List[str] = []
        for name in all_object_types.values():
            if name not in exception_list:
                object_names_list.append(name)

    @staticmethod
    def get_all_object_types():
        return all_object_types.copy()

    @staticmethod
    def create_from_credentials_object(credentials: CredentialProfile) -> Optional['APIConnector']:
        return APIConnector(credentials.fqdn, credentials.port, credentials.api_user,
                            credentials.api_key, skip_ssl_cert_check=not credentials.verify_ssl,
                            org_id=credentials.org_id, name=credentials.name)

    @staticmethod
    def create_from_credentials_in_file(fqdn_or_profile_name: str, request_if_missing: bool = False,
                                        credential_file: Optional[str] = None) -> Optional['APIConnector']:

        credentials = pylo.get_credentials_from_file(fqdn_or_profile_name, credential_file)

        if credentials is not None:
            return APIConnector.create_from_credentials_object(credentials)

        if not request_if_missing:
            return None

        print('Cannot find credentials for host "{}".\nPlease input an API user:'.format(fqdn_or_profile_name), end='')
        user = input()
        print('API password:', end='')
        password = getpass.getpass()
        print('Server port:', end='')
        port = int(input())
        print('A name for this connection (ie: MyCompany PROD')
        name = input()

        connector = pylo.APIConnector(fqdn_or_profile_name, port, user, password, skip_ssl_cert_check=True, name=name)
        return connector

    def _make_base_url(self, path: str='') -> str:
        # remove leading '/' from path if exists
        if len(path) > 0 and path[0] == '/':
            path = path[1:]
        url = "https://{0}:{1}/{2}".format(self.fqdn, self.port, path)
        return url

    def _make_api_url(self, path: str = '', include_org_id=False) -> str:
        url = self._make_base_url('/api/v2')
        if include_org_id:
            url += '/orgs/' + str(self.org_id)
        url += path

        return url

    def do_get_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False, params=None, skip_product_version_check=False,
                    retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                    retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached,
                    return_headers: bool = False):

        return self._do_call('GET', path, json_arguments=json_arguments, include_org_id=include_org_id,
                             json_output_expected=json_output_expected, async_call=async_call, skip_product_version_check=skip_product_version_check, params=params,
                             retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                             retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached,
                             return_headers=return_headers)

    def do_post_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False, params=None,
                     retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                     retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._do_call('POST', path, json_arguments=json_arguments, include_org_id=include_org_id,
                             json_output_expected=json_output_expected, async_call=async_call, params=params,
                             retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                             retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def do_put_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False, params=None,
                    retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                    retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._do_call('PUT', path, json_arguments=json_arguments, include_org_id=include_org_id,
                             json_output_expected=json_output_expected, async_call=async_call, params=params,
                             retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                             retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def do_delete_call(self, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False, params=None,
                       retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                       retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached):

        return self._do_call('DELETE', path, json_arguments=json_arguments, include_org_id=include_org_id,
                             json_output_expected=json_output_expected, async_call=async_call, params=params,
                             retry_count_if_api_call_limit_reached=retry_count_if_api_call_limit_reached,
                             retry_wait_time_if_api_call_limit_reached=retry_wait_time_if_api_call_limit_reached)

    def _do_call(self, method, path, json_arguments=None, include_org_id=True, json_output_expected=True, async_call=False,
                 skip_product_version_check=False, params=None,
                 retry_count_if_api_call_limit_reached=default_retry_count_if_api_call_limit_reached,
                 retry_wait_time_if_api_call_limit_reached=default_retry_wait_time_if_api_call_limit_reached,
                 return_headers: bool = False):

        if self.version is None and not skip_product_version_check:
            self.collect_pce_infos()

        url = self._make_api_url(path, include_org_id)

        headers = {'Accept': 'application/json'}

        if json_arguments is not None:
            headers['Content-Type'] = 'application/json'

        if async_call:
            headers['Prefer'] = 'respond-async'

        while True:

            log.info("Request URL: " + url)

            try:
                req = self._cached_session.request(method, url, headers=headers, auth=(self.api_user, self.api_key),
                                                   verify=(not self.skipSSLCertCheck), json=json_arguments,
                                                   params=params)
            except Exception as e:
                raise pylo.PyloApiEx("PCE connectivity or low level issue: {}".format(e))

            answer_size = len(req.content) / 1024
            log.info("URL downloaded (size "+str( int(answer_size) )+"KB) Reply headers:\n" +
                     "HTTP " + method + " " + url + " STATUS " + str(req.status_code) + " " + req.reason)
            log.info(req.headers)
            # log.info("Request Body:" + pylo.nice_json(json_arguments))
            # log.info("Request returned code "+ str(req.status_code) + ". Raw output:\n" + req.text[0:2000])

            if async_call:
                if (method == 'GET' or method == 'POST') and req.status_code != 202:
                    orig_request = req.request  # type: requests.PreparedRequest
                    raise Exception("Status code for Async call should be 202 but " + str(req.status_code)
                                    + " " + req.reason + " was returned with the following body: " + req.text +
                                    "\n\n Request was: " + orig_request.url + "\nHEADERS: " + str(orig_request.headers) +
                                    "\nBODY:\n" + str(orig_request.body))

                if 'Location' not in req.headers:
                    raise Exception('Header "Location" was not found in API answer!')
                if 'Retry-After' not in req.headers:
                    raise Exception('Header "Retry-After" was not found in API answer!')

                job_location = req.headers['Location']
                retry_interval = int(req.headers['Retry-After'])

                retry_loop_times = 0

                while True:
                    log.info("Sleeping " + str(retry_interval) + " seconds before polling for job status, elapsed " + str(retry_interval*retry_loop_times) + " seconds so far" )
                    retry_loop_times += 1
                    time.sleep(retry_interval)
                    job_poll = self.do_get_call(job_location, include_org_id=False)
                    if 'status' not in job_poll:
                        raise Exception('Job polling request did not return a "status" field')
                    job_poll_status = job_poll['status']

                    if job_poll_status == 'failed':
                        if 'result' in job_poll and 'message' in job_poll['result']:
                            raise Exception('Job polling return with status "Failed": ' + job_poll['result']['message'])
                        else:
                            raise Exception('Job polling return with status "Failed": ' + job_poll)

                    if job_poll_status == 'done':
                        if 'result' not in job_poll:
                            raise Exception('Job is marked as done but has no "result"')
                        if 'href' not in job_poll['result']:
                            raise Exception("Job is marked as done but did not return a href to download resulting Dataset")

                        result_href = job_poll['result']['href']
                        break

                    log.info("Job status is " + job_poll_status)

                log.info("Job is done, we will now download the resulting dataset")
                dataset = self.do_get_call(result_href, include_org_id=False)

                return dataset

            if method == 'GET' and req.status_code != 200 \
                    or\
                    method == 'POST' and req.status_code != 201 and req.status_code != 204 and req.status_code != 200 and req.status_code != 202\
                    or\
                    method == 'DELETE' and req.status_code != 204 \
                    or \
                    method == 'PUT' and req.status_code != 204 and req.status_code != 200:

                if req.status_code == 429:
                    # too many requests sent in short amount of time? [{"token":"too_many_requests_error", ....}]
                    json_out = req.json()
                    if len(json_out) > 0:
                        if "token" in json_out[0]:
                            if json_out[0]['token'] == 'too_many_requests_error':
                                if retry_count_if_api_call_limit_reached < 1:
                                    raise pylo.PyloApiTooManyRequestsEx(
                                        'API has hit DOS protection limit (X calls per minute)', json_out)

                                retry_count_if_api_call_limit_reached = retry_count_if_api_call_limit_reached - 1
                                log.info(
                                    "API has returned 'too_many_requests_error', we will sleep for {} seconds and retry {} more times".format(
                                        retry_wait_time_if_api_call_limit_reached,
                                        retry_count_if_api_call_limit_reached))
                                time.sleep(retry_wait_time_if_api_call_limit_reached)
                                continue

                if req.status_code == 403:
                    raise pylo.PyloApiRequestForbiddenEx('API returned error status "' + str(req.status_code) + ' ' + req.reason
                                                        + '" and error message: ' + req.text)

                raise pylo.PyloApiEx('API returned error status "' + str(req.status_code) + ' ' + req.reason
                                + '" and error message: ' + req.text)

            if return_headers:
                return req.headers

            if json_output_expected:
                log.info("Parsing API answer to JSON (with a size of " + str( int(answer_size) ) + "KB)")
                json_out = req.json()
                log.info("Done!")
                if answer_size < 5:
                    log.info("Resulting JSON object:")
                    log.info(json.dumps(json_out, indent=2, sort_keys=True))
                else:
                    log.info("Answer is too large to be printed")
                return json_out

            return req.text

        raise pylo.PyloApiEx("Unexpected API output or race condition")

    def get_software_version(self) -> Optional['pylo.SoftwareVersion']:
        self.collect_pce_infos()
        return self.version

    def get_software_version_string(self) -> str:
        self.collect_pce_infos()
        return self.version_string

    def get_objects_count_by_type(self, object_type: str) -> int:

        def extract_count(headers):
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
        elif object_type == 'label_dimensions':
            return extract_count(self.do_get_call('/label_dimensions', async_call=False, return_headers=True))
        else:
            raise pylo.PyloEx("Unsupported object type '{}'".format(object_type))

    def get_pce_objects(self, include_deleted_workloads=False, list_of_objects_to_load: Optional[List[str]] = None, force_async_mode=False):

        objects_to_load = {}
        if list_of_objects_to_load is not None:
            all_types = pylo.APIConnector.get_all_object_types()
            for object_type in list_of_objects_to_load:
                if object_type not in all_types:
                    raise pylo.PyloEx("Unknown object type '{}'".format(object_type))
                objects_to_load[object_type] = True
        else:
            objects_to_load = pylo.APIConnector.get_all_object_types()

        self.get_software_version()

        # whatever the request was, label dimensions are not optional if PCE is 22.2+
        if self.version.is_greater_or_equal_than(pylo.SoftwareVersion("22.2.0")):
            objects_to_load['label_dimensions'] = 'label_dimensions'
        else:
            if 'label_dimensions' in objects_to_load:
                del objects_to_load['label_dimensions']


        threads_count = 4
        data: PCEObjectsJsonStructure = pylo.Organization.create_fake_empty_config()
        errors = []
        thread_queue = Queue()

        def get_objects(q: Queue, thread_num: int, force_async_mode=False):
            while True:
                object_type, errors = q.get()
                try:
                    if len(errors) > 0:
                        q.task_done()
                        continue
                    if object_type == 'workloads':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls or force_async_mode:
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
                    elif object_type == 'label_dimensions':
                        if self.get_objects_count_by_type(object_type) > default_max_objects_for_sync_calls:
                            data['label_dimensions'] = self.objects_label_dimension_get()
                        else:
                            data['label_dimensions'] = self.objects_label_dimension_get(async_mode=False, max_results=default_max_objects_for_sync_calls)
                    else:
                        raise pylo.PyloEx("Unsupported object type '{}'".format(object_type))
                except Exception as e:
                    errors.append(e)

                q.task_done()

        for i in range(threads_count):
            worker = Thread(target=get_objects, args=(thread_queue, i, force_async_mode,))
            worker.daemon = True
            worker.start()

        for type in objects_to_load.keys():
            thread_queue.put((type, errors,))

        thread_queue.join()

        if len(errors) > 0:
            raise errors[0]

        return data

    def collect_pce_infos(self):
        if self.version is not None:  # Make sure we collect data only once
            return
        path = "/product_version"
        json_output = self.do_get_call(path, include_org_id=False, skip_product_version_check=True)

        self.version_string = json_output['version']
        self.version = pylo.SoftwareVersion(json_output['long_display'])

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

    def rule_coverage_query(self, data: List[RuleCoverageQueryEntryJsonStructure], include_boundary_rules=True):
        params = None
        if include_boundary_rules is not None:
            params = {'include_deny_rules': include_boundary_rules}
        return self.do_post_call(path='/sec_policy/draft/rule_coverage', json_arguments=data, include_org_id=True, json_output_expected=True, async_call=False, params=params)

    def objects_label_get(self, max_results: int = None, async_mode=True) -> List[LabelObjectJsonStructure]:
        path = '/labels'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_label_update(self, href: str, data: LabelObjectUpdateJsonStructure):
        path = href
        return self.do_put_call(path=path, json_arguments=data, json_output_expected=False, include_org_id=False)

    def objects_label_delete(self, href: Union[str, 'pylo.Label']):
        path = href
        if type(href) is pylo.Label:
            path = href.href

        return self.do_delete_call(path=path, json_output_expected=False, include_org_id=False)

    def objects_label_create(self, label_name: str, label_type: str):
        path = '/labels'
        data: LabelObjectCreationJsonStructure = {'key': label_type, 'value': label_name}
        return self.do_post_call(path=path, json_arguments=data)

    def objects_labelgroup_get(self, max_results: int = None, async_mode=True) -> List[LabelGroupObjectJsonStructure]:
        path = '/sec_policy/draft/label_groups'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_labelgroup_update(self, href: str, data: LabelGroupObjectUpdateJsonStructure):
        path = href
        return self.do_put_call(path=path, json_arguments=data, json_output_expected=False, include_org_id=False)

    def objects_label_dimension_get(self, max_results: int = None, async_mode=False) -> List[LabelDimensionObjectStructure]:
        path = '/label_dimensions'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results
        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_virtual_service_get(self, max_results: int = None, async_mode=True) -> List[VirtualServiceObjectJsonStructure]:
        path = '/sec_policy/draft/virtual_services'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        results = self.do_get_call(path=path, async_call=async_mode, params=data)
        # check type
        if type(results) is list:
            return results
        raise pylo.PyloEx("Unexpected result type '{}' while expecting an array of Virtual Service objects".format(type(results)), results)

    def objects_iplist_get(self, max_results: int = None, async_mode=True, search_name: str = None) -> List[IPListObjectJsonStructure]:
        path = '/sec_policy/draft/ip_lists'
        data = {}

        if search_name is not None:
            data['name'] = search_name

        if max_results is not None:
            data['max_results'] = max_results

        results: List[IPListObjectJsonStructure] = self.do_get_call(path=path, async_call=async_mode, params=data)
        # check type
        if type(results) is list:
            return results

        raise pylo.PyloEx("Unexpected result type '{}' while expecting an array of IP List objects".format(type(results)), results)

    def objects_iplist_create(self, json_blob: IPListObjectCreationJsonStructure):
        path = '/sec_policy/draft/ip_lists'
        return self.do_post_call(path=path, json_arguments=json_blob)

    def objects_iplists_get_default_any(self) -> Optional[str]:
        """
           Returns the href of the default 'ANY' IP List or None (which is a bad sign!)
        :return:
        """
        response = self.objects_iplist_get(max_results=10, async_mode=False, search_name='0.0.0.0')

        for item in response:
            if item['created_by']['href'] == '/users/0':
                return item['href']

        return None

    def objects_ven_get(self,
                        include_deleted=False,
                        filter_by_ip: str = None,
                        filter_by_label: Optional[WorkloadsGetQueryLabelFilterJsonStructure] = None,
                        filter_by_name: str = None,
                        max_results: int = None,
                        async_mode=True,
                        representation: Optional[Literal['ven_labels']] = None
                        ) -> List[VenObjectJsonStructure]:
        path = '/vens'
        data = {}

        if include_deleted:
            data['include_deleted'] = 'yes'

        if filter_by_ip is not None:
            data['ip_address'] = filter_by_ip

        if filter_by_label is not None:
            # filter_by_label must be converted to json text
            data['labels'] = json.dumps(filter_by_label)

        if filter_by_name is not None:
            data['name'] = filter_by_name

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=async_mode, params=data)

    def objects_workload_get(self,
                             include_deleted=False,
                             filter_by_ip: str = None,
                             filter_by_label: WorkloadsGetQueryLabelFilterJsonStructure=None,
                             filter_by_name: str = None,
                             filter_by_managed: bool = None,
                             filer_by_policy_health: Literal['active', 'warning', 'error'] = None,
                             max_results: int = None,
                             async_mode=True) -> List[WorkloadObjectJsonStructure]:
        path = '/workloads'
        data = {}

        if include_deleted:
            data['include_deleted'] = 'yes'

        if filter_by_ip is not None:
            data['ip_address'] = filter_by_ip

        if filter_by_label is not None:
            # filter_by_label must be converted to json text
            data['labels'] = json.dumps(filter_by_label)

        if filter_by_name is not None:
            data['name'] = filter_by_name

        if filter_by_managed is not None:
            data['managed'] = 'true' if filter_by_managed else 'false'

        if filer_by_policy_health is not None:
            data['policy_health'] = filer_by_policy_health

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

    def objects_workload_update_bulk(self, json_object: List[WorkloadBulkUpdateEntryJsonStructure]) \
            -> List[WorkloadBulkUpdateResponseEntry]:
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

    def object_workload_get_active_policies(self, workload_href: str):
        path = '/sec_policy/active/policy_view'
        data = {'workload': workload_href}
        return self.do_get_call(path=path, async_call=False, params=data, include_org_id=True, json_output_expected=True)

    class WorkloadMultiDeleteTracker:
        _errors: Dict[str, str]
        _hrefs: Dict[str, bool]
        _workloads: Dict[str, 'pylo.Workload']  # dict of workloads by HREF
        connector: 'pylo.APIConnector'

        def __init__(self, connector: 'pylo.APIConnector'):
            self.connector = connector
            self._hrefs = {}
            self._errors = {}
            self._workloads = {}

        @property
        def workloads(self) -> List['pylo.Workload']:
            """
            Return a copy of the list of workloads.
            :return:
            """
            return list(self._workloads.values())

        @property
        def workloads_by_href(self) -> Dict[str, 'pylo.Workload']:
            """
            Return a copy of the dict of workloads by href
            :return:
            """
            return self._workloads.copy()

        @property
        def hrefs(self) -> List[str]:
            """
            Return a copy of the list of hrefs
            :return:
            """
            return list(self._hrefs.keys())

        def add_workload(self, wkl: 'pylo.Workload'):
            self._hrefs[wkl.href] = True
            self._workloads[wkl.href] = wkl

        def add_href(self, href: str):
            self._hrefs[href] = True

        def add_error(self, href: str, message: str):
            self._errors[href] = message

        def get_error_by_wlk(self, wkl: 'pylo.Workload') -> Optional[str]:
            found = self._errors.get(wkl.href, pylo.objectNotFound)
            if found is pylo.objectNotFound:
                return None
            return found

        def get_error_by_href(self, href: str) -> Union[str, None]:
            return self._errors.get(href)

        def execute(self, unpair_agents=False):

            if len(self._hrefs) < 1:
                raise pylo.PyloEx("WorkloadMultiDeleteTracker is empty")

            try:
                result = self.connector.objects_workload_delete_multi(list(self._hrefs.keys()))
            except Exception as ex:  #global exception means something really bad happened we log errors for all workloads
                for href in self._hrefs.keys():
                    self._errors[href] = str(ex)
                return


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
                if unpair_agents and error is not None and error_string.find("method_not_allowed_error") > -1:
                    agents_to_unpair.append(href)
                elif error is not None and len(error) > 0:
                    self._errors[href] = error_string

            if len(agents_to_unpair) > 0:
                self._unpair_agents(agents_to_unpair)

        def _unpair_agents(self, workloads_hrefs: [str]):
            for href in workloads_hrefs:
                retry_count = 5
                api_result = None

                while retry_count >= 0:
                    retry_count -= 1
                    try:
                        api_result = self.connector.objects_workload_unpair_multi([href])
                        break

                    except pylo.PyloApiTooManyRequestsEx as ex:
                        if retry_count <= 0:
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

    def objects_workload_delete_multi(self, href_or_workload_array: Union[List['pylo.Workload'],List[str]]):
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

    def objects_workload_create_single_unmanaged(self, json_object: WorkloadObjectCreateJsonStructure):
        path = '/workloads'
        return self.do_post_call(path=path, json_arguments=json_object)

    def objects_workload_create_bulk_unmanaged(self, workloads_json_payload: [WorkloadObjectCreateJsonStructure]):
        path = '/workloads/bulk_create'
        return self.do_put_call(path=path, json_arguments=workloads_json_payload)

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

    def objects_network_device_get(self,
                                       max_results: int = None) -> List[NetworkDeviceObjectJsonStructure]:
        path = '/network_devices'
        data = {}

        if max_results is not None:
            data['max_results'] = max_results

        return self.do_get_call(path=path, async_call=False, params=data)

    def object_network_device_endpoints_get(self, network_device_href: str) -> List[NetworkDeviceEndpointObjectJsonStructure]:
        path = '{}/network_endpoints'.format(network_device_href)
        data = {}

        return self.do_get_call(path=path, async_call=False, include_org_id=False )

    def object_network_device_endpoint_create(self, network_device_href: str, name: str,
                                              endpoint_type: Literal['switch_port'], workloads_href: List[str]) \
            -> List[NetworkDeviceEndpointObjectJsonStructure]:

        path = '{}/network_endpoints'.format(network_device_href)

        workloads_href_objects = []
        for workload_href in workloads_href:
            workloads_href_objects.append({'href': workload_href})

        data = {'config': {'name': name, 'endpoint_type': endpoint_type}, 'workloads': workloads_href_objects}

        return self.do_post_call(path=path, async_call=False, include_org_id=False, json_arguments=data, json_output_expected=True)

    def objects_ruleset_get(self, max_results: int = None, async_mode=True) -> List[RulesetObjectJsonStructure]:
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

    def objects_ruleset_update(self, ruleset_href: str, update_data: RulesetObjectUpdateStructure):
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
                            consumers: List[Union[WorkloadHrefRef, IPListHrefRef, VirtualServiceHrefRef, 'pylo.IPList', 'pylo.Label', 'pylo.LabelGroup']],
                            providers: List[Union[WorkloadHrefRef, IPListHrefRef, VirtualServiceHrefRef, 'pylo.IPList', 'pylo.Label', 'pylo.LabelGroup']],
                            services: List[Union['pylo.Service', 'pylo.DirectServiceInRule', RuleDirectServiceReferenceObjectJsonStructure]],
                            description='', machine_auth=False, secure_connect=False, enabled=True,
                            stateless=False, consuming_security_principals=None,
                            resolve_consumers_as_virtual_services=True, resolve_consumers_as_workloads=True,
                            resolve_providers_as_virtual_services=True, resolve_providers_as_workloads=True) \
            -> Dict[str, Any]:

        if consuming_security_principals is None:
            consuming_security_principals = []

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
                services_json.append(item.get_api_reference_json())

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

        path = ruleset_href + '/sec_rules'

        return self.do_post_call(path, json_arguments=data, json_output_expected=True, include_org_id=False)

    def objects_securityprincipal_get(self, max_results: int = None, async_mode=True) -> List[SecurityPrincipalObjectJsonStructure]:
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

        def get_failed_items(self) -> Dict[str, 'APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem']:
            results: Dict[str, 'pylo.APIConnector.ApiAgentCompatibilityReport.ApiAgentCompatibilityReportItem'] = {}
            for infos in self._items.values():
                if infos.status != 'green':
                    results[infos.name] = infos

            return results

    def agent_get_compatibility_report(self, agent_href: str = None, agent_id: str = None, return_raw_json=True) \
            -> Union['pylo.APIConnector.ApiAgentCompatibilityReport', Dict[str, Any]]:
        if agent_href is None and agent_id is None:
            raise pylo.PyloEx('you need to provide a HREF or an ID')
        if agent_href is not None and agent_id is not None:
            raise pylo.PyloEx('you need to provide a HREF or an ID but not BOTH')

        include_org_id_in_api_query = False

        if agent_href is None:
            path = '/agents/{}/compatibility_report'.format(agent_id)
            include_org_id_in_api_query = True
        else:
            path = '{}/compatibility_report'.format(agent_href)

        if return_raw_json:
            return self.do_get_call(path=path, include_org_id=include_org_id_in_api_query)

        retry_count = 5
        api_result = None

        while retry_count >= 0:
            retry_count -= 1
            try:
                api_result = self.do_get_call(path=path, include_org_id=include_org_id_in_api_query)
                break

            except pylo.PyloApiTooManyRequestsEx as ex:
                if retry_count <= 0:
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
        """
        Reassign an agent to a different PCE
        :param agent_href:
        :param target_pce:
        :return:
        """
        path = agent_href + '/update'
        data = {"target_pce_fqdn": target_pce}
        return self.do_put_call(path, json_arguments=data, include_org_id=False, json_output_expected=False)

    def explorer_async_queries_all_status_get(self):
        """
        Get the status of all async queries
        """
        return self.do_get_call('/traffic_flows/async_queries', json_output_expected=True, include_org_id=True)

    def explorer_async_query_get_specific_request_status(self, request_href: str):
        all_statuses = self.explorer_async_queries_all_status_get()
        for status in all_statuses:
            if status['href'] == request_href:
                return status

        raise pylo.PyloObjectNotFound("Request with ID {} not found".format(request_href))

    def explorer_search(self, filters: Union[Dict, 'pylo.ExplorerFilterSetV1'],
                        max_running_time_seconds=1800,
                        check_for_update_interval_seconds=10) -> 'pylo.ExplorerResultSetV1':
        path = "/traffic_flows/async_queries"
        if isinstance(filters, pylo.ExplorerFilterSetV1):
            data = filters.generate_json_query()
        else:
            data = filters

        query_queued_json_response = self.do_post_call(path, json_arguments=data, include_org_id=True,
                                                       json_output_expected=True)

        if 'status' not in query_queued_json_response:
            raise pylo.PyloApiEx("Invalid response from API, missing 'status' property", query_queued_json_response)

        if query_queued_json_response['status'] != "queued":
            raise pylo.PyloApiEx("Invalid response from API, 'status' property is not 'QUEUED'", query_queued_json_response)

        if 'href' not in query_queued_json_response:
            raise pylo.PyloApiEx("Invalid response from API, missing 'href' property", query_queued_json_response)

        query_href = query_queued_json_response['href']
        # check that query_href is a string
        if not isinstance(query_href, str):
            raise pylo.PyloApiEx("Invalid response from API, 'href' property is not a string", query_queued_json_response)

        # get current timestamp to ensure we don't wait too long
        start_time = time.time()

        query_status = None # Json response from API for specific query

        while True:
            # check that we don't wait too long
            if time.time() - start_time > max_running_time_seconds:
                raise pylo.PyloApiEx("Timeout while waiting for query to complete", query_queued_json_response)

            queries_status_json_response = self.explorer_async_query_get_specific_request_status(query_href)
            if queries_status_json_response['status'] == "completed":
                query_status = queries_status_json_response
                break

            if queries_status_json_response['status'] not in ["queued", "working"]:
                raise pylo.PyloApiEx("Query failed with status {}".format(queries_status_json_response['status']),
                                     queries_status_json_response)

            time.sleep(check_for_update_interval_seconds)

        if query_status is None:
            raise pylo.PyloEx("Unexpected logic where query_status is None", query_queued_json_response)

        query_json_response = self.do_get_call(query_href + "/download", json_output_expected=True, include_org_id=False)

        result = pylo.ExplorerResultSetV1(query_json_response,
                                          owner=self,
                                          emulated_process_exclusion=filters.exclude_processes_emulate)

        return result

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
            new_report = pylo.ClusterHealth(single_output)
            dict_of_health_reports[new_report.fqdn] = new_report

        return dict_of_health_reports

    def new_rule_search_query(self) -> 'pylo.RuleSearchQuery':
        return pylo.RuleSearchQuery(self)

    def new_explorer_query(self, max_results: int = 1500, max_running_time_seconds: int = 1800,
                           check_for_update_interval_seconds: int = 10) -> 'pylo.ExplorerQuery':
        return pylo.ExplorerQuery(self, max_results, max_running_time_seconds, check_for_update_interval_seconds)


    def new_audit_log_query(self, max_results: int = 10000, max_running_time_seconds: int = 1800,
                            check_for_update_interval_seconds: int = 10) -> 'pylo.AuditLogQuery':
        return pylo.AuditLogQuery(self, max_results, max_running_time_seconds )

    def audit_log_query(self, max_results = 1000, event_type: Optional[str] = None) -> List[AuditLogApiReplyEventJsonStructure]:
        url = '/events'
        args = {'max_results': max_results}
        if event_type is not None:
            args['event_type'] = event_type

        return self.do_get_call(path=url, params=args)


    def get_pce_ui_workload_url(self, href: str) -> str:
        # extract UUID from workload HREF:
        uuid = href.split('/')[-1]
        return self._make_base_url('/#/workloads/' + uuid )



