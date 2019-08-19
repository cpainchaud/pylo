import json
import time
import os
import sys
import getpass
from threading import Thread
from queue import Queue

import pylo
from pylo import log

import requests

#urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()


def get_field_or_die(field_name: str, data):
    if type(data) is not dict:
        raise pylo.PyloEx("Data argument should of type DICT")

    field = data.get(field_name)

    if field is None:
        raise pylo.PyloEx("Could not find field named '{}' in data".format(field_name))
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
        self.version = None
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
                    if 'ignore-ssl' in cur:
                        ssl_value = cur['ignore-ssl']
                        if type(ssl_value) is str:
                            if ssl_value.lower() == 'yes':
                                ignore_ssl = True
                    return APIConnector( hostname, cur['port'], cur['user'], cur['key'], skip_ssl_cert_check=ignore_ssl)

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

    def do_get_call(self, path, json_arguments=None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False, params=None):
        return self._doCall('GET', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall, params=params)

    def do_post_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('POST', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)

    def do_put_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('PUT', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)

    def do_delete_call(self, path, json_arguments = None, includeOrgID=True, jsonOutputExpected=True, asyncCall=False):
        return self._doCall('DELETE', path, json_arguments=json_arguments, include_org_id=includeOrgID,
                            jsonOutputExpected=jsonOutputExpected, asyncCall=asyncCall)

    def _doCall(self, method, path, json_arguments=None, include_org_id=True, jsonOutputExpected=True, asyncCall=False, params=None):

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
                                "\nBODY:\n" + str(orig_request.body) )

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
                method == 'POST' and req.status_code != 201 \
                or\
                method == 'DELETE' and req.status_code != 204 \
                or \
                method == 'PUT' and req.status_code != 204 and req.status_code != 200:
            raise Exception('API returned error status "' + str(req.status_code) + ' ' + req.reason
                            + '" and error message: ' + req.text)

        if jsonOutputExpected:
            log.info("Parsing API answer to JSON (with a size of " + str( int(answerSize) ) + "KB)")
            jout = req.json()
            log.info("Done!")
            if answerSize < 2:
                log.info("Resulting JSON object:")
                log.info(json.dumps(jout, indent=2, sort_keys=True))
            return jout

        return req.text

    def getSoftwareVersion(self):
        self.collectPceInfos()
        return self.version

    def getSoftwareVersionString(self):
        self.collectPceInfos()
        return self.version_string

    def get_pce_objects(self, include_deleted_workloads=False):

        threads_count = 7
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

    def collectPceInfos(self):
        if self.version is not None:  # Make sure we collect data only once
            return
        path = "/product_version"
        jout = self.do_get_call(path, includeOrgID=False)

        self.version_string = jout['version']
        self.version = 0

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
            raise Exception("Requested to create a Label '%s' with wrong type '%s'" % (label_name, label_type) )
        data = {'key': label_type, 'value': label_name}
        return self.do_post_call(path=path, json_arguments=data)

    def objects_labelgroup_get(self):
        path = '/sec_policy/draft/label_groups'
        return self.do_get_call(path=path, asyncCall=True)

    def objects_iplist_get(self):
        path = '/sec_policy/draft/ip_lists'
        return self.do_get_call(path=path, asyncCall=True)

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

    def objects_workload_delete(self, href):
        """

        :type href: str|pylo.Workload
        """
        path = href
        if type(href) is pylo.Workload:
            path = href.href

        return self.do_delete_call(path=path, jsonOutputExpected=False, includeOrgID=False)

    def objects_workload_delete_multi(self, href_array):
        """

        :type href_array: list[str]|list[pylo.Workload]
        """

        if len(href_array) < 1:
            return

        json_data = []

        if type(href_array[0]) is str:
            for href in href_array:
                json_data.append({"href": href})
        else:
            for href in href_array:
                json_data.append({"href": href.href})

        print(json_data)

        path = "/workloads/bulk_delete"

        return self.do_put_call(path=path, json_arguments=json_data, jsonOutputExpected=False)

    def objects_workload_create_single_unmanaged(self, json_object):
        path = '/workloads'
        return self.do_post_call(path=path, json_arguments=json_object)

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
            return get_field_or_die('href',self.do_post_call(path=path, json_arguments=json_object))

        if name is None:
            raise pylo.PyloEx("You need to provide a group name")
        if sid is None:
            raise pylo.PyloEx("You need to provide a SID")

        return get_field_or_die('href', self.do_post_call(path=path, json_arguments={'name': name, 'sid': sid}))

