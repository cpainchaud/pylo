import sys
from typing import Optional, List, Dict, Literal

import pylo
from pylo.API.APIConnector import APIConnector

class ExplorerResult:
    _draft_mode_policy_decision: Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]
    destination_workload_labels_href: List[str]
    source_workload_labels_href: List[str]

    def __init__(self, data):
        self.raw_json = data
        self.num_connections = data['num_connections']

        self.policy_decision_string = data['policy_decision']
        self._draft_mode_policy_decision = None

        self.source_ip_fqdn: Optional[str] = None
        self.destination_ip_fqdn: Optional[str] = None

        src = data['src']
        self.source_ip: str = src['ip']
        self._source_iplists = src.get('ip_lists')
        self._source_iplists_href: List[str] = []
        if self._source_iplists is not None:
            for href in self._source_iplists:
                self._source_iplists_href.append(href['href'])

        self.source_workload_href: Optional[str] = None
        workload_data = src.get('workload')
        if workload_data is not None:
            self.source_workload_href: Optional[str] = workload_data.get('href')
            if self.source_workload_href is None:
                raise pylo.PyloApiUnexpectedSyntax("Explorer API has return a record referring to a Workload with no HREF given:", data)

            self.source_workload_labels_href: Optional[List[str]] = []
            workload_labels_data = workload_data.get('labels')
            if workload_labels_data is not None:
                for label_data in workload_labels_data:
                    self.source_workload_labels_href.append(label_data.get('href'))

        dst = data['dst']
        self.destination_ip: str = dst['ip']
        self.destination_ip_fqdn = dst.get('fqdn')
        self._destination_iplists = dst.get('ip_lists')
        self._destination_iplists_href: List[str] = []
        if self._destination_iplists is not None:
            for href in self._destination_iplists:
                self._destination_iplists_href.append(href['href'])

        self.destination_workload_href: Optional[str] = None
        workload_data = dst.get('workload')
        if workload_data is not None:
            self.destination_workload_href = workload_data.get('href')
            if self.destination_workload_href is None:
                raise pylo.PyloApiUnexpectedSyntax("Explorer API has return a record referring to a Workload with no HREF given:", data)

            self.destination_workload_labels_href: Optional[List[str]] = []
            workload_labels_data = workload_data.get('labels')
            if workload_labels_data is not None:
                for label_data in workload_labels_data:
                    self.destination_workload_labels_href.append(label_data.get('href'))

        service_json = data['service']
        self.service_json = service_json

        self.service_protocol: int = service_json['proto']
        self.service_port: Optional[int] = service_json.get('port')
        self.process_name: Optional[str] = service_json.get('process_name')
        self.username: Optional[str] = service_json.get('user_name')

        self.first_detected: str = data['timestamp_range']['first_detected']
        self.last_detected: str = data['timestamp_range']['last_detected']

        self._cast_type: Optional[str] = data.get('transmission')

    def service_to_str(self, protocol_first=True):
        if protocol_first:
            if self.service_port is None or self.service_port == 0:
                return 'proto/{}'.format(self.service_protocol)

            if self.service_protocol == 17:
                return 'udp/{}'.format(self.service_port)

            if self.service_protocol == 6:
                return 'tcp/{}'.format(self.service_port)
        else:
            if self.service_port is None or self.service_port == 0:
                return '{}/proto'.format(self.service_protocol)

            if self.service_protocol == 17:
                return '{}/udp'.format(self.service_port)

            if self.service_protocol == 6:
                return '{}/tcp'.format(self.service_port)

    def service_to_str_array(self):
        if self.service_port is None or self.service_port == 0:
            return [self.service_protocol, 'proto']

        if self.service_protocol == 17:
            return [self.service_port, 'udp']

        if self.service_protocol == 6:
            return [self.service_port, 'tcp']

        return ['n/a', 'n/a']

    def source_is_workload(self):
        return self.source_workload_href is not None

    def destination_is_workload(self):
        return self.destination_workload_href is not None

    def get_source_workload_href(self):
        return self.source_workload_href

    def get_destination_workload_href(self):
        return self.destination_workload_href

    def get_source_workload(self, org_for_resolution: 'pylo.Organization') -> Optional['pylo.Workload']:
        if self.source_workload_href is None:
            return None
        return org_for_resolution.WorkloadStore.find_by_href_or_create_tmp(self.source_workload_href, '*DELETED*')

    def get_destination_workload(self, org_for_resolution: 'pylo.Organization') -> Optional['pylo.Workload']:
        if self.destination_workload_href is None:
            return None
        return org_for_resolution.WorkloadStore.find_by_href_or_create_tmp(self.destination_workload_href, '*DELETED*')

    def get_source_labels_href(self) -> Optional[List[str]]:
        if not self.source_is_workload():
            return None
        return self.source_workload_labels_href

    def get_destination_labels_href(self) -> Optional[List[str]]:
        if not self.destination_is_workload():
            return None
        return self.destination_workload_labels_href

    def get_source_iplists(self, org_for_resolution: 'pylo.Organization') ->Dict[str, 'pylo.IPList']:
        if self._source_iplists is None:
            return {}

        result = {}

        for record in self._source_iplists:
            href = record.get('href')
            if href is None:
                raise pylo.PyloEx('Cannot find HREF for IPList in Explorer result json', record)
            iplist = org_for_resolution.IPListStore.find_by_href(href)
            if iplist is None:
                raise pylo.PyloEx('Cannot find HREF for IPList in Explorer result json', record)

            result[href] = iplist

        return result

    def get_source_iplists_href(self) -> Optional[List[str]]:
        if self.source_is_workload():
            return None
        if self._source_iplists_href is None:
            return []
        return self._source_iplists_href.copy()

    def get_destination_iplists_href(self) -> Optional[List[str]]:
        if self.destination_is_workload():
            return None

        if self._destination_iplists_href is None:
            return []
        return self._destination_iplists_href.copy()

    def get_destination_iplists(self, org_for_resolution: 'pylo.Organization') ->Dict[str, 'pylo.IPList']:
        if self._destination_iplists is None:
            return {}

        result = {}

        for record in self._destination_iplists:
            href = record.get('href')
            if href is None:
                raise pylo.PyloEx('Cannot find HREF for IPList in Explorer result json', record)
            iplist = org_for_resolution.IPListStore.find_by_href(href)
            if iplist is None:
                raise pylo.PyloEx('Cannot find HREF for IPList in Explorer result json', record)

            result[href] = iplist

        return result

    def pd_is_potentially_blocked(self):
        return self.policy_decision_string == 'potentially_blocked'

    def cast_is_broadcast(self):
        return self._cast_type == 'broadcast'

    def cast_is_multicast(self):
        return self._cast_type == 'multicast'

    def cast_is_unicast(self):
        return self._cast_type is not None

    def set_draft_mode_policy_decision(self, decision: Literal['allowed', 'blocked', 'blocked_by_boundary']):
        self._draft_mode_policy_decision = decision

    def draft_mode_policy_decision_is_blocked(self) -> Optional[bool]:
        """
        @return: None if draft_mode was not enabled
        """
        return self._draft_mode_policy_decision is not None and \
            (self._draft_mode_policy_decision == 'blocked' or self._draft_mode_policy_decision == 'blocked_by_boundary')

    def draft_mode_policy_decision_is_allowed(self) -> Optional[bool]:
        """
        @return: None if draft_mode was not enabled
        """
        return self._draft_mode_policy_decision is not None and self._draft_mode_policy_decision == "allowed"

    def draft_mode_policy_decision_is_unavailable(self) -> Optional[bool]:
        """
        @return: None if draft_mode was not enabled
        """
        return self._draft_mode_policy_decision is None

    def draft_mode_policy_decision_is_not_defined(self) -> Optional[bool]:
        return self._draft_mode_policy_decision is None

    def draft_mode_policy_decision_to_str(self) -> str:
        if self._draft_mode_policy_decision is None:
            return 'not_available'
        return self._draft_mode_policy_decision


class ExplorerResultSetV1:

    owner: 'APIConnector'

    class Tracker:
        def __init__(self, owner):
            self.owner = owner

    def __init__(self, data, owner: 'APIConnector', emulated_process_exclusion={}):
        self._raw_results = data
        if len(emulated_process_exclusion) > 0:
            new_data = []
            for record in self._raw_results:
                if 'process_name' in record['service']:
                    if record['service']['process_name'] in emulated_process_exclusion:
                        continue
                new_data.append(record)
            self._raw_results = new_data

        self.owner = owner
        self.tracker = ExplorerResultSetV1.Tracker(self)

    def count_results(self):
        return len(self._raw_results)

    def get_record(self, line: int):
        if line < 0:
            raise pylo.PyloEx('Invalid line #: {}'.format(line))
        if line >= len(self._raw_results):
            raise pylo.PyloEx('Line # doesnt exists, requested #{} while this set contains only {} (starts at 0)'.
                              format(line, len(self._raw_results)))

        return ExplorerResult(self._raw_results[line])

    @staticmethod
    def merge_similar_records_only_process_and_user_differs(records: List[ExplorerResult]) -> List[ExplorerResult]:
        class HashTable:
            def __init__(self):
                self.entries: Dict[str, List[ExplorerResult]] = {}

            def load(self, records: List[ExplorerResult]):

                for record in records:
                    hash = record.source_ip + record.destination_ip + str(record.source_workload_href) + \
                           str(record.destination_workload_href) + record.service_to_str() + \
                           record.policy_decision_string + record.draft_mode_policy_decision_to_str()
                    hashEntry = self.entries.get(hash)
                    if hashEntry is None:
                        self.entries[hash] = [record]
                    else:
                        hashEntry.append(record)

            def results(self) -> List[ExplorerResult]:

                results: List[ExplorerResult] = []

                for hashEntry in self.entries.values():
                    if len(hashEntry) == 1:
                        results.append(hashEntry[0])
                        continue

                    record_to_keep = hashEntry.pop()
                    merged_users = []
                    merged_processes = []
                    count_connections = 0

                    last_detected = record_to_keep.last_detected
                    first_detected = record_to_keep.first_detected

                    for record in hashEntry:
                        if record.username is not None and len(record.username) > 0:
                            merged_users.append(record.username)
                        if record.process_name is not None and len(record.process_name) > 0:
                            merged_processes.append(record.process_name)
                        if last_detected < record.last_detected:
                            last_detected = record.last_detected
                        if first_detected > record.first_detected:
                            first_detected = record.first_detected

                        count_connections = count_connections + record.num_connections

                    merged_users = list(set(merged_users))
                    merged_processes = list(set(merged_processes))

                    record_to_keep.process_name = merged_processes
                    record_to_keep.username = merged_users
                    record_to_keep.num_connections = count_connections
                    record_to_keep.last_detected = last_detected
                    record_to_keep.first_detected = first_detected

                    results.append(record_to_keep)

                return results

        hash_table: HashTable = HashTable()
        hash_table.load(records)
        results = hash_table.results()

        return results

    def get_all_records(self,
                        draft_mode=False,
                        deep_analysis=True,
                        draft_mode_request_count_per_batch=50
                        ) -> List[ExplorerResult]:
        result = []
        for data in self._raw_results:
            try:
                new_record = ExplorerResult(data)
                result.append(new_record)

            except pylo.PyloApiUnexpectedSyntax as error:
                pylo.log.warn(error)

        if len(result) > 0 and draft_mode:
            draft_reply_to_record_table: List[ExplorerResult] = []

            global_query_data = []

            for record in result:
                service_json: Dict = record.service_json.copy()
                service_json['protocol'] = service_json.pop('proto')
                if 'port' in service_json and service_json['protocol'] != 17 and service_json['protocol'] != 6:
                    service_json.pop('port')
                if 'user_name' in service_json:
                    service_json.pop('user_name')

                local_query_data = {
                    "resolve_labels_as": {"source": ["workloads"], "destination": ["workloads"]},
                    "services": [service_json]
                }

                if not record.source_is_workload() and not record.destination_is_workload():
                    raise pylo.PyloEx("Both Source and Destinations are not workloads, it's unexpected")

                if record.source_is_workload():
                    if deep_analysis:
                        local_query_data['source'] = {'workload': {'href': record.get_source_workload_href()}}
                    else:
                        local_query_data['source'] = {'labels': []}
                        for href in record.get_source_labels_href():
                            local_query_data['source']['labels'].append({'href': href})

                    if record.destination_is_workload():
                        if deep_analysis:
                            local_query_data['destination'] = {'workload': {'href': record.get_destination_workload_href()}}
                        else:
                            local_query_data['destination'] = {'labels': []}
                            for href in record.get_destination_labels_href():
                                local_query_data['destination']['labels'].append({'href': href})

                        draft_reply_to_record_table.append(record)
                        global_query_data.append(local_query_data)
                    else:

                        iplists_href = record.get_destination_iplists_href()
                        for iplist_href in iplists_href:
                            local_unique_query_data = local_query_data.copy()
                            local_unique_query_data['destination'] = {'ip_list': {'href': iplist_href}}

                            draft_reply_to_record_table.append(record)
                            global_query_data.append(local_unique_query_data)

                else:

                    if deep_analysis:
                        local_query_data['destination'] = {'workload': {'href': record.get_destination_workload_href()}}
                    else:
                        local_query_data['destination'] = {'labels': []}
                        for href in record.get_destination_labels_href():
                            local_query_data['destination']['labels'].append({'href': href})

                    if record.source_is_workload():
                        local_query_data['source'] = {'labels': []}
                        for href in record.get_source_labels_href():
                            local_query_data['source']['labels'].append({'href': href})

                        draft_reply_to_record_table.append(record)
                        global_query_data.append(local_query_data)
                    else:

                        iplists_href = record.get_source_iplists_href()
                        for iplist_href in iplists_href:
                            local_unique_query_data = local_query_data.copy()
                            local_unique_query_data['source'] = {'ip_list': {'href': iplist_href}}

                            draft_reply_to_record_table.append(record)
                            global_query_data.append(local_unique_query_data)

            pylo.log.debug("{} items in Rule Coverage query queue".format(len(global_query_data)))

            index = 0
            while index < len(global_query_data):
                local_index = index
                local_last_index = index + draft_mode_request_count_per_batch - 1
                if local_last_index >= len(global_query_data):
                    local_last_index = len(global_query_data) - 1

                query_data = []

                for query_index in range(local_index, local_last_index+1):
                    query_data.append(global_query_data[query_index])

                # print(query_data)
                res = self.owner.rule_coverage_query(query_data)
                # print(res)

                edges = res.get('edges')
                if edges is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "edges"', res)

                if len(edges) != len(query_data):
                    raise pylo.PyloEx("rule_coverage has returned {} records while {} where requested".format(len(edges), len(query_data)))

                for query_index in range(local_index, local_last_index+1):
                    # print(local_index)
                    # print(query_index)
                    # print(len(edges))
                    response_data = edges[query_index-local_index]
                    # print(response_data)
                    if type(response_data) is not list or len(response_data) != 1:
                        raise pylo.PyloEx("rule_coverage has returned invalid data: {}\n against query: {}".format(pylo.nice_json(response_data),
                                                                                                                   pylo.nice_json(query_data[query_index])))

                    rule_list = response_data[0]
                    # print(rule_list)
                    explorer_result = draft_reply_to_record_table[query_index]
                    if explorer_result.draft_mode_policy_decision_is_not_defined():
                        explorer_result.set_draft_mode_policy_decision_blocked(blocked=len(rule_list) < 1)

                    elif explorer_result.draft_mode_policy_decision_is_blocked():
                        explorer_result.set_draft_mode_policy_decision_blocked(blocked=len(rule_list) < 1)


                index += draft_mode_request_count_per_batch

        return result


class RuleCoverageQueryManager:

    class QueryServices:
        def __init__(self):
            self.service_hash_to_index: Dict[str, int] = {}
            self.services_array: List[Dict] = []
            self.service_index_to_log_ids: Dict[int, List[int]] = {}
            self.service_index_policy_coverage: Dict[int, List[str]] = {} # for a service ID (used as key) returns a list of matching rules HREF
            self.service_index_to_boundary_policy_coverage: Dict[int, List[str]] = {} # for a service ID (used as key) returns a list of matching boundary rules HREF

        def add_service(self, service_record: Dict, log_id: int):
            service_hash = '' + str(service_record.get('proto', 'no_proto')) + '/' + str(service_record.get('port', 'no_port')) + '/' \
                           + '/' + str(service_record.get('process_name', 'no_process_name')) \
                           + '/' + str(service_record.get('windows_service_name', 'no_windows_service_name'))
                            #  username is not allows in rule_coverage

            # print(service_hash)

            if service_hash not in self.service_hash_to_index:
                self.service_hash_to_index[service_hash] = len(self.services_array)
                self.services_array.append(service_record)

            service_index = self.service_hash_to_index[service_hash]
            if service_index not in self.service_index_to_log_ids:
                self.service_index_to_log_ids[service_index] = []

            if log_id not in self.service_index_to_log_ids[service_index]:
                self.service_index_to_log_ids[service_index].append(log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
            policy_decision = None
            found_boundary_block = False

            for service_id, list_of_log_ids in self.service_index_to_log_ids.items():
                if log_id in list_of_log_ids:
                    policy_decision = 'blocked'
                    policy_coverage = self.service_index_policy_coverage[service_id]
                    if len(policy_coverage) > 0:
                        return 'allowed'

                    boundary_policy_coverage = self.service_index_to_boundary_policy_coverage[service_id]
                    if len(boundary_policy_coverage) > 0:
                        found_boundary_block = True

            if found_boundary_block:
                return 'blocked_by_boundary'

            return policy_decision

    class IPListToWorkloadQuery:
        def __init__(self, ip_list_href: str, workload_href: str):
            self.ip_list_href = ip_list_href
            self.workload_href = workload_href
            self.services = RuleCoverageQueryManager.QueryServices()

        def add_service(self, service_record: Dict, log_id: int):
            self.services.add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
            return self.services.get_policy_decision_for_log_id(log_id)

        def generate_api_payload(self) -> Dict:
            payload = {"resolve_labels_as": {"source": ["workloads"], "destination": ["workloads"]}, "services": [],
                       'source': {'ip_list': {'href': self.ip_list_href}},
                       'destination': {'workload': {'href': self.workload_href}}
                       }

            for service_id in range(0, len(self.services.services_array)):
                service = self.services.services_array[service_id]
                # print(service)
                service_json: Dict = service.copy()
                service_json['protocol'] = service_json.pop('proto')
                if 'port' in service_json and service_json['protocol'] != 17 and service_json['protocol'] != 6:
                    service_json.pop('port')
                if 'user_name' in service_json:
                    service_json.pop('user_name')
                payload['services'].append(service_json)

            return payload

        def process_response(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_policy_coverage[index] = rules_array

        def process_response_boundary_deny(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_to_boundary_policy_coverage[index] = rules_array

    class WorkloadToIPListQuery:
        def __init__(self, workload_href: str, ip_list_href: str):
            self.workload_href = workload_href
            self.ip_list_href = ip_list_href
            self.services = RuleCoverageQueryManager.QueryServices()

        def add_service(self, service_record: Dict, log_id: int):
            self.services.add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
            return self.services.get_policy_decision_for_log_id(log_id)

        def generate_api_payload(self) -> Dict:
            payload = {"resolve_labels_as": {"source": ["workloads"], "destination": ["workloads"]}, "services": [],
                       'destination': {'ip_list': {'href': self.ip_list_href}},
                       'source': {'workload': {'href': self.workload_href}}
                       }

            for service_id in range(0, len(self.services.services_array)):
                service = self.services.services_array[service_id]
                # print(service)
                service_json: Dict = service.copy()
                service_json['protocol'] = service_json.pop('proto')
                if 'port' in service_json and service_json['protocol'] != 17 and service_json['protocol'] != 6:
                    service_json.pop('port')
                if 'user_name' in service_json:
                    service_json.pop('user_name')
                payload['services'].append(service_json)

            return payload

        def process_response(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_policy_coverage[index] = rules_array

        def process_response_boundary_deny(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_to_boundary_policy_coverage[index] = rules_array

    class WorkloadToWorkloadQuery:
        def __init__(self, src_workload_href: str, dst_workload_href: str):
            self.src_workload_href = src_workload_href
            self.dst_workload_href = dst_workload_href
            self.services = RuleCoverageQueryManager.QueryServices()

        def add_service(self, service_record: Dict, log_id: int):
            self.services.add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
            return self.services.get_policy_decision_for_log_id(log_id)

        def generate_api_payload(self) -> Dict:
            payload = {"resolve_labels_as": {"source": ["workloads"], "destination": ["workloads"]}, "services": [],
                       'source': {'workload': {'href': self.src_workload_href}},
                       'destination': {'workload': {'href': self.dst_workload_href}}
                       }

            for service_id in range(0, len(self.services.services_array)):
                service = self.services.services_array[service_id]
                # print(service)
                service_json: Dict = service.copy()
                service_json['protocol'] = service_json.pop('proto')
                if 'port' in service_json and service_json['protocol'] != 17 and service_json['protocol'] != 6:
                    service_json.pop('port')
                if 'user_name' in service_json:
                    service_json.pop('user_name')
                payload['services'].append(service_json)

            return payload

        def process_response(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_policy_coverage[index] = rules_array

        def process_response_boundary_deny(self, rules: Dict[str, str], response: [[str]]):
            if len(response) != len(self.services.services_array):
                raise Exception('Unexpected response from rule coverage query with mis-matching services count vs reply')

            for index, single_response in enumerate(response):
                rules_array: [Dict] = []
                for rule in single_response:
                    rules_array.append(rules[rule])
                self.services.service_index_to_boundary_policy_coverage[index] = rules_array

    class IPListToWorkloadQueryManager:
        def __init__(self, include_boundary_rules: bool = True):
            self.queries: Dict[str, 'RuleCoverageQueryManager.IPListToWorkloadQuery'] = {}
            self.include_boundary_rules = include_boundary_rules

        def add_query(self, log_id: int, ip_list_href: str, workload_href: str, service_record):
            hash_key = ip_list_href + workload_href
            if hash_key not in self.queries:
                self.queries[hash_key] = RuleCoverageQueryManager.IPListToWorkloadQuery(ip_list_href, workload_href)

            self.queries[hash_key].add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal["allowed", "blocked", "blocked_by_boundary"]]:
            policy_decision: Optional[Literal["allowed", "blocked", "blocked_by_boundary"]] = None
            found_blocked_by_boundary = False

            for query in self.queries.values():
                policy_decision = query.get_policy_decision_for_log_id(log_id) or policy_decision
                if policy_decision == 'allowed':
                    return policy_decision
                if query.get_policy_decision_for_log_id(log_id) == 'blocked_by_boundary':
                    found_blocked_by_boundary = True

            if found_blocked_by_boundary:
                return 'blocked_by_boundary'

            return policy_decision

        def execute(self, connector: APIConnector, queries_per_batch: int):
            # split queries into arrays of size queries_per_batch
            query_batches: List[List[RuleCoverageQueryManager.IPListToWorkloadQuery]] = []
            query_batch: List[RuleCoverageQueryManager.IPListToWorkloadQuery] = []
            for query in self.queries.values():
                query_batch.append(query)
                if len(query_batch) == queries_per_batch:
                    query_batches.append(query_batch)
                    query_batch = []
            if len(query_batch) > 0:
                query_batches.append(query_batch)

            # print(f'{len(query_batches)} batches of {queries_per_batch} queries')

            for query_batch in query_batches:
                # print(f'Executing batch of {len(query_batch)} queries')
                payload = []
                for query in query_batch:
                    payload.append(query.generate_api_payload())

                api_response = connector.rule_coverage_query(payload, include_boundary_rules=self.include_boundary_rules)
                # print(api_response)
                # print('-------------------------------------------------------')

                edges = api_response.get('edges')
                if edges is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "edges"', api_response)

                rules = api_response.get('rules')
                if rules is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "rules"', api_response)

                if len(edges) != len(query_batch):
                    raise pylo.PyloEx("rule_coverage has returned {} records while {} where requested".format(len(edges), len(query_batch)))

                for response_index, edge in enumerate(edges):
                    query = query_batch[response_index]
                    # print(f'Processing edge {edge} against query {query.ip_list_href} -> {query.workload_href} -> {len(query.services.services_array)}')
                    query.process_response(rules, edge)

                if self.include_boundary_rules:
                    deny_edges = api_response.get('deny_edges')
                    if deny_edges is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_edges"', api_response)
                    if len(deny_edges) != len(query_batch):
                        raise pylo.PyloEx("rule_coverage has returned {} deny_edges while {} where requested".format(len(deny_edges), len(query_batch)))

                    deny_rules = api_response.get('deny_rules')
                    if deny_rules is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_rules"', api_response)

                    for response_index, edge in enumerate(deny_edges):
                        query = query_batch[response_index]
                        # print(f'Processing deny_edge {edge} against query {query.src_workload_href} -> {query.dst_workload_href} -> {len(query.services.services_array)}')
                        query.process_response_boundary_deny(deny_rules, edge)

    class WorkloadToIPListQueryManager:
        def __init__(self, include_boundary_rules: bool = True):
            self.queries: Dict[str, 'RuleCoverageQueryManager.WorkloadToIPListQuery'] = {}
            self.include_boundary_rules = include_boundary_rules

        def add_query(self, log_id: int, workload_href: str, ip_list_href: str, service_record):
            hash_key = workload_href + ip_list_href
            if hash_key not in self.queries:
                self.queries[hash_key] = RuleCoverageQueryManager.WorkloadToIPListQuery(workload_href, ip_list_href)

            self.queries[hash_key].add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal["allowed", "blocked", "blocked_by_boundary"]]:
            policy_decision = None
            found_blocked_by_boundary = False

            for query in self.queries.values():
                policy_decision = query.get_policy_decision_for_log_id(log_id) or policy_decision
                if policy_decision == 'allowed':
                    return policy_decision
                if query.get_policy_decision_for_log_id(log_id) == 'blocked_by_boundary':
                    found_blocked_by_boundary = True

            if found_blocked_by_boundary:
                return 'blocked_by_boundary'

            return policy_decision

        def execute(self, connector: APIConnector, queries_per_batch: int):
            # split queries into arrays of size queries_per_batch
            query_batches: List[List[RuleCoverageQueryManager.IPListToWorkloadQuery]] = []
            query_batch: List[RuleCoverageQueryManager.IPListToWorkloadQuery] = []
            for query in self.queries.values():
                query_batch.append(query)
                if len(query_batch) == queries_per_batch:
                    query_batches.append(query_batch)
                    query_batch = []
            if len(query_batch) > 0:
                query_batches.append(query_batch)

            # print(f'{len(query_batches)} batches of {queries_per_batch} queries')

            for query_batch in query_batches:
                # print(f'Executing batch of {len(query_batch)} queries')
                payload = []
                for query in query_batch:
                    payload.append(query.generate_api_payload())

                api_response = connector.rule_coverage_query(payload, include_boundary_rules=self.include_boundary_rules)
                # print(api_response)
                # print('-------------------------------------------------------')

                edges = api_response.get('edges')
                if edges is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "edges"', api_response)

                rules = api_response.get('rules')
                if rules is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "rules"', api_response)

                if len(edges) != len(query_batch):
                    raise pylo.PyloEx("rule_coverage has returned {} records while {} where requested".format(len(edges), len(query_batch)))

                for response_index, edge in enumerate(edges):
                    query = query_batch[response_index]
                    # print(f'Processing edge {edge} against query {query.ip_list_href} -> {query.workload_href} -> {len(query.services.services_array)}')
                    query.process_response(rules, edge)

                if self.include_boundary_rules:
                    deny_edges = api_response.get('deny_edges')
                    if deny_edges is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_edges"', api_response)
                    if len(deny_edges) != len(query_batch):
                        raise pylo.PyloEx("rule_coverage has returned {} deny_edges while {} where requested".format(len(deny_edges), len(query_batch)))

                    deny_rules = api_response.get('deny_rules')
                    if deny_rules is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_rules"', api_response)

                    for response_index, edge in enumerate(deny_edges):
                        query = query_batch[response_index]
                        # print(f'Processing deny_edge {edge} against query {query.src_workload_href} -> {query.dst_workload_href} -> {len(query.services.services_array)}')
                        query.process_response_boundary_deny(deny_rules, edge)

    class WorkloadToWorkloadQueryManager:
        def __init__(self, include_boundary_rules: bool = True):
            self.queries: Dict[str, 'RuleCoverageQueryManager.WorkloadToWorkloadQuery'] = {}
            self.include_boundary_rules = include_boundary_rules

        def add_query(self, log_id: int, src_workload_href: str, dst_workload_href: str, service_record):
            hash_key = src_workload_href + dst_workload_href
            if hash_key not in self.queries:
                self.queries[hash_key] = RuleCoverageQueryManager.WorkloadToWorkloadQuery(src_workload_href, dst_workload_href)

            self.queries[hash_key].add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal["allowed", "blocked", "blocked_by_boundary"]]:
            policy_decision = None
            for query in self.queries.values():
                policy_decision = query.get_policy_decision_for_log_id(log_id) or policy_decision
                if policy_decision == 'allowed':
                    return policy_decision

            return policy_decision

        def execute(self, connector: APIConnector, queries_per_batch: int):
            # split queries into arrays of size queries_per_batch
            query_batches: List[List[RuleCoverageQueryManager.IPListToWorkloadQuery]] = []
            query_batch: List[RuleCoverageQueryManager.IPListToWorkloadQuery] = []
            for query in self.queries.values():
                query_batch.append(query)
                if len(query_batch) == queries_per_batch:
                    query_batches.append(query_batch)
                    query_batch = []
            if len(query_batch) > 0:
                query_batches.append(query_batch)

            # print(f'{len(query_batches)} batches of {queries_per_batch} queries')

            for query_batch in query_batches:
                # print(f'Executing batch of {len(query_batch)} queries')
                payload = []
                for query in query_batch:
                    payload.append(query.generate_api_payload())

                api_response = connector.rule_coverage_query(payload, include_boundary_rules=self.include_boundary_rules)
                # print(api_response)
                # print('-------------------------------------------------------')

                edges = api_response.get('edges')
                if edges is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "edges"', api_response)

                rules = api_response.get('rules')
                if rules is None:
                    raise pylo.PyloEx('rule_coverage request has returned no "rules"', api_response)

                if len(edges) != len(query_batch):
                    raise pylo.PyloEx("rule_coverage has returned {} records while {} where requested".format(len(edges), len(query_batch)))

                for response_index, edge in enumerate(edges):
                    query = query_batch[response_index]
                    # print(f'Processing edge {edge} against query {query.src_workload_href} -> {query.dst_workload_href} -> {len(query.services.services_array)}')
                    query.process_response(rules, edge)

                if self.include_boundary_rules:
                    deny_edges = api_response.get('deny_edges')
                    if deny_edges is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_edges"', api_response)
                    if len(deny_edges) != len(query_batch):
                        raise pylo.PyloEx("rule_coverage has returned {} deny_edges while {} where requested".format(len(deny_edges), len(query_batch)))

                    deny_rules = api_response.get('deny_rules')
                    if deny_rules is None:
                        raise pylo.PyloEx('rule_coverage request has returned no "deny_rules"', api_response)

                    for response_index, edge in enumerate(deny_edges):
                        query = query_batch[response_index]
                        # print(f'Processing deny_edge {edge} against query {query.src_workload_href} -> {query.dst_workload_href} -> {len(query.services.services_array)}')
                        query.process_response_boundary_deny(deny_rules, edge)

    def __init__(self, owner: APIConnector):
        self.owner = owner
        self.iplist_to_workload_query_manager = RuleCoverageQueryManager.IPListToWorkloadQueryManager()
        self.workload_to_iplist_query_manager = RuleCoverageQueryManager.WorkloadToIPListQueryManager()
        self.workload_to_workload_query_manager = RuleCoverageQueryManager.WorkloadToWorkloadQueryManager()
        self.log_id = 0
        self.log_to_id: Dict[ExplorerResult, int] = {}
        self.count_invalid_records = 0
        self.any_iplist_href = self.owner.objects_iplists_get_default_any()
        if self.any_iplist_href is None:
            raise pylo.PyloEx('No "any" iplist found')

    def add_query_from_explorer_results(self, explorer_results: List[ExplorerResult]) -> None:
        for explorer_result in explorer_results:
            self.add_query_from_explorer_result(explorer_result)

    def add_query_from_explorer_result(self, log: ExplorerResult):
        self.log_id += 1
        self.log_to_id[log] = self.log_id


        if not log.source_is_workload():
            if log.destination_is_workload():

                iplist_hrefs = log.get_source_iplists_href()
                if iplist_hrefs is None:
                    iplist_hrefs = [self.any_iplist_href]
                else:
                    iplist_hrefs.append(self.any_iplist_href)

                for iplist_href in iplist_hrefs:
                    self.iplist_to_workload_query_manager.add_query(log_id=self.log_id,
                                                                    ip_list_href=iplist_href,
                                                                    workload_href=log.get_destination_workload_href(),
                                                                    service_record=log.service_json)
            else:  # IPList to IPList should never happen!
                self.count_invalid_records += 1
                pass
        else:
            if not log.destination_is_workload():
                iplist_hrefs = log.get_destination_iplists_href()
                if iplist_hrefs is None:
                    iplist_hrefs = [self.any_iplist_href]
                else:
                    iplist_hrefs.append(self.any_iplist_href)

                for iplist_href in iplist_hrefs:
                    self.workload_to_iplist_query_manager.add_query(log_id=self.log_id,
                                                                    ip_list_href=iplist_href,
                                                                    workload_href=log.get_source_workload_href(),
                                                                    service_record=log.service_json)
            else:
                self.workload_to_workload_query_manager.add_query(log_id=self.log_id,
                                                                  src_workload_href=log.get_source_workload_href(),
                                                                  dst_workload_href=log.get_destination_workload_href(),
                                                                  service_record=log.service_json)

    def count_queries(self):
        return len(self.iplist_to_workload_query_manager.queries)\
               + len(self.workload_to_iplist_query_manager.queries)\
               + len(self.workload_to_workload_query_manager.queries)

    def count_real_queries(self):
        _log_ids = {}
        for query in self.iplist_to_workload_query_manager.queries.values():
            for log_ids in query.services.service_index_to_log_ids.values():
                for log_id in log_ids:
                    _log_ids[log_id] = True

        for query in self.workload_to_iplist_query_manager.queries.values():
            for log_ids in query.services.service_index_to_log_ids.values():
                for log_id in log_ids:
                    _log_ids[log_id] = True

        for query in self.workload_to_workload_query_manager.queries.values():
            for log_ids in query.services.service_index_to_log_ids.values():
                for log_id in log_ids:
                    _log_ids[log_id] = True

        return len(_log_ids)

    def _get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
        decision = self.iplist_to_workload_query_manager.get_policy_decision_for_log_id(log_id)
        if decision == 'allowed':
            return decision

        newDecision = self.workload_to_iplist_query_manager.get_policy_decision_for_log_id(log_id)
        decision = newDecision or decision
        if decision == 'allowed':
            return decision

        newDecision = self.workload_to_workload_query_manager.get_policy_decision_for_log_id(log_id) or decision
        decision = newDecision or decision
        if decision == 'allowed':
            return decision

        return decision

    def apply_policy_decisions_to_logs(self):
        for log, log_id in self.log_to_id.items():
            decision = self._get_policy_decision_for_log_id(log_id)
            if decision is None:
                # if len(log.get_source_iplists_href()) == 0 and len(log.get_destination_iplists_href()) == 0 is None:
                #     #  happens when source or destination is part of no IPList
                #     decision = 'blocked'
                # else:
                pylo.log.error(pylo.nice_json(log.raw_json))
                raise pylo.PyloEx('No decision found for log_id {}'.format(log_id), log.raw_json)

            log.set_draft_mode_policy_decision(decision)

    def execute(self):
        queries_per_batch = 100
        self.iplist_to_workload_query_manager.execute(self.owner, queries_per_batch)
        self.workload_to_iplist_query_manager.execute(self.owner, queries_per_batch)
        self.workload_to_workload_query_manager.execute(self.owner, queries_per_batch)

        self.apply_policy_decisions_to_logs()




