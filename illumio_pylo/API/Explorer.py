import sys
from typing import Optional, List, Dict, Literal, TypeVar, Generic, Union
from datetime import datetime, timedelta

import illumio_pylo as pylo
from .JsonPayloadTypes import RuleCoverageQueryEntryJsonStructure
from illumio_pylo.API.APIConnector import APIConnector

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

    def __init__(self, data, owner: 'APIConnector', emulated_process_exclusion={}):
        self.owner = owner
        self._raw_results = data
        if len(emulated_process_exclusion) > 0:
            new_data = []
            for record in self._raw_results:
                if 'process_name' in record['service']:
                    if record['service']['process_name'] in emulated_process_exclusion:
                        continue
                new_data.append(record)
            self._raw_results = new_data

        self._records: List[ExplorerResult] = []
        self._gen_records()

    def _gen_records(self):
        for data in self._raw_results:
            try:
                new_record = ExplorerResult(data)
                self._records.append(new_record)

            except pylo.PyloApiUnexpectedSyntax as error:
                pylo.log.warn(error)

    def count_records(self):
        return len(self._raw_results)

    def get_record(self, line: int):
        if line < 0:
            raise pylo.PyloEx('Invalid line #: {}'.format(line))
        if line >= len(self._raw_results):
            raise pylo.PyloEx('Line # doesnt exists, requested #{} while this set contains only {} (starts at 0)'.
                              format(line, len(self._raw_results)))

        return ExplorerResult(self._raw_results[line])

    def get_all_records(self) -> List[ExplorerResult]:
        return self._records

    def merge_similar_records_only_process_and_user_differs(self):
        class HashTable:
            def __init__(self):
                self.entries: Dict[str, List[ExplorerResult]] = {}

            def load(self, records: List[ExplorerResult]):
                for record in records:
                    hash = record.source_ip + record.destination_ip + str(record.source_workload_href) + \
                           str(record.destination_workload_href) + record.service_to_str() + \
                           record.policy_decision_string + record.draft_mode_policy_decision_to_str()
                    entry_from_hash = self.entries.get(hash)
                    if entry_from_hash is None:
                        self.entries[hash] = [record]
                    else:
                        entry_from_hash.append(record)

            def results(self) -> List[ExplorerResult]:
                results_list: List[ExplorerResult] = []

                for hashEntry in self.entries.values():
                    if len(hashEntry) == 1:
                        results_list.append(hashEntry[0])
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

                    results_list.append(record_to_keep)

                return results_list

        hash_table: HashTable = HashTable()
        hash_table.load(self._records)
        self._records = hash_table.results()

    def apply_draft_policy_decision_to_all_records(self):
        draft_manager = RuleCoverageQueryManager(self.owner)
        draft_manager.add_query_from_explorer_results(self._records)
        draft_manager.execute()



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

    class ObjectToObjectQuery:
        def __init__(self, src_href: str, src_type: Literal['ip_list','workload'], dst_href: str, dst_type: Literal['ip_list','workload']):
            self.src_href = src_href
            self.dst_href = dst_href
            self.src_type = src_type
            self.dst_type = dst_type
            self.services = RuleCoverageQueryManager.QueryServices()

        def add_service(self, service_record: Dict, log_id: int):
            self.services.add_service(service_record, log_id)

        def get_policy_decision_for_log_id(self, log_id: int) -> Optional[Literal['allowed', 'blocked', 'blocked_by_boundary']]:
            return self.services.get_policy_decision_for_log_id(log_id)

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

        def generate_api_payload(self) -> RuleCoverageQueryEntryJsonStructure:
            payload: RuleCoverageQueryEntryJsonStructure = {"resolve_labels_as": {"source": ["workloads"], "destination": ["workloads"]}, "services": [],
                       'source': {self.src_type: {'href': self.src_href}},
                       'destination': {self.dst_type: {'href': self.dst_href}}
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

    class QueryManager:
        def __init__(self, src_type:Literal['ip_list','workload'], dst_type:Literal['ip_list','workload'] ,include_boundary_rules: bool = True):
            self.queries: Dict[str, RuleCoverageQueryManager.ObjectToObjectQuery] = {}
            self.include_boundary_rules = include_boundary_rules
            self.src_type = src_type
            self.dst_type = dst_type

        def execute(self, connector: APIConnector, queries_per_batch: int):
            # split queries into arrays of size queries_per_batch
            query_batches: List[List[RuleCoverageQueryManager.ObjectToObjectQuery]] = []
            query_batch: List[RuleCoverageQueryManager.ObjectToObjectQuery] = []
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

        def add_query(self, log_id: int, src_href: str, dst_href: str, service_record):
            hash_key = src_href + dst_href
            if hash_key not in self.queries:
                self.queries[hash_key] = RuleCoverageQueryManager.ObjectToObjectQuery(src_href, self.src_type, dst_href, self.dst_type)

            self.queries[hash_key].add_service(service_record, log_id)


    def __init__(self, owner: APIConnector):
        self.owner = owner
        self.iplist_to_workload_query_manager = RuleCoverageQueryManager.QueryManager('ip_list', 'workload')
        self.workload_to_iplist_query_manager = RuleCoverageQueryManager.QueryManager('workload', 'ip_list')
        self.workload_to_workload_query_manager = RuleCoverageQueryManager.QueryManager('workload', 'workload')
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
                                                                    src_href=iplist_href,
                                                                    dst_href=log.get_destination_workload_href(),
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
                                                                    src_href=log.get_source_workload_href(),
                                                                    dst_href=iplist_href,
                                                                    service_record=log.service_json)
            else:
                self.workload_to_workload_query_manager.add_query(log_id=self.log_id,
                                                                  src_href=log.get_source_workload_href(),
                                                                  dst_href=log.get_destination_workload_href(),
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

class ExplorerFilterSetV1:
    exclude_processes_emulate: Dict[str, str]
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
        self._consumer_labels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        self._consumer_exclude_labels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        self._provider_labels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}
        self._provider_exclude_labels: Dict[str, Union[pylo.Label, pylo.LabelGroup]] = {}

        self._consumer_workloads = {}
        self._provider_workloads = {}

        self._consumer_iplists = {}
        self._consumer_iplists_exclude = {}
        self._provider_iplists = {}
        self._provider_iplists_exclude = {}


        self.max_results = max_results
        self._policy_decision_filter = []
        self._time_from = None
        self._time_to = None

        self._include_direct_services = []

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
            # since 21.5 labelgroups can be included directly
            # for nested_label in label_or_href.expand_nested_to_array():
            #    prop_dict[nested_label.href] = nested_label
            prop_dict[label_or_href.href] = label_or_href
            return
        else:
            raise pylo.PyloEx("Unsupported object type {}".format(type(label_or_href)))

    def consumer_include_label(self, label_or_href):
        """

        @type label_or_href: str|pylo.Label|pylo.LabelGroup
        """
        self.__filter_prop_add_label(self._consumer_labels, label_or_href)

    def consumer_exclude_label(self, label_or_href: Union[str, 'pylo.Label', 'pylo.LabelGroup']):
        self.__filter_prop_add_label(self._consumer_exclude_labels, label_or_href)

    def consumer_exclude_labels(self, labels: List[Union[str, 'pylo.Label', 'pylo.LabelGroup']]):
        for label in labels:
            self.consumer_exclude_label(label)

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

    def consumer_include_iplist(self, iplist_or_href: Union[str, 'pylo.IPList']):
        if isinstance(iplist_or_href, str):
            self._consumer_iplists[iplist_or_href] = iplist_or_href
            return

        if isinstance(iplist_or_href, pylo.IPList):
            self._consumer_iplists[iplist_or_href.href] = iplist_or_href.href
            return

        raise pylo.PyloEx("Unsupported object type {}".format(type(iplist_or_href)))

    def consumer_exclude_cidr(self, ipaddress: str):
        self.__filter_consumer_ip_exclude.append(ipaddress)

    def consumer_exclude_iplist(self, iplist_or_href: Union[str, 'pylo.IPList']):
        if isinstance(iplist_or_href, str):
            self._consumer_iplists_exclude[iplist_or_href] = iplist_or_href
            return

        if isinstance(iplist_or_href, pylo.IPList):
            self._consumer_iplists_exclude[iplist_or_href.href] = iplist_or_href.href
            return

        raise pylo.PyloEx("Unsupported object type {}".format(type(iplist_or_href)))

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

    def provider_include_iplist(self, iplist_or_href: Union[str, 'pylo.IPList']):
        if isinstance(iplist_or_href, str):
            self._provider_iplists[iplist_or_href] = iplist_or_href
            return

        if isinstance(iplist_or_href, pylo.IPList):
            self._provider_iplists[iplist_or_href.href] = iplist_or_href.href
            return

        raise pylo.PyloEx("Unsupported object type {}".format(type(iplist_or_href)))

    def provider_exclude_label(self, label_or_href: Union[str, 'pylo.Label', 'pylo.LabelGroup']):
        self.__filter_prop_add_label(self._provider_exclude_labels, label_or_href)

    def provider_exclude_labels(self, labels_or_hrefs: List[Union[str, 'pylo.Label', 'pylo.LabelGroup']]):
        for label in labels_or_hrefs:
            self.provider_exclude_label(label)

    def provider_exclude_cidr(self, ipaddress: str):
        self.__filter_provider_ip_exclude.append(ipaddress)

    def provider_exclude_iplist(self, iplist_or_href: Union[str, 'pylo.IPList']):
        if isinstance(iplist_or_href, str):
            self._provider_iplists_exclude[iplist_or_href] = iplist_or_href
            return

        if isinstance(iplist_or_href, pylo.IPList):
            self._provider_iplists_exclude[iplist_or_href.href] = iplist_or_href.href
            return

        raise pylo.PyloEx("Unsupported object type {}".format(type(iplist_or_href)))

    def provider_exclude_ip4map(self, map: 'pylo.IP4Map'):
        for item in map.to_list_of_cidr_string(skip_netmask_for_32=True):
            self.provider_exclude_cidr(item)

    def provider_include_cidr(self, ipaddress: str):
        self.__filter_provider_ip_include.append(ipaddress)

    def provider_include_ip4map(self, map: 'pylo.IP4Map'):
        for item in map.to_list_of_cidr_string():
            self.provider_include_cidr(item)

    def service_include_add(self, service: Union['pylo.DirectServiceInRule',str]):
        if isinstance(service, str):
            self._include_direct_services.append(pylo.DirectServiceInRule.create_from_text(service))
            return
        self._include_direct_services.append(service)

    def service_include_add_protocol(self, protocol: int):
        self._include_direct_services.append(pylo.DirectServiceInRule(proto=protocol))

    def service_include_add_protocol_tcp(self):
        self._include_direct_services.append(pylo.DirectServiceInRule(proto=6))

    def service_include_add_protocol_udp(self):
        self._include_direct_services.append(pylo.DirectServiceInRule(proto=17))

    def service_exclude_add(self, service: 'pylo.DirectServiceInRule'):
        self._exclude_direct_services.append(service)

    def service_exclude_add_protocol(self, protocol: int):
        self._exclude_direct_services.append(pylo.DirectServiceInRule(proto=protocol))

    def service_exclude_add_protocol_tcp(self):
        self._exclude_direct_services.append(pylo.DirectServiceInRule(proto=6))

    def service_exclude_add_protocol_udp(self):
        self._exclude_direct_services.append(pylo.DirectServiceInRule(proto=17))

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

    def set_time_to_x_seconds_ago(self, seconds: int):
        self._time_to = datetime.utcnow() - timedelta(seconds=seconds)

    def set_time_to_x_days_ago(self, days: int):
        return self.set_time_to_x_seconds_ago(days*60*60*24)

    def filter_on_policy_decision_unknown(self):
        self._policy_decision_filter.append('unknown')

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
            "max_results": self.max_results,
            "query_name": "api call"
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
            for label in self._consumer_labels.values():
                if label.is_label():
                    tmp.append({'label': {'href': label.href}})
                else:
                    tmp.append({'label_group': {'href': label.href}})
            filters['sources']['include'].append(tmp)

        if len(self._consumer_workloads) > 0:
            tmp = []
            for workload_href in self._consumer_workloads.keys():
                tmp.append({'workload': {'href': workload_href}})
            filters['sources']['include'].append(tmp)

        if len(self._consumer_iplists) > 0:
            tmp = []
            for iplist_href in self._consumer_iplists.keys():
                tmp.append({'ip_list': {'href': iplist_href}})
            filters['sources']['include'].append(tmp)

        if len(self.__filter_consumer_ip_include) > 0:
            tmp = []
            for ip_txt in self.__filter_consumer_ip_include:
                tmp.append({'ip_address': ip_txt})
            filters['sources']['include'].append(tmp)

        if len(self._provider_labels) > 0:
            tmp = []
            for label in self._provider_labels.values():
                if label.is_label():
                    tmp.append({'label': {'href': label.href}})
                else:
                    pass
                    tmp.append({'label_group': {'href': label.href}})
            filters['destinations']['include'].append(tmp)

        if len(self._provider_workloads) > 0:
            tmp = []
            for workload_href in self._provider_workloads.keys():
                tmp.append({'workload': {'href': workload_href}})
            filters['destinations']['include'].append(tmp)

        if len(self._provider_iplists) > 0:
            tmp = []
            for iplist_href in self._provider_iplists.keys():
                tmp.append({'ip_list': {'href': iplist_href}})
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

        if len(self._consumer_iplists_exclude) > 0:
            for iplist_href in self._consumer_iplists_exclude.keys():
                filters['sources']['exclude'].append({'ip_list': {'href': iplist_href}})

        if len(self.__filter_consumer_ip_exclude) > 0:
            for ipaddress in self.__filter_consumer_ip_exclude:
                filters['sources']['exclude'].append({'ip_address': ipaddress})

        provider_exclude_json = []
        if len(self._provider_exclude_labels) > 0:
            for label_href in self._provider_exclude_labels.keys():
                filters['destinations']['exclude'].append({'label': {'href': label_href}})

        if len(self._provider_iplists_exclude) > 0:
            for iplist_href in self._provider_iplists_exclude.keys():
                filters['destinations']['exclude'].append({'ip_list': {'href': iplist_href}})

        if len(self.__filter_provider_ip_exclude) > 0:
            for ipaddress in self.__filter_provider_ip_exclude:
                filters['destinations']['exclude'].append({'ip_address': ipaddress})

        if len(self._include_direct_services) > 0:
            for service in self._include_direct_services:
                filters['services']['include'] .append(service.get_api_json())

        if len(self._exclude_direct_services) > 0:
            for service in self._exclude_direct_services:
                filters['services']['exclude'].append(service.get_api_json())

        if len(self._exclude_processes) > 0:
            for process in self._exclude_processes:
                filters['services']['exclude'].append({'process_name': process})

        # print(filters)
        return filters


class ExplorerQuery:
    def __init__(self, connector: APIConnector, max_results: int = 1500, max_running_time_seconds: int = 1800,
                 check_for_update_interval_seconds: int = 10):
        self.api: APIConnector = connector
        self.filters = ExplorerFilterSetV1(max_results=max_results)
        self.results: Optional[ExplorerResultSetV1] = None
        self.max_running_time_seconds = max_running_time_seconds
        self.check_for_update_interval_seconds = check_for_update_interval_seconds


    def execute(self) -> ExplorerResultSetV1:
        """
        Execute the query and stores the results in the 'results' property.
        It will also return said results for convenience.
        :return:
        """
        self.results = self.api.explorer_search(self.filters, max_running_time_seconds=self.max_running_time_seconds,
                                                check_for_update_interval_seconds=self.check_for_update_interval_seconds)
        return self.results





