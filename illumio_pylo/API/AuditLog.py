import logging
from typing import Optional, List, Literal

import illumio_pylo as pylo
from illumio_pylo.API.JsonPayloadTypes import AuditLogApiReplyEventJsonStructure, AuditLogApiEventType


class AuditLogQueryResult:
    def __init__(self, raw_json: AuditLogApiReplyEventJsonStructure):
        self.raw_json: AuditLogApiReplyEventJsonStructure = raw_json

    def type_is(self, log_type: AuditLogApiEventType):
        return self.raw_json == log_type


class AuditLogQueryResultSet:
    def __init__(self, json_data: List[AuditLogApiReplyEventJsonStructure]):
        self.results: List[AuditLogQueryResult] = []
        #read json data in reverse order
        for log in json_data[::-1]:
            self.results.append(AuditLogQueryResult(log))


class AuditLogFilterSet:
    def __init__(self):
        self.event_type: Optional[str] = None
        self.datetime_starts_from = None
        self.datetime_ends_at = None


class AuditLogQuery:
    def __init__(self,  connector: 'pylo.APIConnector', max_results: int = 1000, max_running_time_seconds: int = 120):
        self.api: 'pylo.APIConnector' = connector
        self.filters: AuditLogFilterSet = AuditLogFilterSet()
        self.max_running_time_seconds = max_running_time_seconds
        self.max_results = max_results

    def execute(self) -> AuditLogQueryResultSet:
        json_result = self.api.audit_log_query(max_results=self.max_results, event_type= self.filters.event_type)
        result_set = AuditLogQueryResultSet(json_result)
        return result_set

