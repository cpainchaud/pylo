import os
import json
from typing import *
import sys
from typing import Dict, Any, List, TypedDict

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo



core_service_label_group_name: str = ''
onboarded_apps_label_group: str = ''

excluded_ranges = pylo.IP4Map()
excluded_broadcast = pylo.IP4Map()
excluded_direct_services: List['pylo.DirectServiceInRule'] = []

excluded_processes: Dict[str, str] = {}

pce_listing: Dict[str, pylo.APIConnector] = {}

excel_doc = pylo.ArraysToExcel()


class ExcelStruct:
    class Titles:
        def __init__(self):
            self.fingerprint = 'DO NOT EDIT'
            self.workloads = 'Server Estate'
            self.rulesets = 'Rulesets'
            self.inbound_identified = 'Inbound Id'
            self.outbound_identified = 'Outbound Id'
            self.inbound_onboarded = 'I Onboarded'
            self.inbound_unidentified = 'I Unknown'
            self.inbound_cs_identified = 'I CoreService'
            self.outbound_onboarded = 'O Onboarded'
            self.outbound_unidentified = 'O Unknown'
            self.outbound_cs_identified = 'O CoreService'

    class Columns:
        workloads = ['hostname', 'role', 'application', 'environment', 'location',
                     'mode', 'interfaces', 'ven version', 'os', 'os_detail']
        rulesets = [
            {'name': 'ruleset', 'nice_name': 'Ruleset', },
            {'name': 'scopes', 'nice_name': 'Scopes'},
            # {'name': 'extra_scope', 'nice_name': 'Extra Scope'},
            {'name': 'consumers', 'nice_name': 'Source'},
            {'name': 'providers', 'nice_name': 'Destination'},
            {'name': 'services', 'nice_name': 'Services'} ]
        fingerprint = ['app', 'env', 'loc']
        inbound_identified = ['src_ip', 'src_hostname', 'src_role', 'src_application', 'src_environment', 'src_location',
                                           'dst_ip', 'dst_hostname', 'dst_role', # 'dst_application', 'dst_environment', 'dst_location',
                                           'dst_port', 'dst_proto', 'count', 'process_name', 'username',
                                           'last_seen', 'first_seen',
                                           'to_be_implemented']
        outbound_identified = ['src_ip', 'src_hostname', 'src_role', # 'src_application', 'src_environment', 'src_location',
                                            'dst_ip', 'dst_hostname', 'dst_role', 'dst_application', 'dst_environment', 'dst_location',
                                            'dst_port', 'dst_proto', 'count', 'process_name', 'username',
                                            'last_seen', 'first_seen', 'to_be_implemented']
        inbound_onboarded = ['src_ip', 'src_hostname', 'src_role', 'src_application', 'src_environment', 'src_location',
                                          'dst_ip', 'dst_hostname', 'dst_role', # 'dst_application', 'dst_environment', 'dst_location',
                                          'dst_port', 'dst_proto', 'count', 'process_name', 'username',
                                          'last_seen', 'first_seen',
                                          'to_be_implemented']
        outbound_onboarded =   ['src_ip', 'src_hostname', 'src_role', # 'src_application', 'src_environment', 'src_location',
                                             'dst_ip', 'dst_hostname', 'dst_role', 'dst_application', 'dst_environment', 'dst_location',
                                             'dst_port', 'dst_proto', 'count', 'process_name', 'username',
                                             'to_be_implemented', 'last_seen', 'first_seen',]
        inbound_unidentified = ['src_ip', 'src_name', 'src_iplists',
                                             'dst_ip', 'dst_hostname', 'dst_role', # 'dst_application', 'dst_environment', 'dst_location',
                                             'dst_port', 'dst_proto', 'count',
                                             'process_name', 'username', 'last_seen', 'first_seen',]
        outbound_unidentified = ['src_ip', 'src_hostname', 'src_role', # 'src_application', 'src_environment', 'src_location',
                                              'dst_ip', 'dst_name', 'dst_iplists',
                                              'dst_port', 'dst_proto', 'count', 'process_name', 'username', 'last_seen', 'first_seen',
                                              ]
        inbound_cs_identified = ['src_ip', 'src_hostname', 'src_role', 'src_application', 'src_environment', 'src_location',
                                              'dst_ip', 'dst_hostname', 'dst_role', # 'dst_application', 'dst_environment', 'dst_location',
                                              'dst_port', 'dst_proto', 'count',
                                              'process_name', 'username', 'last_seen', 'first_seen']
        outbound_cs_identified = ['src_ip', 'src_hostname', 'src_role', # 'src_application', 'src_environment', 'src_location',
                                               'dst_ip', 'dst_hostname', 'dst_role', 'dst_application', 'dst_environment', 'dst_location',
                                               'dst_port', 'dst_proto', 'count',
                                               'process_name', 'username' 'last_seen', 'first_seen',]

    def __init__(self):
        self.title = self.Titles()
        self.columns = self.Columns()


excel_struct = ExcelStruct()


class __Data(TypedDict):
    title: str
    columns: List[str]
    force_all_wrap_text: bool


excel_sheets_creation_order: List[__Data] = [

    # Inbound
    {'title': excel_struct.title.inbound_identified, 'columns': excel_struct.columns.inbound_identified, 'force_all_wrap_text': False},
    {'title': excel_struct.title.inbound_onboarded, 'columns': excel_struct.columns.inbound_onboarded, 'force_all_wrap_text': False},
    {'title': excel_struct.title.inbound_unidentified, 'columns': excel_struct.columns.inbound_unidentified, 'force_all_wrap_text': False},

    # Outbound
    {'title': excel_struct.title.outbound_identified, 'columns': excel_struct.columns.outbound_identified, 'force_all_wrap_text': False},
    {'title': excel_struct.title.outbound_onboarded, 'columns': excel_struct.columns.outbound_onboarded, 'force_all_wrap_text': False},
    {'title': excel_struct.title.outbound_unidentified, 'columns': excel_struct.columns.outbound_unidentified, 'force_all_wrap_text': False},


    {'title': excel_struct.title.inbound_cs_identified, 'columns': excel_struct.columns.inbound_cs_identified, 'force_all_wrap_text': False},
    {'title': excel_struct.title.outbound_cs_identified, 'columns': excel_struct.columns.outbound_cs_identified, 'force_all_wrap_text': False},

    {'title': excel_struct.title.workloads, 'columns': excel_struct.columns.workloads, 'force_all_wrap_text': False},
    {'title': excel_struct.title.rulesets, 'columns': excel_struct.columns.rulesets, 'force_all_wrap_text': False},
    {'title': excel_struct.title.fingerprint, 'columns': excel_struct.columns.fingerprint, 'force_all_wrap_text': False},
]

for(sheet_data) in excel_sheets_creation_order:
    excel_doc.create_sheet(sheet_data['title'], sheet_data['columns'], sheet_data['force_all_wrap_text'])


def load_config_file(filename='c2_config.json') -> bool:
    config_filename = filename
    if not os.path.isfile(config_filename):
        raise pylo.PyloEx("Cannot find config file '{}'".format(config_filename))

    with open(config_filename) as json_file:
        data = json.load(json_file)

        pce_listing_data = data.get('pce_listing')
        if pce_listing_data is not None:
            pce_data: Dict
            for pce_data in pce_listing_data:
                org_id = pce_data.get('org_id')
                if org_id is None:
                    org_id = 1
                connector = pylo.APIConnector(hostname=pce_data['fqdn'], port=pce_data['port'],
                                              apiuser=pce_data['api_user'], apikey=pce_data['api_key'],
                                              orgID=org_id, skip_ssl_cert_check=True)
                pce_listing[pce_data['name'].lower()] = connector

        global core_service_label_group_name
        core_service_label_group_name = data.get('core_service_label_group')
        if core_service_label_group_name is None:
            raise pylo.PyloEx('Config file is missing property "core_service_label_group_name"')

        global onboarded_apps_label_group
        onboarded_apps_label_group = data.get('onboarded_apps_label_group')
        if onboarded_apps_label_group is None:
            raise pylo.PyloEx('Config file is missing property "onboarded_apps_label_group"')

        excluded_ranges_data: List[str] = data.get('excluded_networks')
        if excluded_ranges_data is not None:
            if type(excluded_ranges_data) is not list:
                raise pylo.PyloEx("excluded_ranges is not a list:", excluded_ranges_data)

            for network_range in excluded_ranges_data:
                excluded_ranges.add_from_text(network_range)

        excluded_broadcast_data: List[str] = data.get('excluded_broadcast_addresses')
        if excluded_broadcast_data is not None:
            if type(excluded_broadcast_data) is not list:
                raise pylo.PyloEx("excluded_broadcast_addresses is not a list:", excluded_broadcast_data)

            for broadcast_ip in excluded_broadcast_data:
                excluded_broadcast.add_from_text(broadcast_ip)

        excluded_services_data: List[str] = data.get('excluded_services')
        if excluded_services_data is not None:
            if type(excluded_services_data) is not list:
                raise pylo.PyloEx("excluded_services is not a list:", excluded_services_data)

            for service in excluded_services_data:
                excluded_direct_services.append(pylo.DirectServiceInRule.create_from_text(service, protocol_first=False))

        excluded_processes_data: List[str] = data.get('excluded_processes')
        if excluded_processes_data is not None:
            for process in excluded_processes_data:
                excluded_processes[process] = process

    return True


def print_stats():
    print("  - PCE listing entries count: {}".format(len(pce_listing)))
    print("  - Network address exclusions entries count: {}".format(excluded_ranges.count_entries()))
    print("  - Broadcast IP manual exclusion entries count: {}".format(excluded_broadcast.count_entries()))
    print("  - Service exclusions entries count: {}".format(len(excluded_direct_services)))
    print("  - Process names exclusions entries count: {}".format(len(excluded_processes)))





