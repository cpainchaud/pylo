import os
import json
from typing import *
import sys
from typing import Dict, Any, List, TypedDict
import socket

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
            self.inbound_identified = 'Inbound Assets'
            self.outbound_identified = 'Outbound Assets'
#            self.inbound_onboarded = 'I Onboarded'
            self.inbound_unidentified = 'Inbound IP'
            self.inbound_cs_identified = 'I CoreService'
#            self.outbound_onboarded = 'O Onboarded'
            self.outbound_unidentified = 'Outbound IP'
            self.outbound_cs_identified = 'O CoreService'

    class Columns:

        class Field:
            class __Data(TypedDict):
                nice_name: Optional[str]
                max_width: Optional[int]
            src_app: __Data = {'max_width': 15, 'name': 'src_application'}
            src_loc: __Data = {'max_width': 15, 'name': 'src_location'}
            src_hostname: __Data = {'max_width': 15, 'name': 'src_hostname'}
            dst_app: __Data = {'max_width': 15, 'name': 'dst_application'}
            dst_hostname: __Data = {'max_width': 15, 'name': 'dst_hostname'}

            proc_name: __Data = {'max_width': 15, 'name': 'process_name'}
            username: __Data = {'max_width': 15, 'name': 'username'}

        fields = Field()

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
        inbound_identified = ['src_ip', fields.src_hostname, 'src_role', fields.src_app, 'src_environment', fields.src_loc,
                                           'dst_ip', fields.dst_hostname, 'dst_role', # fields.dst_app, 'dst_environment', 'dst_location',
                                           'dst_port', 'dst_proto', 'count', fields.proc_name, fields.username,'allow',
                                           'last_seen', 'first_seen', 'onboarded']
        outbound_identified = ['src_ip', fields.src_hostname, 'src_role', # 'src_application', 'src_environment', fields.src_loc,
                                            'dst_ip', fields.dst_hostname, 'dst_role', fields.dst_app, 'dst_environment', 'dst_location',
                                            'dst_port', 'dst_proto', 'count', fields.proc_name, fields.username, 'allow',
                                            'last_seen', 'first_seen', 'onboarded']
        inbound_onboarded = ['src_ip', fields.src_hostname, 'src_role', fields.src_app, 'src_environment', fields.src_loc,
                                          'dst_ip', fields.dst_hostname, 'dst_role', # fields.dst_app, 'dst_environment', 'dst_location',
                                          'dst_port', 'dst_proto', 'count', fields.proc_name, fields.username, 'allow',
                                          'last_seen', 'first_seen']
        outbound_onboarded = ['src_ip', fields.src_hostname, 'src_role', # 'src_application', 'src_environment', fields.src_loc,
                                             'dst_ip', fields.dst_hostname, 'dst_role', fields.dst_app, 'dst_environment', 'dst_location',
                                             'dst_port', 'dst_proto', 'count', fields.proc_name, fields.username,
                                             'allow', 'last_seen', 'first_seen',]
        inbound_unidentified = ['src_ip', fields.src_hostname, 'src_iplists',
                                             'dst_ip', fields.dst_hostname, 'dst_role', # fields.dst_app, 'dst_environment', 'dst_location',
                                             'dst_port', 'dst_proto', 'count',
                                             fields.proc_name, fields.username, 'allow', 'last_seen', 'first_seen']
        outbound_unidentified = ['src_ip', fields.src_hostname, 'src_role', # 'src_application', 'src_environment', fields.src_loc,
                                              'dst_ip', fields.dst_hostname, 'dst_iplists',
                                              'dst_port', 'dst_proto', 'count', fields.proc_name, fields.username, 'allow', 'last_seen', 'first_seen',
                                              ]
        inbound_cs_identified = ['src_ip', fields.src_hostname, 'src_role', fields.src_app, 'src_environment', fields.src_loc,
                                              'dst_ip', fields.dst_hostname, 'dst_role', # fields.dst_app, 'dst_environment', 'dst_location',
                                              'dst_port', 'dst_proto', 'count',
                                              fields.proc_name, fields.username, 'allow', 'last_seen', 'first_seen']
        outbound_cs_identified = ['src_ip', fields.src_hostname, 'src_role', # 'src_application', 'src_environment', fields.src_loc,
                                               'dst_ip', fields.dst_hostname, 'dst_role', fields.dst_app, 'dst_environment', 'dst_location',
                                               'dst_port', 'dst_proto', 'count',
                                               fields.proc_name, fields.username, 'allow', 'last_seen', 'first_seen']

    def __init__(self):
        self.title = self.Titles()
        self.columns = self.Columns()


excel_struct = ExcelStruct()


class __Data(TypedDict):
    title: str
    columns: List[str]
    force_all_wrap_text: bool

default_sheets_options = {'multivalue_cell_delimiter': "\n"}
excel_sheets_creation_order: List[__Data] = [

    # Inbound
    {'title': excel_struct.title.inbound_identified, 'columns': excel_struct.columns.inbound_identified, 'force_all_wrap_text': True, 'color': 'CCCC00',
     'order_by': ['dst_port', 'dst_proto', 'src_application', 'src_role', 'src_ip']},
    #{'title': excel_struct.title.inbound_onboarded, 'columns': excel_struct.columns.inbound_onboarded, 'force_all_wrap_text': True},
    {'title': excel_struct.title.inbound_unidentified, 'columns': excel_struct.columns.inbound_unidentified, 'force_all_wrap_text': True, 'color': 'CCCC00',
     'order_by': ['dst_port', 'dst_proto', 'dst_role', 'src_ip']},

    # Outbound
    {'title': excel_struct.title.outbound_identified, 'columns': excel_struct.columns.outbound_identified, 'force_all_wrap_text': True, 'color': 'CCCC00',
     'order_by': ['dst_port', 'dst_proto', 'dst_application', 'dst_role', 'dst_ip']},
    #{'title': excel_struct.title.outbound_onboarded, 'columns': excel_struct.columns.outbound_onboarded, 'force_all_wrap_text': True},
    {'title': excel_struct.title.outbound_unidentified, 'columns': excel_struct.columns.outbound_unidentified, 'force_all_wrap_text': True, 'color': 'CCCC00',
     'order_by': ['dst_port', 'dst_proto', 'src_role', 'dst_ip']},


    {'title': excel_struct.title.inbound_cs_identified, 'columns': excel_struct.columns.inbound_cs_identified, 'force_all_wrap_text': True},
    {'title': excel_struct.title.outbound_cs_identified, 'columns': excel_struct.columns.outbound_cs_identified, 'force_all_wrap_text': True},

    {'title': excel_struct.title.workloads, 'columns': excel_struct.columns.workloads, 'force_all_wrap_text': True},
    {'title': excel_struct.title.rulesets, 'columns': excel_struct.columns.rulesets, 'force_all_wrap_text': True},
    {'title': excel_struct.title.fingerprint, 'columns': excel_struct.columns.fingerprint, 'force_all_wrap_text': True},
]

for(sheet_data) in excel_sheets_creation_order:
    excel_doc.create_sheet(sheet_data['title'], sheet_data['columns'], sheet_data['force_all_wrap_text'], sheet_data.get('color'),
                           order_by=sheet_data.get('order_by', None),
                           multivalues_cell_delimiter=sheet_data.get('multivalue_cell_delimiter', default_sheets_options['multivalue_cell_delimiter']))


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


_cached_dns_resolutions: Dict[str, str] = {}

def get_dns_resolution(ip_address_str: str) -> str:
    global _cached_dns_resolutions

    if ip_address_str in _cached_dns_resolutions:
        return _cached_dns_resolutions[ip_address_str]

    try:
        dns_name = socket.gethostbyaddr(ip_address_str)[0]
        _cached_dns_resolutions[ip_address_str] = dns_name
    except socket.herror:
        _cached_dns_resolutions[ip_address_str] = ''

    return _cached_dns_resolutions[ip_address_str]










