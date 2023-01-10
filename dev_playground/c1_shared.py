import os
import json
from typing import *
import sys
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import pylo


excel_doc_sheet_fingerprint_title = 'DO NOT EDIT'
excel_doc_sheet_inbound_identified_title = 'Id Inbound'
excel_doc_sheet_outbound_identified_title = 'Id Outbound'

excluded_ranges = pylo.IP4Map()
excluded_broadcast = pylo.IP4Map()
excluded_direct_services: List['pylo.DirectServiceInRule'] = []

excluded_processes: Dict[str, str] = {}

pce_listing: Dict[str, pylo.APIConnector] = {}


def load_config_file(filename='c1_config.json') -> bool:
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
                                              org_id=org_id, skip_ssl_cert_check=True)
                pce_listing[pce_data['name'].lower()] = connector


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





