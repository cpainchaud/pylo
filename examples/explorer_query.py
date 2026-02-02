"""
In this example we will show how to query traffic logs from the PCE using the Explorer V2 APIs.
We will make a query for all traffic logs matching the following conditions:
- Source (consumer) has a label 'E-PRODUCTION' or 'E-PREPROD' or is part of IPList 'I-Prod-Networks'
- Destination (provider) can be any workload or IP
- Service is TCP port 80 or 443 or ICMP protocol
- Policy decision is 'allowed'
- Traffic was detected within the last 5 days
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))  # only required if you run this script from the examples folder without installing pylo
import illumio_pylo as pylo

# PCE connection parameters
pce_hostname = 'pce1.company.com'
pce_port = 9443
pce_api_user = 'api_xxxxxxxxx'
pce_api_key = 'xxxxxxxxxxxxxxxxxxxx'
pce_org_id = 1
pce_verify_ssl = True

print("Loading organization from PCE '{}'... ".format(pce_hostname), end='', flush=True)
organization = pylo.get_organization(pce_hostname, pce_port, pce_api_user, pce_api_key, pce_org_id, pce_verify_ssl)
print("OK!")

# Create a new V2 Explorer query with a max of 1500 results
explorer_query = organization.connector.new_explorer_query_v2(max_results=1500)

# Define source filter criteria
source_labels_names = ['E-PRODUCTION', 'E-PREPROD']
source_ip_list_name = 'I-Prod-Networks'

# Create a source filter that combines labels and IPList (treated with OR logic)
source_filter = explorer_query.filters.new_source_filter()

# Lookup and add Label objects to the source filter
for label_name in source_labels_names:
    label_search_result = organization.LabelStore.find_label_by_name(label_name, raise_exception_if_not_found=False, case_sensitive=False)
    if len(label_search_result) == 0:
        raise pylo.PyloEx("Label '{}' not found in PCE!".format(label_name))
    elif len(label_search_result) > 1:
        raise pylo.PyloEx("Multiple labels found for name '{}', please use a more specific name!".format(label_name))
    source_filter.add_label(label_search_result[0])

# Lookup and add IPList object to the source filter
source_iplist = organization.IPListStore.find_by_name(source_ip_list_name)
if source_iplist is None:
    raise pylo.PyloEx("IPList '{}' not found in PCE!".format(source_ip_list_name))
source_filter.add_iplist(source_iplist)

# Filter by services (ICMP, HTTP, HTTPS)
explorer_query.filters.service_include_add_protocol(1)  # ICMP
explorer_query.filters.service_include_add('tcp/80')  # HTTP
explorer_query.filters.service_include_add('tcp/443')  # HTTPS

# Filter by policy decision (only allowed traffic)
explorer_query.filters.filter_on_policy_decision_allowed()

# Filter by time range (last 5 days)
explorer_query.filters.set_time_from_x_days_ago(5)

# Execute the query and retrieve traffic logs
print("Querying PCE for traffic logs matching the filter... ", end='', flush=True)
traffic_logs = explorer_query.execute()
records = traffic_logs.get_all_records()
print("OK! Found {} traffic log(s)".format(len(records)))

# Print the results
for record in records:
    # Format source information
    if record.source_is_workload():
        workload = record.get_source_workload(organization)
        # Get labels as a formatted string
        label_values = [record.source_workload_labels_by_type.get(lt) for lt in organization.LabelStore.label_types]
        label_values = [lv for lv in label_values if lv is not None]
        labels_str = '|'.join(label_values) if label_values else 'unlabeled'
        consumer_text = "Workload '{}' ({})".format(workload.name if workload else record.source_workload_hostname, labels_str)
    else:
        consumer_text = "IP '{}'".format(record.source_ip)

    # Format destination information
    if record.destination_is_workload():
        workload = record.get_destination_workload(organization)
        # Get labels as a formatted string
        label_values = [record.destination_workload_labels_by_type.get(lt) for lt in organization.LabelStore.label_types]
        label_values = [lv for lv in label_values if lv is not None]
        labels_str = '|'.join(label_values) if label_values else 'unlabeled'
        provider_text = "Workload '{}' ({})".format(workload.name if workload else record.destination_workload_hostname, labels_str)
    else:
        provider_text = "IP '{}'".format(record.destination_ip)

    # Format service information
    service_text = record.service_to_str()

    # Print the traffic log entry
    print("Traffic log: {} -> {} via {} (policy decision: {})".format(
        consumer_text, provider_text, service_text, record.policy_decision_string))

