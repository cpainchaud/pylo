"""
In this example we will show how to query traffic logs from the PCE using the Explorer APIs.
We will make a query for all traffic logs matching the following conditions:
- Consumer is has a label 'E-PRODUCTION' or 'E-PREPROD'  or is part of IPList 'I-Prod-Networks'
- Provider is workloads named 'webserver1'
- Service is TCP port 80 or 443 or ICMP
- policy decision is allowed
- log was last seen between now and 5 days ago
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

explorer_query = organization.connector.new_explorer_query()

# Filter by a specific device
consumer_labels_names = ['E-PRODUCTION', 'E-PREPROD']
consumer_ip_list_name = 'I-Prod-Networks'

# Lookup for Label objects
consumer_labels = organization.LabelStore.find_label_by_name(consumer_labels_names, raise_exception_if_not_found=True)
# add these labels to the filter
for label in consumer_labels:
    explorer_query.filters.consumer_include_label(label)

# Lookup for IPList object
consumer_iplist = organization.IPListStore.find_by_name(consumer_ip_list_name)
if consumer_iplist is None:
    raise Exception("IPList '{}' not found in PCE!".format(consumer_ip_list_name))
# add this IPList to the filter
explorer_query.filters.consumer_include_iplist(consumer_iplist)

# Filter by services
explorer_query.filters.service_include_add_protocol(1)  # icmp
explorer_query.filters.service_include_add('tcp/80')  # HTTP
explorer_query.filters.service_include_add('tcp/443')  # HTTPS

# Filter by policy decision
explorer_query.filters.filter_on_policy_decision_allowed()

#filter on last seen time
explorer_query.filters.set_time_from_x_days_ago(5)


# Query the PCE for traffic logs matching the filter
print("Querying PCE for traffic logs matching the filter... ", end='', flush=True)
traffic_logs = explorer_query.execute()
print("OK! found {} traffic logs".format(traffic_logs.count_records()))

# Print the results
records = traffic_logs.get_all_records()

for record in records:
    if record.source_is_workload():
        workload = record.get_source_workload(organization)
        consumer_text = "Workload '{}'()".format(workload.name, workload.get_labels_str())
    else:
        consumer_text = "IP '{}'".format(record.source_ip)

    if record.destination_is_workload():
        workload = record.get_destination_workload(organization)
        provider_text = "Workload '{}'()".format(workload.name, workload.get_labels_str())
    else:
        provider_text = "IP '{}'".format(record.destination_ip)

    service_text = record.service_to_str()

    print("Traffic log: {} -> {} via {} (policy decision: {})".format(consumer_text, provider_text, service_text, record.policy_decision_string))

