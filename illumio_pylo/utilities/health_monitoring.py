import os
import sys
import argparse
import math
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo as pylo


# <editor-fold desc="Argparse stuff">
parser = argparse.ArgumentParser(description='TODO LATER', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--pick-report-hostname', '-p', type=str, required=False, default=None,
                    help='if script cannot guess which PCE hostname you were looking for, use this option to force it')
parser.add_argument('--debug', action='store_true',
                    help='extra debugging messages for developers')
# </editor-fold>

args = vars(parser.parse_args())

if args['debug']:
    pylo.log_set_debug()

hostname = args['pce']

pylo.log.debug(" * Looking for credentials for PCE '{}'... ".format(hostname))
connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)
pylo.log.debug("OK!")

pylo.log.debug(" * Getting cluster health details... ")
health_collection = connector.cluster_health_get(return_object=True)
pylo.log.debug("OK!")

picked_hostname = connector.hostname
if args['pick_report_hostname'] is not None:
    picked_hostname = args['pick_report_hostname']

health_data = health_collection.get(picked_hostname)

if health_data is None:
    pylo.log.error("No report for hostname '{}' was found in the collection! The following were available: {}.\n"
    "You can use --pick-report-hostname to force usage of specific one.".format(picked_hostname, pylo.string_list_to_text(health_collection.keys())))
    sys.exit(1)

pylo.log.debug("Status details:\n{}\n".format(health_data.to_string()))

warning_messages = []
error_messages = []

if health_data.status_is_warning():
    warning_messages.append("Cluster has reported global 'warning' status read the entire report or issues troubleshooting command to get more details")
elif health_data.status_is_error():
    warning_messages.append("Cluster has reported global 'error' status read the entire report or issues troubleshooting command to get more details")

broken_nodes = {}
working_nodes = {}

for node in health_data.nodes_dict.values():
    if node.is_offline_or_unreachable():
        broken_nodes[node] = node
        warning_messages.append("Node {}/{}/IP:{} is offline or not reachable by other members".format(node.name, node.type, node.ip_address))
    else:
        working_nodes[node] = node

for node in working_nodes.values():
    troubled_services = node.get_troubled_services()
    if len(troubled_services) > 0:
        broken_nodes[node] = node
        warning_messages.append("Node '{}'/{}/IP:{} has several non-functional services: {}".format(node.name, node.type, node.ip_address, pylo.string_list_to_text(troubled_services)))


data1_is_broken = False
data0_is_broken = False
for node in broken_nodes:
    if node.type == 'data0':
        data0_is_broken = True
    if node.type == 'data1':
        data1_is_broken = True

if data1_is_broken or data0_is_broken:
    error_messages.append("Data1 or Data0 is down so Database has no resiliency anymore, please fix this situation as soon as possible.")

report = {'warning_messages': warning_messages, 'error_messages': error_messages}

if len(error_messages) > 0:
    report['general_status'] = "error"
    exit_code = 1
elif len(warning_messages) > 0:
    report['general_status'] = "warning"
    exit_code = 2
else:
    report['general_status'] = "normal"
    exit_code = 0

print("{}".format(pylo.nice_json(report)))

exit(exit_code)



