import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from typing import Dict, Any

import illumio_pylo as pylo
import sys
import argparse
import time
import json
import csv
import re
import codecs
from queue import Queue
from threading import Thread
import traceback


log = pylo.get_logger()

parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
parser.add_argument('--debug', type=bool, nargs='?', required=False, default=False, const=True,
                    help='Enabled extra debug output')

parser.add_argument('--input', type=str, required=True,
                    help='CSV filename with the logs of interest')

parser.add_argument('--threads-count', type=int, required=False, default=4,
                    help='Number of concurrent threads to be used for API polling')

args = vars(parser.parse_args())

hostname = args['pce']
input_file = args['input']
threads_count = args['threads_count']


if args['debug']:
    pylo.log_set_debug()

connector = pylo.APIConnector.create_from_credentials_in_file(hostname, request_if_missing=True)

csv_headers = None
log_entries = []

print("* Concurrent threads requested: {}".format(threads_count))

print("* Opening CSV file '{}' ...".format(input_file), end='', flush=True)
with open(input_file, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if csv_headers is None:
                csv_headers = row
                csv_headers.append('recalculated pd')
                continue
            log_entries.append(row)
print("OK!")
print("  * CSV has {} lines".format(len(log_entries)))

print("\n Now parsing each line:")
line_count = 0
skip_count = 0
thread_queue = Queue()
resolution_cache = {}


def do_log_resolution(q: Queue, thread_num: int):
    global skip_count
    global resolution_cache

    local_connector = pylo.APIConnector(fqdn=connector.fqdn, port=connector.port,
                                        api_user=connector.api_user, api_key=connector.api_key,
                                        skip_ssl_cert_check= connector.skipSSLCertCheck, org_id=connector.org_id)

    while True:

        log, local_log_number = q.get()

        output_text = ''

        try:
            output_text += "* entry #{} Thread #{}\n".format(local_log_number, thread_num)

            consumer_ip = log[0]
            consumer_name = log[1]
            consumer_hostname = log[2]
            consumer_resolved_name = '*unresolved*'
            if consumer_name is not None and len(consumer_name) > 0:
                consumer_resolved_name = consumer_name
            elif consumer_hostname is not None and len(consumer_hostname) > 0:
                consumer_resolved_name = consumer_hostname

            consumer_role = log[3]
            if len(consumer_role) < 1:
                consumer_role = None

            consumer_app = log[4]
            if len(consumer_app) < 1:
                consumer_app = None

            consumer_env = log[5]
            if len(consumer_env) < 1:
                consumer_env = None

            consumer_loc = log[6]
            if len(consumer_loc) < 1:
                consumer_loc = None

            output_text += "  - consumer: {} - {}|{}|{}|{} - {}\n".format(consumer_resolved_name, consumer_role, consumer_app, consumer_env, consumer_loc, consumer_ip)

            provider_ip = log[7]
            provider_name = log[8]
            provider_hostname = log[9]
            provider_resolved_name = '*unresolved*'
            if provider_name is not None and len(provider_name) > 0:
                provider_resolved_name = provider_name
            elif provider_hostname is not None and len(provider_hostname) > 0:
                provider_resolved_name = provider_hostname

            provider_role = log[10]
            if len(provider_role) < 1:
                provider_role = None

            provider_app = log[11]
            if len(provider_app) < 1:
                provider_app = None

            provider_env = log[12]
            if len(provider_env) < 1:
                provider_env = None

            provider_loc = log[13]
            if len(provider_loc) < 1:
                provider_loc = None

            output_text += "  - provider: {} - {}|{}|{}|{} - {}\n".format(provider_resolved_name, provider_role, provider_app, provider_env, provider_loc, provider_ip)

            port = log[14]
            protocol = log[15]
            service_name = log[17]
            output_text += "  - services: port {} proto {} name {}\n".format(port, protocol, service_name)

            consumer_is_unknown = (consumer_resolved_name == '*unresolved*')
            provider_is_unknown = (provider_resolved_name == '*unresolved*')

            if consumer_is_unknown and provider_is_unknown:
                output_text += "  ** SKIPPING: both consumers and providers are unknown\n"
                log.append('SKIPPED: both consumers and providers are unknown')
                skip_count += 1
                print(output_text)
                q.task_done()
                continue

            consumer_workload_href = None
            provider_workload_href = None

            if not consumer_is_unknown and consumer_ip not in resolution_cache:
                workloads = local_connector.objects_workload_get(include_deleted=False, async_mode=False, filter_by_ip=consumer_ip, max_results=1)
                if len(workloads) == 0:
                    output_text += "  ** SKIPPING: cannot find workload with IP address {}\n".format(consumer_ip)
                    log.append('SKIPPED: cannot resolve consumer IP')
                    skip_count += 1
                    print(output_text)
                    q.task_done()
                    continue

                consumer_workload_href = workloads[0].get('href')
                if consumer_workload_href is None:
                    output_text += "  ** SKIPPING: API unexpected results while looking for 'matches' {}\n".format(consumer_ip)
                    log.append('SKIPPED: cannot resolve consumer IP, API error')
                    skip_count += 1
                    print(output_text)
                    q.task_done()
                    continue
                resolution_cache[consumer_ip] = consumer_workload_href
            elif not consumer_is_unknown:
                consumer_workload_href = resolution_cache[consumer_ip]

            if not provider_is_unknown and provider_ip not in resolution_cache:
                workloads = local_connector.objects_workload_get(include_deleted=False, async_mode=False, filter_by_ip=provider_ip, max_results=1)
                if len(workloads) == 0:
                    output_text += "  ** SKIPPING: cannot find workload with IP address {}\n".format(provider_ip)
                    log.append('SKIPPED: cannot resolve provider IP')
                    skip_count += 1
                    print(output_text)
                    q.task_done()
                    continue
                provider_workload_href = workloads[0].get('href')
                if provider_workload_href is None:
                    output_text += "  ** SKIPPING: API unexpected results while looking for 'matches' {}\n".format(provider_ip)
                    log.append('SKIPPED: cannot resolve provider IP, API error')
                    skip_count += 1
                    print(output_text)
                    q.task_done()
                    continue
                resolution_cache[provider_ip] = provider_workload_href
            elif not provider_is_unknown:
                provider_workload_href = resolution_cache[provider_ip]


            arg_prep_src_ip = None
            if consumer_is_unknown:
                arg_prep_src_ip = consumer_ip

            arg_prep_dst_ip = None
            if provider_is_unknown:
                arg_prep_dst_ip = provider_ip

            output_text += "  - calculating Allow/Block decision... "

            check_result = local_connector.policy_check(protocol=protocol, port=port,
                                                  src_ip=arg_prep_src_ip, src_href=consumer_workload_href,
                                                  dst_ip=arg_prep_dst_ip, dst_href=provider_workload_href)

            if len(check_result) < 1:
                log.append('blocked')
                output_text += "BLOCKED\n"
            else:
                log.append('allowed')
                output_text += "ALLOWED\n"

        except Exception as e:
            skip_count += 1
            output_text += "  ***** SKIPPED: An API ERROR OCCURED!! *****\n{}\n{}\n".format(e, traceback.format_exc())
            log.append("SKIPPED: FAILED TO QUERY API")
            # log.append("SKIPPED: FAILED TO QUERY API: {}".format(e))

        finally:
            print(output_text)
            q.task_done()



for log in log_entries:
    line_count += 1
    thread_queue.put((log, line_count,))

for i in range(threads_count):
    worker = Thread(target=do_log_resolution, args=(thread_queue, i))
    worker.setDaemon(True)
    worker.start()


thread_queue.join()


output_file = os.path.splitext(os.path.abspath(input_file))[0] + '_recalculated.csv'

print("\n\n* Saving groups to '{}'... ".format(output_file), flush=True, end='')
with open(output_file, 'w', newline='') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    filewriter.writerow(csv_headers)
    filewriter.writerows(log_entries)
print("OK!")
print("* {} logs calculations were skipped (check logs for more specifics)".format(skip_count))

print("\n******* END OF SCRIPT *******\n")

