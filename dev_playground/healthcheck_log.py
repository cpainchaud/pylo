import requests
import argparse
import datetime
import time


requests.packages.urllib3.disable_warnings()


parser = argparse.ArgumentParser(description='TODO LATER')
parser.add_argument('--pce', type=str, required=True,
                    help='hostname of the PCE')
args = vars(parser.parse_args())
hostname = args['pce']

now = datetime.datetime.now()

file_name = hostname + '.log'
msg = "{} |{}| SCRIPT STARTED, saving logs in {}".format(now, hostname, file_name)
file = open(file_name, 'a')
file.write(msg+"\n")
print(msg)

url_v2 = 'https://' + hostname + ':8440/api/v2/node_available'
url_v1 = 'https://' + hostname + ':8440/api/v1/node_available'
url = url_v1

while True:

    error = False
    msg = '*BLANK*'

    try:
        result = requests.request('GET', url, verify=False)
        msg = '{} |{}| STATUS CODE {}: {}'.format(datetime.datetime.now(),
                                                  hostname,
                                                  result.status_code,
                                                  result.reason)

    except Exception as e:
        error = True
        msg = '{} |{}| STATUS CODE {}: {}'.format(datetime.datetime.now(),
                                                  hostname,
                                                  'ERROR',
                                                  e.__str__())

    file.write(msg+"\n")
    file.flush()
    print(msg)
    time.sleep(5)


