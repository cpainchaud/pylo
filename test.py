import pylo
from pylo import log
import logging


log.setLevel(logging.DEBUG)


con = pylo.APIConnector(hostname="192.168.253.10", port=8443, skip_ssl_cert_check=True, apiuser='api_185c2399e24b631c2',
                        apikey='2dbaf8fe5bb9278e26388e5d35229a6797ff5c622154b6289edfc03e7c0f9782')

con.collectPceInfos()

print('*** Successful connection to PCE ' + con.hostname + ' running ASP version ' + con.getSoftwareVersionString())

print("\n")
print("** Now loading Organization from API...")
org = pylo.Organization(1)
org.load_from_api(con)

print("*** Successfully loaded all Organization configuration")





