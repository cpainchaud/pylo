import illumio_pylo as pylo
from illumio_pylo import log
import logging


log.setLevel(logging.DEBUG)


con = pylo.APIConnector(fqdn="192.168.253.10", port=8443, skip_ssl_cert_check=True, api_user='api_185c2399e24b631c2',
                        api_key='2dbaf8fe5bb9278e26388e5d35229a6797ff5c622154b6289edfc03e7c0f9782')

con.collect_pce_infos()

print('*** Successful connection to PCE ' + con.hostname + ' running ASP version ' + con.get_software_version_string())

print("\n")
print("** Now loading Organization from API...")
org = pylo.Organization(1)
org.load_from_api(con)

print("*** Successfully loaded all Organization configuration")





