# Pylo

## Introduction
A framework and set of utilities to interact with Illumio's PCE (Policy Compute Engine)


## Example

###  Remove TCP/3389 from all rules using fully object oriented framework
```python
import illumio_pylo as pylo

pce_hostname = 'pce212-beauty-contest.illumio.microsegment.io'

pylo.log_set_debug()
org = pylo.Organization(1)

print("* Loading PCE objects from API: ", end='', flush=True)
org.load_from_saved_credentials(pce_hostname, prompt_for_api_key=True)
print("OK!")

print("* PCE statistics: ", end='', flush=True)
print(org.stats_to_str())

print()

for ruleset in org.RulesetStore.itemsByHRef.values():
    for rule in ruleset.rules_byHref.values():
        for service in rule.services.get_direct_services():
            if service.is_tcp() and service.to_port is None and service.port == 3389:
                print("Rule {} is concerned".format(rule.href))
                rule.services.remove_direct_service(service)
                rule.services.api_sync()


```

### Creating an IPList using raw API calls and json payloads

```python
import illumio_pylo as pylo

pce_hostname = 'pce212-beauty-contest.illumio.microsegment.io'

connector = pylo.APIConnector.create_from_credentials_in_file(pce_hostname, request_if_missing=True)

if connector is None:
    print("****ERROR**** No cached credentials found for PCE {}".format())
    exit(1)

print("PCE Software version is {}".format(connector.get_software_version_string()))

print("* Now downloading Workload JSON...", end='', flush=True)
all_workloads_json = connector.objects_workload_get(max_results=999999, async_mode=False)
print("OK")

print()

print("* Now listing workloads names from JSON data:", end='', flush=True)
for workload_json in all_workloads_json:
    print(" - {} / href:{}".format(workload_json['name'], workload_json['href']))

print()

print("* attempting to create an IP¨List", end='', flush=True)
data = {'name': 'an IPList 2', "ip_ranges": [
    {"from_ip": "192.168.0.0/24"},
    {"from_ip": "172.16.0.0/24"},
]}
result = connector.objects_iplist_create(data)

if 'href' not in result:
    print("****ERROR**** Object was not created, PCE response was: ".format(result))
    exit(1)

print("OK! created with HREF={}".format(result['href']))

print()

```
