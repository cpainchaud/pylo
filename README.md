[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/cpainchaud/pylo)

# Pylo

## Overview / Index
A quick index to help you navigate this repository:

- [Introduction](#introduction) — what Pylo is and its intent.
- [API Framework](#api-framework) — short code examples showing common tasks.
- [CLI Tools](#cli-tools) — pointers to example scripts and how-to docs for automation utilities.
- Docs: [docs/cli/label-delete-unused.md](docs/cli/label-delete-unused.md) — developer and CLI documentation (see `docs/cli/`).
- Example scipts: [examples/](examples/) — runnable example scripts demonstrating library usage.
- Tests: [tests/README_test_fixtures.md](tests/README_test_fixtures.md) — pytest-based tests and fixtures.
- License: [LICENSE](LICENSE) — project license and attribution.


## Introduction
A Python API Framework and set of tools to interact with Illumio's PCE (Policy Compute Engine)


## API Framework

### Remove TCP/3389 from all rules using fully object oriented framework
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
    print("****ERROR**** No cached credentials found for PCE {}".format(pce_hostname))
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

## CLI Tools

Pylo includes several documented command-like utilities intended for operational tasks against an Illumio PCE. Full, detailed usage (options, examples, and flags) is available in the `docs/cli/` directory.

## Binary distribution
Windows binaries: Prebuilt standalone Windows executables are provided so you can run the CLI tools without installing Python or any dependencies. Check the project's [Releases page](https://github.com/cpainchaud/pylo/releases) for all numbered releases but also:
- [Latest release](https://github.com/cpainchaud/pylo/releases/tag/latest) — includes the most recent stable version with all CLI tools and documentation.
- [DEV release](https://github.com/cpainchaud/pylo/releases/tag/dev-latest) — includes the most recent development version with experimental features and bug fixes.

## Documented CLI utilities
- [`cred-manager`](docs/cli/cred-manager.md) — Manage cached PCE credentials used by the CLI utilities (create, update, delete, list).
- [`iplist-analyzer`](docs/cli/iplist-analyzer.md) — Analyze IP lists to find overlaps, gaps, and statistics for IP range usage.
- [`iplist-import`](docs/cli/iplist-import.md) — Import IP ranges into the PCE as IPList objects from CSV/JSON sources.
- [`label-delete-unused`](docs/cli/label-delete-unused.md) — Identify unused labels across the PCE and optionally delete them; generates CSV/XLSX/JSON reports and runs in dry-run mode by default.
- [`pce-objects-cache-updater`](docs/cli/pce-objects-cache-updater.md) — Refresh or rebuild the local cache of PCE objects used by offline/CLI tools.
- [`rule-export`](docs/cli/rule-export.md) — Export firewall/security rules from the PCE into CSV/JSON formats for reporting or migration.
- [`traffic-export`](docs/cli/traffic-export.md) — Export traffic records from the PCE with flexible filtering (labels, IP lists, time ranges), formatting, and column customization.
- [`ven-compatibility-report-export`](docs/cli/ven-compatibility-report-export.md) — Generate compatibility reports for VENs (visibility/compatibility across versions or configurations).
- [`ven-duplicate-remover`](docs/cli/ven-duplicate-remover.md) — Detect and help remove duplicate workload/VEN entries that share the same hostname; includes protection rules and dry-run by default.
- [`ven-idle-to-visibility`](docs/cli/ven-idle-to-visibility.md) — Convert idle VENs to visible state or generate reports to recover idle VENs (visibility management utilities).
- [`ven-upgrade`](docs/cli/ven-upgrade.md) — Assist with VEN upgrade workflows (reporting and helper steps to prepare VENs for upgrades).
- [`workload-export`](docs/cli/workload-export.md) — Export workload objects and related metadata from the PCE.
- [`workload-import`](docs/cli/workload-import.md) — Import workloads into the PCE from structured CSV/JSON inputs.
- [`workload-resync-names`](docs/cli/workload-resync-names.md) — Resynchronize workload names (useful when hostnames or naming conventions change).

Where to find full usage
- See the individual command docs in `docs/cli/` for complete option lists, examples, and recommended safe workflows (each doc contains usage examples and notes).
- Most commands provide their own `--help` output; consult the corresponding `docs/cli/<command>.md` file for the exact invocation and flags.
