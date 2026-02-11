# Workload Export Command

## Overview

The `workload-export` command exports workload information from the PCE into structured reports. It supports powerful filtering options including SQL-like queries, file-based filters, and extensive customization of output columns and formats.

## Command Syntax

```bash
pylo workload-export [OPTIONS]
```

## Options

### Output Format Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format: `csv`, `xlsx`, or `json` (can be repeated) |
| `--output-dir` | `-o` | string | `output` | Directory where to save output files |
| `--output-filename` | - | string | Auto-generated | Custom filename for the report |

### Filter Options

#### SQL-Like Query Filter

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--filter-query` | `-q` | string | Filter using SQL-like query expression |

**Query Syntax:**
- Operators: `==`, `!=`, `<`, `>`, `<=`, `>=`, `contains`, `matches` (regex)
- Logic: `and`, `or`, `not`, `(`, `)`
- Fields: `name`, `hostname`, `online`, `managed`, `deleted`, `ip_address`, `last_heartbeat`, `mode`, `env`, `app`, `role`, `loc`, `os_id`, etc.

**Examples:**
```bash
# Simple equality
--filter-query "env == 'Production'"

# Multiple conditions
--filter-query "env == 'Production' and online == true"

# Contains operator
--filter-query "name contains 'web'"

# Complex logic
--filter-query "(env == 'Prod' or env == 'Test') and role == 'Web'"

# Regex matching
--filter-query "hostname matches '^web.*'"

# Date comparison
--filter-query "last_heartbeat <= '2024-01-01'"
```

#### File-Based Filters

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--filter-file` | `-i` | string | CSV/Excel file with filter criteria |
| `--filter-file-delimiter` | - | string | CSV delimiter (default: `,`) |
| `--filter-fields` | - | choices | Fields to match: `hostname`, `app`, `ip` (can be repeated) |
| `--keep-filters-in-report` | - | flag | Include filter columns in output |

### Other Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--verbose` | `-v` | flag | Verbose output showing processing details |
| `--save-location` | - | string | Directory to save reports (default: `./`) |

## Report Columns

### Standard Columns

| Column | Description |
|--------|-------------|
| `name` | Workload name (forced name or hostname) |
| `hostname` | Workload hostname |
| `label_<type>` | One column per label dimension (env, app, role, loc, etc.) |
| `online` | Whether workload is online |
| `managed` | Whether workload has VEN agent (managed vs unmanaged) |
| `status` | Workload status (e.g., "active", "idle", "offline") |
| `agent.last_heartbeat` | Last VEN agent heartbeat timestamp |
| `agent.sec_policy_sync_state` | Security policy sync status |
| `agent.sec_policy_applied_at` | When policy was last applied |
| `link_to_pce` | Clickable URL to workload in PCE UI |
| `href` | PCE API reference |
| `agent.href` | VEN agent API reference |

### Extensibility

The command supports custom columns via plugin architecture (see code for `ExtraColumn` class).

## Examples

### Basic Export

Export all workloads to CSV:

```bash
pylo workload-export
```

### Filter by Query

Export production workloads:

```bash
pylo workload-export --filter-query "env == 'Production'"
```

Export online web servers:

```bash
pylo workload-export \
  --filter-query "role == 'Web' and online == true"
```

Complex query with multiple conditions:

```bash
pylo workload-export \
  --filter-query "(env == 'Production' or env == 'Staging') and (role == 'Web' or role == 'API') and online == true"
```

### Filter by File

Create filter file:
```csv
hostname
web-server-01
web-server-02
db-server-01
```

Export matching workloads:

```bash
pylo workload-export \
  --filter-file servers.csv \
  --filter-fields hostname
```

Filter by IP address:

```csv
ip
10.1.1.10
10.1.1.11
192.168.1.50
```

```bash
pylo workload-export \
  --filter-file ips.csv \
  --filter-fields ip \
  --keep-filters-in-report
```

### Multiple Output Formats

```bash
pylo workload-export \
  --report-format xlsx \
  --report-format json \
  --output-filename production-workloads
```

### Verbose Mode

```bash
pylo workload-export --verbose --filter-query "env == 'Test'"
```

## Filter Query Language

### Supported Fields

- **Basic**: `name`, `hostname`, `description`
- **Status**: `online`, `managed`, `deleted`
- **Network**: `ip_address`
- **Agent**: `last_heartbeat`, `mode`
- **Labels**: `env`, `app`, `role`, `loc` (and any custom label types)
- **System**: `os_id`

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equals | `env == 'Production'` |
| `!=` | Not equals | `managed != false` |
| `<` | Less than | `last_heartbeat < '2024-01-01'` |
| `>` | Greater than | `last_heartbeat > '2024-01-01'` |
| `<=` | Less than or equal | |
| `>=` | Greater than or equal | |
| `contains` | String contains | `name contains 'web'` |
| `matches` | Regex match | `hostname matches '^prod-.*'` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Both conditions must be true | `env == 'Prod' and online == true` |
| `or` | Either condition can be true | `role == 'Web' or role == 'API'` |
| `not` | Negates condition | `not deleted` |
| `( )` | Groups conditions | `(env == 'Prod' or env == 'Test') and online` |

### Value Types

- **Strings**: Single or double quotes: `'Production'` or `"Production"`
- **Booleans**: `true`, `false`
- **Numbers**: `123`, `45.6`
- **Dates**: ISO 8601 format: `'2024-01-15'` or `'2024-01-15T10:30:00'`
- **Null**: `null`

## Common Workflows

### Compliance Reporting

Export all production workloads with agent status:

```bash
pylo workload-export \
  --filter-query "env == 'Production'" \
  --report-format xlsx \
  --output-filename compliance-report-production
```

### Offline Workloads

Find workloads that are offline:

```bash
pylo workload-export \
  --filter-query "online == false" \
  --report-format xlsx
```

### Outdated Agents

Find workloads with old agent heartbeats:

```bash
pylo workload-export \
  --filter-query "last_heartbeat <= '2024-01-01'" \
  --report-format xlsx \
  --output-filename outdated-agents
```

### Specific Application Export

Export all database servers:

```bash
pylo workload-export \
  --filter-query "app == 'Database'" \
  --report-format xlsx
```

### Unmanaged Workloads

Export unmanaged workloads only:

```bash
pylo workload-export \
  --filter-query "managed == false" \
  --report-format xlsx
```

### Multi-Environment Export

Export test and dev environments:

```bash
pylo workload-export \
  --filter-query "env == 'Test' or env == 'Development'" \
  --report-format xlsx
```

## File-Based Filtering

### Hostname Matching

The filter matches the **short hostname** (FQDN stripped):
- Filter: `web-01`
- Matches: `web-01`, `web-01.company.com`, `WEB-01.COMPANY.COM`
- Case-insensitive comparison

### IP Matching

Exact IP address match:
- Must match exactly one of the workload's interfaces
- Workloads can have multiple interfaces; any match counts

### Application Label Matching

Case-insensitive label name match:
- Filter: `WebApp`
- Matches: workloads with app label `WebApp`, `webapp`, `WEBAPP`

## Performance Considerations

- **Large Environments**: Export of thousands of workloads may take several minutes
- **Filter Queries**: Processed in-memory after fetching all workloads
- **File Filters**: More efficient than manual filtering in spreadsheets
- **JSON Format**: Larger file size than CSV but structured for programmatic use

## Troubleshooting

### No Workloads Matched

**Problem**: Report is empty or has very few entries

**Causes:**
- Filter query too restrictive
- Typo in filter query
- No workloads match criteria

**Solutions:**
- Simplify filter query
- Check field names and values
- Run without filters to see all workloads
- Use `--verbose` to see processing details

### Filter Query Syntax Error

**Problem**: "Filter query error" message

**Cause:** Invalid query syntax

**Solutions:**
- Check quotes around strings
- Verify operator spelling (`and`, not `AND`)
- Ensure parentheses are balanced
- Test with simpler query first

### File Filter No Matches

**Problem**: File-based filter finds no matches

**Solutions:**
- Verify `--filter-fields` matches CSV columns
- Check for typos in CSV data
- Ensure `--filter-file-delimiter` matches CSV format
- Use `--verbose` to see matching attempts

## Related Commands

- `workload-import` - Import unmanaged workloads
- `workload-update` - Update workload labels
- `workload-resync-names` - Reset workload names
- `workload-used-in-rules-finder` - Find workloads used in rules
