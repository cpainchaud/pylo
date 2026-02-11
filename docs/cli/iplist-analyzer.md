# IP List Analyzer Command

## Overview

The `iplist-analyzer` command analyzes IP lists in the PCE to determine which workloads are covered by each IP list. It generates a comprehensive report showing IP coverage, workload matches, and application groupings for each IP list defined in your organization.

## Command Syntax

```bash
pylo iplist-analyzer [OPTIONS]
```

## Use Cases

- **IP List Validation**: Verify which workloads are covered by specific IP lists
- **Coverage Analysis**: Identify gaps in IP list coverage
- **Cleanup Planning**: Find unused or redundant IP lists
- **Documentation**: Generate comprehensive reports of IP list usage
- **Troubleshooting**: Understand why specific rules are or aren't matching traffic

## Options

### Output Format Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format: `csv`, `xlsx`, or `json` (can be repeated for multiple formats) |
| `--output-dir` | `-o` | string | `output` | Directory where to save the output file(s) |
| `--output-filename` | - | string | Auto-generated | Custom filename for the report |

**Note**: When `--output-filename` is not specified, a timestamped filename is automatically generated in the format: `iplist-analyzer_YYYYMMDD-HHMMSS.<format>`

## Report Columns

The generated report includes the following columns:

| Column | Description |
|--------|-------------|
| `name` | Name of the IP list |
| `members` | Raw IP entries from the IP list (IP addresses, CIDR ranges, or IP ranges) |
| `ip4_mapping` | Processed IPv4 address ranges (consolidated and normalized) |
| `ip4_count` | Total number of IPv4 addresses covered by this IP list |
| `ip4_uncovered_count` | Number of IPv4 addresses not matched by any workload |
| `covered_workloads_count` | Number of workloads whose IPs match this IP list |
| `covered_workloads_list` | Names of all workloads covered by this IP list |
| `covered_workloads_appgroups` | Unique application groups (label combinations) of covered workloads |
| `href` | PCE API reference (HREF) for the IP list |

## How It Works

1. **Build IP Caches**: Creates IP4 mappings for all managed workloads and IP lists
2. **Analyze Coverage**: For each IP list, determines which workload IPs fall within its ranges
3. **Track Application Groups**: Identifies unique label combinations for matched workloads
4. **Calculate Coverage**: Determines how many IPs in each list are actually used by workloads

### IP Matching Algorithm

- Converts all IP ranges, CIDR blocks, and individual IPs to normalized IP4 maps
- Uses efficient IP range subtraction to determine coverage
- Tracks both covered and uncovered IP addresses
- Groups workloads by their application labels for easy analysis

## Examples

### Basic Analysis

Generate a CSV report of all IP lists:

```bash
pylo iplist-analyzer
```

### Multiple Output Formats

Generate both Excel and JSON reports:

```bash
pylo iplist-analyzer --report-format xlsx --report-format json
```

### Custom Output Location

Save report to a specific directory with custom filename:

```bash
pylo iplist-analyzer \
  --output-dir /path/to/reports \
  --output-filename ip-coverage-analysis \
  --report-format xlsx
```

## Interpreting Results

### High Coverage IP Lists

IP lists with `ip4_uncovered_count` close to zero are well-utilized:
- Most IPs in the list match actual workloads
- Good candidates for retention and rule creation

### Low Coverage IP Lists

IP lists with high `ip4_uncovered_count` relative to `ip4_count`:
- May contain outdated or unused IP ranges
- Could be candidates for cleanup or refinement
- Might indicate missing workload discovery

### Empty Coverage

IP lists with `covered_workloads_count` of zero:
- No managed workloads match the IP list
- May be used for unmanaged workloads or external resources
- Could be unused and safe to delete (verify in rules first)

## Output Examples

### CSV Output

```csv
name,members,ip4_mapping,ip4_count,ip4_uncovered_count,covered_workloads_count,covered_workloads_list,covered_workloads_appgroups,href
Private_Networks,"10.0.0.0/8
172.16.0.0/12
192.168.0.0/16","10.0.0.0-10.255.255.255
172.16.0.0-172.31.255.255
192.168.0.0-192.168.255.255",17891328,15234567,42,"web-01
web-02
db-01","R-WEB|A-FRONTEND|E-PROD
R-DB|A-BACKEND|E-PROD",/orgs/1/sec_policy/draft/ip_lists/123
```

### Console Output

During execution, the command displays progress:

```
 * Building Workloads IP4 mapping... OK
 * Building IPLists IP4 mapping... OK
 * Now analyzing IPLists:
  - Private_Networks//orgs/1/sec_policy/draft/ip_lists/123
matched workload   web-01
matched workload   web-02
matched workload   db-01
  - Public_NAT_IPs//orgs/1/sec_policy/draft/ip_lists/124
matched workload   edge-lb-01
 ** DONE **
```

## Common Workflows

### Find Unused IP Lists

1. Run the analyzer:
   ```bash
   pylo iplist-analyzer --report-format xlsx
   ```

2. Open the Excel report and filter for:
   - `covered_workloads_count = 0`
   - High `ip4_uncovered_count` values

3. Cross-reference with rule usage before deletion

### Validate IP List Accuracy

1. Generate a report for current state
2. Review `covered_workloads_list` column
3. Verify the workloads match expected members
4. Update IP lists if unexpected workloads appear

### Plan IP List Consolidation

1. Analyze all IP lists
2. Look for overlapping `covered_workloads_appgroups`
3. Identify IP lists covering the same application groups
4. Consider merging redundant IP lists

## Performance Considerations

- **Large Environments**: Analysis time increases with workload and IP list count
- **Memory Usage**: IP4 mapping caches are held in memory during analysis
- **PCE Load**: Uses API calls to fetch workload and IP list data

**Recommended Limits**:
- Works efficiently with thousands of workloads
- Can handle hundreds of IP lists
- Very large IP ranges (e.g., entire /8 networks) may increase processing time

## Integration with Other Commands

### Workflow: Cleanup Unused IP Lists

```bash
# 1. Analyze IP lists
pylo iplist-analyzer --report-format xlsx --output-filename current-state

# 2. Review report and identify unused IP lists

# 3. Use label-delete-unused to remove them (if they're not in rules)
pylo label-delete-unused --proceed --yes
```

### Workflow: Validate Rule Coverage

```bash
# 1. Export rules to see IP list usage
pylo rule-export --report-format xlsx

# 2. Analyze IP list coverage
pylo iplist-analyzer --report-format xlsx

# 3. Compare to ensure IP lists used in rules actually cover workloads
```

## Troubleshooting

### No Workloads Matched

**Possible Causes:**
- IP lists contain only external IP addresses
- Workloads are unmanaged or don't have IP addresses assigned
- IP ranges in lists don't overlap with workload IPs

**Solutions:**
- Check workload discovery status
- Verify IP list contents match your network ranges
- Review interface configuration on workloads

### High Uncovered Count

**Possible Causes:**
- Broad IP ranges that include unused addresses
- Incomplete workload discovery
- IP lists designed for future growth

**Solutions:**
- Review if broad ranges are intentional
- Ensure workload discovery is complete
- Consider refining IP lists to actual usage

### Empty Report

**Possible Causes:**
- No IP lists defined in the PCE
- Connection or permission issues

**Solutions:**
- Verify IP lists exist: check PCE UI
- Check API permissions for IP list access
- Review credential configuration

## Related Commands

- `iplist-import` - Import IP lists from CSV/Excel
- `rule-export` - Export rules that reference IP lists
- `workload-export` - Export workload IP addresses for analysis
