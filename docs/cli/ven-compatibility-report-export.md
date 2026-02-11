# VEN Compatibility Report Export Command

## Overview

The `ven-compatibility-report-export` command generates compatibility reports for VEN (Virtual Enforcement Node) agents in IDLE mode. It downloads and analyzes compatibility reports from the PCE to determine if agents can be safely upgraded or moved to enforcement modes (BUILD or TEST).

## Command Syntax

```bash
pylo ven-compatibility-report-export [OPTIONS]
```

## Purpose

Before moving VEN agents from IDLE mode to BUILD or TEST mode (illumination), it's crucial to verify compatibility. This command:

- Downloads compatibility reports for each IDLE agent
- Identifies agents that are ready for illumination
- Highlights compatibility issues that need attention
- Generates comprehensive reports for planning and tracking

## Options

### Filter Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--filter-label` | `-fl` | string | Filter workloads by specific label (can be repeated) |
| `--limit` | `-l` | integer | Limit the number of agents to process |

### Output Format Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format: `csv`, `xlsx`, or `json` (can be repeated) |
| `--output-dir` | `-o` | string | `output` | Directory where to save output files |
| `--output-filename` | - | string | Auto-generated | Custom filename for the report |

## Report Columns

The generated report includes:

| Column | Description |
|--------|-------------|
| `name` | Workload name |
| `hostname` | Workload hostname |
| `label_<type>` | One column per label type (env, app, role, loc, etc.) |
| `operating_system` | Operating system identifier |
| `report_failed` | Whether the compatibility report shows failures (`yes`, `no`, `not-available`) |
| `details` | Details of failed compatibility checks (if any) |
| `link_to_pce` | Clickable link to workload in PCE UI |
| `href` | PCE API reference for the workload |

## Compatibility Report Status

### Report Statuses

| Status | Meaning | Action |
|--------|---------|--------|
| `green` | All compatibility checks passed | Safe to move to BUILD/TEST mode |
| `red` / `yellow` | Some compatibility checks failed | Review details before proceeding |
| `not-available` | No compatibility report exists | Agent may not be online or report not yet generated |

### Common Compatibility Issues

The `details` column may contain:

- **Kernel Version**: Kernel too old or incompatible
- **iptables/nftables**: Firewall tooling incompatibility
- **SELinux/AppArmor**: Security module conflicts
- **Network Configuration**: Interface or routing issues
- **Dependencies**: Missing required system libraries
- **Hardware**: Insufficient resources or unsupported architecture

## Examples

### Basic Report Generation

Export compatibility reports for all IDLE agents:

```bash
pylo ven-compatibility-report-export
```

### Filter by Label

Export reports only for production environment workloads:

```bash
pylo ven-compatibility-report-export --filter-label E-Production
```

Multiple label filters (workload must match ALL labels):

```bash
pylo ven-compatibility-report-export \
  --filter-label E-Production \
  --filter-label A-WebApp
```

### Limit Processing

Test with a small subset first:

```bash
pylo ven-compatibility-report-export --limit 10
```

### Multiple Output Formats

Generate both Excel and JSON reports:

```bash
pylo ven-compatibility-report-export \
  --report-format xlsx \
  --report-format json
```

### Custom Output Location

```bash
pylo ven-compatibility-report-export \
  --output-dir /reports/ven-compatibility \
  --output-filename ven-readiness-check \
  --report-format xlsx
```

### Complete Example

```bash
pylo ven-compatibility-report-export \
  --filter-label E-Production \
  --filter-label A-CriticalApp \
  --limit 50 \
  --report-format xlsx \
  --report-format csv \
  --output-dir ./ven-reports
```

## Workflow

### Pre-Illumination Workflow

1. **Generate Compatibility Reports**:
   ```bash
   pylo ven-compatibility-report-export \
     --filter-label E-Production \
     --report-format xlsx
   ```

2. **Review Report**:
   - Open Excel file
   - Sort by `report_failed` column
   - Focus on `yes` entries first

3. **Address Failures**:
   - Review `details` column for each failed agent
   - Fix compatibility issues (OS updates, dependency installation, etc.)

4. **Re-check**:
   - Re-run the command after fixes
   - Verify failures are resolved

5. **Proceed with Illumination**:
   - Use `ven-idle-to-visibility` command to move agents to BUILD/TEST mode
   - See `ven-idle-to-visibility.md` for details

## Understanding Output

### Sample Console Output

```
 * Found 150 IDLE Agents
 * Parsing Labels filter...
   - Adding label 'E-Production' to the filter
 * Applying filters to the list of Agents...OK! 75 Agents left after filtering

 ** Request Compatibility Report for each Agent in IDLE mode **
 - Agent #1/75: wkl NAME:'web-server-01' HREF:/orgs/1/workloads/abc123 Labels:R-Web|A-Frontend|E-Production|L-US-East
    - Downloading report (it may be delayed by API flood protection)...OK
    - Report status is 'green'
 - Agent #2/75: wkl NAME:'web-server-02' HREF:/orgs/1/workloads/def456 Labels:R-Web|A-Frontend|E-Production|L-US-East
    - Downloading report...OK
    - Report status is 'red'


*** Statistics ***
+----------------------------------------+-------+
| item                                    | Value |
+----------------------------------------+-------+
| IDLE Agents count                       |    75 |
| Agents with successful report count     |    68 |
| SKIPPED because not online count        |     3 |
| SKIPPED because report was not found    |     2 |
| Agents with failed reports              |     2 |
+----------------------------------------+-------+

OK!
```

### Interpreting Results

**High Success Rate (> 90%)**:
- Environment is likely ready for illumination
- Review and fix the few failures
- Can proceed confidently

**Medium Success Rate (70-90%)**:
- Some compatibility issues exist
- Review patterns in failures (same OS, same location, etc.)
- May need systematic fixes before bulk illumination

**Low Success Rate (< 70%)**:
- Significant compatibility issues
- May indicate environment-wide problems
- Consider phased approach or addressing root causes first

## Performance and API Considerations

### API Flood Protection

The PCE may rate-limit compatibility report requests. The command automatically handles this by:
- Displaying "may be delayed by API flood protection" messages
- Retrying failed requests
- Spacing out requests when necessary

### Processing Time

- **Per Agent**: 1-5 seconds (depending on API response and rate limiting)
- **100 Agents**: 2-8 minutes
- **1000 Agents**: 20-80 minutes

**Recommendations**:
- Use `--limit` for initial testing
- Run during off-peak hours for large batches
- Use `--filter-label` to process specific groups incrementally

## Common Workflows

### Phased Illumination Approach

Phase 1 - Test Environment:
```bash
pylo ven-compatibility-report-export \
  --filter-label E-Test \
  --report-format xlsx \
  --output-filename phase1-test-env
```

Phase 2 - Development Environment:
```bash
pylo ven-compatibility-report-export \
  --filter-label E-Dev \
  --report-format xlsx \
  --output-filename phase2-dev-env
```

Phase 3 - Production Environment:
```bash
pylo ven-compatibility-report-export \
  --filter-label E-Production \
  --report-format xlsx \
  --output-filename phase3-production
```

### Application-by-Application

```bash
# Check WebApp compatibility
pylo ven-compatibility-report-export \
  --filter-label A-WebApp \
  --output-filename webapp-compat

# Check Database compatibility
pylo ven-compatibility-report-export \
  --filter-label A-Database \
  --output-filename database-compat

# Check API compatibility
pylo ven-compatibility-report-export \
  --filter-label A-API \
  --output-filename api-compat
```

### Incremental Processing

```bash
# Process in batches to avoid long-running operations
for i in {1..10}; do
  pylo ven-compatibility-report-export \
    --limit 100 \
    --output-filename batch-${i} \
    --report-format csv
  sleep 60  # Wait between batches
done
```

## Troubleshooting

### No IDLE Agents Found

**Problem**: "Found 0 IDLE Agents"

**Possible Causes:**
- All agents are already in BUILD/TEST/ENFORCED mode
- Agents haven't been installed yet
- Label filters are too restrictive

**Solutions:**
- Check agent status in PCE UI
- Remove or adjust `--filter-label` options
- Verify workloads have agents installed

### Report Not Available

**Problem**: Many agents showing `report_failed: not-available`

**Possible Causes:**
- Agents are not online
- Compatibility reports haven't been generated yet
- PCE version doesn't support compatibility reports

**Solutions:**
- Ensure agents are online: check heartbeat in PCE
- Wait for reports to generate (may take a few minutes after agent comes online)
- Verify PCE version supports compatibility reports (18.2.0+)

### Agent Skipped (Not Online)

**Problem**: Agents skipped because not online

**Solutions:**
- Check agent status on workload
- Investigate why agents are offline (network, service stopped, etc.)
- Re-run command after bringing agents online

### Slow Processing

**Problem**: Command is very slow

**Causes:**
- API rate limiting
- Large number of agents
- PCE server load

**Solutions:**
- Use `--limit` to process smaller batches
- Run during off-peak hours
- Filter by label to reduce scope
- Be patient - this is normal for large environments

## Best Practices

1. **Start Small**: Use `--limit` for initial tests

2. **Filter Strategically**: Use `--filter-label` to process groups incrementally

3. **Save Reports**: Keep compatibility reports for documentation and tracking

4. **Regular Checks**: Run compatibility checks regularly, not just before illumination

5. **Monitor Trends**: Track failure rates over time to identify recurring issues

6. **Document Failures**: Keep notes on compatibility issues and resolutions

7. **Coordinate with Teams**: Share reports with OS/infrastructure teams for issue resolution

## Related Commands

- `ven-idle-to-visibility` - Move agents from IDLE to BUILD/TEST mode (use after compatibility check)
- `ven-upgrade` - Upgrade VEN agents to newer versions
- `workload-export` - Export workload information including agent status
- `ven-duplicate-remover` - Remove duplicate VEN agents

## Integration with ven-idle-to-visibility

After generating and reviewing compatibility reports:

```bash
# Step 1: Generate compatibility report
pylo ven-compatibility-report-export \
  --filter-label E-Production \
  --report-format xlsx

# Step 2: Review report and fix issues

# Step 3: Move compatible agents to illumination
pylo ven-idle-to-visibility \
  --filter-label E-Production \
  --mode build \
  --confirm
```

For more details, see the `ven-idle-to-visibility` command documentation.
