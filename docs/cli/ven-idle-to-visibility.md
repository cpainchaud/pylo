# VEN Idle to Visibility Command

## Overview

The `ven-idle-to-visibility` command moves VEN agents from IDLE mode to visibility mode (BUILD or TEST). It checks compatibility reports before making changes and provides a safe, controlled way to enable illumination on workloads.

## Command Syntax

```bash
pylo ven-idle-to-visibility --mode {build|test} [OPTIONS]
```

## Required Arguments

| Argument | Short | Type | Choices | Description |
|----------|-------|------|---------|-------------|
| `--mode` | `-m` | string | `build`, `test` | Target mode: BUILD (view proposed policy) or TEST (see enforcement impact) |

## Options

### Filter Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--filter-env-label` | `-env` | string | Filter by environment label (can be repeated) |
| `--filter-app-label` | `-app` | string | Filter by application label (can be repeated) |
| `--filter-role-label` | `-role` | string | Filter by role label (can be repeated) |
| `--filter-loc-label` | `-loc` | string | Filter by location label (can be repeated) |
| `--filter-on-href-from-file` | - | string | CSV file containing workload HREFs to process |

### Compatibility Options

| Option | Type | Description |
|--------|------|-------------|
| `--ignore-incompatibilities` | list | Specific incompatibilities to ignore (space-separated) |
| `--ignore-all-incompatibilities` | flag | Skip compatibility checks entirely (use with caution!) |

### Execution Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--confirm` | `-c` | flag | Actually perform the mode change (without this, dry-run only) |

## Modes Explained

### BUILD Mode

- **Purpose**: See what policy would be applied
- **Effect**: Agents calculate policy but don't enforce
- **Use**: Initial visibility, policy validation
- **Safety**: Completely non-disruptive

### TEST Mode

- **Purpose**: See what would be blocked without actually blocking
- **Effect**: Agents log what would be blocked
- **Use**: Validate policy before enforcement
- **Safety**: Non-disruptive but shows real enforcement impact

## How It Works

### Workflow

1. **Find IDLE Agents**: Identifies all VEN agents in IDLE mode
2. **Apply Filters**: Narrows down based on labels or HREF file
3. **Check Compatibility**: Downloads compatibility reports for each agent
4. **Validate**: Ensures agents can safely switch modes
5. **Execute** (if `--confirm`): Requests PCE to change agent modes
6. **Generate Report**: Creates CSV/Excel report of results

### Compatibility Checking

Unless `--ignore-all-incompatibilities` is used, the command:
- Downloads compatibility report for each agent
- Verifies report status is "green"
- Only proceeds with compatible agents
- Skips agents with compatibility issues

## Examples

### Dry-Run (Default Behavior)

Check which agents would be moved without making changes:

```bash
pylo ven-idle-to-visibility --mode build --filter-env-label E-Test
```

### Move Agents to BUILD Mode

```bash
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Test \
  --confirm
```

### Move Agents to TEST Mode

```bash
pylo ven-idle-to-visibility \
  --mode test \
  --filter-env-label E-Production \
  --filter-app-label A-WebApp \
  --confirm
```

### Use HREF File for Specific Workloads

```bash
# Create CSV file with workload HREFs
cat > workloads.csv << EOF
href
/orgs/1/workloads/abc123
/orgs/1/workloads/def456
EOF

# Process only these workloads
pylo ven-idle-to-visibility \
  --mode build \
  --filter-on-href-from-file workloads.csv \
  --confirm
```

### Ignore Specific Compatibility Issues

If you know certain issues are safe to ignore:

```bash
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Test \
  --ignore-incompatibilities "kernel_version" "iptables_version" \
  --confirm
```

### Force Mode Change (Skip All Compatibility Checks)

**WARNING**: Use only if you understand the risks!

```bash
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Test \
  --ignore-all-incompatibilities \
  --confirm
```

## Multiple Label Filters

When using multiple filters, workloads must match **ALL** specified labels:

```bash
# Only workloads with BOTH E-Production AND A-Database labels
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Production \
  --filter-app-label A-Database \
  --confirm
```

## Output Reports

The command generates timestamped reports:
- `ven-mode-update-results_YYYYMMDD-HHMMSS.csv`
- `ven-mode-update-results_YYYYMMDD-HHMMSS.xlsx`

### Report Columns

| Column | Description |
|--------|-------------|
| `name` | Workload name |
| `hostname` | Workload hostname |
| `role` | Role label |
| `app` | Application label |
| `env` | Environment label |
| `loc` | Location label |
| `changed_mode` | Whether mode was changed (`yes`, `no`) |
| `details` | Reason for skipping or failure details |
| `href` | Workload HREF |

## Common Workflows

### Safe Phased Approach

**Phase 1: Test Environment**
```bash
# Dry-run first
pylo ven-idle-to-visibility --mode build --filter-env-label E-Test

# Review output, then confirm
pylo ven-idle-to-visibility --mode build --filter-env-label E-Test --confirm
```

**Phase 2: Development Environment**
```bash
pylo ven-idle-to-visibility --mode build --filter-env-label E-Dev --confirm
```

**Phase 3: Production - BUILD Mode**
```bash
pylo ven-idle-to-visibility --mode build --filter-env-label E-Production --confirm
```

**Phase 4: Production - TEST Mode**
```bash
pylo ven-idle-to-visibility --mode test --filter-env-label E-Production --confirm
```

### Application-by-Application

```bash
# WebApp first
pylo ven-idle-to-visibility \
  --mode build \
  --filter-app-label A-WebApp \
  --confirm

# Then API services
pylo ven-idle-to-visibility \
  --mode build \
  --filter-app-label A-API \
  --confirm

# Finally databases
pylo ven-idle-to-visibility \
  --mode build \
  --filter-app-label A-Database \
  --confirm
```

### Complete Pre-Illumination Workflow

```bash
# Step 1: Generate compatibility reports
pylo ven-compatibility-report-export \
  --filter-label E-Production \
  --report-format xlsx

# Step 2: Review compatibility report
# Open Excel file, review failures, fix issues

# Step 3: Dry-run mode change
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Production

# Step 4: Execute mode change
pylo ven-idle-to-visibility \
  --mode build \
  --filter-env-label E-Production \
  --confirm

# Step 5: Monitor in PCE UI
# Verify agents successfully changed to BUILD mode
```

## Statistics Output

After execution, the command displays statistics:

```
*** Statistics ***
 - IDLE Agents count (after filters):            150
 - Agents mode changed count:                    142
 - SKIPPED because not online count:              5
 - SKIPPED because report was not found:          2
 - Agents with failed reports:                    1
```

## Understanding Results

### Success Indicators

- High "mode changed" count
- Low "failed reports" count
- Most agents online

### Warning Signs

- Many agents skipped (not online)
- High failed reports count
- Compatibility issues

## Troubleshooting

### No Agents Changed

**Problem**: Mode changed count is 0

**Causes:**
- `--confirm` flag not used (dry-run mode)
- All agents failed compatibility checks
- No IDLE agents match filters

**Solutions:**
- Add `--confirm` flag
- Review compatibility reports
- Check filter criteria
- Verify agents are in IDLE mode

### Agents Skipped (Not Online)

**Problem**: Many agents skipped because not online

**Solutions:**
- Check VEN service status on workloads
- Investigate network connectivity
- Review agent heartbeat in PCE
- Re-run after bringing agents online

### Compatibility Report Not Found

**Problem**: "Report does not exist" for many agents

**Solutions:**
- Wait for PCE to generate reports (can take a few minutes)
- Ensure agents are online and connected
- Verify PCE version supports compatibility reports (18.2.0+)

### Mode Change Failed

**Problem**: API error when changing mode

**Solutions:**
- Check API user permissions
- Verify workload is not deleted or in transition
- Review PCE logs for errors
- Retry the operation

## Best Practices

1. **Always Dry-Run First**: Never use `--confirm` without testing first

2. **Check Compatibility**: Run `ven-compatibility-report-export` before this command

3. **Start Small**: Begin with test environments and small groups

4. **Phased Approach**: Don't illuminate entire production at once

5. **Monitor After**: Watch PCE UI and agent behavior after mode changes

6. **Keep Records**: Save output reports for documentation

7. **Coordinate**: Inform application teams before changing agent modes

8. **Have Rollback Plan**: Know how to revert agents to IDLE if needed

## Safety Considerations

### BUILD Mode

- **Completely Safe**: No traffic disruption
- **Recommended First Step**: Always start with BUILD

### TEST Mode

- **Still Safe**: No blocking, only logging
- **More Information**: Shows actual enforcement impact
- **Recommended Before Enforcement**: Validate policy effectiveness

### Ignoring Compatibility

- **High Risk**: May cause agent failures or system issues
- **Only Use When**: You've verified issues are safe to ignore
- **Document Decisions**: Keep records of why checks were bypassed

## Related Commands

- `ven-compatibility-report-export` - Check compatibility before running this command
- `ven-upgrade` - Upgrade VEN versions
- `workload-export` - Export workload and agent information
- `ven-duplicate-remover` - Clean up duplicate VEN agents

## Next Steps After Illumination

1. **Monitor in PCE**: Check agent status in PCE UI
2. **Review Policy**: Analyze traffic patterns in BUILD/TEST mode
3. **Refine Rules**: Adjust rules based on visibility data
4. **Move to Enforcement**: When ready, move agents to ENFORCED mode (via PCE UI or API)
